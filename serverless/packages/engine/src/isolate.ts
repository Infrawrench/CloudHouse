import { exports } from "cloudflare:workers";
import { Catalog } from "./catalog";
import { pendingWaits, READ_DEADLINE_MS, SHARD_DEADLINE_MS, withDeadline } from "./deadline";
import { DEFAULT_LIMITS, Database, type EngineLoad, type Io, type Limits, type MergeMessage, type ShardRequest, type Timings } from "./database";
import { Engine } from "./engine";
import type { Env } from "./env";
import { answerBody, type Answer, type QueryRequest } from "./http";
import { bufferedInput, MAX_QUERY_BODY, type QueryInput, readAll, streamedInput } from "./input";
import { engineHost, preferWarm } from "./pool";

/// `BAD_ARGUMENTS`.
const BAD_ARGUMENTS = 36;
import type { ShardAnswer } from "./database";

/// The engine of this isolate. It is created by the first query rather than at the top level, because the global
/// scope of a Worker has to finish within the startup time limit. The tables of `ObjectMergeTree` are in the catalog
/// and in R2, and every isolate sees them; the tables of `Memory` last as long as the isolate does.
let database: Promise<Database> | undefined;
/// `database` once it has started, for `engineLoad`, which must answer without waiting.
let started: Database | undefined;
/// When `database` began to start, while it starts.
let starting_since: number | undefined;

function getDatabase(env: Env): Promise<Database> {
	if (database === undefined) {
		const since = Date.now();
		starting_since = since;
		const current: Promise<Database> = createDatabase(env).then(
			(db) => {
				if (database === current) {
					started = db;
					starting_since = undefined;
				}
				return db;
			},
			(error: unknown) => {
				if (database === current) {
					database = undefined;
					starting_since = undefined;
				}
				throw error;
			},
		);
		database = current;
	}
	return database;
}

/// Gives up the engine of this isolate, which is stuck (`EngineHost.resetIfStuck`): the next work starts a new one.
/// The work that waits on the old one goes on waiting, until its object is reset.
export function abandonEngine(): void {
	database = undefined;
	started = undefined;
	starting_since = undefined;
}

/// Runs `work` on the database of this isolate. An exception that escapes it may have left the engine half way through
/// a query, with its changes neither committed nor cleared: the next work starts with a new engine.
async function withDatabase<T>(env: Env, work: (db: Database) => Promise<T>): Promise<T> {
	const current = getDatabase(env);
	const db = await current;
	try {
		return await work(db);
	} catch (error) {
		if (database === current) {
			abandonEngine();
		}
		throw error;
	}
}

/// The static data of the engine is a static asset, because only the module counts against the size limit.
async function createDatabase(env: Env): Promise<Database> {
	const response = await env.ASSETS.fetch("https://assets.local/clickhouse-isolate.data");
	if (!response.ok) {
		throw new Error(`Cannot read the data of the engine: ${response.status} ${response.statusText}`);
	}
	const catalog = new Catalog(() => env.COORDINATION.get(env.COORDINATION.idFromName("catalog")));
	const [engine] = await Promise.all([response.arrayBuffer().then((data) => Engine.create(data)), catalog.connect()]);
	return new Database(engine, catalog, env.PARTS, limits(env));
}

/// The limits, from the variables of the Worker where they are set.
export function limits(env: Env): Limits {
	const number = (name: keyof Limits, value: string | undefined) => {
		if (value === undefined) {
			return DEFAULT_LIMITS[name];
		}
		const parsed = Number(value);
		if (!Number.isInteger(parsed) || parsed <= 0) {
			throw new Error(`${name} is not a positive number: ${value}`);
		}
		return parsed;
	};
	return {
		shardBytes: number("shardBytes", env.SHARD_BYTES),
		maxShards: number("maxShards", env.MAX_SHARDS),
		mergeBytes: number("mergeBytes", env.MERGE_BYTES),
		mergeParts: number("mergeParts", env.MERGE_PARTS),
		mergeThreshold: number("mergeThreshold", env.MERGE_THRESHOLD),
		hedgeMs: number("hedgeMs", env.HEDGE_MS),
	};
}

/// The ranges of parts that this isolate has read, the most recently used last: a part never changes, so the blocks
/// that queries read often are read once. At most `RANGE_CACHE_BYTES`, compressed, beside the memory of the engine.
const RANGE_CACHE_BYTES = 4 * 1024 * 1024;
const range_cache = new Map<string, Uint8Array>();
let range_cache_bytes = 0;

function cacheRange(key: string, data: Uint8Array): void {
	if (data.length > RANGE_CACHE_BYTES / 4) {
		return;
	}
	range_cache.set(key, data);
	range_cache_bytes += data.length;
	for (const [oldest, value] of range_cache) {
		if (range_cache_bytes <= RANGE_CACHE_BYTES) {
			break;
		}
		range_cache.delete(oldest);
		range_cache_bytes -= value.length;
	}
}

/// The I/O of this isolate for one request: parts come through `PartCache`, which Workers Caching keeps in front of
/// R2, shards are the `EngineHost` objects `shard-<index>`, and merges go to the queue `MERGES`.
export function makeIo(env: Env, background: (promise: Promise<unknown>) => void): Io {
	/// The loopback binding of `PartCache`, which goes through the cache of the Worker.
	const cache = (exports as unknown as { PartCache: Fetcher }).PartCache;
	const stats = { pages: 0, bytes: 0, hits: 0 };
	const read = async (part: string, offset: number, size: number) => {
		const response = await cache.fetch(`https://parts/${part}/${offset}/${size}`);
		if (response.status !== 200) {
			throw new Error(`Cannot read part ${part}: ${response.status} ${await response.text()}`);
		}
		return { data: new Uint8Array(await response.arrayBuffer()), hit: response.headers.get("CF-Cache-Status") === "HIT" };
	};
	/// The engine waits for a read with the lock of its database held, so a read that hangs fails instead (`deadline.ts`).
	const fetchRange = async (part: string, offset: number, size: number) => {
		const { data, hit } = await withDeadline(read(part, offset, size), READ_DEADLINE_MS, `The read of part ${part}`);
		stats.pages += 1;
		stats.bytes += data.length;
		stats.hits += hit ? 1 : 0;
		return data;
	};
	return {
		background,
		stats,
		async readRange(part, offset, size) {
			const key = `${part}:${offset}:${size}`;
			const cached = range_cache.get(key);
			if (cached !== undefined) {
				range_cache.delete(key);
				range_cache.set(key, cached);
				return cached;
			}
			const data = await fetchRange(part, offset, size);
			cacheRange(key, data);
			return data;
		},
		/// The fan-out asks for its shards in one go, before any answers, so they all see the same order.
		async runShard(index: number, request: ShardRequest): Promise<ShardAnswer> {
			const object = shardObjects(env)[index]!;
			let answer: ShardAnswer;
			try {
				/// The coordinator waits for its shards with the lock of its database held, see `deadline.ts`.
				const host = engineHost(env.ENGINE_HOST, `shard-${object}`, env.LOCATION_HINT);
				answer = await withDeadline(host.shard(request) as Promise<ShardAnswer>, SHARD_DEADLINE_MS, `Shard ${object}`);
			} catch (error) {
				shard_cold_at.set(object, Date.now());
				throw error;
			}
			if (answer.cold) {
				shard_cold_at.set(object, Date.now());
			}
			return answer;
		},
		async scheduleMerge(message: MergeMessage) {
			await env.MERGES.send(message);
		},
	};
}

/// Runs a query in the engine of this isolate. `timing` is for `Server-Timing`: only I/O moves the clock of a
/// Worker, so `startup` is the read of the static data by a new engine, and the rest are waits on the catalog, on
/// parts and shards, and on commits.
export async function runQuery(env: Env, io: Io, request: QueryRequest): Promise<Answer> {
	const cold = database === undefined;
	/// The shards of this engine are likely not running either: they start while it does, rather than when it fans out.
	if (cold) {
		io.background(warmShards(env));
	}
	const start = Date.now();
	const db = await getDatabase(env);
	const startup = Date.now() - start;
	const timings: Timings = { sync: 0, load: 0, commit: 0 };
	/// With a `query` parameter, the body is the data that follows it and streams; without, the body is the query.
	let text = "";
	let input: QueryInput | undefined;
	if (request.query !== null) {
		text = request.body === null ? request.query : `${request.query}\n`;
		input = request.body === null ? undefined : streamedInput(request.body, request.encoding);
	} else if (request.body !== null) {
		const body = await readAll(request.body, MAX_QUERY_BODY);
		if (body === undefined) {
			const message = `A query in the body is at most ${MAX_QUERY_BODY} bytes: send the query in the \`query\` parameter, and its data in the body.`;
			return { ok: false, code: BAD_ARGUMENTS, message, timing: "" };
		}
		input = bufferedInput(body, request.encoding);
	}
	const result = await withDatabase(env, (db) => db.query(text, request.format, io, request.readonly, timings, input));
	const timing = [
		...(cold ? [`startup;dur=${startup}`] : []),
		`sync;dur=${timings.sync}`,
		`load;dur=${timings.load}`,
		`commit;dur=${timings.commit}`,
		/// Not durations: the MiB of linear memory of the engine, which only grows, and of cached ranges; the ranges that
		/// the query read, and how many came from the cache; the KiB that they had; the shards that it fanned out to,
		/// the time of the slowest, and how many of them were slow enough that this engine ran their part too.
		`memory;dur=${Math.round(db.memoryBytes / 1024 / 1024)}`,
		`cache;dur=${Math.round(range_cache_bytes / 1024 / 1024)}`,
		`reads;dur=${io.stats.pages}`,
		`hits;dur=${io.stats.hits}`,
		`kib;dur=${Math.round(io.stats.bytes / 1024)}`,
		...(timings.shards ? [`shards;dur=${timings.shards}`, `slowest_shard;dur=${timings.slowest_shard}`, `hedged;dur=${timings.hedged ?? 0}`] : []),
	].join(", ");
	return result.ok ? { ok: true, body: answerBody(result.chunks), timing } : { ok: false, code: result.code, message: result.message, timing };
}

/// There are twice as many shard objects as a fan-out uses, and the coordinator uses those that have answered warm
/// lately, see `preferWarm`. `shard_cold_at` is when each last answered with a new engine, in this isolate.
const shard_cold_at = new Map<number, number>();

function shardObjects(env: Env): number[] {
	return preferWarm(2 * limits(env).maxShards, shard_cold_at);
}

/// Starts the engines of the shards that the next fan-out will use.
function warmShards(env: Env): Promise<unknown> {
	const objects = shardObjects(env).slice(0, limits(env).maxShards);
	return Promise.allSettled(objects.map((object) => engineHost(env.ENGINE_HOST, `shard-${object}`, env.LOCATION_HINT).warm()));
}

/// How busy the engine of this isolate is: idle if it has not started, since a query then waits only for it to start.
/// How busy the engine of this isolate is: an engine that starts is busy starting.
export function engineLoad(now: number = Date.now()): EngineLoad {
	const waits = pendingWaits(now);
	if (starting_since !== undefined) {
		return { running_ms: now - starting_since, queued: 0, step: "startup", waits };
	}
	return started === undefined ? { running_ms: 0, queued: 0 } : { ...started.busy(now), waits };
}

export async function runWarm(env: Env): Promise<void> {
	await withDatabase(env, (db) => db.warm());
}

export async function runShard(env: Env, io: Io, request: ShardRequest): Promise<ShardAnswer> {
	const cold = database === undefined;
	return { ...(await withDatabase(env, (db) => db.shard(request, io))), cold };
}

export async function runMerge(env: Env, io: Io, message: MergeMessage): Promise<number> {
	return withDatabase(env, (db) => db.merge(message, io));
}
