import { exports } from "cloudflare:workers";
import { Catalog } from "./catalog";
import { DEFAULT_LIMITS, Database, type Io, type Limits, type MergeMessage, type ShardRequest, type Timings } from "./database";
import { Engine } from "./engine";
import type { Env } from "./env";
import type { Answer, QueryRequest } from "./http";
import { bufferedInput, MAX_QUERY_BODY, type QueryInput, readAll, streamedInput } from "./input";
import { engineHost } from "./pool";

/// `BAD_ARGUMENTS`.
const BAD_ARGUMENTS = 36;
import type { ShardAnswer } from "./database";

/// The engine of this isolate. It is created by the first query rather than at the top level, because the global
/// scope of a Worker has to finish within the startup time limit. The tables of `ObjectMergeTree` are in the catalog
/// and in R2, and every isolate sees them; the tables of `Memory` last as long as the isolate does.
let database: Promise<Database> | undefined;

function getDatabase(env: Env): Promise<Database> {
	database ??= createDatabase(env).catch((error: unknown) => {
		database = undefined;
		throw error;
	});
	return database;
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
			database = undefined;
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
	const fetchRange = async (part: string, offset: number, size: number) => {
		const response = await cache.fetch(`https://parts/${part}/${offset}/${size}`);
		if (response.status !== 200) {
			throw new Error(`Cannot read part ${part}: ${response.status} ${await response.text()}`);
		}
		const data = new Uint8Array(await response.arrayBuffer());
		stats.pages += 1;
		stats.bytes += data.length;
		stats.hits += response.headers.get("CF-Cache-Status") === "HIT" ? 1 : 0;
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
		runShard(index: number, request: ShardRequest): Promise<ShardAnswer> {
			return engineHost(env.ENGINE_HOST, `shard-${index}`, env.LOCATION_HINT).shard(request);
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
	return result.ok ? { ok: true, body: result.body, timing } : { ok: false, code: result.code, message: result.message, timing };
}

/// Starts the engines of the shards.
function warmShards(env: Env): Promise<unknown> {
	const shards = limits(env).maxShards;
	return Promise.allSettled(Array.from({ length: shards }, (_, i) => engineHost(env.ENGINE_HOST, `shard-${i}`, env.LOCATION_HINT).warm()));
}

export async function runWarm(env: Env): Promise<void> {
	await withDatabase(env, (db) => db.warm());
}

export async function runShard(env: Env, io: Io, request: ShardRequest): Promise<ShardAnswer> {
	return withDatabase(env, (db) => db.shard(request, io));
}

export async function runMerge(env: Env, io: Io, message: MergeMessage): Promise<number> {
	return withDatabase(env, (db) => db.merge(message, io));
}
