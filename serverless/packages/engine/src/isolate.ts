import { exports } from "cloudflare:workers";
import { Catalog } from "./catalog";
import { DEFAULT_LIMITS, Database, type Io, type Limits, type MergeMessage, type ShardRequest, type Timings } from "./database";
import { Engine } from "./engine";
import type { Env } from "./env";
import type { Answer } from "./http";
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
	};
}

/// The ranges of parts that this isolate has read, the most recently used last: a part never changes, so the blocks
/// that queries read often are read once. At most `RANGE_CACHE_BYTES`, compressed, beside the memory of the engine.
const RANGE_CACHE_BYTES = 16 * 1024 * 1024;
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
	/// Workers Caching keeps the whole part and answers a range from it. Without the cache, as in `wrangler dev`,
	/// `PartCache` answers with the whole part, and the range is taken from it here.
	const fetchRange = async (part: string, offset: number, size: number) => {
		const response = await cache.fetch(`https://parts/${part}`, { headers: { Range: `bytes=${offset}-${offset + size - 1}` } });
		if (response.status === 206) {
			return new Uint8Array(await response.arrayBuffer());
		}
		if (response.status === 200) {
			return new Uint8Array(await response.arrayBuffer()).slice(offset, offset + size);
		}
		throw new Error(`Cannot read part ${part}: ${response.status} ${await response.text()}`);
	};
	return {
		background,
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
			return env.ENGINE_HOST.get(env.ENGINE_HOST.idFromName(`shard-${index}`)).shard(request);
		},
		async scheduleMerge(message: MergeMessage) {
			await env.MERGES.send(message);
		},
	};
}

/// Runs a query in the engine of this isolate. `timing` is for `Server-Timing`: only I/O moves the clock of a
/// Worker, so `startup` is the read of the static data by a new engine, and the rest are waits on the catalog, on
/// parts and shards, and on commits.
export async function runQuery(env: Env, io: Io, text: Uint8Array, format: string, readonly: boolean): Promise<Answer> {
	const cold = database === undefined;
	const start = Date.now();
	const db = await getDatabase(env);
	const startup = Date.now() - start;
	const timings: Timings = { sync: 0, load: 0, commit: 0 };
	const result = await db.query(text, format, io, readonly, timings);
	const timing = [
		...(cold ? [`startup;dur=${startup}`] : []),
		`sync;dur=${timings.sync}`,
		`load;dur=${timings.load}`,
		`commit;dur=${timings.commit}`,
		/// Not a duration: the MiB of linear memory of the engine, which only grows, and of cached ranges.
		`memory;dur=${Math.round(db.memoryBytes / 1024 / 1024)}`,
		`cache;dur=${Math.round(range_cache_bytes / 1024 / 1024)}`,
	].join(", ");
	return result.ok ? { ok: true, body: result.body, timing } : { ok: false, code: result.code, message: result.message, timing };
}

export async function runShard(env: Env, io: Io, request: ShardRequest): Promise<ShardAnswer> {
	return (await getDatabase(env)).shard(request, io);
}

export async function runMerge(env: Env, io: Io, message: MergeMessage): Promise<number> {
	return (await getDatabase(env)).merge(message, io);
}
