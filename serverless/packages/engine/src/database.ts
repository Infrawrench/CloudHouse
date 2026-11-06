import { DUPLICATE, type Catalog, type Snapshot, type Table, tableKey } from "./catalog";
import { type CatalogChange, type Engine, type FanOutRequest, NOT_FOUND_EXPECTED_DATA_PART, type QueryOutput, type QueryResult } from "./engine";
import { chooseMerge, groupParts, partKey, shardParts } from "./parts";
import type { QueryInput } from "./input";
import { ReadAhead } from "./readahead";

/// How many times a query runs again because the catalog has moved on before it could commit.
const MAX_ATTEMPTS = 5;
/// How many times a query runs again to load parts or the results of shards: once per table that it reads, at most.
const MAX_LOAD_ROUNDS = 16;
/// The code of the exception of ClickHouse for a write in a read-only query.
const READONLY = 164;
/// `ABORTED`: a query whose data streamed has to be sent again.
const ABORTED = 236;
/// `RECEIVED_ERROR_FROM_REMOTE_IO_SERVER`: the engine could not read or write a part, which happens when a part that
/// the query saw has been merged away since: the engine syncs and runs the query again.
const PART_IO_FAILED = 86;

/// The sizes that decide when a table fans out and which parts merge, in bytes of parts (compressed).
export interface Limits {
	/// A table larger than this fans out, to shards of about this size.
	shardBytes: number;
	maxShards: number;
	/// A merge makes a part of at most this size, of at most `mergeParts` parts, so that it fits in one isolate.
	mergeBytes: number;
	mergeParts: number;
	/// A commit that leaves a table with this many parts schedules a merge.
	mergeThreshold: number;
	/// A shard that has not answered after three times the time that half of the shards took, and at least this many
	/// milliseconds, is most likely an engine that is starting: the coordinator runs its part of the query too. Warm
	/// shards differ by up to a second among themselves, and the coordinator's part takes its CPU, so not sooner.
	hedgeMs: number;
}

/// A part decodes to a few times its size, and a merge holds its parts and the merged one: 4 MiB of parts keep a
/// shard or a merge well inside the memory of an engine.
export const DEFAULT_LIMITS: Limits = {
	shardBytes: 4 * 1024 * 1024,
	maxShards: 16,
	mergeBytes: 4 * 1024 * 1024,
	mergeParts: 10,
	mergeThreshold: 8,
	hedgeMs: 1500,
};

/// The query of one shard of a fan-out: the coordinator ran its query against catalog `version`, and the shard reads
/// `parts` of the table.
export interface ShardRequest {
	/// The definition of the table as the coordinator has it: the shard runs its query on the same one.
	createQuery: string;
	database: string;
	table: string;
	parts: string[];
	query: string;
	/// The other sides of the joins and `IN`s of the query, which the coordinator has read once.
	broadcast?: Uint8Array;
}

/// `cold` if the shard started its engine for the query.
export type ShardAnswer = ({ ok: true; data: Uint8Array } | { ok: false; stale: boolean; message: string }) & { cold?: boolean };

/// A merge to run in the background: of the smallest parts, or of all of them in groups to apply the TTL.
export interface MergeMessage {
	database: string;
	table: string;
	ttl: boolean;
}

/// What the work of a database needs from the request that it serves.
export interface Io {
	/// Runs `promise` after the response, as `ExecutionContext.waitUntil` does.
	background(promise: Promise<unknown>): void;
	/// Reads the bytes `[offset, offset + size)` of a part: from the cache or R2. The engine calls it while it runs a
	/// query.
	readRange(part: string, offset: number, size: number): Promise<Uint8Array>;
	/// The ranges of parts that the request has read, their bytes, and how many came from the cache.
	stats: { pages: number; bytes: number; hits: number };
	/// Runs shard `index` of a fan-out in another engine.
	runShard(index: number, request: ShardRequest): Promise<ShardAnswer>;
	/// Queues a merge.
	scheduleMerge(message: MergeMessage): Promise<void>;
}

/// The time a query spends waiting on the catalog, on parts and on shards, in milliseconds, for `Server-Timing`. A
/// Worker's clock only moves on I/O, so the time the engine computes is not in it.
export interface Timings {
	sync: number;
	load: number;
	commit: number;
	/// The shards of the fan-outs of the query, and the time of the slowest.
	shards?: number;
	slowest_shard?: number;
	/// The shards whose part of the query the coordinator ran itself, because they were slow to answer.
	hedged?: number;
}

/// The tables of `ObjectMergeTree` of one isolate: the engine, kept in sync with the catalog, with the parts in R2.
///
/// A query runs against the version of the catalog that the engine is in sync with. If it changes the catalog,
/// its new parts go to R2, and its changes are committed in one transaction against that version. If the catalog
/// has moved on in the meantime, the query runs again against the new version, so that the queries that change
/// the catalog are serializable.
///
/// A query that reads a table alone, and more than `Limits.shardBytes` of its blocks after pruning by its filter, fans
/// out: it is processed up to `WithMergeableState` by shards, other engines that read a part of its parts each, and
/// finished here. A point query reads its few blocks here.
export class Database {
	/// What the engine has: the version of the catalog that it is in sync with, or -1 if it has to sync again.
	private state: Snapshot = { version: -1, databases: new Set(), tables: new Map() };
	/// The work runs one piece at a time: the engine is shared by the requests of the isolate.
	private queue: Promise<unknown> = Promise.resolve();
	/// When the work that holds the lock took it, what it is, and how many wait for it, for `busy`.
	private held_since: number | undefined;
	private held_by: string | undefined;
	private waiting = 0;
	/// Why the last query ran again, if it was for an error of the engine, which a query that runs again too many
	/// times returns.
	private retry_reason: QueryOutput | undefined;

	constructor(
		private readonly engine: Engine,
		private readonly catalog: Catalog,
		private readonly parts: R2Bucket,
		private readonly limits: Limits = DEFAULT_LIMITS,
		private readonly log: (message: string) => void = console.error,
	) {}

	/// The size of the linear memory of the engine.
	get memoryBytes(): number {
		return this.engine.memoryBytes;
	}

	/// A `readonly` query can change nothing: the engine runs it with `readonly` = 2, and its changes are not committed.
	/// `input` is the body of the request, which follows `query`.
	query(
		query: string | Uint8Array,
		format: string,
		io: Io,
		readonly = false,
		timings: Timings = { sync: 0, load: 0, commit: 0 },
		input?: QueryInput,
	): Promise<QueryOutput> {
		return this.run(query, format, io, readonly, timings, input);
	}

	/// Runs the query of a shard on the parts that the coordinator gave it, with the definition of the table that the
	/// coordinator has. Parts never change, so the shard does not need the version of the catalog of the coordinator:
	/// writes to other parts or tables meanwhile do not matter.
	///
	/// Durable Objects can share an isolate, and so an engine: a shard can be in the isolate of its coordinator,
	/// which holds the lock while it waits for its shards. So a shard with the same definition runs without the lock:
	/// it changes nothing, and the engine shows it its parts for the duration of its query only. A shard with another
	/// definition syncs, under the lock, and it is never in the isolate of the coordinator then.
	async shard(request: ShardRequest, io: Io): Promise<ShardAnswer> {
		const key = tableKey(request.database, request.table);
		const same = () => this.state.tables.get(key)?.createQuery === request.createQuery;
		if (!same()) {
			await this.exclusive("shard sync", () => this.sync());
			if (!same()) {
				return { ok: false, stale: true, message: `The definition of ${request.database}.${request.table} has changed` };
			}
		}
		const result = await this.runShardQuery(request, io);
		if (result.ok) {
			return { ok: true, data: result.body };
		}
		/// A part that a merge has replaced since the coordinator listed it may be gone: the coordinator runs again. The
		/// catalog is read without the lock, which the coordinator may hold in this isolate.
		const parts = (await this.catalog.snapshot()).tables.get(key)?.parts ?? [];
		const gone = request.parts.some((part) => !parts.includes(part));
		return { ok: false, stale: gone, message: result.message };
	}

	private async runShardQuery(request: ShardRequest, io: Io): Promise<QueryResult> {
		const read_ahead = new ReadAhead((part, offset, size) => io.readRange(part, offset, size));
		const reader = {
			readRange: (part: string, offset: number, size: number) => read_ahead.readRange(part, offset, size),
			prefetch: (part: string, offset: number, size: number) => read_ahead.prefetch(part, offset, size),
			writePart: async (part: string) => {
				throw new Error(`A shard does not write parts, but it wrote ${part}`);
			},
		};
		try {
			return await this.engine.shardQuery(request.query, request.database, request.table, request.parts, reader, request.broadcast);
		} catch (error) {
			return { ok: false, code: 0, message: String(error) };
		}
	}

	/// Brings the engine up to date with the catalog, which attaches the tables, and runs a query, which compiles the
	/// code that queries run first.
	warm(): Promise<void> {
		return this.exclusive("warm", async () => {
			await this.sync();
			const nothing = async () => {
				throw new Error("Warming the engine reads no parts");
			};
			const result = await this.engine.query("SELECT 1", "", false, { readRange: nothing, prefetch() {}, writePart: nothing });
			if (!result.ok) {
				throw new Error(`Cannot warm the engine: ${result.message}`);
			}
		});
	}

	/// Runs a merge from the queue. A merge that loses to a concurrent change runs again against the new catalog.
	merge(message: MergeMessage, io: Io): Promise<number> {
		return this.exclusive("merge", async () => {
			let merged = 0;
			for (let attempt = 1; attempt <= MAX_ATTEMPTS; ++attempt) {
				await this.sync();
				const table = this.state.tables.get(tableKey(message.database, message.table));
				if (table === undefined) {
					return merged;
				}
				const groups = message.ttl
					? groupParts(table.parts, this.limits.mergeBytes)
					: [chooseMerge(table.parts, this.limits.mergeBytes, this.limits.mergeParts)].filter((group) => group !== undefined);
				let conflict = false;
				for (const group of groups) {
					const outcome = await this.mergeParts(table, group, io);
					if (outcome === "conflict") {
						conflict = true;
						break;
					}
					merged += outcome === "merged" ? 1 : 0;
				}
				if (!conflict) {
					return merged;
				}
			}
			this.log(`The merges of ${message.database}.${message.table} lost to concurrent changes ${MAX_ATTEMPTS} times`);
			return merged;
		});
	}

	/// A query runs in the engine under the lock. If it only adds or replaces parts, which the engine does not show
	/// until they are committed, the lock is released for the upload and the commit, and other queries run meanwhile.
	private async run(query: string | Uint8Array, format: string, io: Io, readonly: boolean, timings: Timings, input?: QueryInput): Promise<QueryOutput> {
		this.retry_reason = undefined;
		for (let attempt = 1; ; ++attempt) {
			const step = await this.exclusive("query", () => this.prepare(query, format, io, readonly, timings, input));
			if (step.kind === "done") {
				return step.result;
			}
			if (step.kind === "commit") {
				const start = Date.now();
				const committed = await this.commitParts(step.snapshot, step.changes, step.written, io);
				timings.commit += Date.now() - start;
				if (committed) {
					return step.result;
				}
			}
			if (attempt === MAX_ATTEMPTS) {
				return this.retry_reason ?? { ok: false, code: 0, message: `The catalog changed during the query ${MAX_ATTEMPTS} times in a row, try again.` };
			}
		}
	}

	/// Runs the query against the catalog that the engine is in sync with, and commits the changes to definitions.
	private async prepare(query: string | Uint8Array, format: string, io: Io, readonly: boolean, timings: Timings, input?: QueryInput): Promise<Step> {
		let start = Date.now();
		await this.sync();
		timings.sync += Date.now() - start;
		/// The parts that the query has put in R2, which go if it does not commit them.
		const written: string[] = [];
		const result = await this.execute(query, format, readonly, timings, io, written, input);
		if (result === "stale") {
			io.background(this.deleteParts(written));
			return { kind: "retry" };
		}
		if (!result.ok) {
			await this.discardChanges();
			io.background(this.deleteParts(written));
			return { kind: "done", result };
		}
		const changes = await this.engine.changes();
		if (changes.length === 0) {
			return { kind: "done", result };
		}
		if (readonly) {
			await this.discardChanges();
			io.background(this.deleteParts(written));
			return { kind: "done", result: { ok: false, code: READONLY, message: "A read-only query cannot change the catalog." } };
		}
		if (changes.some((change) => change.kind !== "replace_parts")) {
			start = Date.now();
			const committed = await this.commit(changes, io, written);
			timings.commit += Date.now() - start;
			return committed ? { kind: "done", result } : { kind: "retry" };
		}
		await this.engine.clearChanges();
		/// The definitions as the query saw them: an `ALTER` committed meanwhile makes the commit fail.
		const tables = new Map([...this.state.tables].map(([key, table]) => [key, { ...table, parts: [...table.parts], detached: [...table.detached] }]));
		const snapshot: Snapshot = { version: this.state.version, databases: new Set(this.state.databases), tables };
		return { kind: "commit", snapshot, changes, written, result };
	}

	/// Runs the query, which reads the parts that it needs as it goes, and has the shards run their queries for the
	/// tables that fan out. Returns `"stale"` if the engine is behind the catalog: a shard is ahead of it.
	private async execute(
		query: string | Uint8Array,
		format: string,
		readonly: boolean,
		timings: Timings,
		io: Io,
		written: string[],
		input?: QueryInput,
	): Promise<QueryOutput | "stale"> {
		/// The time the engine waits for parts, which is less than the time the reads take, as they overlap.
		const read_ahead = new ReadAhead((part, offset, size) => io.readRange(part, offset, size));
		const reader = {
			prefetch: (part: string, offset: number, size: number) => read_ahead.prefetch(part, offset, size),
			readRange: async (part: string, offset: number, size: number) => {
				const start = Date.now();
				const data = await read_ahead.readRange(part, offset, size);
				timings.load += Date.now() - start;
				return data;
			},
			/// Each part that the query writes goes to R2 at once, so that the engine holds one at a time.
			writePart: async (part: string, data: Uint8Array) => {
				const start = Date.now();
				written.push(part);
				await this.parts.put(partKey(part), data);
				timings.commit += Date.now() - start;
			},
		};
		try {
			for (let round = 1; ; ++round) {
				/// A body that streamed cannot be read again once the engine has read some of it.
				const body = input?.open();
				if (input !== undefined && body === undefined) {
					return { ok: false, code: ABORTED, message: "The catalog changed while the query read its data, which streamed: send the query again." };
				}
				const result = await this.engine.query(query, format, readonly, reader, body && { reader: body, encoding: input!.encoding });
				if (!result.ok && result.code === PART_IO_FAILED) {
					await this.discardChanges();
					this.state.version = -1;
					this.retry_reason = result;
					return "stale";
				}
				if (result.ok || result.code !== NOT_FOUND_EXPECTED_DATA_PART || round === MAX_LOAD_ROUNDS) {
					return result;
				}
				await this.discardChanges();
				/// The query runs again, and writes its parts again.
				io.background(this.deleteParts(written.splice(0)));
				const requests = await this.engine.fanOutRequests();
				if (requests.length === 0) {
					return result;
				}
				const start = Date.now();
				const fanned = await Promise.all(requests.map((request) => this.fanOut(request, io, timings)));
				timings.load += Date.now() - start;
				const failed = fanned.find((outcome) => outcome !== "done" && outcome !== "stale");
				if (failed !== undefined) {
					return { ok: false, code: 0, message: failed.message };
				}
				if (fanned.includes("stale")) {
					this.state.version = -1;
					return "stale";
				}
			}
		} finally {
			await this.engine.clearFanOut();
		}
	}

	/// Runs the query for the shards of a table on its parts, split among them, and gives the results to the engine.
	private async fanOut(request: FanOutRequest, io: Io, timings: Timings): Promise<"done" | "stale" | { message: string }> {
		const table = this.state.tables.get(tableKey(request.database, request.table));
		if (table === undefined) {
			throw new Error(`The engine fans out ${request.database}.${request.table}, which is not in the catalog`);
		}
		const groups = shardParts(table.parts, this.limits.shardBytes, this.limits.maxShards);
		const requests = groups.map((parts) => ({
			createQuery: table.createQuery,
			database: table.database,
			table: table.table,
			parts,
			query: request.query,
			...(request.broadcast.length > 0 ? { broadcast: request.broadcast } : {}),
		}));
		const answers = await this.runShards(requests, io, timings);
		timings.shards = (timings.shards ?? 0) + groups.length;
		for (const answer of answers) {
			if (!answer.ok) {
				return answer.stale ? "stale" : { message: `A shard failed: ${answer.message}` };
			}
		}
		for (const answer of answers) {
			if (answer.ok) {
				await this.engine.addFanOutResult(request, answer.data);
			}
		}
		return "done";
	}

	/// Runs the shards, and the part of a shard that is slow to answer here too: an engine that starts takes a second or
	/// more, where the others take a fraction of that, and a query that fans out to many shards would otherwise wait for
	/// one that starts more often than not. The parts of the slow shards run here one at a time, and each shard has the
	/// answer that comes first. If its part fails here, the shard's own answer counts, whatever it is.
	private async runShards(requests: ShardRequest[], io: Io, timings: Timings): Promise<ShardAnswer[]> {
		const start = Date.now();
		const answers: ShardAnswer[] = [];
		const pending = new Set(requests.keys());
		let changed = Promise.withResolvers<void>();
		/// The answers so far, which the isolate holds until they all are in.
		let bytes = 0;
		let too_large: ShardAnswer | undefined;
		const settle = (index: number, answer: ShardAnswer) => {
			if (!pending.delete(index)) {
				return;
			}
			bytes += answer.ok ? answer.data.length : 0;
			if (bytes > MAX_SHARD_RESULTS_BYTES && too_large === undefined) {
				too_large = {
					ok: false,
					stale: false,
					message: `The shards of the query sent back more than ${MAX_SHARD_RESULTS_BYTES / 1024 / 1024} MiB of results: aggregate the rows, filter them more, or add a LIMIT`,
				};
				pending.clear();
			}
			answers[index] = answer;
			timings.slowest_shard = Math.max(timings.slowest_shard ?? 0, Date.now() - start);
			changed.resolve();
			changed = Promise.withResolvers<void>();
		};
		for (const [index, request] of requests.entries()) {
			io.runShard(index, request).then(
				(answer) => settle(index, answer),
				(error: unknown) => settle(index, { ok: false, stale: false, message: String(error) }),
			);
		}
		const half = Math.ceil(requests.length / 2);
		while (pending.size > 0 && requests.length - pending.size < half) {
			await changed.promise;
		}
		if (too_large !== undefined) {
			return [too_large];
		}
		/// With one shard, `half` is all of them.
		if (pending.size > 0) {
			const deadline = start + Math.max(this.limits.hedgeMs, 3 * (Date.now() - start));
			while (pending.size > 0 && Date.now() < deadline) {
				let timer = 0;
				await Promise.race([changed.promise, new Promise<void>((resolve) => (timer = setTimeout(resolve, deadline - Date.now())))]);
				clearTimeout(timer);
			}
			for (const index of [...pending]) {
				if (!pending.has(index)) {
					continue;
				}
				const result = await this.runShardQuery(requests[index]!, io);
				if (result.ok && pending.has(index)) {
					timings.hedged = (timings.hedged ?? 0) + 1;
					settle(index, { ok: true, data: result.body });
				}
			}
			while (pending.size > 0) {
				await changed.promise;
			}
		}
		return too_large === undefined ? answers : [too_large];
	}

	/// Merges `group`, some of the parts of the table, into one, with `OPTIMIZE`, which merges the visible parts.
	private async mergeParts(table: Table, group: string[], io: Io): Promise<"merged" | "nothing" | "conflict"> {
		await this.engine.setParts(table.database, table.table, group);
		try {
			const timings: Timings = { sync: 0, load: 0, commit: 0 };
			const written: string[] = [];
			const result = await this.execute(`OPTIMIZE TABLE ${quote(table.database)}.${quote(table.table)} FINAL`, "", false, timings, io, written);
			if (result === "stale") {
				io.background(this.deleteParts(written));
				return "conflict";
			}
			if (!result.ok) {
				await this.discardChanges();
				io.background(this.deleteParts(written));
				throw new Error(`Cannot merge the parts of ${table.database}.${table.table}: ${result.message}`);
			}
			const changes = await this.engine.changes();
			if (changes.length === 0) {
				return "nothing";
			}
			return (await this.commit(changes, io, written)) ? "merged" : "conflict";
		} finally {
			/// After a commit, the table in `state` has its new parts.
			const current = this.state.tables.get(tableKey(table.database, table.table));
			await this.engine.setParts(table.database, table.table, current?.parts ?? [], current?.detached ?? []);
		}
	}

	/// Brings the engine up to the current version of the catalog.
	private async sync(): Promise<void> {
		/// An isolate that has never synced reads the whole catalog anyway.
		if (this.state.version !== -1 && (await this.catalog.version()) === this.state.version) {
			return;
		}
		const target = await this.catalog.snapshot();
		for (const database of target.databases) {
			if (!this.state.databases.has(database)) {
				await this.engine.hostQuery(`CREATE DATABASE IF NOT EXISTS ${quote(database)} ENGINE = Memory`);
			}
		}
		for (const [key, table] of this.state.tables) {
			const wanted = target.tables.get(key);
			if (wanted === undefined || wanted.createQuery !== table.createQuery) {
				await this.detach(table);
				this.state.tables.delete(key);
			}
		}
		/// A database that another isolate dropped, after its tables.
		for (const database of this.state.databases) {
			if (!target.databases.has(database)) {
				await this.engine.hostQuery(`DROP DATABASE IF EXISTS ${quote(database)}`);
			}
		}
		for (const [key, table] of target.tables) {
			const current = this.state.tables.get(key);
			if (current === undefined) {
				await this.attach(table);
			}
			if (current === undefined || !sameParts(current.parts, table.parts) || !sameParts(current.detached, table.detached)) {
				await this.engine.setParts(table.database, table.table, table.parts, table.detached);
			}
			await this.updateFanOut(table);
			this.state.tables.set(key, { ...table, parts: [...table.parts], detached: [...table.detached] });
		}
		this.state.databases = target.databases;
		this.state.version = target.version;
	}

	/// Commits the changes, under the lock, with `written`, the parts that the query has put in R2, which go if it
	/// does not commit. Returns false if the catalog has moved on.
	private async commit(changes: CatalogChange[], io: Io, written: string[]): Promise<boolean> {
		let version: number | undefined | typeof DUPLICATE;
		try {
			version = await this.catalog.commit(this.state, changes);
		} catch (error) {
			await this.discardChanges();
			this.state.version = -1;
			io.background(this.deleteParts(written));
			throw error;
		}
		if (version === DUPLICATE) {
			await this.discardChanges();
			io.background(this.deleteParts(written));
			return true;
		}
		if (version === undefined) {
			await this.discardChanges();
			this.state.version = -1;
			io.background(this.deleteParts(written));
			return false;
		}
		await this.apply(changes, version, io);
		await this.engine.clearChanges();
		return true;
	}

	/// Commits changes of parts only, without the lock. The engine has given them up, so a commit that fails leaves
	/// nothing to undo but `written`, the parts that the query has put in R2.
	private async commitParts(snapshot: Snapshot, changes: CatalogChange[], written: string[], io: Io): Promise<boolean> {
		let version: number | undefined | typeof DUPLICATE;
		try {
			version = await this.catalog.commit(snapshot, changes);
		} catch (error) {
			io.background(this.deleteParts(written));
			throw error;
		}
		/// Another attempt at this `INSERT` has committed: this one succeeded too, as far as the client can tell.
		if (version === DUPLICATE) {
			io.background(this.deleteParts(written));
			return true;
		}
		if (version === undefined) {
			io.background(this.deleteParts(written));
			return false;
		}
		await this.exclusive("apply", () => this.apply(changes, version, io));
		return true;
	}

	/// Applies committed changes to the engine. The engine is at the new version only if no other commit came between:
	/// otherwise the next query syncs.
	private async apply(changes: CatalogChange[], version: number, io: Io): Promise<void> {
		const current = this.state.version === version - 1;
		const unused: string[] = [];
		const changed = new Set<string>();
		for (const change of changes) {
			const key = tableKey(change.database, change.table);
			switch (change.kind) {
				case "create_database":
					this.state.databases.add(change.database);
					break;
				case "drop_database":
					this.state.databases.delete(change.database);
					break;
				case "create_table":
					this.state.databases.add(change.database);
					this.state.tables.set(key, {
						database: change.database,
						table: change.table,
						createQuery: change.create_query,
						ttl: change.ttl,
						definitionVersion: 0,
						parts: [],
						detached: [],
					});
					break;
				case "alter_table": {
					const table = this.state.tables.get(key);
					if (table === undefined) {
						throw new Error(`The query altered ${key}, which is not in the catalog`);
					}
					table.createQuery = change.create_query;
					table.ttl = change.ttl;
					table.definitionVersion += 1;
					break;
				}
				case "drop_table":
					unused.push(...(this.state.tables.get(key)?.parts ?? []), ...(this.state.tables.get(key)?.detached ?? []));
					this.state.tables.delete(key);
					await this.engine.setParts(change.database, change.table, []);
					await this.engine.setFanOut(change.database, change.table, 0);
					break;
				case "rename_table": {
					const table = this.state.tables.get(key);
					if (table === undefined) {
						throw new Error(`The query renamed ${key}, which is not in the catalog`);
					}
					this.state.tables.delete(key);
					await this.engine.setParts(change.database, change.table, []);
					await this.engine.setFanOut(change.database, change.table, 0);
					this.state.databases.add(change.to_database);
					const renamed: Table = {
						...table,
						database: change.to_database,
						table: change.to_table,
						createQuery: change.create_query,
						ttl: change.ttl,
						definitionVersion: 0,
					};
					this.state.tables.set(tableKey(change.to_database, change.to_table), renamed);
					await this.engine.setParts(renamed.database, renamed.table, renamed.parts, renamed.detached);
					await this.updateFanOut(renamed);
					break;
				}
				case "exchange_tables": {
					const to = tableKey(change.to_database, change.to_table);
					const [table, other] = [this.state.tables.get(key), this.state.tables.get(to)];
					if (table === undefined || other === undefined) {
						throw new Error(`The query exchanged ${key} and ${to}, which are not both in the catalog`);
					}
					const at_to: Table = { ...table, database: change.to_database, table: change.to_table, createQuery: change.create_query, ttl: change.ttl, definitionVersion: other.definitionVersion + 1 };
					const at_key: Table = { ...other, database: change.database, table: change.table, createQuery: change.to_create_query, ttl: change.to_ttl, definitionVersion: table.definitionVersion + 1 };
					this.state.tables.set(to, at_to);
					this.state.tables.set(key, at_key);
					for (const moved of [at_to, at_key]) {
						await this.engine.setParts(moved.database, moved.table, moved.parts, moved.detached);
						await this.updateFanOut(moved);
					}
					break;
				}
				case "replace_parts": {
					unused.push(...change.removed, ...change.dropped_detached);
					const table = this.state.tables.get(key);
					/// Dropped since, by a commit that this engine has synced to.
					if (table === undefined) {
						break;
					}
					const gone = new Set([...change.removed, ...change.detached]);
					table.parts = table.parts.filter((part) => !gone.has(part));
					table.parts.push(...change.attached, ...(change.added === null ? [] : [change.added]), ...change.shared);
					const back = new Set([...change.attached, ...change.dropped_detached]);
					table.detached = [...table.detached.filter((part) => !back.has(part)), ...change.detached];
					await this.engine.setParts(change.database, change.table, table.parts, table.detached);
					await this.updateFanOut(table);
					changed.add(key);
					break;
				}
			}
		}
		if (current) {
			this.state.version = version;
		}

		io.background(this.deleteUnreferenced(unused));
		for (const key of changed) {
			const table = this.state.tables.get(key);
			if (table !== undefined && table.parts.length >= this.limits.mergeThreshold) {
				io.background(io.scheduleMerge({ database: table.database, table: table.table, ttl: false }));
			}
		}
	}

	/// Gives up on the changes of the last query. The engine has already applied the ones to its tables, so the
	/// tables that the query created, altered or dropped are put back as the catalog has them.
	private async discardChanges(): Promise<void> {
		const touched = new Map<string, boolean>();
		const databases: string[] = [];
		for (const change of await this.engine.changes()) {
			if (change.kind === "create_database" || change.kind === "drop_database") {
				databases.push(change.database);
			} else if (change.kind === "rename_table" || change.kind === "exchange_tables") {
				touched.set(tableKey(change.database, change.table), change.kind === "exchange_tables");
				touched.set(tableKey(change.to_database, change.to_table), true);
			} else if (change.kind !== "replace_parts") {
				touched.set(tableKey(change.database, change.table), change.kind !== "drop_table");
			}
		}
		await this.engine.clearChanges();
		/// A dropped database comes back before its tables do.
		for (const database of databases) {
			if (this.state.databases.has(database)) {
				await this.engine.hostQuery(`CREATE DATABASE IF NOT EXISTS ${quote(database)} ENGINE = Memory`);
			}
		}
		for (const [key, exists] of touched) {
			const table = this.state.tables.get(key);
			if (table !== undefined) {
				await this.attach(table);
				await this.engine.setParts(table.database, table.table, table.parts, table.detached);
			} else if (exists) {
				const [database, name] = JSON.parse(key) as [string, string];
				await this.engine.hostQuery(`DROP TABLE IF EXISTS ${quote(database)}.${quote(name)}`);
			}
		}
		for (const database of databases) {
			if (!this.state.databases.has(database)) {
				await this.engine.hostQuery(`DROP DATABASE IF EXISTS ${quote(database)}`);
			}
		}
	}

	private async attach(table: Table): Promise<void> {
		await this.engine.hostQuery(`CREATE DATABASE IF NOT EXISTS ${quote(table.database)} ENGINE = Memory`);
		await this.engine.hostQuery(`DROP TABLE IF EXISTS ${quote(table.database)}.${quote(table.table)}`);
		await this.engine.hostQuery(table.createQuery);
	}

	private async detach(table: Table): Promise<void> {
		await this.engine.hostQuery(`DROP TABLE IF EXISTS ${quote(table.database)}.${quote(table.table)}`);
		await this.engine.setParts(table.database, table.table, []);
		await this.engine.setFanOut(table.database, table.table, 0);
	}

	private async updateFanOut(table: Table): Promise<void> {
		await this.engine.setFanOut(table.database, table.table, table.parts.length > 1 ? this.limits.shardBytes : 0);
	}

	/// Deletes parts that a commit took out of a table, unless another table has them: tables share the parts of
	/// `ATTACH PARTITION ... FROM`, `REPLACE PARTITION ... FROM` and `MOVE PARTITION`. The catalog is read after the
	/// commit, and a table only gets a part from another table that has it, which the commit checks, so a part that no
	/// table has then stays so.
	private async deleteUnreferenced(names: string[]): Promise<void> {
		if (names.length === 0) {
			return;
		}
		const snapshot = await this.catalog.snapshot();
		const referenced = new Set([...snapshot.tables.values()].flatMap((table) => [...table.parts, ...table.detached]));
		await this.deleteParts(names.filter((name) => !referenced.has(name)));
	}

	/// Deletes parts that no table has. A query in another isolate may still be about to read one of them: it finds it
	/// gone, syncs and runs again.
	private async deleteParts(names: string[]): Promise<void> {
		for (let i = 0; i < names.length; i += 1000) {
			await this.parts.delete(names.slice(i, i + 1000).map(partKey));
		}
	}

	private async exclusive<T>(what: string, f: () => Promise<T>): Promise<T> {
		const previous = this.queue;
		let release!: () => void;
		this.queue = new Promise<void>((resolve) => (release = resolve));
		this.waiting += 1;
		try {
			await previous;
		} finally {
			this.waiting -= 1;
		}
		this.held_since = Date.now();
		this.held_by = what;
		try {
			return await f();
		} finally {
			this.held_since = undefined;
			this.held_by = undefined;
			release();
		}
	}

	/// How long the work that holds the lock has run, and how much more is waiting for it: the gateway sends a query to
	/// the less busy of two engines (`askPool`), so that it does not wait behind a long query, or a stuck one.
	busy(now: number = Date.now()): EngineLoad {
		return {
			running_ms: this.held_since === undefined ? 0 : now - this.held_since,
			queued: this.waiting,
			...(this.held_by === undefined ? {} : { step: this.held_by }),
		};
	}
}

/// The most that the shards of one fan-out send back in all: the coordinator holds their answers until they all are in,
/// in the Worker and in the engine, which decodes them. A shard sends back at most 4 MiB (`max_shard_result_bytes`).
export const MAX_SHARD_RESULTS_BYTES = 24 * 1024 * 1024;

/// What `Database.busy` says. `step` is the work that holds the lock, and `waits` the calls with a deadline that the
/// isolate waits on (`pendingWaits`): a lock that is held with no such call is waiting on something else.
export interface EngineLoad {
	running_ms: number;
	queued: number;
	step?: string;
	waits?: string[];
}

/// What a query that ran in the engine leaves to do.
type Step =
	| { kind: "done"; result: QueryOutput }
	| { kind: "retry" }
	| { kind: "commit"; snapshot: Snapshot; changes: CatalogChange[]; written: string[]; result: QueryOutput };

function sameParts(a: readonly string[], b: readonly string[]): boolean {
	return a.length === b.length && a.every((part, i) => part === b[i]);
}

/// Quotes an identifier for a query.
export function quote(name: string): string {
	return "`" + name.replaceAll("\\", "\\\\").replaceAll("`", "\\`") + "`";
}
