import { type Catalog, type Snapshot, type Table, tableKey } from "./catalog";
import { type CatalogChange, type Engine, type FanOutRequest, NOT_FOUND_EXPECTED_DATA_PART, type QueryResult } from "./engine";
import { chooseMerge, groupParts, partKey, shardParts } from "./parts";
import { ReadAhead } from "./readahead";

/// How many times a query runs again because the catalog has moved on before it could commit.
const MAX_ATTEMPTS = 5;
/// How many times a query runs again to load parts or the results of shards: once per table that it reads, at most.
const MAX_LOAD_ROUNDS = 16;
/// The code of the exception of ClickHouse for a write in a read-only query.
const READONLY = 164;

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
}

/// A part decodes to a few times its size, and a merge holds its parts and the merged one: 4 MiB of parts keep a
/// shard or a merge well inside the memory of an engine.
export const DEFAULT_LIMITS: Limits = {
	shardBytes: 4 * 1024 * 1024,
	maxShards: 16,
	mergeBytes: 4 * 1024 * 1024,
	mergeParts: 10,
	mergeThreshold: 8,
};

/// The query of one shard of a fan-out: the coordinator ran its query against catalog `version`, and the shard reads
/// `parts` of the table.
export interface ShardRequest {
	version: number;
	database: string;
	table: string;
	parts: string[];
	query: string;
}

export type ShardAnswer = { ok: true; data: Uint8Array } | { ok: false; stale: boolean; message: string };

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

	/// A `readonly` query can change nothing: the engine runs it with `readonly` = 1, and its changes are not committed.
	query(
		query: string | Uint8Array,
		format: string,
		io: Io,
		readonly = false,
		timings: Timings = { sync: 0, load: 0, commit: 0 },
	): Promise<QueryResult> {
		return this.run(query, format, io, readonly, timings);
	}

	/// Runs the query of a shard, at the version of the catalog that the coordinator ran its query against.
	///
	/// Durable Objects can share an isolate, and so an engine: a shard can be in the isolate of its coordinator,
	/// which holds the lock while it waits for its shards. So a shard that is at the version of the request runs
	/// without the lock: it changes nothing but the loaded parts, and the engine shows it its parts for the duration
	/// of its query only. A shard at another version syncs, under the lock, and it is never the coordinator then.
	async shard(request: ShardRequest, io: Io): Promise<ShardAnswer> {
		const stale = () => ({ ok: false as const, stale: true, message: `The shard is at version ${this.state.version} of the catalog, not ${request.version}` });
		if (this.state.version !== request.version) {
			await this.exclusive(() => this.sync());
			if (this.state.version !== request.version) {
				return stale();
			}
		}
		const table = this.state.tables.get(tableKey(request.database, request.table));
		if (table === undefined || request.parts.some((part) => !table.parts.includes(part))) {
			throw new Error(`The parts of the shard are not in ${request.database}.${request.table}`);
		}
		const reader = new ReadAhead((part, offset, size) => io.readRange(part, offset, size));
		const result = await this.engine.shardQuery(request.query, request.database, request.table, request.parts, reader);
		/// Other work of the isolate may have synced to another version while this shard ran.
		if (this.state.version !== request.version) {
			return stale();
		}
		return result.ok ? { ok: true, data: result.body } : { ok: false, stale: false, message: result.message };
	}

	/// Runs a merge from the queue. A merge that loses to a concurrent change runs again against the new catalog.
	merge(message: MergeMessage, io: Io): Promise<number> {
		return this.exclusive(async () => {
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
	private async run(query: string | Uint8Array, format: string, io: Io, readonly: boolean, timings: Timings): Promise<QueryResult> {
		for (let attempt = 1; ; ++attempt) {
			const step = await this.exclusive(() => this.prepare(query, format, io, readonly, timings));
			if (step.kind === "done") {
				return step.result;
			}
			if (step.kind === "commit") {
				const start = Date.now();
				const committed = await this.commitParts(step.snapshot, step.changes, step.uploads, io);
				timings.commit += Date.now() - start;
				if (committed) {
					return step.result;
				}
			}
			if (attempt === MAX_ATTEMPTS) {
				return { ok: false, code: 0, message: `The catalog changed during the query ${MAX_ATTEMPTS} times in a row, try again.` };
			}
		}
	}

	/// Runs the query against the catalog that the engine is in sync with, and commits the changes to definitions.
	private async prepare(query: string | Uint8Array, format: string, io: Io, readonly: boolean, timings: Timings): Promise<Step> {
		let start = Date.now();
		await this.sync();
		timings.sync += Date.now() - start;
		const result = await this.execute(query, format, readonly, timings, io);
		if (result === "stale") {
			return { kind: "retry" };
		}
		if (!result.ok) {
			await this.discardChanges();
			return { kind: "done", result };
		}
		const changes = await this.engine.changes();
		if (changes.length === 0) {
			return { kind: "done", result };
		}
		if (readonly) {
			await this.discardChanges();
			return { kind: "done", result: { ok: false, code: READONLY, message: "A read-only query cannot change the catalog." } };
		}
		if (changes.some((change) => change.kind !== "replace_parts")) {
			start = Date.now();
			const committed = await this.commit(changes, io);
			timings.commit += Date.now() - start;
			return committed ? { kind: "done", result } : { kind: "retry" };
		}
		const uploads = await this.takeUploads(changes);
		await this.engine.clearChanges();
		/// The definitions as the query saw them: an `ALTER` committed meanwhile makes the commit fail.
		const tables = new Map([...this.state.tables].map(([key, table]) => [key, { ...table, parts: [...table.parts], detached: [...table.detached] }]));
		const snapshot: Snapshot = { version: this.state.version, databases: new Set(this.state.databases), tables };
		return { kind: "commit", snapshot, changes, uploads, result };
	}

	/// Runs the query, which reads the parts that it needs as it goes, and has the shards run their queries for the
	/// tables that fan out. Returns `"stale"` if the engine is behind the catalog: a shard is ahead of it.
	private async execute(
		query: string | Uint8Array,
		format: string,
		readonly: boolean,
		timings: Timings,
		io: Io,
	): Promise<QueryResult | "stale"> {
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
		};
		try {
			for (let round = 1; ; ++round) {
				const result = await this.engine.query(query, format, readonly, reader);
				if (result.ok || result.code !== NOT_FOUND_EXPECTED_DATA_PART || round === MAX_LOAD_ROUNDS) {
					return result;
				}
				await this.discardChanges();
				const requests = await this.engine.fanOutRequests();
				if (requests.length === 0) {
					return result;
				}
				const start = Date.now();
				const fanned = await Promise.all(requests.map((request) => this.fanOut(request, io)));
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
	private async fanOut(request: FanOutRequest, io: Io): Promise<"done" | "stale" | { message: string }> {
		const table = this.state.tables.get(tableKey(request.database, request.table));
		if (table === undefined) {
			throw new Error(`The engine fans out ${request.database}.${request.table}, which is not in the catalog`);
		}
		const groups = shardParts(table.parts, this.limits.shardBytes, this.limits.maxShards);
		const answers = await Promise.all(
			groups.map((parts, index) =>
				io.runShard(index, { version: this.state.version, database: table.database, table: table.table, parts, query: request.query }),
			),
		);
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

	/// Merges `group`, some of the parts of the table, into one, with `OPTIMIZE`, which merges the visible parts.
	private async mergeParts(table: Table, group: string[], io: Io): Promise<"merged" | "nothing" | "conflict"> {
		await this.engine.setParts(table.database, table.table, group);
		try {
			const timings: Timings = { sync: 0, load: 0, commit: 0 };
			const result = await this.execute(`OPTIMIZE TABLE ${quote(table.database)}.${quote(table.table)} FINAL`, "", false, timings, io);
			if (result === "stale") {
				return "conflict";
			}
			if (!result.ok) {
				await this.discardChanges();
				throw new Error(`Cannot merge the parts of ${table.database}.${table.table}: ${result.message}`);
			}
			const changes = await this.engine.changes();
			if (changes.length === 0) {
				return "nothing";
			}
			return (await this.commit(changes, io)) ? "merged" : "conflict";
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

	/// Uploads the new parts and commits the changes, under the lock. Returns false if the catalog has moved on.
	private async commit(changes: CatalogChange[], io: Io): Promise<boolean> {
		const uploads = await this.takeUploads(changes);
		await Promise.all(uploads.map((upload) => this.upload(upload)));

		let version: number | undefined;
		try {
			version = await this.catalog.commit(this.state, changes);
		} catch (error) {
			await this.discardChanges();
			this.state.version = -1;
			io.background(this.deleteParts(uploads.map((upload) => upload.name)));
			throw error;
		}
		if (version === undefined) {
			await this.discardChanges();
			this.state.version = -1;
			io.background(this.deleteParts(uploads.map((upload) => upload.name)));
			return false;
		}
		await this.apply(changes, version, io);
		await this.engine.clearChanges();
		return true;
	}

	/// Uploads the new parts and commits changes of parts only, without the lock. The engine has given them up, so a
	/// commit that fails leaves nothing to undo.
	private async commitParts(snapshot: Snapshot, changes: CatalogChange[], uploads: Upload[], io: Io): Promise<boolean> {
		await Promise.all(uploads.map((upload) => this.upload(upload)));
		let version: number | undefined;
		try {
			version = await this.catalog.commit(snapshot, changes);
		} catch (error) {
			io.background(this.deleteParts(uploads.map((upload) => upload.name)));
			throw error;
		}
		if (version === undefined) {
			io.background(this.deleteParts(uploads.map((upload) => upload.name)));
			return false;
		}
		await this.exclusive(() => this.apply(changes, version, io));
		return true;
	}

	private async takeUploads(changes: CatalogChange[]): Promise<Upload[]> {
		const uploads: Upload[] = [];
		for (const [i, change] of changes.entries()) {
			if (change.kind === "replace_parts" && change.added !== null) {
				uploads.push({ name: change.added, data: await this.engine.changeData(i) });
			}
			if (change.kind === "replace_parts") {
				uploads.push(...change.copied.map((copy) => ({ name: copy.to, copyOf: copy.from })));
			}
		}
		return uploads;
	}

	/// Puts a new part in R2: its data, or a copy of a part of another table, streamed.
	private async upload(upload: Upload): Promise<void> {
		if ("data" in upload) {
			await this.parts.put(partKey(upload.name), upload.data);
			return;
		}
		const source = await this.parts.get(partKey(upload.copyOf));
		if (source === null) {
			throw new Error(`Cannot copy part ${upload.copyOf}, which is gone: it was merged meanwhile, try again`);
		}
		const { readable, writable } = new FixedLengthStream(source.size);
		await Promise.all([source.body.pipeTo(writable), this.parts.put(partKey(upload.name), readable)]);
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
					table.parts.push(...change.attached, ...(change.added === null ? [] : [change.added]), ...change.copied.map((copy) => copy.to));
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

		io.background(this.deleteParts(unused));
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

	/// Deletes parts that the catalog no longer has. A query in another isolate may still be about to read one of
	/// them: it finds it gone, syncs and runs again.
	private async deleteParts(names: string[]): Promise<void> {
		for (let i = 0; i < names.length; i += 1000) {
			await this.parts.delete(names.slice(i, i + 1000).map(partKey));
		}
	}

	private async exclusive<T>(f: () => Promise<T>): Promise<T> {
		const previous = this.queue;
		let release!: () => void;
		this.queue = new Promise<void>((resolve) => (release = resolve));
		await previous;
		try {
			return await f();
		} finally {
			release();
		}
	}
}

/// A new part: its data, or the part of another table that it is a copy of.
type Upload = { name: string; data: Uint8Array } | { name: string; copyOf: string };

/// What a query that ran in the engine leaves to do.
type Step =
	| { kind: "done"; result: QueryResult }
	| { kind: "retry" }
	| { kind: "commit"; snapshot: Snapshot; changes: CatalogChange[]; uploads: Upload[]; result: QueryResult };

function sameParts(a: readonly string[], b: readonly string[]): boolean {
	return a.length === b.length && a.every((part, i) => part === b[i]);
}

/// Quotes an identifier for a query.
export function quote(name: string): string {
	return "`" + name.replaceAll("\\", "\\\\").replaceAll("`", "\\`") + "`";
}
