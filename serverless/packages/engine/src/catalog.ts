import { type Coordination, ErrorCode, type MultiResult, type Op, type OpResult } from "coordination";
import { CATALOG_DEADLINE_MS, withDeadline } from "./deadline";
import type { CatalogChange } from "./engine";

/// The catalog of the tables of `ObjectMergeTree`: their definitions and their parts, in a `Coordination` object.
///
///   /catalog                          its version is the version of the catalog, set by every commit
///   /catalog/<database>
///   /catalog/<database>/<table>       the table: `{"create_query": ..., "ttl": ...}`
///   /catalog/<database>/<table>/<part>    `detached` for a detached part
///
/// Names are URI-encoded in paths. A rename moves the nodes of the parts of a table to its new node. A commit is one transaction that also sets `/catalog`, so that its version is the
/// version of the catalog. A commit that changes a definition (creates, alters or drops a table) sets it at the version
/// it was made against, so it fails against an old one. A commit that only adds and removes parts (`INSERT`, merges,
/// mutations, `TRUNCATE`) does not need the catalog unchanged: only the definition of its tables, which it checks with
/// the version of their nodes, and the parts it removes, which fail to remove if they are gone. So concurrent inserts
/// do not conflict, and a merge only conflicts with another one of the same parts.
///
///   /deduplication/<table>.<token>    an `INSERT` with the setting `insert_deduplication_token` that has been committed
///
/// `<table>` is the `czxid` of the node of the table, so a table that is dropped and created again has no tokens, and
/// the node is created in the transaction of the parts: whichever of two `INSERT`s with a token commits first wins, even
/// if the client gave up on it and sent it again meanwhile. These are outside `/catalog`, which every engine reads
/// whole. They go after `DEDUPLICATION_WINDOW_MS` (`expireDeduplication`).

const ROOT = "/catalog";
const DEDUPLICATION_ROOT = "/deduplication";
/// Longer than any client retries for, and short enough that the tokens of a day stay few.
export const DEDUPLICATION_WINDOW_MS = 24 * 60 * 60 * 1000;
/// `Catalog.commit`: an `INSERT` with this token has been committed already, so this one changes nothing.
export const DUPLICATE = "duplicate";

export interface Table {
	database: string;
	table: string;
	createQuery: string;
	/// Whether the table has a TTL, which merges apply.
	ttl: boolean;
	/// The version of the node of the table, which an `ALTER` changes.
	definitionVersion: number;
	parts: string[];
	/// The detached parts (`ALTER TABLE ... DETACH PARTITION`).
	detached: string[];
}

interface TableDefinition {
	create_query: string;
	ttl: boolean;
}

export interface Snapshot {
	version: number;
	databases: Set<string>;
	tables: Map<string, Table>;
}

export function tableKey(database: string, table: string): string {
	return JSON.stringify([database, table]);
}

const databasePath = (database: string) => `${ROOT}/${encodeURIComponent(database)}`;
const tablePath = (database: string, table: string) => `${databasePath(database)}/${encodeURIComponent(table)}`;
const partPath = (database: string, table: string, part: string) => `${tablePath(database, table)}/${part}`;

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const EXPIRE_BATCH = 500;
const DETACHED = encoder.encode("detached");

/// How long a session lives without requests. The catalog has no ephemeral nodes, so a session only needs to
/// outlive one request.
const SESSION_TIMEOUT_MS = 60_000;

export class Catalog {
	private session: number | undefined;
	private deduplication_root_exists = false;

	/// A stub belongs to the request that made it, so every call makes one.
	constructor(private readonly coordination: () => DurableObjectStub<Coordination>) {}

	/// Creates the session ahead of the first request to the catalog.
	async connect(): Promise<void> {
		this.session ??= await withDeadline(this.coordination().createSession(SESSION_TIMEOUT_MS), CATALOG_DEADLINE_MS, "The catalog");
	}

	async version(): Promise<number> {
		const result = await this.exec({ type: "exists", path: ROOT });
		if (result.error === ErrorCode.ZNONODE) {
			const created = await this.exec({ type: "create", path: ROOT });
			if (created.error !== ErrorCode.ZOK && created.error !== ErrorCode.ZNODEEXISTS) {
				throw new Error(`Cannot create the catalog: error ${created.error}`);
			}
			return this.version();
		}
		return statOf(result).version;
	}

	/// Reads the whole catalog, in one atomic read.
	async snapshot(): Promise<Snapshot> {
		let result = await this.withSession((session) => this.coordination().tree(session, ROOT), (tree) => tree.error);
		if (result.error === ErrorCode.ZNONODE) {
			await this.version();
			result = await this.withSession((session) => this.coordination().tree(session, ROOT), (tree) => tree.error);
		}
		if (result.error !== ErrorCode.ZOK || result.stat === undefined) {
			throw new Error(`Cannot read the catalog: error ${result.error}`);
		}

		const databases = new Set<string>();
		const tables = new Map<string, Table>();
		for (const node of result.nodes) {
			const [database, table, part, ...rest] = node.path.slice(ROOT.length + 1).split("/").map(decodeURIComponent);
			if (database === undefined || rest.length > 0) {
				throw new Error(`Not a node of the catalog: ${node.path}`);
			}
			if (table === undefined) {
				databases.add(database);
			} else if (part === undefined) {
				const definition = JSON.parse(decoder.decode(node.data)) as TableDefinition;
				tables.set(tableKey(database, table), {
					database,
					table,
					createQuery: definition.create_query,
					ttl: definition.ttl,
					definitionVersion: node.version,
					parts: [],
					detached: [],
				});
			} else {
				const entry = tables.get(tableKey(database, table));
				if (entry === undefined) {
					throw new Error(`The catalog has a part of ${database}.${table}, which it does not have`);
				}
				(decoder.decode(node.data) === "detached" ? entry.detached : entry.parts).push(part);
			}
		}
		return { version: result.stat.version, databases, tables };
	}

	/// Commits the changes of a query that ran against `snapshot`. Returns the new version, or `undefined` if the
	/// catalog has moved on since `snapshot`, and the query has to run again against the new catalog, or `DUPLICATE` if an
	/// `INSERT` with the same token has been committed, and this one must leave nothing behind.
	async commit(snapshot: Snapshot, changes: readonly CatalogChange[]): Promise<number | undefined | typeof DUPLICATE> {
		const parts_only = changes.every((change) => change.kind === "replace_parts");
		const ops: Op[] = parts_only ? [] : [{ type: "set", path: ROOT, data: new Uint8Array(), version: snapshot.version }];
		if (parts_only) {
			for (const key of new Set(changes.map((change) => tableKey(change.database, change.table)))) {
				const table = snapshot.tables.get(key);
				if (table === undefined) {
					throw new Error(`The query changed the parts of ${key}, which is not in the catalog`);
				}
				ops.push({ type: "check", path: tablePath(table.database, table.table), version: table.definitionVersion });
			}
		}
		/// The tokens come before the parts: a retry of an `INSERT` that committed says so, whatever else has changed since.
		const token_paths = new Set<string>();
		for (const change of changes) {
			if (change.kind === "replace_parts" && change.deduplication_token !== undefined) {
				const table = await this.exec({ type: "exists", path: tablePath(change.database, change.table) });
				/// A table that has gone since the query ran: the transaction fails on its check.
				const id = table.error === ErrorCode.ZNONODE ? "gone" : statOf(table).czxid;
				token_paths.add(`${DEDUPLICATION_ROOT}/${id}.${encodeURIComponent(change.deduplication_token)}`);
			}
		}
		const token_ops: number[] = [];
		if (token_paths.size > 0) {
			await this.ensureDeduplicationRoot();
			for (const path of token_paths) {
				token_ops.push(ops.length);
				ops.push({ type: "create", path });
			}
		}
		const databases = new Set(snapshot.databases);
		const parts = new Map([...snapshot.tables].map(([key, table]) => [key, new Set(table.parts)]));
		const detached = new Map([...snapshot.tables].map(([key, table]) => [key, new Set(table.detached)]));
		const definition = (create_query: string, ttl: boolean) =>
			encoder.encode(JSON.stringify({ create_query, ttl } satisfies TableDefinition));
		/// Moves the nodes of the parts of a table to another one.
		const moveParts = (from: [string, string], to: [string, string]) => {
			for (const part of parts.get(tableKey(...from)) ?? []) {
				ops.push({ type: "remove", path: partPath(...from, part) }, { type: "create", path: partPath(...to, part) });
			}
			for (const part of detached.get(tableKey(...from)) ?? []) {
				ops.push({ type: "remove", path: partPath(...from, part) }, { type: "create", path: partPath(...to, part), data: DETACHED });
			}
		};

		for (const change of changes) {
			const key = tableKey(change.database, change.table);
			switch (change.kind) {
				case "create_database":
					if (!databases.has(change.database)) {
						ops.push({ type: "create", path: databasePath(change.database) });
						databases.add(change.database);
					}
					break;
				case "drop_database":
					/// After the drops of its tables, which come first.
					if (databases.has(change.database)) {
						ops.push({ type: "remove", path: databasePath(change.database) });
						databases.delete(change.database);
					}
					break;
				case "create_table":
					if (!databases.has(change.database)) {
						ops.push({ type: "create", path: databasePath(change.database) });
						databases.add(change.database);
					}
					ops.push({
						type: "create",
						path: tablePath(change.database, change.table),
						data: encoder.encode(JSON.stringify({ create_query: change.create_query, ttl: change.ttl } satisfies TableDefinition)),
					});
					parts.set(key, new Set());
					detached.set(key, new Set());
					break;
				case "alter_table":
					ops.push({
						type: "set",
						path: tablePath(change.database, change.table),
						data: encoder.encode(JSON.stringify({ create_query: change.create_query, ttl: change.ttl } satisfies TableDefinition)),
					});
					break;
				case "drop_table":
					for (const part of [...(parts.get(key) ?? []), ...(detached.get(key) ?? [])]) {
						ops.push({ type: "remove", path: partPath(change.database, change.table, part) });
					}
					ops.push({ type: "remove", path: tablePath(change.database, change.table) });
					parts.delete(key);
					detached.delete(key);
					break;
				case "rename_table": {
					const to = tableKey(change.to_database, change.to_table);
					if (!databases.has(change.to_database)) {
						ops.push({ type: "create", path: databasePath(change.to_database) });
						databases.add(change.to_database);
					}
					ops.push({ type: "create", path: tablePath(change.to_database, change.to_table), data: definition(change.create_query, change.ttl) });
					moveParts([change.database, change.table], [change.to_database, change.to_table]);
					ops.push({ type: "remove", path: tablePath(change.database, change.table) });
					parts.set(to, parts.get(key) ?? new Set());
					detached.set(to, detached.get(key) ?? new Set());
					parts.delete(key);
					detached.delete(key);
					break;
				}
				case "exchange_tables": {
					const to = tableKey(change.to_database, change.to_table);
					ops.push(
						{ type: "set", path: tablePath(change.to_database, change.to_table), data: definition(change.create_query, change.ttl), version: -1 },
						{ type: "set", path: tablePath(change.database, change.table), data: definition(change.to_create_query, change.to_ttl), version: -1 },
					);
					moveParts([change.database, change.table], [change.to_database, change.to_table]);
					moveParts([change.to_database, change.to_table], [change.database, change.table]);
					const [from_parts, from_detached] = [parts.get(key) ?? new Set<string>(), detached.get(key) ?? new Set<string>()];
					parts.set(key, parts.get(to) ?? new Set());
					detached.set(key, detached.get(to) ?? new Set());
					parts.set(to, from_parts);
					detached.set(to, from_detached);
					break;
				}
				case "replace_parts": {
					const path = (part: string) => partPath(change.database, change.table, part);
					for (const part of [...change.removed, ...change.dropped_detached]) {
						ops.push({ type: "remove", path: path(part) });
						parts.get(key)?.delete(part);
						detached.get(key)?.delete(part);
					}
					for (const part of change.detached) {
						ops.push({ type: "set", path: path(part), data: DETACHED, version: -1 });
						parts.get(key)?.delete(part);
						detached.get(key)?.add(part);
					}
					for (const part of change.attached) {
						ops.push({ type: "set", path: path(part), data: new Uint8Array(), version: -1 });
						detached.get(key)?.delete(part);
						parts.get(key)?.add(part);
					}
					/// A table shares a part of another table that has it still.
					for (const part of change.shared) {
						ops.push({ type: "check", path: partPath(change.shared_from[0], change.shared_from[1], part), version: -1 });
					}
					for (const part of [...(change.added === null ? [] : [change.added]), ...change.shared]) {
						ops.push({ type: "create", path: path(part) });
						parts.get(key)?.add(part);
					}
					break;
				}
			}
		}

		if (parts_only) {
			ops.push({ type: "set", path: ROOT, data: new Uint8Array(), version: -1 });
		}

		const result = await this.multi(ops);
		if (token_ops.some((index) => result.results[index]?.error === ErrorCode.ZNODEEXISTS)) {
			return DUPLICATE;
		}
		if (result.error === ErrorCode.ZOK) {
			return parts_only ? statOf(result.results[ops.length - 1]!).version : snapshot.version + 1;
		}
		/// The catalog has moved on (a definition), the table has changed or is gone, or a part to remove is gone.
		const conflicts: ErrorCode[] = parts_only ? [ErrorCode.ZBADVERSION, ErrorCode.ZNONODE] : [ErrorCode.ZBADVERSION];
		if (result.results.some((op) => conflicts.includes(op.error))) {
			return undefined;
		}
		const failed = result.results.findIndex((op) => op.error !== ErrorCode.ZOK && op.error !== ErrorCode.ZRUNTIMEINCONSISTENCY);
		throw new Error(`Cannot commit to the catalog: error ${result.error} at ${JSON.stringify(ops[failed])}`);
	}

	/// Forgets the tokens of `INSERT`s older than `DEDUPLICATION_WINDOW_MS`. Returns how many.
	async expireDeduplication(now: number = Date.now()): Promise<number> {
		const tree = await this.withSession((session) => this.coordination().tree(session, DEDUPLICATION_ROOT), (result) => result.error);
		if (tree.error === ErrorCode.ZNONODE) {
			return 0;
		}
		if (tree.error !== ErrorCode.ZOK) {
			throw new Error(`Cannot read the tokens of deduplication: error ${tree.error}`);
		}
		const expired = tree.nodes.filter((node) => now - node.ctime > DEDUPLICATION_WINDOW_MS).map((node) => node.path);
		for (let i = 0; i < expired.length; i += EXPIRE_BATCH) {
			const result = await this.multi(expired.slice(i, i + EXPIRE_BATCH).map((path) => ({ type: "remove", path })));
			if (result.error !== ErrorCode.ZOK) {
				throw new Error(`Cannot delete the tokens of deduplication: error ${result.error}`);
			}
		}
		return expired.length;
	}

	private async ensureDeduplicationRoot(): Promise<void> {
		if (this.deduplication_root_exists) {
			return;
		}
		const created = await this.exec({ type: "create", path: DEDUPLICATION_ROOT });
		if (created.error !== ErrorCode.ZOK && created.error !== ErrorCode.ZNODEEXISTS) {
			throw new Error(`Cannot create the tokens of deduplication: error ${created.error}`);
		}
		this.deduplication_root_exists = true;
	}

	private async exec(op: Op): Promise<OpResult> {
		return this.withSession((session) => this.coordination().exec(session, op), (result) => result.error);
	}

	private async multi(ops: Op[]): Promise<MultiResult> {
		if (ops.length === 0) {
			return { error: ErrorCode.ZOK, results: [] };
		}
		/// A write transaction that misses its deadline may still be applied, as with the timeout of the HTTP interface.
		const what = ops.some((op) => op.type === "create" || op.type === "remove" || op.type === "set")
			? "The commit to the catalog (which may still be applied)"
			: "The catalog";
		return this.withSession((session) => this.coordination().multi(session, ops), (result) => result.error, what);
	}

	/// Runs `f` in the session of this isolate, which is created again if it has expired. Each call has a deadline: a
	/// query syncs with the catalog with the lock of its database held (`deadline.ts`).
	private async withSession<T>(f: (session: number) => Promise<T>, error: (result: T) => ErrorCode, what = "The catalog"): Promise<T> {
		const call = <R>(promise: Promise<R>) => withDeadline(promise, CATALOG_DEADLINE_MS, what);
		this.session ??= await call(this.coordination().createSession(SESSION_TIMEOUT_MS));
		const result = await call(f(this.session));
		if (error(result) !== ErrorCode.ZSESSIONEXPIRED) {
			return result;
		}
		this.session = await call(this.coordination().createSession(SESSION_TIMEOUT_MS));
		return call(f(this.session));
	}
}

function statOf(result: OpResult) {
	if (result.error !== ErrorCode.ZOK || result.stat === undefined) {
		throw new Error(`Cannot read the catalog: error ${result.error}`);
	}
	return result.stat;
}
