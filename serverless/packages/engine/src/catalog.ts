import { type Coordination, ErrorCode, type MultiResult, type Op, type OpResult } from "coordination";
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

const ROOT = "/catalog";

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
const DETACHED = encoder.encode("detached");

/// How long a session lives without requests. The catalog has no ephemeral nodes, so a session only needs to
/// outlive one request.
const SESSION_TIMEOUT_MS = 60_000;

export class Catalog {
	private session: number | undefined;

	/// A stub belongs to the request that made it, so every call makes one.
	constructor(private readonly coordination: () => DurableObjectStub<Coordination>) {}

	/// Creates the session ahead of the first request to the catalog.
	async connect(): Promise<void> {
		this.session ??= await this.coordination().createSession(SESSION_TIMEOUT_MS);
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
	/// catalog has moved on since `snapshot`, and the query has to run again against the new catalog.
	async commit(snapshot: Snapshot, changes: readonly CatalogChange[]): Promise<number | undefined> {
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

	private async exec(op: Op): Promise<OpResult> {
		return this.withSession((session) => this.coordination().exec(session, op), (result) => result.error);
	}

	private async multi(ops: Op[]): Promise<MultiResult> {
		if (ops.length === 0) {
			return { error: ErrorCode.ZOK, results: [] };
		}
		return this.withSession((session) => this.coordination().multi(session, ops), (result) => result.error);
	}

	/// Runs `f` in the session of this isolate, which is created again if it has expired.
	private async withSession<T>(f: (session: number) => Promise<T>, error: (result: T) => ErrorCode): Promise<T> {
		this.session ??= await this.coordination().createSession(SESSION_TIMEOUT_MS);
		const result = await f(this.session);
		if (error(result) !== ErrorCode.ZSESSIONEXPIRED) {
			return result;
		}
		this.session = await this.coordination().createSession(SESSION_TIMEOUT_MS);
		return f(this.session);
	}
}

function statOf(result: OpResult) {
	if (result.error !== ErrorCode.ZOK || result.stat === undefined) {
		throw new Error(`Cannot read the catalog: error ${result.error}`);
	}
	return result.stat;
}
