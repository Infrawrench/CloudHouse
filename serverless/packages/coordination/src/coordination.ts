import { DurableObject } from "cloudflare:workers";
import type { Env } from "./env";
import { baseName, isValidPath, parentPath, sequentialSuffix } from "./paths";
import {
	ErrorCode,
	EventType,
	ListFilter,
	isReadOp,
	type MultiResult,
	type Op,
	type OpResult,
	type ReadOp,
	type Stat,
	type TreeResult,
	type WatchEvent,
	type WriteOp,
} from "./protocol";

const DEFAULT_SESSION_TIMEOUT_MS = 30_000;

type NodeRow = {
	path: string;
	parent: string | null;
	data: ArrayBuffer;
	czxid: number;
	mzxid: number;
	ctime: number;
	mtime: number;
	version: number;
	cversion: number;
	aversion: number;
	ephemeral_owner: number;
	num_children: number;
	pzxid: number;
	seq_num: number;
};

/// A data watch is set by `get` and `exists` and fires on create, set and remove of the node.
/// A child watch is set by `list` and fires on create or remove of a child, or of the node itself.
type WatchKind = "data" | "child";

/// Thrown inside a write transaction to roll it back.
class Abort {
	constructor(readonly results: OpResult[]) {}
}

/// A replacement for Keeper: one object holds the whole tree of one cluster in its SQLite storage.
/// A Durable Object processes one request at a time, which gives the linearizability Keeper gets
/// from Raft, and `transactionSync` gives the atomicity of `multi`.
///
/// Sessions are kept alive by any request and expire through the alarm, which removes their
/// ephemeral nodes. Watches are one-shot, as in ZooKeeper; their events are queued per session
/// and delivered by `pollEvents`.
export class Coordination extends DurableObject<Env> {
	private readonly sql: SqlStorage;

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		this.sql = ctx.storage.sql;
		migrate(this.sql);
	}

	async createSession(timeoutMs: number = DEFAULT_SESSION_TIMEOUT_MS): Promise<number> {
		const id = this.bumpCounter("session");
		this.sql.exec(
			"INSERT INTO sessions (id, timeout_ms, expires_at) VALUES (?, ?, ?)",
			id,
			timeoutMs,
			Date.now() + timeoutMs,
		);
		await this.scheduleExpiry();
		return id;
	}

	async heartbeat(session: number): Promise<ErrorCode> {
		return this.touchSession(session) ? ErrorCode.ZOK : ErrorCode.ZSESSIONEXPIRED;
	}

	async closeSession(session: number): Promise<void> {
		this.ctx.storage.transactionSync(() => this.dropSession(session));
		await this.scheduleExpiry();
	}

	async exec(session: number, op: Op, options: { watch?: boolean } = {}): Promise<OpResult> {
		if (!this.touchSession(session)) {
			return { error: ErrorCode.ZSESSIONEXPIRED };
		}
		if (isReadOp(op)) {
			return this.read(session, op, options.watch ?? false);
		}
		const result = this.writeTransaction(session, [op]).results[0];
		if (result === undefined) {
			throw new Error("A write transaction of one op returned no result");
		}
		return result;
	}

	/// All ops must be writes (an atomic transaction) or all reads (independent, no watches).
	async multi(session: number, ops: Op[]): Promise<MultiResult> {
		if (!this.touchSession(session)) {
			return failAll(ops, ErrorCode.ZSESSIONEXPIRED);
		}
		const reads = ops.filter(isReadOp);
		if (reads.length === ops.length) {
			const results = reads.map((op) => this.read(session, op, false));
			const failed = results.find((result) => result.error !== ErrorCode.ZOK);
			return { error: failed?.error ?? ErrorCode.ZOK, results };
		}
		if (reads.length !== 0) {
			return failAll(ops, ErrorCode.ZBADARGUMENTS);
		}
		return this.writeTransaction(session, ops as WriteOp[]);
	}

	/// Reads a node and everything under it in one atomic read, with the data of every node. Not a Keeper operation:
	/// it saves the readers of a whole subtree, such as a catalog, a round trip per level.
	async tree(session: number, path: string): Promise<TreeResult> {
		if (!this.touchSession(session)) {
			return { error: ErrorCode.ZSESSIONEXPIRED, nodes: [] };
		}
		if (!isValidPath(path)) {
			return { error: ErrorCode.ZBADARGUMENTS, nodes: [] };
		}
		const root = this.getNode(path);
		if (root === undefined) {
			return { error: ErrorCode.ZNONODE, nodes: [] };
		}
		/// The paths under `path` are the ones between `path/` and `path0`, as `0` follows `/`.
		const prefix = path === "/" ? "/" : `${path}/`;
		const end = path === "/" ? "0" : `${path}0`;
		const nodes = this.sql
			.exec<{ path: string; data: ArrayBuffer; version: number; ctime: number }>(
				"SELECT path, data, version, ctime FROM nodes WHERE path >= ? AND path < ? ORDER BY path",
				prefix,
				end,
			)
			.toArray()
			.filter((row) => row.path !== "/")
			.map((row) => ({ path: row.path, data: new Uint8Array(row.data), version: row.version, ctime: row.ctime }));
		return { error: ErrorCode.ZOK, stat: toStat(root), nodes };
	}

	/// Acknowledges the events up to `ackSeq` and returns the rest. Delivery is at-least-once:
	/// an event is returned again until a later call acknowledges it.
	async pollEvents(
		session: number,
		ackSeq: number = 0,
	): Promise<{ error: ErrorCode; events: WatchEvent[] }> {
		if (!this.touchSession(session)) {
			return { error: ErrorCode.ZSESSIONEXPIRED, events: [] };
		}
		this.sql.exec("DELETE FROM events WHERE session = ? AND seq <= ?", session, ackSeq);
		const events = this.sql
			.exec<{ seq: number; type: number; path: string }>(
				"SELECT seq, type, path FROM events WHERE session = ? ORDER BY seq",
				session,
			)
			.toArray()
			.map((row) => ({ seq: row.seq, type: row.type as EventType, path: row.path }));
		return { error: ErrorCode.ZOK, events };
	}

	override async alarm(): Promise<void> {
		const expired = this.sql
			.exec<{ id: number }>("SELECT id FROM sessions WHERE expires_at <= ?", Date.now())
			.toArray();
		for (const { id } of expired) {
			this.ctx.storage.transactionSync(() => this.dropSession(id));
		}
		await this.scheduleExpiry();
	}

	private writeTransaction(session: number, ops: WriteOp[]): MultiResult {
		try {
			return this.ctx.storage.transactionSync(() => {
				const zxid = this.bumpCounter("zxid");
				const results: OpResult[] = [];
				for (const op of ops) {
					const result = this.write(session, zxid, op);
					results.push(result);
					if (result.error !== ErrorCode.ZOK) {
						throw new Abort(results);
					}
				}
				return { error: ErrorCode.ZOK, results };
			});
		} catch (e) {
			if (!(e instanceof Abort)) {
				throw e;
			}
			const failed = e.results[e.results.length - 1]!.error;
			const rest = ops.slice(e.results.length).map(() => ({ error: ErrorCode.ZRUNTIMEINCONSISTENCY }));
			return { error: failed, results: [...e.results, ...rest] };
		}
	}

	private write(session: number, zxid: number, op: WriteOp): OpResult {
		switch (op.type) {
			case "create":
				return this.create(session, zxid, op);
			case "remove":
				return this.remove(zxid, op);
			case "set":
				return this.set(zxid, op);
			case "check":
				return this.check(op);
		}
	}

	private create(session: number, zxid: number, op: Extract<WriteOp, { type: "create" }>): OpResult {
		/// The sequential suffix is not known until the parent is read, so validate a stand-in.
		const candidate = op.sequential ? op.path + sequentialSuffix(0) : op.path;
		if (!isValidPath(candidate) || candidate === "/") {
			return { error: candidate === "/" ? ErrorCode.ZNODEEXISTS : ErrorCode.ZBADARGUMENTS };
		}
		const parent = this.getNode(parentPath(candidate));
		if (parent === undefined) {
			return { error: ErrorCode.ZNONODE };
		}
		if (parent.ephemeral_owner !== 0) {
			return { error: ErrorCode.ZNOCHILDRENFOREPHEMERALS };
		}
		const path = op.sequential ? op.path + sequentialSuffix(parent.seq_num) : op.path;
		if (this.getNode(path) !== undefined) {
			return { error: ErrorCode.ZNODEEXISTS };
		}

		const now = Date.now();
		this.sql.exec(
			`INSERT INTO nodes (path, parent, data, czxid, mzxid, ctime, mtime, version, cversion, aversion,
				ephemeral_owner, num_children, pzxid, seq_num)
			VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, 0, ?, 0, ?, 0)`,
			path,
			parent.path,
			toArrayBuffer(op.data ?? new Uint8Array()),
			zxid,
			zxid,
			now,
			now,
			op.ephemeral ? session : 0,
			zxid,
		);
		this.sql.exec(
			`UPDATE nodes SET num_children = num_children + 1, cversion = cversion + 1, pzxid = ?,
				seq_num = seq_num + 1
			WHERE path = ?`,
			zxid,
			parent.path,
		);
		this.fireWatches(path, "data", EventType.CREATED);
		this.fireWatches(parent.path, "child", EventType.CHILD);
		return { error: ErrorCode.ZOK, pathCreated: path };
	}

	private remove(zxid: number, op: Extract<WriteOp, { type: "remove" }>): OpResult {
		if (!isValidPath(op.path) || op.path === "/") {
			return { error: ErrorCode.ZBADARGUMENTS };
		}
		const node = this.getNode(op.path);
		if (node === undefined) {
			return { error: ErrorCode.ZNONODE };
		}
		if (!versionMatches(op.version, node.version)) {
			return { error: ErrorCode.ZBADVERSION };
		}
		if (node.num_children !== 0) {
			return { error: ErrorCode.ZNOTEMPTY };
		}
		this.deleteNode(zxid, node);
		return { error: ErrorCode.ZOK };
	}

	private set(zxid: number, op: Extract<WriteOp, { type: "set" }>): OpResult {
		if (!isValidPath(op.path)) {
			return { error: ErrorCode.ZBADARGUMENTS };
		}
		const node = this.getNode(op.path);
		if (node === undefined) {
			return { error: ErrorCode.ZNONODE };
		}
		if (!versionMatches(op.version, node.version)) {
			return { error: ErrorCode.ZBADVERSION };
		}
		this.sql.exec(
			"UPDATE nodes SET data = ?, version = version + 1, mzxid = ?, mtime = ? WHERE path = ?",
			toArrayBuffer(op.data),
			zxid,
			Date.now(),
			op.path,
		);
		this.fireWatches(op.path, "data", EventType.CHANGED);
		return { error: ErrorCode.ZOK, stat: toStat(this.getNode(op.path)!) };
	}

	private check(op: Extract<WriteOp, { type: "check" }>): OpResult {
		if (!isValidPath(op.path)) {
			return { error: ErrorCode.ZBADARGUMENTS };
		}
		const node = this.getNode(op.path);
		if (op.notExists) {
			const exists = node !== undefined && versionMatches(op.version, node.version);
			return { error: exists ? ErrorCode.ZNODEEXISTS : ErrorCode.ZOK };
		}
		if (node === undefined) {
			return { error: ErrorCode.ZNONODE };
		}
		return { error: versionMatches(op.version, node.version) ? ErrorCode.ZOK : ErrorCode.ZBADVERSION };
	}

	private read(session: number, op: ReadOp, watch: boolean): OpResult {
		if (!isValidPath(op.path)) {
			return { error: ErrorCode.ZBADARGUMENTS };
		}
		const node = this.getNode(op.path);
		switch (op.type) {
			case "exists":
				/// Unlike `get` and `list`, `exists` sets a watch on a missing node, to wait for its creation.
				if (watch) {
					this.addWatch(session, op.path, "data");
				}
				return node === undefined ? { error: ErrorCode.ZNONODE } : { error: ErrorCode.ZOK, stat: toStat(node) };
			case "get":
				if (node === undefined) {
					return { error: ErrorCode.ZNONODE };
				}
				if (watch) {
					this.addWatch(session, op.path, "data");
				}
				return { error: ErrorCode.ZOK, stat: toStat(node), data: new Uint8Array(node.data) };
			case "list": {
				if (node === undefined) {
					return { error: ErrorCode.ZNONODE };
				}
				if (watch) {
					this.addWatch(session, op.path, "child");
				}
				const filter = op.filter ?? ListFilter.ALL;
				const ownerCondition =
					filter === ListFilter.PERSISTENT_ONLY
						? "AND ephemeral_owner = 0"
						: filter === ListFilter.EPHEMERAL_ONLY
							? "AND ephemeral_owner != 0"
							: "";
				const children = this.sql
					.exec<{ path: string }>(
						`SELECT path FROM nodes WHERE parent = ? ${ownerCondition} ORDER BY path`,
						op.path,
					)
					.toArray()
					.map((row) => baseName(row.path));
				return { error: ErrorCode.ZOK, stat: toStat(node), children };
			}
		}
	}

	private deleteNode(zxid: number, node: NodeRow): void {
		this.sql.exec("DELETE FROM nodes WHERE path = ?", node.path);
		this.sql.exec(
			"UPDATE nodes SET num_children = num_children - 1, cversion = cversion + 1, pzxid = ? WHERE path = ?",
			zxid,
			node.parent,
		);
		this.fireWatches(node.path, "data", EventType.DELETED);
		this.fireWatches(node.path, "child", EventType.DELETED);
		this.fireWatches(node.parent!, "child", EventType.CHILD);
	}

	/// Returns false if the session does not exist or has expired; otherwise extends it.
	private touchSession(session: number): boolean {
		const row = this.sql
			.exec<{ timeout_ms: number; expires_at: number }>(
				"SELECT timeout_ms, expires_at FROM sessions WHERE id = ?",
				session,
			)
			.toArray()[0];
		if (row === undefined) {
			return false;
		}
		const now = Date.now();
		if (row.expires_at <= now) {
			/// The alarm has not run yet, but the session is already over.
			this.ctx.storage.transactionSync(() => this.dropSession(session));
			return false;
		}
		this.sql.exec("UPDATE sessions SET expires_at = ? WHERE id = ?", now + row.timeout_ms, session);
		return true;
	}

	private dropSession(session: number): void {
		const ephemerals = this.sql
			.exec<NodeRow>("SELECT * FROM nodes WHERE ephemeral_owner = ?", session)
			.toArray();
		if (ephemerals.length !== 0) {
			const zxid = this.bumpCounter("zxid");
			for (const node of ephemerals) {
				this.deleteNode(zxid, node);
			}
		}
		this.sql.exec("DELETE FROM watches WHERE session = ?", session);
		this.sql.exec("DELETE FROM events WHERE session = ?", session);
		this.sql.exec("DELETE FROM sessions WHERE id = ?", session);
	}

	private async scheduleExpiry(): Promise<void> {
		const next = this.sql
			.exec<{ next: number | null }>("SELECT MIN(expires_at) AS next FROM sessions")
			.one().next;
		if (next === null) {
			await this.ctx.storage.deleteAlarm();
		} else {
			await this.ctx.storage.setAlarm(next);
		}
	}

	private addWatch(session: number, path: string, kind: WatchKind): void {
		this.sql.exec("INSERT OR IGNORE INTO watches (session, path, kind) VALUES (?, ?, ?)", session, path, kind);
	}

	private fireWatches(path: string, kind: WatchKind, type: EventType): void {
		this.sql.exec(
			"INSERT INTO events (session, type, path) SELECT session, ?, path FROM watches WHERE path = ? AND kind = ?",
			type,
			path,
			kind,
		);
		this.sql.exec("DELETE FROM watches WHERE path = ? AND kind = ?", path, kind);
	}

	private getNode(path: string): NodeRow | undefined {
		return this.sql.exec<NodeRow>("SELECT * FROM nodes WHERE path = ?", path).toArray()[0];
	}

	private bumpCounter(name: "zxid" | "session"): number {
		return this.sql
			.exec<{ value: number }>("UPDATE counters SET value = value + 1 WHERE name = ? RETURNING value", name)
			.one().value;
	}
}

function migrate(sql: SqlStorage): void {
	sql.exec(`CREATE TABLE IF NOT EXISTS nodes (
		path TEXT PRIMARY KEY,
		parent TEXT,
		data BLOB NOT NULL,
		czxid INTEGER NOT NULL,
		mzxid INTEGER NOT NULL,
		ctime INTEGER NOT NULL,
		mtime INTEGER NOT NULL,
		version INTEGER NOT NULL,
		cversion INTEGER NOT NULL,
		aversion INTEGER NOT NULL,
		ephemeral_owner INTEGER NOT NULL,
		num_children INTEGER NOT NULL,
		pzxid INTEGER NOT NULL,
		seq_num INTEGER NOT NULL
	)`);
	sql.exec("CREATE INDEX IF NOT EXISTS nodes_by_parent ON nodes (parent)");
	sql.exec("CREATE INDEX IF NOT EXISTS nodes_by_owner ON nodes (ephemeral_owner) WHERE ephemeral_owner != 0");
	sql.exec(`CREATE TABLE IF NOT EXISTS sessions (
		id INTEGER PRIMARY KEY,
		timeout_ms INTEGER NOT NULL,
		expires_at INTEGER NOT NULL
	)`);
	sql.exec(`CREATE TABLE IF NOT EXISTS watches (
		session INTEGER NOT NULL,
		path TEXT NOT NULL,
		kind TEXT NOT NULL,
		PRIMARY KEY (session, path, kind)
	)`);
	sql.exec("CREATE INDEX IF NOT EXISTS watches_by_path ON watches (path, kind)");
	sql.exec(`CREATE TABLE IF NOT EXISTS events (
		seq INTEGER PRIMARY KEY,
		session INTEGER NOT NULL,
		type INTEGER NOT NULL,
		path TEXT NOT NULL
	)`);
	sql.exec("CREATE TABLE IF NOT EXISTS counters (name TEXT PRIMARY KEY, value INTEGER NOT NULL)");
	sql.exec("INSERT OR IGNORE INTO counters (name, value) VALUES ('zxid', 0), ('session', 0)");
	sql.exec(
		`INSERT OR IGNORE INTO nodes VALUES ('/', NULL, x'', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)`,
	);
}

function versionMatches(expected: number | undefined, actual: number): boolean {
	return expected === undefined || expected === -1 || expected === actual;
}

function failAll(ops: Op[], error: ErrorCode): MultiResult {
	return { error, results: ops.map(() => ({ error })) };
}

function toArrayBuffer(data: Uint8Array): ArrayBuffer {
	return data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength) as ArrayBuffer;
}

function toStat(node: NodeRow): Stat {
	return {
		czxid: node.czxid,
		mzxid: node.mzxid,
		ctime: node.ctime,
		mtime: node.mtime,
		version: node.version,
		cversion: node.cversion,
		aversion: node.aversion,
		ephemeralOwner: node.ephemeral_owner,
		dataLength: node.data.byteLength,
		numChildren: node.num_children,
		pzxid: node.pzxid,
	};
}
