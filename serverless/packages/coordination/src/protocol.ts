/// The coordination protocol: the operations the engine needs from Keeper, with the same
/// semantics and numeric codes as `Coordination::IKeeper` in `src/Common/ZooKeeper/IKeeper.h`,
/// so the engine-side client is a thin translation layer.

/// Mirrors `Coordination::Error`.
export const ErrorCode = {
	ZOK: 0,
	ZRUNTIMEINCONSISTENCY: -2,
	ZBADARGUMENTS: -8,
	ZNONODE: -101,
	ZBADVERSION: -103,
	ZNOCHILDRENFOREPHEMERALS: -108,
	ZNODEEXISTS: -110,
	ZNOTEMPTY: -111,
	ZSESSIONEXPIRED: -112,
} as const;
export type ErrorCode = (typeof ErrorCode)[keyof typeof ErrorCode];

/// Mirrors `Coordination::Event`.
export const EventType = {
	CREATED: 1,
	DELETED: 2,
	CHANGED: 3,
	CHILD: 4,
} as const;
export type EventType = (typeof EventType)[keyof typeof EventType];

/// Mirrors `Coordination::ListRequestType`.
export const ListFilter = {
	ALL: 0,
	PERSISTENT_ONLY: 1,
	EPHEMERAL_ONLY: 2,
} as const;
export type ListFilter = (typeof ListFilter)[keyof typeof ListFilter];

/// Mirrors `Coordination::Stat`.
export interface Stat {
	czxid: number;
	mzxid: number;
	ctime: number;
	mtime: number;
	version: number;
	cversion: number;
	aversion: number;
	ephemeralOwner: number;
	dataLength: number;
	numChildren: number;
	pzxid: number;
}

/// A version of -1 matches any version, as in ZooKeeper.
export type WriteOp =
	| { type: "create"; path: string; data?: Uint8Array; ephemeral?: boolean; sequential?: boolean }
	| { type: "remove"; path: string; version?: number }
	| { type: "set"; path: string; data: Uint8Array; version?: number }
	| { type: "check"; path: string; version?: number; notExists?: boolean };

export type ReadOp =
	| { type: "get"; path: string }
	| { type: "exists"; path: string }
	| { type: "list"; path: string; filter?: ListFilter };

export type Op = WriteOp | ReadOp;

export interface OpResult {
	error: ErrorCode;
	/// `create`: the actual path, with the sequential suffix if any.
	pathCreated?: string;
	/// `get`, `exists`, `set`, `list`: the stat of the node after the operation.
	stat?: Stat;
	/// `get`.
	data?: Uint8Array;
	/// `list`: child names (not full paths), sorted.
	children?: string[];
}

export interface MultiResult {
	/// The first error, or `ZOK`.
	error: ErrorCode;
	/// One per op. When a write transaction fails, the ops before the failing one keep their
	/// results (and are rolled back), the failing one has its error, and the ops after it have
	/// `ZRUNTIMEINCONSISTENCY` - the same convention as `TestKeeper` and ZooKeeper.
	results: OpResult[];
}

/// `tree`: the root's stat, and every node under it, sorted by path.
export interface TreeResult {
	error: ErrorCode;
	stat?: Stat;
	nodes: { path: string; data: Uint8Array; version: number; ctime: number }[];
}

export interface WatchEvent {
	/// Monotonic per coordination object; pass the last one seen to `pollEvents` to acknowledge.
	seq: number;
	type: EventType;
	path: string;
}

export function isReadOp(op: Op): op is ReadOp {
	return op.type === "get" || op.type === "exists" || op.type === "list";
}
