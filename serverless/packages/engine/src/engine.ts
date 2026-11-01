import createClickHouse, { type ClickHouseModule } from "../dist/clickhouse-isolate.mjs";
import wasm from "../dist/clickhouse-isolate.wasm";
import { loadData } from "./data";
import type { InputReader } from "./input";

/// The outcome of a query: its output, or the message of the exception and its code.
export type QueryResult = { ok: true; body: Uint8Array } | { ok: false; code: number; message: string };

/// The code of the exception of a query on a table that fans out, before the shards have run, see `ObjectMergeTree.h`.
export const NOT_FOUND_EXPECTED_DATA_PART = 230;

/// A change that a query makes to the catalog, as `ch_isolate_changes` describes it (`CatalogChange`).
export type CatalogChange =
	| { kind: "create_table" | "alter_table"; database: string; table: string; create_query: string; ttl: boolean }
	| { kind: "create_database" | "drop_database"; database: string; table: "" }
	| { kind: "drop_table"; database: string; table: string }
	| { kind: "rename_table"; database: string; table: string; to_database: string; to_table: string; create_query: string; ttl: boolean }
	| {
			kind: "exchange_tables";
			database: string;
			table: string;
			to_database: string;
			to_table: string;
			/// The definition of the table that is now `to_database`.`to_table`.
			create_query: string;
			ttl: boolean;
			/// The definition of the table that is now `database`.`table`.
			to_create_query: string;
			to_ttl: boolean;
	  }
	| {
			kind: "replace_parts";
			database: string;
			table: string;
			added: string | null;
			rows?: number;
			removed: string[];
			/// The commands on partitions: parts to detach, and detached parts to attach and to delete.
			detached: string[];
			attached: string[];
			dropped_detached: string[];
			/// Parts of the table `shared_from` that the table gets too.
			shared: string[];
			shared_from: [string, string];
	  };

/// The query for the shards of a table that fans out, from `ch_isolate_fanout_requests`.
export interface FanOutRequest {
	database: string;
	table: string;
	query: string;
}

/// Reads and writes parts, for the engine while it runs a query: the engine announces the ranges that it is going to
/// read, then reads them, and puts each part that it writes in object storage as soon as it has written it.
export interface PartIo {
	readRange(part: string, offset: number, size: number): Promise<Uint8Array>;
	prefetch(part: string, offset: number, size: number): void;
	writePart(part: string, data: Uint8Array): Promise<void>;
}

/// An exception of a call of the engine other than a query.
export class EngineError extends Error {
	constructor(
		readonly code: number,
		message: string,
	) {
		super(message);
		this.name = "EngineError";
	}
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/// One instance of the engine. A query suspends the engine while the host reads parts (JSPI), and the isolate runs
/// other work meanwhile, so the calls of the engine take turns: none starts while another is suspended.
export class Engine {
	private queue: Promise<unknown> = Promise.resolve();
	/// How the query that runs reads parts.
	private reader: PartIo | undefined;
	/// The body of the request of the query that runs.
	private input: InputReader | undefined;

	private constructor(private readonly module: ClickHouseModule) {}

	/// `data` is the static data of the engine, the asset `clickhouse-isolate.data`.
	static async create(data: ArrayBuffer, log: (line: string) => void = console.error): Promise<Engine> {
		let engine: Engine | undefined;
		const module = await createClickHouse({
			/// A Worker cannot compile WebAssembly at run time; the module is compiled at deploy time and imported.
			instantiateWasm(imports, done) {
				const instance = new WebAssembly.Instance(wasm, imports);
				/// The runtime calls the constructors of the engine once `done` returns, and they read the data.
				loadData(exportedMemory(instance), data);
				done(instance, wasm);
				return instance.exports;
			},
			print: log,
			printErr: log,
			host: {
				readRange(part, offset, size) {
					return engine!.currentReader(part).readRange(part, offset, size);
				},
				prefetch(part, offset, size) {
					engine!.currentReader(part).prefetch(part, offset, size);
				},
				writePart(part, data) {
					return engine!.currentReader(part).writePart(part, data);
				},
				readInput(size) {
					if (engine!.input === undefined) {
						throw new Error("The engine reads the body of a query that has none");
					}
					return engine!.input.read(size);
				},
			},
		});
		engine = new Engine(module);
		engine.check(module._ch_isolate_init());
		return engine;
	}

	/// Runs `query`, whose text may be followed by the data of an `INSERT`, then by `input`, the body of the request
	/// compressed with `encoding`. `format` is the output format of a query without a `FORMAT` clause, by default
	/// `TabSeparated`. A `readonly` query runs with the setting `readonly` = 2: it can change settings but not write.
	/// The query reads and writes parts with `reader`.
	query(
		query: string | Uint8Array,
		format: string,
		readonly: boolean,
		reader: PartIo,
		input?: { reader: InputReader; encoding: string },
	): Promise<QueryResult> {
		return this.exclusive(async () => {
			this.input = input?.reader;
			const code = await this.callAsync(
				(q, f, e) => this.module._ch_isolate_query(q[0], q[1], f[0], f[1], readonly ? 1 : 0, input ? 1 : 0, e[0], e[1]),
				reader,
				query,
				format,
				input?.encoding ?? "",
			).finally(() => {
				this.input = undefined;
			});
			const body = this.takeResult();
			return code === 0 ? { ok: true, body } : { ok: false, code, message: decoder.decode(body) };
		});
	}

	/// Runs the query of a shard up to `WithMergeableState` on `parts` of the table, and returns its blocks. The other
	/// queries keep seeing all the visible parts of the table.
	shardQuery(query: string, database: string, table: string, parts: readonly string[], reader: PartIo): Promise<QueryResult> {
		const spec = [database, table, ...parts].join("\n") + "\n";
		return this.exclusive(async () => {
			const code = await this.callAsync((q, s) => this.module._ch_isolate_shard_query(q[0], q[1], s[0], s[1]), reader, query, spec);
			const body = this.takeResult();
			return code === 0 ? { ok: true, body } : { ok: false, code, message: decoder.decode(body) };
		});
	}

	/// Runs a query that brings the engine up to date with the catalog: it may create, alter and drop tables of
	/// `ObjectMergeTree` without making changes to the catalog.
	hostQuery(query: string): Promise<void> {
		return this.exclusive(async () => this.check(this.call((q) => this.module._ch_isolate_host_query(q[0], q[1]), query)));
	}

	setParts(database: string, table: string, parts: readonly string[], detached: readonly string[] = []): Promise<void> {
		const spec = [database, table, ...parts, ...detached.map((part) => `detached/${part}`)].join("\n") + "\n";
		return this.exclusive(async () => this.check(this.call((s) => this.module._ch_isolate_set_parts(s[0], s[1]), spec)));
	}

	/// The changes of the last query to the catalog.
	changes(): Promise<CatalogChange[]> {
		return this.exclusive(async () => {
			this.check(this.module._ch_isolate_changes());
			return lines(decoder.decode(this.takeResult())).map((line) => JSON.parse(line) as CatalogChange);
		});
	}

	/// After the changes are committed or given up on.
	clearChanges(): Promise<void> {
		return this.exclusive(async () => this.check(this.module._ch_isolate_clear_changes()));
	}

	/// A query on the table fans out to shards when the blocks that its filter needs are more than `bytes`; 0 for never.
	setFanOut(database: string, table: string, bytes: number): Promise<void> {
		const spec = `${database}\n${table}\n${bytes}\n`;
		return this.exclusive(async () => this.check(this.call((s) => this.module._ch_isolate_set_fanout(s[0], s[1]), spec)));
	}

	/// The queries for shards that the last query needs.
	fanOutRequests(): Promise<FanOutRequest[]> {
		return this.exclusive(async () => {
			this.check(this.module._ch_isolate_fanout_requests());
			return lines(decoder.decode(this.takeResult())).map((line) => JSON.parse(line) as FanOutRequest);
		});
	}

	addFanOutResult(request: FanOutRequest, data: Uint8Array): Promise<void> {
		const spec = `${request.database}\n${request.table}\n${request.query}`;
		return this.exclusive(async () =>
			this.check(this.call((s, d) => this.module._ch_isolate_add_fanout_result(s[0], s[1], d[0], d[1]), spec, data)),
		);
	}

	clearFanOut(): Promise<void> {
		return this.exclusive(async () => this.check(this.module._ch_isolate_clear_fanout()));
	}

	/// The size of the linear memory, which only grows.
	get memoryBytes(): number {
		return this.module.HEAPU8.length;
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

	/// Copies the arguments into the memory of the engine, as pairs of a pointer and a size, for the duration of `f`.
	private call<T>(f: (...args: [bigint, bigint][]) => T, ...args: (string | Uint8Array)[]): T {
		const pointers: number[] = [];
		try {
			const pairs = args.map((arg): [bigint, bigint] => {
				const bytes = typeof arg === "string" ? encoder.encode(arg) : arg;
				const ptr = this.module._malloc(Math.max(bytes.length, 1));
				pointers.push(ptr);
				this.module.HEAPU8.set(bytes, ptr);
				return [BigInt(ptr), BigInt(bytes.length)];
			});
			return f(...pairs);
		} finally {
			for (const ptr of pointers) {
				this.module._free(ptr);
			}
		}
	}

	private currentReader(part: string): PartIo {
		if (this.reader === undefined) {
			throw new Error(`The engine reads part ${part} outside of a query`);
		}
		return this.reader;
	}

	/// As `call`, for a function that may suspend, with `reader` as the reader of parts meanwhile.
	private async callAsync(f: (...args: [bigint, bigint][]) => Promise<number>, reader: PartIo, ...args: (string | Uint8Array)[]): Promise<number> {
		const pointers: number[] = [];
		this.reader = reader;
		try {
			const pairs = args.map((arg): [bigint, bigint] => {
				const bytes = typeof arg === "string" ? encoder.encode(arg) : arg;
				const ptr = this.module._malloc(Math.max(bytes.length, 1));
				pointers.push(ptr);
				this.module.HEAPU8.set(bytes, ptr);
				return [BigInt(ptr), BigInt(bytes.length)];
			});
			return await f(...pairs);
		} finally {
			this.reader = undefined;
			for (const ptr of pointers) {
				this.module._free(ptr);
			}
		}
	}

	private check(code: number): void {
		if (code !== 0) {
			throw new EngineError(code, decoder.decode(this.takeResult()));
		}
	}

	private takeResult(): Uint8Array {
		const data = Number(this.module._ch_isolate_result_data());
		const size = Number(this.module._ch_isolate_result_size());
		return this.module.HEAPU8.slice(data, data + size);
	}
}

function lines(text: string): string[] {
	return text.split("\n").filter((line) => line !== "");
}

function exportedMemory(instance: WebAssembly.Instance): WebAssembly.Memory {
	const memories = Object.values(instance.exports).filter((value) => value instanceof WebAssembly.Memory);
	if (memories.length !== 1) {
		throw new Error(`The engine exports ${memories.length} memories instead of one`);
	}
	return memories[0] as WebAssembly.Memory;
}
