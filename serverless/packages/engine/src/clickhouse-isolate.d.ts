/// The Emscripten output of the `clickhouse-isolate` target, copied into `dist` by `npm run build`.

declare module "*/clickhouse-isolate.mjs" {
	/// Pointers and sizes are 64-bit (Memory64). Emscripten converts them to `number` for its own functions, such
	/// as `_malloc`, but the functions of the engine take and return them as `bigint`.
	export interface ClickHouseModule {
		HEAPU8: Uint8Array;
		_malloc(size: number): number;
		_free(ptr: number): void;
		_ch_isolate_init(): number;
		/// JSPI: the functions that run queries suspend while the host reads parts, and return promises.
		_ch_isolate_query(
			query: bigint,
			query_size: bigint,
			format: bigint,
			format_size: bigint,
			readonly: number,
			with_input: number,
			encoding: bigint,
			encoding_size: bigint,
		): Promise<number>;
		_ch_isolate_host_query(query: bigint, query_size: bigint): number;
		_ch_isolate_set_parts(spec: bigint, spec_size: bigint): number;
		_ch_isolate_changes(): number;
		_ch_isolate_clear_changes(): number;
		_ch_isolate_set_fanout(spec: bigint, spec_size: bigint): number;
		_ch_isolate_fanout_requests(): number;
		_ch_isolate_fanout_broadcast(index: bigint): number;
		_ch_isolate_add_fanout_result(spec: bigint, spec_size: bigint, data: bigint, data_size: bigint): number;
		_ch_isolate_clear_fanout(): number;
		_ch_isolate_shard_query(query: bigint, query_size: bigint, spec: bigint, spec_size: bigint, broadcast: bigint, broadcast_size: bigint): Promise<number>;
		_ch_isolate_result_data(): bigint;
		_ch_isolate_result_size(): bigint;
	}

	export interface ClickHouseModuleOptions {
		instantiateWasm(
			imports: WebAssembly.Imports,
			done: (instance: WebAssembly.Instance, module: WebAssembly.Module) => void,
		): WebAssembly.Exports;
		print?(line: string): void;
		printErr?(line: string): void;
		/// The imports of `host.js`.
		host: {
			readRange(part: string, offset: number, size: number): Promise<Uint8Array>;
			prefetch(part: string, offset: number, size: number): void;
			writePart(part: string, data: Uint8Array): Promise<void>;
			readInput(size: number): Promise<Uint8Array>;
			writeOutput(data: Uint8Array): void;
		};
	}

	export default function createClickHouse(options: ClickHouseModuleOptions): Promise<ClickHouseModule>;
}

declare module "*.wasm" {
	const module: WebAssembly.Module;
	export default module;
}
