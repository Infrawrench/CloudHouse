/// The static data of the engine, which `scripts/split-data.mjs` moves out of the module into a static asset.

const MAGIC = "CHDATA01";

/// Copies the data segments into the linear memory. It has to run before anything in the module does, in place of
/// the data section that the module no longer has.
export function loadData(memory: WebAssembly.Memory, data: ArrayBuffer): void {
	const bytes = new Uint8Array(data);
	const view = new DataView(data);
	if (new TextDecoder().decode(bytes.subarray(0, MAGIC.length)) !== MAGIC) {
		throw new Error("The data of the engine is not in the expected format");
	}
	const heap = new Uint8Array(memory.buffer);
	const count = view.getUint32(MAGIC.length, true);
	let offset = MAGIC.length + 4;
	for (let i = 0; i < count; ++i) {
		const address = Number(view.getBigUint64(offset, true));
		const size = view.getUint32(offset + 8, true);
		offset += 12;
		if (address + size > heap.length) {
			throw new Error(`Data segment ${i} does not fit in the initial memory of the engine`);
		}
		heap.set(bytes.subarray(offset, offset + size), address);
		offset += size;
	}
	if (offset !== bytes.length) {
		throw new Error("The data of the engine has trailing bytes");
	}
}
