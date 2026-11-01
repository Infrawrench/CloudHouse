/// Moves the data segments of a WebAssembly module into a file of their own.
///
/// The static data of the engine (string literals, tables, type names) is about a sixth of the module, and
/// only the module counts against the size limit of a Worker: the data goes into a static asset instead, and
/// `Engine.create` copies it into the linear memory before the constructors of the engine run.
///
/// The data file is a sequence of segments, each an address and a size (little-endian `u64` and `u32`) followed
/// by the bytes, after a header of `MAGIC` and the number of segments (`u32`).

export const MAGIC = "CHDATA01";

const SECTION_DATA = 11;
const SECTION_DATA_COUNT = 12;
const OPCODE_I32_CONST = 0x41;
const OPCODE_I64_CONST = 0x42;
const OPCODE_END = 0x0b;

function readUnsigned(bytes, pos) {
	let result = 0n;
	let shift = 0n;
	for (;;) {
		const byte = bytes[pos++];
		result |= BigInt(byte & 0x7f) << shift;
		shift += 7n;
		if ((byte & 0x80) === 0) {
			return { value: result, pos };
		}
	}
}

function readSigned(bytes, pos) {
	let result = 0n;
	let shift = 0n;
	let byte;
	do {
		byte = bytes[pos++];
		result |= BigInt(byte & 0x7f) << shift;
		shift += 7n;
	} while (byte & 0x80);
	if (byte & 0x40) {
		result -= 1n << shift;
	}
	return { value: result, pos };
}

function readU32(bytes, pos) {
	const { value, pos: next } = readUnsigned(bytes, pos);
	return { value: Number(value), pos: next };
}

/// Parses the data section: every segment has to be active, in memory 0, at a constant address.
function parseSegments(bytes, start, end) {
	const segments = [];
	let { value: count, pos } = readU32(bytes, start);
	for (let i = 0; i < count; ++i) {
		let flags;
		({ value: flags, pos } = readU32(bytes, pos));
		if (flags === 2) {
			let memory;
			({ value: memory, pos } = readU32(bytes, pos));
			if (memory !== 0) {
				throw new Error(`Data segment ${i} is in memory ${memory}`);
			}
		} else if (flags !== 0) {
			throw new Error(`Data segment ${i} is not active (flags ${flags})`);
		}
		const opcode = bytes[pos++];
		if (opcode !== OPCODE_I64_CONST && opcode !== OPCODE_I32_CONST) {
			throw new Error(`The address of data segment ${i} is not a constant (opcode ${opcode})`);
		}
		let address;
		({ value: address, pos } = readSigned(bytes, pos));
		if (bytes[pos++] !== OPCODE_END) {
			throw new Error(`The address of data segment ${i} is not a single constant`);
		}
		let size;
		({ value: size, pos } = readU32(bytes, pos));
		segments.push({ address, data: bytes.subarray(pos, pos + size) });
		pos += size;
	}
	if (pos !== end) {
		throw new Error("The data section has trailing bytes");
	}
	return segments;
}

/// Returns the module without its data and data count sections, and the data file.
export function splitData(module) {
	const bytes = new Uint8Array(module);
	const kept = [bytes.subarray(0, 8)];
	let segments;
	let pos = 8;
	while (pos < bytes.length) {
		const section_start = pos;
		const id = bytes[pos++];
		let size;
		({ value: size, pos } = readU32(bytes, pos));
		const end = pos + size;
		if (id === SECTION_DATA) {
			segments = parseSegments(bytes, pos, end);
		} else if (id !== SECTION_DATA_COUNT) {
			kept.push(bytes.subarray(section_start, end));
		}
		pos = end;
	}
	if (segments === undefined) {
		throw new Error("The module has no data section");
	}

	const data_size = segments.reduce((sum, segment) => sum + 12 + segment.data.length, MAGIC.length + 4);
	const data = new Uint8Array(data_size);
	const view = new DataView(data.buffer);
	data.set(new TextEncoder().encode(MAGIC));
	view.setUint32(MAGIC.length, segments.length, true);
	let offset = MAGIC.length + 4;
	for (const segment of segments) {
		view.setBigUint64(offset, BigInt.asUintN(64, segment.address), true);
		view.setUint32(offset + 8, segment.data.length, true);
		data.set(segment.data, offset + 12);
		offset += 12 + segment.data.length;
	}

	const stripped = new Uint8Array(kept.reduce((sum, part) => sum + part.length, 0));
	let stripped_offset = 0;
	for (const part of kept) {
		stripped.set(part, stripped_offset);
		stripped_offset += part.length;
	}
	return { module: stripped, data, segments: segments.length };
}
