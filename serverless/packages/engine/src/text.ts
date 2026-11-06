/// Text functions that the engine runs in the host, as V8 has the Unicode tables that it does not (`HostFunctions.cpp`).

const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
const lenient = new TextDecoder("utf-8", { fatal: false, ignoreBOM: true });
const encoder = new TextEncoder();

/// By their numbers in `HostFunctions.cpp`.
const OPERATIONS: ((text: string) => string)[] = [
	(text) => text.toLowerCase(),
	(text) => text.toUpperCase(),
	(text) => text.normalize("NFC"),
	(text) => text.normalize("NFD"),
	(text) => text.normalize("NFKC"),
	(text) => text.normalize("NFKD"),
];

/// Runs an operation on each string of a block, given as their bytes and where each ends. A string that is not valid
/// UTF-8 stays as it is. The block is in the memory of the engine, which does not move during this call.
export function transformStrings(operation: number, chars: Uint8Array, offsets: BigUint64Array): { chars: Uint8Array; offsets: BigUint64Array } {
	const transform = OPERATIONS[operation];
	if (transform === undefined) {
		throw new Error(`No operation on strings number ${operation}`);
	}
	let out = new Uint8Array(Math.max(chars.length + (chars.length >> 2), 64));
	const out_offsets = new BigUint64Array(offsets.length);
	let position = 0;
	let start = 0;
	for (let row = 0; row < offsets.length; ++row) {
		const end = Number(offsets[row]);
		const bytes = chars.subarray(start, end);
		start = end;
		let text: string | undefined;
		try {
			text = transform(decoder.decode(bytes));
		} catch {
			text = undefined;
		}
		/// UTF-8 takes at most three bytes for each code unit of a string in JavaScript.
		const needed = text === undefined ? bytes.length : text.length * 3;
		if (position + needed > out.length) {
			const grown = new Uint8Array(Math.max(out.length * 2, position + needed));
			grown.set(out.subarray(0, position));
			out = grown;
		}
		if (text === undefined) {
			out.set(bytes, position);
			position += bytes.length;
		} else {
			position += encoder.encodeInto(text, out.subarray(position)).written;
		}
		out_offsets[row] = BigInt(position);
	}
	return { chars: out.subarray(0, position), offsets: out_offsets };
}

const collators: Intl.Collator[] = [];
const collator_numbers = new Map<string, number>();

/// The number of the collator of a locale, such as `de` or `tr-u-kn-true`.
export function openCollator(locale: string): number {
	const known = collator_numbers.get(locale);
	if (known !== undefined) {
		return known;
	}
	if (Intl.Collator.supportedLocalesOf([locale]).length === 0) {
		throw new Error(`Unsupported collation locale: ${locale}`);
	}
	collators.push(new Intl.Collator(locale));
	collator_numbers.set(locale, collators.length - 1);
	return collators.length - 1;
}

export function compareCollated(collator: number, a: Uint8Array, b: Uint8Array): number {
	return collators[collator]!.compare(lenient.decode(a), lenient.decode(b));
}
