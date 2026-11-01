/// The body of the request of a query, which the engine reads as it runs the query (`ch_host_read_input`).

/// Reads a body, a piece at a time.
export interface InputReader {
	/// At most `size` bytes of it, none at its end.
	read(size: number): Promise<Uint8Array>;
}

/// The body of a query, compressed with `encoding` if it is not empty. A query may run more than once, when the
/// catalog changes while it runs: `open` gives the body from its start, or `undefined` if it cannot, because it
/// streamed and the engine has read some of it.
export interface QueryInput {
	encoding: string;
	open(): InputReader | undefined;
}

/// The most of a body that holds the query itself, which is read at once so that the query can run again. The data
/// of an `INSERT` goes after a `query` parameter instead, and streams.
export const MAX_QUERY_BODY = 16 * 1024 * 1024;

/// A body that is all here.
export function bufferedInput(data: Uint8Array, encoding: string): QueryInput {
	return {
		encoding,
		open() {
			let offset = 0;
			return {
				async read(size) {
					const piece = data.subarray(offset, offset + size);
					offset += piece.length;
					return piece;
				},
			};
		},
	};
}

/// A body that streams. It can be opened again until the engine reads from it.
export function streamedInput(stream: ReadableStream<Uint8Array>, encoding: string): QueryInput {
	const reader = new ChunkReader(stream);
	return {
		encoding,
		open() {
			return reader.started ? undefined : reader;
		},
	};
}

/// Reads a whole body, of at most `limit` bytes. Returns `undefined` if it is longer.
export async function readAll(stream: ReadableStream<Uint8Array>, limit: number): Promise<Uint8Array | undefined> {
	const chunks: Uint8Array[] = [];
	let size = 0;
	const reader = stream.getReader();
	for (let chunk = await reader.read(); !chunk.done; chunk = await reader.read()) {
		size += chunk.value.length;
		if (size > limit) {
			await reader.cancel();
			return undefined;
		}
		chunks.push(chunk.value);
	}
	const result = new Uint8Array(size);
	let offset = 0;
	for (const chunk of chunks) {
		result.set(chunk, offset);
		offset += chunk.length;
	}
	return result;
}

/// Gives the chunks of a stream in pieces of at most the size that the engine asks for.
class ChunkReader implements InputReader {
	started = false;
	private readonly reader: ReadableStreamDefaultReader<Uint8Array>;
	private rest: Uint8Array = new Uint8Array();

	constructor(stream: ReadableStream<Uint8Array>) {
		this.reader = stream.getReader();
	}

	async read(size: number): Promise<Uint8Array> {
		this.started = true;
		while (this.rest.length === 0) {
			const { done, value } = await this.reader.read();
			if (done) {
				return new Uint8Array();
			}
			this.rest = value;
		}
		const piece = this.rest.subarray(0, size);
		this.rest = this.rest.subarray(piece.length);
		return piece;
	}
}
