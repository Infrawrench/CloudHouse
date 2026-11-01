/// Reads the parts of a query ahead of the engine. The engine announces the ranges that it is going to read
/// (`prefetch`), and they are read in that order, several at a time, up to `max_bytes` at once, while the engine works
/// on the ones before. A read of a range that was not announced goes straight to `read`.
export class ReadAhead {
	private readonly spans: Span[] = [];
	private inflight = 0;

	constructor(
		private readonly read: (part: string, offset: number, size: number) => Promise<Uint8Array>,
		/// The ranges share the 128 MB of the isolate with the engine, which may have 100 MiB.
		private readonly max_bytes = 6 * 1024 * 1024,
	) {}

	prefetch(part: string, offset: number, size: number): void {
		this.spans.push({ part, offset, size, data: undefined });
		this.pump();
	}

	async readRange(part: string, offset: number, size: number): Promise<Uint8Array> {
		const span = this.spans.find((candidate) => candidate.part === part && candidate.offset <= offset && offset + size <= candidate.offset + candidate.size);
		if (span === undefined) {
			return this.read(part, offset, size);
		}
		const data = await this.start(span);
		/// The reads of a span go in order, so the last one frees it.
		if (offset + size === span.offset + span.size) {
			this.spans.splice(this.spans.indexOf(span), 1);
			this.inflight -= span.size;
			this.pump();
		}
		return data.subarray(offset - span.offset, offset - span.offset + size);
	}

	private pump(): void {
		for (const span of this.spans) {
			if (span.data !== undefined) {
				continue;
			}
			if (this.inflight > 0 && this.inflight + span.size > this.max_bytes) {
				return;
			}
			void this.start(span).catch(() => {});
		}
	}

	private start(span: Span): Promise<Uint8Array> {
		if (span.data === undefined) {
			this.inflight += span.size;
			span.data = this.read(span.part, span.offset, span.size);
		}
		return span.data;
	}
}

interface Span {
	part: string;
	offset: number;
	size: number;
	data: Promise<Uint8Array> | undefined;
}
