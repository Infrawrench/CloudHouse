import { WorkerEntrypoint } from "cloudflare:workers";
import type { Env } from "./env";
import { parsePart, partKey } from "./parts";

/// Serves ranges of parts from R2, behind Workers Caching: a part never changes, so an engine reads a range from the
/// cache of its data center if another engine there has read it before. Only the engines call it, through
/// `ctx.exports`, as `https://parts/<name>/<offset>/<length>`.
///
/// The cache keeps the ranges that engines read, not parts: on a miss, the range is read from R2, not the part, and
/// the ranges that queries read again, such as the indexes of parts, are hits.
export class PartCache extends WorkerEntrypoint<Env> {
	override async fetch(request: Request): Promise<Response> {
		const [encoded, offset_text, length_text] = new URL(request.url).pathname.slice(1).split("/");
		const part = parsePart(encoded ?? "");
		const offset = Number(offset_text);
		const length = Number(length_text);
		if (!Number.isInteger(offset) || !Number.isInteger(length) || offset < 0 || length <= 0 || offset + length > part.bytes) {
			return new Response("No such range\n", { status: 400, headers: { "Cache-Control": "no-store" } });
		}
		const object = await this.env.PARTS.get(partKey(part.name), { range: { offset, length } });
		if (object === null) {
			return new Response("Not found\n", { status: 404, headers: { "Cache-Control": "no-store" } });
		}
		return new Response(object.body, {
			headers: { "Cache-Control": "public, max-age=31536000, immutable", "Content-Length": String(length) },
		});
	}
}
