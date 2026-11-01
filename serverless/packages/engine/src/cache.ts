import { WorkerEntrypoint } from "cloudflare:workers";
import type { Env } from "./env";
import { parsePart, partKey } from "./parts";

/// Serves parts from R2, behind Workers Caching: a part never changes, so an engine that loads it reads it from the
/// cache of its data center, if another engine there has read it before. Only the engines call it, through
/// `ctx.exports`, as `https://parts/<name>`.
export class PartCache extends WorkerEntrypoint<Env> {
	override async fetch(request: Request): Promise<Response> {
		const name = parsePart(new URL(request.url).pathname.slice(1)).name;
		const object = await this.env.PARTS.get(partKey(name));
		if (object === null) {
			return new Response("Not found\n", { status: 404, headers: { "Cache-Control": "no-store" } });
		}
		return new Response(object.body, {
			headers: { "Cache-Control": "public, max-age=31536000, immutable", "Content-Length": String(object.size) },
		});
	}
}
