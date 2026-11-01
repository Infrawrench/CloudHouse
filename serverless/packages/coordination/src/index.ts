import type { Env } from "./env";

export { Coordination } from "./coordination";
export type { Env } from "./env";
export * from "./protocol";

/// The service has no public HTTP surface: other Workers reach `Coordination` through a
/// Durable Object binding with `script_name` set to this Worker, and call it over RPC.
export default {
	async fetch(): Promise<Response> {
		return new Response("Not found", { status: 404 });
	},
} satisfies ExportedHandler<Env>;
