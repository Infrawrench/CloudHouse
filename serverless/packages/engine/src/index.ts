import { Catalog } from "./catalog";
import type { MergeMessage } from "./database";
import type { Env } from "./env";
import { collectGarbage } from "./garbage";
import { type Answer, handleRequest } from "./http";
import { makeIo, runMerge, runQuery } from "./isolate";
import { askPool, poolSize } from "./pool";

/// The catalog lives in a Durable Object of this Worker, and so do the engines of the pool.
export { Coordination } from "coordination";
export { Engine, type QueryResult } from "./engine";
export type { Env } from "./env";
export { PartCache } from "./cache";
export { EngineHost } from "./host";
export { type Answer, handleRequest, type Tokens } from "./http";
export { askPool, poolSize } from "./pool";

/// The Worker of the engine answers the HTTP interface itself. In production the gateway (`packages/gateway`)
/// answers it, without the module of the engine in its bundle, and this Worker only hosts the Durable Objects.
export default {
	fetch(request, env, ctx): Promise<Response> {
		return handleRequest(request, env, (text, format, readonly, session) => answerQuery(env, ctx, text, format, readonly, session));
	},
	/// Every hour: the parts that the catalog does not have are deleted, and the tables with a TTL are merged, which
	/// deletes their expired rows.
	async scheduled(_controller, env): Promise<void> {
		const deleted = await collectGarbage(env);
		const snapshot = await new Catalog(() => env.COORDINATION.get(env.COORDINATION.idFromName("catalog"))).snapshot();
		const expiring = [...snapshot.tables.values()].filter((table) => table.ttl);
		await Promise.all(expiring.map((table) => env.MERGES.send({ database: table.database, table: table.table, ttl: true })));
		console.log(`Deleted ${deleted} parts that are not in the catalog, queued the TTL of ${expiring.length} tables`);
	},
	/// The merges, which commits queue for the tables with too many parts, and the Cron Trigger for the TTL.
	async queue(batch, env, ctx): Promise<void> {
		const io = makeIo(env, (promise) => ctx.waitUntil(promise));
		for (const message of batch.messages) {
			await runMerge(env, io, message.body);
			message.ack();
		}
	},
} satisfies ExportedHandler<Env, MergeMessage>;

/// Runs the query in the pool, or in this isolate if the pool is empty.
function answerQuery(env: Env, ctx: ExecutionContext, text: Uint8Array, format: string, readonly: boolean, session: string | null): Promise<Answer> {
	const pool = poolSize(env.ENGINE_POOL);
	if (pool === 0) {
		return runQuery(env, makeIo(env, (promise) => ctx.waitUntil(promise)), text, format, readonly);
	}
	return askPool(env.ENGINE_HOST, pool, text, format, readonly, session);
}
