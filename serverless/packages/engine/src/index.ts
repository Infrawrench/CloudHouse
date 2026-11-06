import { Catalog } from "./catalog";
import type { MergeMessage } from "./database";
import type { Env } from "./env";
import { collectGarbage } from "./garbage";
import { type Answer, handleRequest, type QueryRequest } from "./http";
import { makeIo, runQuery } from "./isolate";
import { askPool, engineHost, poolSize } from "./pool";

/// The catalog lives in a Durable Object of this Worker, and so do the engines of the pool.
export { Coordination } from "coordination";
export { Engine, type QueryOutput, type QueryResult } from "./engine";
export type { Env } from "./env";
export { PartCache } from "./cache";
export { EngineHost } from "./host";
export { type Answer, handleRequest, type QueryRequest, type Tokens } from "./http";
export { askPool, poolSize } from "./pool";

/// The Worker of the engine answers the HTTP interface itself. In production the gateway (`packages/gateway`)
/// answers it, without the module of the engine in its bundle, and this Worker only hosts the Durable Objects.
export default {
	fetch(request, env, ctx): Promise<Response> {
		return handleRequest(request, env, (query, session) => answerQuery(env, ctx, query, session));
	},
	/// Every hour: the parts that the catalog does not have are deleted, and the tables with a TTL are merged, which
	/// deletes their expired rows.
	async scheduled(_controller, env): Promise<void> {
		const deleted = await collectGarbage(env);
		const catalog = new Catalog(() => env.COORDINATION.get(env.COORDINATION.idFromName("catalog")));
		const tokens = await catalog.expireDeduplication();
		const snapshot = await catalog.snapshot();
		const expiring = [...snapshot.tables.values()].filter((table) => table.ttl);
		await Promise.all(expiring.map((table) => env.MERGES.send({ database: table.database, table: table.table, ttl: true })));
		console.log(`Deleted ${deleted} parts that are not in the catalog and ${tokens} old deduplication tokens, queued the TTL of ${expiring.length} tables`);
	},
	/// The merges, which commits queue for the tables with too many parts, and the Cron Trigger for the TTL. Each table
	/// has an `EngineHost` for its merges, which runs them one at a time. A consumer runs anywhere, so the object of a
	/// table is created where `LOCATION_HINT` says, and has a name of its own for each hint: the objects that a consumer
	/// created elsewhere before stay there.
	async queue(batch, env, _ctx): Promise<void> {
		for (const message of batch.messages) {
			const { database, table } = message.body;
			const name = `merge-${env.LOCATION_HINT ? `${env.LOCATION_HINT}-` : ""}${JSON.stringify([database, table])}`;
			await engineHost(env.ENGINE_HOST, name, env.LOCATION_HINT).merge(message.body);
			message.ack();
		}
	},
} satisfies ExportedHandler<Env, MergeMessage>;

/// Runs the query in the pool, or in this isolate if the pool is empty.
function answerQuery(env: Env, ctx: ExecutionContext, request: QueryRequest, session: string | null): Promise<Answer> {
	const pool = poolSize(env.ENGINE_POOL);
	if (pool === 0) {
		return runQuery(env, makeIo(env, (promise) => ctx.waitUntil(promise)), request);
	}
	return askPool(env.ENGINE_HOST, pool, request, session, env.LOCATION_HINT);
}
