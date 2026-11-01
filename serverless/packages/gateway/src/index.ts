import type { EngineHost } from "../../engine/src/host";
import { handleRequest, type Tokens } from "engine/http";
import { askPool, poolSize } from "engine/pool";

export interface Env extends Tokens {
	/// The engines, in the Worker `clickhouse-engine-core`.
	ENGINES: DurableObjectNamespace<EngineHost>;
	/// How many of them the queries are spread over.
	ENGINE_POOL: string;
}

/// The HTTP interface of `packages/engine`, answered by the engines of the pool.
export default {
	fetch(request, env): Promise<Response> {
		const pool = poolSize(env.ENGINE_POOL);
		if (pool === 0) {
			throw new Error("The gateway needs ENGINE_POOL of at least one engine");
		}
		return handleRequest(request, env, (text, format, readonly, session) => askPool(env.ENGINES, pool, text, format, readonly, session));
	},
} satisfies ExportedHandler<Env>;
