import type { Coordination } from "coordination";
import type { MergeMessage } from "./database";
import type { EngineHost } from "./host";
import type { Tokens } from "./http";

export interface Env extends Tokens {
	/// The static assets of the Worker, which hold the static data of the engine (`clickhouse-isolate.data`).
	ASSETS: Fetcher;
	/// The catalog of the tables, in one `Coordination` object (`idFromName("catalog")`).
	COORDINATION: DurableObjectNamespace<Coordination>;
	/// The engines that the queries run in, when `ENGINE_POOL` is more than 0.
	ENGINE_HOST: DurableObjectNamespace<EngineHost>;
	/// How many `EngineHost` objects the queries are spread over. With 0, they run in the isolate of the request.
	ENGINE_POOL?: string;
	/// How often the engines of the pool and the shards wake up to stay running, see `EngineHost`. Without it, they
	/// do not.
	KEEP_WARM_SECONDS?: string;
	/// The parts of the tables, as `parts/<name>`.
	PARTS: R2Bucket;
	/// The merges to run in the background, see `MergeMessage`.
	MERGES: Queue<MergeMessage>;
	/// The limits of `Limits`, in bytes and parts, if not the defaults.
	SHARD_BYTES?: string;
	MAX_SHARDS?: string;
	MERGE_BYTES?: string;
	MERGE_PARTS?: string;
	MERGE_THRESHOLD?: string;
}
