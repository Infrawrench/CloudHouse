import { DurableObject } from "cloudflare:workers";
import type { MergeMessage, ShardAnswer, ShardRequest } from "./database";
import type { Env } from "./env";
import type { Answer, QueryRequest } from "./http";
import { makeIo, runMerge, runQuery, runShard, runWarm } from "./isolate";

/// An engine in a Durable Object. A Durable Object stays on one machine, in one isolate, so its engine stays warm
/// between queries, where the isolates that Cloudflare picks for requests start a new engine more often than not
/// when the traffic is low. The Worker spreads the queries over a pool of them (`engine-<n>`, `ENGINE_POOL`), and a
/// query that fans out runs its shards on others (`shard-<n>`). The engine is the same as in a Worker: it syncs with
/// the catalog before every query.
export class EngineHost extends DurableObject<Env> {
	async query(request: QueryRequest): Promise<Answer> {
		return runQuery(this.env, this.io(), request);
	}

	async shard(request: ShardRequest): Promise<ShardAnswer> {
		return runShard(this.env, this.io(), request);
	}

	/// Starts the engine of this object, if it has not, so that it is running when a query needs it: an engine that
	/// starts compiles the code that its first queries run, which takes a second or so.
	async warm(): Promise<void> {
		await runWarm(this.env);
	}

	/// The merges of a table run in one object (`merge-<database>.<table>`), one at a time, so that they do not choose
	/// the same parts and lose to each other.
	async merge(message: MergeMessage): Promise<number> {
		return runMerge(this.env, this.io(), message);
	}

	private io() {
		return makeIo(this.env, (promise) => this.ctx.waitUntil(promise));
	}
}
