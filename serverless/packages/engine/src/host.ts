import { DurableObject } from "cloudflare:workers";
import type { ShardAnswer, ShardRequest } from "./database";
import type { Env } from "./env";
import type { Answer } from "./http";
import { makeIo, runQuery, runShard } from "./isolate";

/// An engine in a Durable Object. A Durable Object stays on one machine, in one isolate, so its engine stays warm
/// between queries, where the isolates that Cloudflare picks for requests start a new engine more often than not
/// when the traffic is low. The Worker spreads the queries over a pool of them (`engine-<n>`, `ENGINE_POOL`), and a
/// query that fans out runs its shards on others (`shard-<n>`). The engine is the same as in a Worker: it syncs with
/// the catalog before every query.
export class EngineHost extends DurableObject<Env> {
	async query(text: Uint8Array, format: string, readonly: boolean): Promise<Answer> {
		return runQuery(this.env, this.io(), text, format, readonly);
	}

	async shard(request: ShardRequest): Promise<ShardAnswer> {
		return runShard(this.env, this.io(), request);
	}

	private io() {
		return makeIo(this.env, (promise) => this.ctx.waitUntil(promise));
	}
}
