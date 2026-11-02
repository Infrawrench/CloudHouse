import { DurableObject } from "cloudflare:workers";
import type { EngineLoad, MergeMessage, ShardAnswer, ShardRequest } from "./database";
import type { Env } from "./env";
import type { Answer, QueryRequest } from "./http";
import { engineLoad, makeIo, runMerge, runQuery, runShard, runWarm } from "./isolate";

/// An engine in a Durable Object. A Durable Object stays on one machine, in one isolate, so its engine stays warm
/// between queries, where the isolates that Cloudflare picks for requests start a new engine more often than not
/// when the traffic is low. The Worker spreads the queries over a pool of them (`engine-<n>`, `ENGINE_POOL`), and a
/// query that fans out runs its shards on others (`shard-<n>`). The engine is the same as in a Worker: it syncs with
/// the catalog before every query.
///
/// With `KEEP_WARM_SECONDS`, an engine of the pool or a shard keeps itself running: Cloudflare evicts an object that
/// has had nothing to do for a minute or two, and the engine that the next query starts takes a second or two to
/// compile its code and attach the tables. So the object has an alarm every `KEEP_WARM_SECONDS`, which syncs the
/// engine with the catalog, until it has had no query for `KEEP_WARM_FOR_MS`.
export class EngineHost extends DurableObject<Env> {
	/// When this object last saved the time of its last use, which it does at most every `LAST_USE_EVERY_MS`.
	private last_use_saved = 0;
	private alarm_checked = false;

	async query(request: QueryRequest): Promise<Answer> {
		this.ctx.waitUntil(this.used());
		return runQuery(this.env, this.io(), request);
	}

	/// How busy the engine is, for the gateway to choose between engines (`askPool`). It reads two numbers, so it answers
	/// at once, even while a query waits on I/O.
	busy(): EngineLoad {
		return engineLoad();
	}

	async shard(request: ShardRequest): Promise<ShardAnswer> {
		this.ctx.waitUntil(this.used());
		return runShard(this.env, this.io(), request);
	}

	/// Starts the engine of this object, if it has not, so that it is running when a query needs it: an engine that
	/// starts compiles the code that its first queries run, which takes a second or so.
	async warm(): Promise<void> {
		await this.used();
		await runWarm(this.env);
	}

	override async alarm(): Promise<void> {
		const interval = keepWarmInterval(this.env);
		const last_use = (await this.ctx.storage.get<number>(LAST_USE_KEY)) ?? 0;
		if (interval === undefined || Date.now() - last_use > KEEP_WARM_FOR_MS) {
			return;
		}
		/// The next alarm is set first, so that an engine that fails to warm keeps trying.
		await this.ctx.storage.setAlarm(Date.now() + interval);
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

	/// Records a use of the object, which keeps it warm. A query does not wait for it.
	private async used(): Promise<void> {
		const interval = keepWarmInterval(this.env);
		if (interval === undefined) {
			return;
		}
		const now = Date.now();
		if (now - this.last_use_saved > LAST_USE_EVERY_MS) {
			this.last_use_saved = now;
			await this.ctx.storage.put(LAST_USE_KEY, now);
		}
		if (!this.alarm_checked) {
			this.alarm_checked = true;
			if ((await this.ctx.storage.getAlarm()) === null) {
				await this.ctx.storage.setAlarm(now + interval);
			}
		}
	}
}

/// How long an object keeps itself warm after its last query.
const KEEP_WARM_FOR_MS = 24 * 60 * 60 * 1000;
const LAST_USE_EVERY_MS = 60 * 60 * 1000;
const LAST_USE_KEY = "last_use";

/// `KEEP_WARM_SECONDS` in milliseconds, or `undefined` if the objects do not keep warm.
function keepWarmInterval(env: Env): number | undefined {
	if (env.KEEP_WARM_SECONDS === undefined) {
		return undefined;
	}
	const seconds = Number(env.KEEP_WARM_SECONDS);
	if (!Number.isInteger(seconds) || seconds <= 0) {
		throw new Error(`KEEP_WARM_SECONDS is not a number of seconds: ${env.KEEP_WARM_SECONDS}`);
	}
	return seconds * 1000;
}
