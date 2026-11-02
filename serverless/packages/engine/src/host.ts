import { DurableObject } from "cloudflare:workers";
import type { EngineLoad, MergeMessage, ShardAnswer, ShardRequest } from "./database";
import type { Env } from "./env";
import type { Answer, QueryRequest } from "./http";
import { abandonEngine, engineLoad, makeIo, runMerge, runQuery, runShard, runWarm } from "./isolate";

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
	/// The calls of this object that run, and when each began, for `resetIfStuck`.
	private calls = new Map<symbol, { what: string; since: number }>();

	async query(request: QueryRequest): Promise<Answer> {
		this.resetIfStuck();
		this.ctx.waitUntil(this.used());
		return this.track("query", runQuery(this.env, this.io(), request));
	}

	/// How busy the engine is, for the gateway to choose between engines (`askPool`). It reads two numbers, so it answers
	/// at once, even while a query waits on I/O.
	busy(): EngineLoad {
		return engineLoad();
	}

	async shard(request: ShardRequest): Promise<ShardAnswer> {
		this.resetIfStuck();
		this.ctx.waitUntil(this.used());
		return this.track("shard", runShard(this.env, this.io(), request));
	}

	/// Starts the engine of this object, if it has not, so that it is running when a query needs it: an engine that
	/// starts compiles the code that its first queries run, which takes a second or so.
	async warm(): Promise<void> {
		this.resetIfStuck();
		await this.used();
		await runWarm(this.env);
	}

	override async alarm(): Promise<void> {
		this.resetIfStuck();
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
		/// Merges queue for the lock behind each other for long, by design: a merge that holds it too long is stuck.
		this.resetIfStuck();
		return runMerge(this.env, this.io(), message);
	}

	/// An engine whose lock has been held for `STUCK_MS` is stuck: every wait of a query has a deadline (`deadline.ts`),
	/// so it waits on something without one. The isolate gives the engine up, and the next work starts a new one. A call
	/// of this object that has run for `STUCK_MS` is stuck on it, or on something else: the object is reset, which fails
	/// its calls, so that their clients have an answer and the memory of the old engine goes. Each object checks itself
	/// at its alarm and on each call: objects that share an isolate share its engine, but not their calls.
	private resetIfStuck(now: number = Date.now()): void {
		const load = engineLoad(now);
		if (load.running_ms > STUCK_MS) {
			console.error(
				`The engine has been busy with ${load.step ?? "nothing"} for ${Math.round(load.running_ms / 1000)} s, ` +
					`with ${load.queued} waiting, and waits on ${load.waits?.length ? load.waits.join(", ") : "nothing with a deadline"}: ` +
					`it is given up for a new one.`,
			);
			abandonEngine();
		}
		const oldest = stuckCall(this.calls.values(), now);
		if (oldest !== undefined) {
			const message = `A ${oldest.what} of ${this.ctx.id.name ?? this.ctx.id.toString()} has run for ${Math.round((now - oldest.since) / 1000)} s: the object is reset.`;
			console.error(message);
			this.ctx.abort(message);
		}
	}

	private async track<T>(what: string, promise: Promise<T>): Promise<T> {
		const key = Symbol();
		this.calls.set(key, { what, since: Date.now() });
		try {
			return await promise;
		} finally {
			this.calls.delete(key);
		}
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

/// How long work may hold the lock of an engine, or a call of an object run, before it is taken to be stuck. Every wait
/// of a query has a deadline of a minute at most, so a query that runs for this long is not waiting on any of them.
export const STUCK_MS = 5 * 60 * 1000;

/// The longest running of `calls`, if it has run for more than `STUCK_MS`.
export function stuckCall(calls: Iterable<{ what: string; since: number }>, now: number = Date.now()): { what: string; since: number } | undefined {
	let oldest: { what: string; since: number } | undefined;
	for (const call of calls) {
		if (oldest === undefined || call.since < oldest.since) {
			oldest = call;
		}
	}
	return oldest !== undefined && now - oldest.since > STUCK_MS ? oldest : undefined;
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
