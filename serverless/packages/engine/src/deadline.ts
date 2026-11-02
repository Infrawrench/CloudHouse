/// Deadlines on the waits of a query. A query holds the lock of its database while it reads parts, syncs with the
/// catalog and waits for its shards, and every later query of the engine, even `SELECT 1`, waits for that lock. A call
/// that never answers (a shard stuck on a wait of its own, a read that a fault of the network leaves hanging) held it
/// forever: the engine answered nothing until Cloudflare reset its object, which the alarms of `KEEP_WARM_SECONDS`
/// put off for up to a day. With a deadline, the call fails, the query fails with it and releases the lock, and an
/// exception that escapes the query gives the next one a new engine (`withDatabase`).

/// A shard reads about 4 MiB of parts, which takes well under a second warm and a few seconds cold.
export const SHARD_DEADLINE_MS = 60_000;
/// One range of a part, at most a few MiB, through the cache in front of R2.
export const READ_DEADLINE_MS = 30_000;
/// One call to the `Coordination` object of the catalog.
export const CATALOG_DEADLINE_MS = 30_000;

export class DeadlineExceeded extends Error {
	override name = "DeadlineExceeded";
}

/// The waits with a deadline of this isolate, and when each began, for `pendingWaits`.
const waits = new Map<symbol, { what: string; since: number }>();

/// `promise`, or a `DeadlineExceeded` if it has not settled within `ms`. The call itself goes on; only the wait ends.
export async function withDeadline<T>(promise: Promise<T>, ms: number, what: string): Promise<T> {
	let timer: ReturnType<typeof setTimeout> | undefined;
	const deadline = new Promise<never>((_, reject) => {
		timer = setTimeout(() => reject(new DeadlineExceeded(`${what} did not answer within ${ms / 1000} s`)), ms);
	});
	const key = Symbol();
	waits.set(key, { what, since: Date.now() });
	try {
		return await Promise.race([promise, deadline]);
	} finally {
		waits.delete(key);
		if (timer !== undefined) {
			clearTimeout(timer);
		}
	}
}

/// What the isolate waits on with a deadline, the longest first, for the diagnosis of an engine that is stuck: a wait
/// that is not here has no deadline.
export function pendingWaits(now: number = Date.now(), limit = 5): string[] {
	return [...waits.values()]
		.sort((a, b) => a.since - b.since)
		.slice(0, limit)
		.map(({ what, since }) => `${what} (${Math.round((now - since) / 1000)} s)`);
}
