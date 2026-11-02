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

/// `promise`, or a `DeadlineExceeded` if it has not settled within `ms`. The call itself goes on; only the wait ends.
export async function withDeadline<T>(promise: Promise<T>, ms: number, what: string): Promise<T> {
	let timer: ReturnType<typeof setTimeout> | undefined;
	const deadline = new Promise<never>((_, reject) => {
		timer = setTimeout(() => reject(new DeadlineExceeded(`${what} did not answer within ${ms / 1000} s`)), ms);
	});
	try {
		return await Promise.race([promise, deadline]);
	} finally {
		if (timer !== undefined) {
			clearTimeout(timer);
		}
	}
}
