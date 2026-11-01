import { Catalog } from "./catalog";
import type { Env } from "./env";

/// How old a part has to be before it can be deleted for not being in the catalog: a query uploads its parts before
/// it commits them, which takes seconds, not an hour.
const GRACE_MS = 60 * 60 * 1000;

/// Deletes the parts in R2 that no table of the catalog has: the parts of a commit that failed and whose deletion
/// did not run, for example because the isolate went away. Returns how many it deleted.
export async function collectGarbage(env: Env, now: number = Date.now()): Promise<number> {
	const catalog = new Catalog(() => env.COORDINATION.get(env.COORDINATION.idFromName("catalog")));
	/// The snapshot is taken before the listing, so a part committed in between is newer than the grace period.
	const snapshot = await catalog.snapshot();
	const live = new Set([...snapshot.tables.values()].flatMap((table) => [...table.parts, ...table.detached].map((part) => `parts/${part}`)));

	let deleted = 0;
	let cursor: string | undefined;
	do {
		const listing = await env.PARTS.list({ prefix: "parts/", cursor });
		const garbage = listing.objects
			.filter((object) => !live.has(object.key) && now - object.uploaded.getTime() > GRACE_MS)
			.map((object) => object.key);
		if (garbage.length > 0) {
			await env.PARTS.delete(garbage);
			deleted += garbage.length;
		}
		cursor = listing.truncated ? listing.cursor : undefined;
	} while (cursor !== undefined);
	return deleted;
}
