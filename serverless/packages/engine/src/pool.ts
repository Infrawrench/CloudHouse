import type { EngineHost } from "./host";
import type { Answer } from "./http";

/// `ENGINE_POOL`: how many `EngineHost` objects the queries are spread over.
export function poolSize(value: string | undefined): number {
	const pool = Number(value ?? "0");
	if (!Number.isInteger(pool) || pool < 0) {
		throw new Error(`ENGINE_POOL is not a number of engines: ${value}`);
	}
	return pool;
}

/// Runs the query in one of the `EngineHost` objects of the pool: the one of the session, or one picked at random.
export async function askPool(
	hosts: DurableObjectNamespace<EngineHost>,
	pool: number,
	text: Uint8Array,
	format: string,
	readonly: boolean,
	session: string | null = null,
): Promise<Answer> {
	const index = session === null ? Math.floor(Math.random() * pool) : hash(session) % pool;
	const host = hosts.get(hosts.idFromName(`engine-${index}`));
	const start = Date.now();
	const answer = await host.query(text, format, readonly);
	return { ...answer, timing: `host;dur=${Date.now() - start}, ${answer.timing}` };
}

/// FNV-1a, to spread the sessions over the pool.
function hash(text: string): number {
	let value = 0x811c9dc5;
	for (const byte of new TextEncoder().encode(text)) {
		value = Math.imul(value ^ byte, 0x01000193) >>> 0;
	}
	return value;
}
