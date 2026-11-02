import type { EngineHost } from "./host";
import type { Answer, QueryRequest } from "./http";

/// `ENGINE_POOL`: how many `EngineHost` objects the queries are spread over.
export function poolSize(value: string | undefined): number {
	const pool = Number(value ?? "0");
	if (!Number.isInteger(pool) || pool < 0) {
		throw new Error(`ENGINE_POOL is not a number of engines: ${value}`);
	}
	return pool;
}

/// The `EngineHost` object `name`, created, if it does not exist, where `location_hint` says (`LOCATION_HINT`).
export function engineHost(hosts: DurableObjectNamespace<EngineHost>, name: string, location_hint?: string): DurableObjectStub<EngineHost> {
	const id = hosts.idFromName(name);
	return location_hint ? hosts.get(id, { locationHint: location_hint as DurableObjectLocationHint }) : hosts.get(id);
}

/// Runs the query in one of the `EngineHost` objects of the pool: the one of the session, or one picked at random.
export async function askPool(
	hosts: DurableObjectNamespace<EngineHost>,
	pool: number,
	request: QueryRequest,
	session: string | null = null,
	location_hint?: string,
): Promise<Answer> {
	const index = session === null ? Math.floor(Math.random() * pool) : hash(session) % pool;
	const host = engineHost(hosts, `engine-${index}`, location_hint);
	const start = Date.now();
	const answer = await host.query(request);
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
