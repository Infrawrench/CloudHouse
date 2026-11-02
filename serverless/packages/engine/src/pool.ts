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

/// How long an object that came back with a new engine is passed over. Cloudflare replaces the isolates of some objects
/// after every request, for hours, while the others keep theirs: an engine that starts costs a second or more.
const COLD_FOR_MS = 10 * 60 * 1000;

/// The objects `0` to `count - 1`, those that have not come back cold for `COLD_FOR_MS` first, each group in order:
/// the queries go to the same objects, which so stay warm.
export function preferWarm(count: number, cold_at: ReadonlyMap<number, number>, now: number = Date.now()): number[] {
	const cold = (index: number) => recentlyCold(cold_at, index, now);
	return Array.from({ length: count }, (_, index) => index).sort((a, b) => Number(cold(a)) - Number(cold(b)));
}

export function recentlyCold(cold_at: ReadonlyMap<number, number>, index: number, now: number = Date.now()): boolean {
	return now - (cold_at.get(index) ?? -Infinity) < COLD_FOR_MS;
}

/// When each engine of the pool last answered with a new engine, in this isolate.
const engine_cold_at = new Map<number, number>();

/// Runs the query in one of the `EngineHost` objects of the pool: the one of the session, or one picked at random
/// among those that have answered warm lately.
export async function askPool(
	hosts: DurableObjectNamespace<EngineHost>,
	pool: number,
	request: QueryRequest,
	session: string | null = null,
	location_hint?: string,
): Promise<Answer> {
	const all = Array.from({ length: pool }, (_, index) => index);
	const warm = all.filter((index) => !recentlyCold(engine_cold_at, index));
	const choices = warm.length > 0 ? warm : all;
	const index = session === null ? choices[Math.floor(Math.random() * choices.length)]! : hash(session) % pool;
	const host = engineHost(hosts, `engine-${index}`, location_hint);
	const start = Date.now();
	let answer: Answer;
	try {
		answer = await host.query(request);
	} catch (error) {
		engine_cold_at.set(index, Date.now());
		throw error;
	}
	if (answer.timing.includes("startup;")) {
		engine_cold_at.set(index, Date.now());
	}
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
