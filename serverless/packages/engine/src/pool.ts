import type { EngineLoad } from "./database";
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

/// When each engine of the pool last answered with a new engine, or seemed stalled (`stalled`), in this isolate.
const engine_cold_at = new Map<number, number>();

/// An engine whose lock has been held this long runs a long query, or is stuck (`EngineHost.resetIfStuck`).
const STALLED_MS = 30_000;

/// Whether a query sent to the engine would wait long: it is starting or running something long, or it did not say.
/// An engine that is stuck never comes back cold, so the gateway passes it over for this instead: otherwise the
/// engines that Cloudflare replaces would be passed over, and the stuck ones would be left to choose from.
export function stalled(load: EngineLoad | undefined): boolean {
	return load === undefined || load.running_ms > STALLED_MS;
}

/// How long the gateway waits for an engine to say how busy it is. An engine that does not answer in time is taken to
/// be busy: its isolate is computing, or stuck.
const LOAD_DEADLINE_MS = 250;

/// Two different engines among `choices`, at random; one if there is only one.
export function pickTwo(choices: readonly number[], random: () => number = Math.random): number[] {
	const first = Math.floor(random() * choices.length);
	if (choices.length < 2) {
		return choices.slice(first, first + 1);
	}
	const second = (first + 1 + Math.floor(random() * (choices.length - 1))) % choices.length;
	return [choices[first]!, choices[second]!];
}

/// The less busy of the candidates: the fewest queries waiting, then the shortest running one. `undefined`, an engine
/// that did not say, comes last. Ties go to the first.
export function leastBusy(candidates: readonly number[], loads: readonly (EngineLoad | undefined)[]): number {
	const score = (load: EngineLoad | undefined) => (load === undefined ? Infinity : load.queued * 60_000 + load.running_ms);
	let best = 0;
	for (let i = 1; i < candidates.length; i++) {
		if (score(loads[i]) < score(loads[best])) {
			best = i;
		}
	}
	return candidates[best]!;
}

async function loadOf(host: DurableObjectStub<EngineHost>): Promise<EngineLoad | undefined> {
	let timer: ReturnType<typeof setTimeout> | undefined;
	try {
		const timeout = new Promise<undefined>((resolve) => (timer = setTimeout(() => resolve(undefined), LOAD_DEADLINE_MS)));
		return await Promise.race([host.busy() as Promise<EngineLoad>, timeout]);
	} catch {
		return undefined;
	} finally {
		if (timer !== undefined) {
			clearTimeout(timer);
		}
	}
}

/// Runs the query in one of the `EngineHost` objects of the pool: the one of the session, or else the less busy of two
/// picked at random among those that have answered warm lately ("the power of two choices"). An engine runs one query
/// at a time, so a query sent at random to the engine of a long query waited for it, and one sent to a stuck engine
/// waited forever. Asking two costs a round trip to each, in parallel, and the query goes where it starts soonest.
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
	const start = Date.now();
	let index: number;
	if (session !== null) {
		index = hash(session) % pool;
	} else {
		/// If both engines are stalled, two others, if there are: the query goes to the less busy of all that answered.
		const asked: number[] = [];
		const loads: (EngineLoad | undefined)[] = [];
		for (let round = 0; round < 2; ++round) {
			const candidates = pickTwo(choices.filter((choice) => !asked.includes(choice)));
			if (candidates.length === 0) {
				break;
			}
			const answers = await Promise.all(candidates.map((candidate) => loadOf(engineHost(hosts, `engine-${candidate}`, location_hint))));
			asked.push(...candidates);
			loads.push(...answers);
			candidates.forEach((candidate, i) => stalled(answers[i]) && engine_cold_at.set(candidate, Date.now()));
			if (!answers.every(stalled)) {
				break;
			}
		}
		index = leastBusy(asked, loads);
	}
	const host = engineHost(hosts, `engine-${index}`, location_hint);
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
