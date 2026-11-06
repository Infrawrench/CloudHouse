import { SELF, createExecutionContext, createMessageBatch, getQueueResult, runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { env, exports } from "cloudflare:workers";
import worker from "../src/index";
import { Catalog } from "../src/catalog";
import { DeadlineExceeded, pendingWaits, withDeadline } from "../src/deadline";
import { STUCK_MS, stuckCall } from "../src/host";
import { answerBody } from "../src/http";
import { leastBusy, pickTwo, preferWarm, stalled } from "../src/pool";
import { describe, expect, it } from "vitest";

const authorization = { Authorization: "Bearer test-token" };

async function query(query: string, init: { body?: string; format?: string } = {}) {
	const url = new URL("http://engine/");
	if (init.body === undefined) {
		const response = await SELF.fetch(withFormat(url, init.format), { method: "POST", body: query, headers: authorization });
		return { status: response.status, text: await response.text(), code: response.headers.get("X-ClickHouse-Exception-Code") };
	}
	url.searchParams.set("query", query);
	const response = await SELF.fetch(withFormat(url, init.format), { method: "POST", body: init.body, headers: authorization });
	return { status: response.status, text: await response.text(), code: response.headers.get("X-ClickHouse-Exception-Code") };
}

function withFormat(url: URL, format: string | undefined) {
	if (format !== undefined) {
		url.searchParams.set("default_format", format);
	}
	return url;
}

describe("engine", () => {
	it("answers ping", async () => {
		const response = await SELF.fetch("http://engine/ping");
		expect(await response.text()).toBe("Ok.\n");
	});

	it("needs the token", async () => {
		const without = await SELF.fetch("http://engine/?query=SELECT%201");
		expect(without.status).toBe(401);
		const wrong = await SELF.fetch("http://engine/?query=SELECT%201", { headers: { Authorization: "Bearer wrong" } });
		expect(wrong.status).toBe(401);
	});

	it("lets the read-only token only read", async () => {
		const read = (query: string) =>
			SELF.fetch("http://engine/", { method: "POST", body: query, headers: { Authorization: "Bearer read-token" } });
		expect(await (await read("SELECT 1")).text()).toBe("1\n");
		for (const query of ["CREATE TABLE r (a UInt8) ENGINE = ObjectMergeTree", "CREATE TABLE r (a UInt8) ENGINE = Memory"]) {
			const response = await read(query);
			expect(response.status).toBe(500);
			expect(response.headers.get("X-ClickHouse-Exception-Code")).toBe("164");
		}
		/// It can tighten its limits, within the constraints of the profile.
		expect(await (await read("SELECT number FROM numbers(3) SETTINGS max_result_rows = 10, max_execution_time = 5")).text()).toBe("0\n1\n2\n");
		const threads = await read("SELECT 1 SETTINGS max_threads = 4");
		expect(threads.headers.get("X-ClickHouse-Exception-Code")).toBe("452");
	});

	it("streams the data of an insert, compressed or not", async () => {
		await query("CREATE TABLE streamed (x UInt32, y String) ORDER BY x");
		const rows = Array.from({ length: 200000 }, (_, i) => JSON.stringify({ x: i, y: "y".repeat(80) })).join("\n");
		const url = `http://engine/?query=${encodeURIComponent("INSERT INTO streamed FORMAT JSONEachRow")}`;
		/// A body that streams, without a length.
		const body = new Blob([rows]).stream();
		expect((await SELF.fetch(url, { method: "POST", body, headers: authorization, duplex: "half" } as RequestInit)).status).toBe(200);
		const gzip = new Blob([rows]).stream().pipeThrough(new CompressionStream("gzip"));
		const compressed = new Uint8Array(await new Response(gzip).arrayBuffer());
		const response = await SELF.fetch(url, { method: "POST", body: compressed, headers: { ...authorization, "Content-Encoding": "gzip" } });
		expect(response.status).toBe(200);
		expect((await query("SELECT count(), sum(x) FROM streamed")).text).toBe("400000\t39999800000\n");
		/// A query in a compressed body.
		const select = new Uint8Array(await new Response(new Blob(["SELECT count() FROM streamed"]).stream().pipeThrough(new CompressionStream("deflate"))).arrayBuffer());
		const answer = await SELF.fetch("http://engine/", { method: "POST", body: select, headers: { ...authorization, "Content-Encoding": "deflate" } });
		expect(await answer.text()).toBe("400000\n");
	});

	it("has the system tables that clients look at", async () => {
		expect((await query("SELECT value FROM system.settings WHERE name = 'max_threads'")).text).toBe("1\n");
		expect((await query("SELECT count() > 100 FROM system.functions")).text).toBe("1\n");
		expect((await query("SELECT is_input, is_output FROM system.formats WHERE name = 'Parquet'")).text).toBe("1\t1\n");
		expect((await query("SELECT count() FROM system.table_engines WHERE name = 'MergeTree'")).text).toBe("1\n");
	});

	it("runs a query", async () => {
		expect(await query("SELECT 1 + 1")).toEqual({ status: 200, text: "2\n", code: null });
		expect((await query("SELECT version()")).text).toMatch(/^\d+\.\d+/);
	});

	it("aggregates", async () => {
		const result = await query("SELECT count(), sum(number), uniqExact(number % 7), avg(number) FROM numbers(100000)");
		expect(result.text).toBe("100000\t4999950000\t7\t49999.5\n");
	});

	it("keeps tables in memory between queries", async () => {
		expect((await query("CREATE TABLE t (id UInt64, name String) ENGINE = Memory")).status).toBe(200);
		expect((await query("INSERT INTO t FORMAT JSONEachRow", { body: '{"id":1,"name":"a"}\n{"id":2,"name":"b"}\n' })).status).toBe(200);
		expect((await query("INSERT INTO t VALUES (3, 'c')")).status).toBe(200);
		expect(await query("SELECT groupArray(name), sum(id) FROM t", { format: "JSONCompactEachRow" })).toMatchObject({
			status: 200,
			text: '[["a","b","c"], 6]\n',
		});
		expect(await query("SHOW TABLES LIKE 't'")).toMatchObject({ status: 200, text: "t\n" });
	});

	it("keeps tables of ObjectMergeTree in the catalog and in R2", async () => {
		expect((await query("CREATE TABLE events (id UInt64, kind String) ENGINE = ObjectMergeTree ORDER BY id")).status).toBe(200);
		expect((await query("INSERT INTO events FORMAT JSONEachRow", { body: '{"id":2,"kind":"b"}\n{"id":1,"kind":"a"}\n' })).status).toBe(200);
		expect(await query("SELECT * FROM events", { format: "JSONCompactEachRow" })).toMatchObject({
			status: 200,
			text: '[1, "a"]\n[2, "b"]\n',
		});
		const listed = await env.PARTS.list({ prefix: "parts/" });
		expect(listed.objects.length).toBeGreaterThan(0);
	});

	it("serves ranges of parts through the cache", async () => {
		const size = 3000;
		const name = `00000000-0000-4000-8000-000000000000_1_${size}_all`;
		const data = Uint8Array.from({ length: size }, (_, i) => i % 251);
		await env.PARTS.put(`parts/${name}`, data);
		const cache = (exports as unknown as { PartCache: Fetcher }).PartCache;
		const response = await cache.fetch(`https://parts/${name}/1000/1500`);
		expect(response.headers.get("Cache-Control")).toContain("immutable");
		expect(new Uint8Array(await response.arrayBuffer())).toEqual(data.slice(1000, 2500));
		expect((await cache.fetch(`https://parts/${name}/2000/1001`)).status).toBe(400);
		expect((await cache.fetch("https://parts/00000000-0000-4000-8000-000000000001_1_1_all/0/1")).status).toBe(404);
	});

	it("warms an engine before its first query, and keeps it warm", async () => {
		const host = env.ENGINE_HOST.get(env.ENGINE_HOST.idFromName("shard-0"));
		await host.warm();
		expect((await query("SELECT 1")).text).toBe("1\n");
		/// The use set an alarm, which wakes the object and sets the next one.
		expect(await runInDurableObject(host, (_, state) => state.storage.getAlarm())).not.toBeNull();
		expect(await runDurableObjectAlarm(host)).toBe(true);
		expect(await runInDurableObject(host, (_, state) => state.storage.getAlarm())).not.toBeNull();
		/// A day without a use, and it stops.
		await runInDurableObject(host, (_, state) => state.storage.put("last_use", Date.now() - 2 * 24 * 60 * 60 * 1000));
		expect(await runDurableObjectAlarm(host)).toBe(true);
		expect(await runInDurableObject(host, (_, state) => state.storage.getAlarm())).toBeNull();
	});

	it("merges from the queue", async () => {
		expect((await query("CREATE TABLE queued (id UInt64) ENGINE = ObjectMergeTree ORDER BY id")).status).toBe(200);
		for (let i = 0; i < 3; ++i) {
			expect((await query(`INSERT INTO queued VALUES (${i})`)).status).toBe(200);
		}
		const catalog = new Catalog(() => env.COORDINATION.get(env.COORDINATION.idFromName("catalog")));
		const parts = async () => [...(await catalog.snapshot()).tables.values()].find((table) => table.table === "queued")!.parts;
		expect((await parts()).length).toBe(3);
		const batch = createMessageBatch("clickhouse-engine-merges", [
			{ id: "1", timestamp: new Date(), attempts: 1, body: { database: "default", table: "queued", ttl: false } },
		]);
		const ctx = createExecutionContext();
		await worker.queue!(batch, env, ctx);
		expect((await getQueueResult(batch, ctx)).explicitAcks).toEqual(["1"]);
		expect((await parts()).length).toBe(1);
		expect(await query("SELECT count(), sum(id) FROM queued")).toMatchObject({ status: 200, text: "3\t3\n" });
	});

	it("has the common functions", async () => {
		const result = await query(
			"SELECT quantileExact(0.5)(number), topK(1)(number % 3), uniqCombined(number), toStartOfWeek(toDate('2026-11-01')), hex(255), bitAnd(12, 10), arrayFlatten([[1], [2, 3]]), splitByRegexp('[,;]', 'a,b;c'), log2(8) FROM numbers(10)",
		);
		expect(result).toMatchObject({ status: 200, text: "5\t[0]\t10\t2026-11-01\tFF\t8\t[1,2,3]\t['a','b','c']\t3\n" });
	});

	it("has more formats and encodings", async () => {
		expect((await query("SELECT base64Encode('ClickHouse'), base64Decode('Q2xpY2tIb3VzZQ=='), tryBase64Decode('!')")).text).toBe("Q2xpY2tIb3VzZQ==\tClickHouse\t\n");
		expect((await query("SELECT base58Encode('abc'), base32Encode('abc')")).text).toBe("ZiCa\tMFRGG===\n");
		expect((await query("SELECT * FROM format(TSKV, 'a=1\tb=x\n')")).text).toBe("1\tx\n");
		expect((await query("SELECT number AS n FROM numbers(2) FORMAT JSONColumns")).text).toBe('{\n\t"n": [0, 1]\n}\n');
		expect((await query("SELECT 1 AS x FORMAT Markdown")).text).toBe("| x |\n|-:|\n| 1 |\n");
		const avro = await SELF.fetch("http://engine/?query=SELECT%20toUInt32(7)%20AS%20x%20FORMAT%20Avro", { headers: authorization });
		const bytes = new Uint8Array(await avro.arrayBuffer());
		expect(new TextDecoder().decode(bytes.slice(0, 3))).toBe("Obj");
		await query("CREATE TABLE avro (x UInt32) ORDER BY x");
		const insert = await SELF.fetch("http://engine/?query=INSERT%20INTO%20avro%20FORMAT%20Avro", { method: "POST", body: bytes, headers: authorization });
		expect(insert.status).toBe(200);
		expect((await query("SELECT x FROM avro")).text).toBe("7\n");
		const msgpack = await SELF.fetch("http://engine/?query=SELECT%20*%20FROM%20format(MsgPack,%20'x%20UInt8',%20'%5Cx07')", { headers: authorization });
		expect(await msgpack.text()).toBe("7\n");
	});

	it("reads and writes Parquet and Arrow", async () => {
		for (const format of ["Parquet", "Arrow"]) {
			const out = await SELF.fetch(`http://engine/?query=${encodeURIComponent(`SELECT number AS n, toString(number) AS s FROM numbers(1000) FORMAT ${format}`)}`, { headers: authorization });
			expect(out.status).toBe(200);
			const bytes = new Uint8Array(await out.arrayBuffer());
			const table = `fmt_${format.toLowerCase()}`;
			await query(`CREATE TABLE ${table} (n UInt64, s String) ORDER BY n`);
			const insert = await SELF.fetch(`http://engine/?query=${encodeURIComponent(`INSERT INTO ${table} FORMAT ${format}`)}`, { method: "POST", body: bytes, headers: authorization });
			expect(insert.status).toBe(200);
			expect((await query(`SELECT count(), sum(n), max(s) FROM ${table}`)).text).toBe("1000\t499500\t999\n");
		}
	});

	it("joins with parallel_hash", async () => {
		expect((await query("SELECT count() FROM numbers(10) AS a JOIN numbers(5) AS b ON a.number = b.number SETTINGS join_algorithm = 'parallel_hash'")).text).toBe("5\n");
	});

	it("returns exceptions with their code", async () => {
		const result = await query("SELECT * FROM nonexistent");
		expect(result.status).toBe(500);
		expect(result.code).toBe("60");
		expect(result.text).toContain("UNKNOWN_TABLE");
	});
});

describe("output of a query", () => {
	const rows = (mib: number) => `SELECT number, repeat('x', 92) FROM numbers(${mib * 10240})`;
	const send = (sql: string) => SELF.fetch("http://engine/", { method: "POST", body: sql, headers: authorization });
	const memory = (response: Response) => Number(response.headers.get("Server-Timing")?.match(/memory;dur=(\d+)/)?.[1]);

	it("goes to the host, not into the memory of the engine, and arrives whole", async () => {
		const first = await send("SELECT 1");
		await first.text();
		const response = await send(rows(20));
		const lines = (await response.text()).split("\n");
		expect(response.status).toBe(200);
		expect(lines.length).toBe(20 * 10240 + 1);
		expect(lines[0]).toBe(`0\t${"x".repeat(92)}`);
		expect(lines[20 * 10240 - 1]).toBe(`${20 * 10240 - 1}\t${"x".repeat(92)}`);
		/// In a string in the engine, 10 MiB of output grew it by 30 MiB.
		expect(memory(response)).toBeLessThanOrEqual(memory(first));
	});

	it("gives a small result whole, and a large one as a stream", async () => {
		expect(answerBody([new Uint8Array([1, 2]), new Uint8Array([3])])).toEqual(new Uint8Array([1, 2, 3]));
		const large = answerBody([new Uint8Array(2 * 1024 * 1024).fill(7), new Uint8Array([8])]);
		expect(large).toBeInstanceOf(ReadableStream);
		const bytes = new Uint8Array(await new Response(large).arrayBuffer());
		expect(bytes.length).toBe(2 * 1024 * 1024 + 1);
		expect(bytes[bytes.length - 1]).toBe(8);
	});

	it("fails over the limit, with status 500 and no partial result", async () => {
		const response = await send(rows(30));
		expect(response.status).toBe(500);
		expect(response.headers.get("X-ClickHouse-Exception-Code")).toBe("396");
		expect(await response.text()).toMatch(/The result of the query is more than 24 MiB/);
		expect(await (await send("SELECT 1")).text()).toBe("1\n");
	});
});

describe("preferWarm", () => {
	it("puts the objects that came back cold lately last", () => {
		const now = 1_000_000_000;
		const cold_at = new Map([
			[1, now - 60_000],
			[3, now - 5_000],
			/// Long enough ago to be tried again.
			[4, now - 60 * 60 * 1000],
		]);
		expect(preferWarm(6, cold_at, now)).toEqual([0, 2, 4, 5, 1, 3]);
		expect(preferWarm(3, new Map(), now)).toEqual([0, 1, 2]);
	});
});

describe("pickTwo", () => {
	it("picks two different engines, or the only one", () => {
		for (const r of [0, 0.3, 0.6, 0.99]) {
			const [a, b] = pickTwo([2, 5, 7], () => r);
			expect(a).not.toBe(b);
			expect([2, 5, 7]).toContain(a);
			expect([2, 5, 7]).toContain(b);
		}
		expect(pickTwo([4])).toEqual([4]);
	});
});

describe("leastBusy", () => {
	it("prefers fewer queued queries, then the shorter running one, and an engine that answered", () => {
		expect(leastBusy([1, 2], [{ running_ms: 900, queued: 0 }, { running_ms: 10, queued: 0 }])).toBe(2);
		expect(leastBusy([1, 2], [{ running_ms: 10, queued: 1 }, { running_ms: 5_000, queued: 0 }])).toBe(2);
		expect(leastBusy([1, 2], [undefined, { running_ms: 600_000, queued: 3 }])).toBe(2);
		expect(leastBusy([1, 2], [{ running_ms: 0, queued: 0 }, { running_ms: 0, queued: 0 }])).toBe(1);
		expect(leastBusy([1, 2], [undefined, undefined])).toBe(1);
	});
});

describe("withDeadline", () => {
	it("passes a result through and fails a wait that runs out", async () => {
		expect(await withDeadline(Promise.resolve(3), 1_000, "x")).toBe(3);
		const never = new Promise<number>(() => {});
		await expect(withDeadline(never, 10, "Shard 4")).rejects.toThrow(DeadlineExceeded);
		await expect(withDeadline(never, 10, "Shard 4")).rejects.toThrow("Shard 4 did not answer within 0.01 s");
	});

	it("lists the waits that are open, the longest first", async () => {
		const never = new Promise<number>(() => {});
		const waiting = [withDeadline(never, 1_000, "The catalog"), withDeadline(never, 1_000, "Shard 2")];
		await withDeadline(Promise.resolve(1), 1_000, "The read of part a");
		expect(pendingWaits(Date.now() + 2_000)).toEqual(["The catalog (2 s)", "Shard 2 (2 s)"]);
		await Promise.allSettled(waiting);
		expect(pendingWaits()).toEqual([]);
	});
});

describe("stalled", () => {
	it("passes over an engine that runs long, starts, or does not answer", () => {
		expect(stalled({ running_ms: 200, queued: 2 })).toBe(false);
		expect(stalled({ running_ms: 31_000, queued: 0, step: "query" })).toBe(true);
		expect(stalled(undefined)).toBe(true);
	});
});

describe("stuckCall", () => {
	it("finds the call that has run for longer than the object waits", () => {
		const now = 10 * STUCK_MS;
		const query = { what: "query", since: now - STUCK_MS - 1 };
		expect(stuckCall([{ what: "shard", since: now - 1_000 }, query], now)).toBe(query);
		expect(stuckCall([{ what: "shard", since: now - STUCK_MS }], now)).toBeUndefined();
		expect(stuckCall([], now)).toBeUndefined();
	});
});
