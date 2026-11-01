import { SELF, createExecutionContext, createMessageBatch, getQueueResult } from "cloudflare:test";
import { env, exports } from "cloudflare:workers";
import worker from "../src/index";
import { Catalog } from "../src/catalog";
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
		for (const query of [
			"CREATE TABLE r (a UInt8) ENGINE = ObjectMergeTree",
			"CREATE TABLE r (a UInt8) ENGINE = Memory",
			"SET max_threads = 2",
		]) {
			const response = await read(query);
			expect(response.status).toBe(500);
			expect(response.headers.get("X-ClickHouse-Exception-Code")).toBe("164");
		}
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
		expect(await query("SHOW TABLES")).toMatchObject({ status: 200, text: "t\n" });
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

	it("serves parts through the cache", async () => {
		const name = "00000000-0000-4000-8000-000000000000_1_1_all";
		await env.PARTS.put(`parts/${name}`, new Uint8Array([7]));
		const cache = (exports as unknown as { PartCache: Fetcher }).PartCache;
		const response = await cache.fetch(`https://parts/${name}`);
		expect(response.headers.get("Cache-Control")).toContain("immutable");
		expect(new Uint8Array(await response.arrayBuffer())).toEqual(new Uint8Array([7]));
		expect((await cache.fetch("https://parts/00000000-0000-4000-8000-000000000001_1_1_all")).status).toBe(404);
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
