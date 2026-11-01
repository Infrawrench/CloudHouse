import { env } from "cloudflare:workers";
import { describe, expect, it } from "vitest";
import { Catalog } from "../src/catalog";
import { DEFAULT_LIMITS, Database, type Io, type Limits, type MergeMessage } from "../src/database";
import { Engine } from "../src/engine";
import { collectGarbage } from "../src/garbage";
import { partKey } from "../src/parts";

const decoder = new TextDecoder();

/// Isolates of one cluster: each has an engine of its own, and they share the catalog and the bucket. The shards of
/// a fan-out from isolate `i` are the isolates from `i` on, and merges are collected for the test to run.
async function cluster(isolates: number, limits: Partial<Limits> = {}, hooks: { beforeShard?: () => Promise<void> } = {}) {
	const data = await (await env.ASSETS.fetch("https://assets.local/clickhouse-isolate.data")).arrayBuffer();
	const id = env.COORDINATION.newUniqueId();
	const background: Promise<unknown>[] = [];
	const merges: MergeMessage[] = [];
	const reads = { count: 0, bytes: 0 };
	const shards = { count: 0 };
	const databases = await Promise.all(
		Array.from({ length: isolates }, async () => {
			const engine = await Engine.create(data, () => {});
			return new Database(engine, new Catalog(() => env.COORDINATION.get(id)), env.PARTS, { ...DEFAULT_LIMITS, ...limits });
		}),
	);
	const io = (self: number): Io => ({
		background: (promise) => background.push(promise),
		stats: { pages: 0, bytes: 0, hits: 0 },
		async readRange(part, offset, size) {
			reads.count += 1;
			reads.bytes += size;
			const object = await env.PARTS.get(partKey(part), { range: { offset, length: size } });
			if (object === null) {
				throw new Error(`Part ${part} is gone`);
			}
			return new Uint8Array(await object.arrayBuffer());
		},
		/// Shard 0 is the coordinator itself, as when two Durable Objects share an isolate.
		async runShard(index, request) {
			shards.count += 1;
			await hooks.beforeShard?.();
			const shard = (self + index) % databases.length;
			return databases[shard]!.shard(request, io(shard));
		},
		async scheduleMerge(message) {
			merges.push(message);
		},
	});
	const run = async (database: Database, query: string) => {
		const result = await database.query(query, "", io(databases.indexOf(database)));
		if (!result.ok) {
			throw new Error(`${query}: ${result.message}`);
		}
		return decoder.decode(result.body);
	};
	const fail = async (database: Database, query: string) => {
		const result = await database.query(query, "", io(databases.indexOf(database)));
		if (result.ok) {
			throw new Error(`${query} succeeded`);
		}
		return result.message;
	};
	const merge = (database: Database, message: MergeMessage) => database.merge(message, io(databases.indexOf(database)));
	const settle = async () => {
		while (background.length > 0) {
			await Promise.all(background.splice(0));
		}
	};
	const catalog = new Catalog(() => env.COORDINATION.get(id));
	const parts = async (table: string) => [...(await catalog.snapshot()).tables.values()].find((entry) => entry.table === table)!.parts;
	return { databases, run, fail, merge, merges, settle, parts, reads, shards };
}

describe("ObjectMergeTree", () => {
	it("shares tables between isolates", async () => {
		const { databases: [a, b], run } = await cluster(2);
		await run(a!, "CREATE TABLE t (id UInt64, name String) ENGINE = ObjectMergeTree ORDER BY id");
		await run(a!, "INSERT INTO t VALUES (2, 'b'), (1, 'a')");
		expect(await run(b!, "SELECT * FROM t ORDER BY id")).toBe("1\ta\n2\tb\n");
		await run(b!, "INSERT INTO t VALUES (3, 'c')");
		expect(await run(a!, "SELECT count(), sum(id) FROM t")).toBe("3\t6\n");
		expect(await run(b!, "SHOW TABLES")).toBe("t\n");
	});

	it("commits concurrent inserts", async () => {
		const { databases, run } = await cluster(3);
		await run(databases[0]!, "CREATE TABLE t (id UInt64) ENGINE = ObjectMergeTree ORDER BY id");
		/// Inserts do not conflict: 30 of them at once, from three isolates, all commit.
		await Promise.all(Array.from({ length: 30 }, (_, i) => run(databases[i % 3]!, `INSERT INTO t VALUES (${i})`)));
		expect(await run(databases[1]!, "SELECT count(), sum(id) FROM t")).toBe("30\t435\n");
		/// An `ALTER` that commits while an insert runs makes the insert run again, with the new definition.
		await Promise.all([run(databases[0]!, "ALTER TABLE t ADD COLUMN v UInt8 DEFAULT 7"), run(databases[2]!, "INSERT INTO t (id) VALUES (100)")]);
		expect(await run(databases[1]!, "SELECT count(), sum(v) FROM t")).toBe("31\t217\n");
	});

	it("queues merges and caps their size", async () => {
		const { databases: [a, b], run, merge, merges, settle, parts } = await cluster(2);
		await run(a!, "CREATE TABLE t (id UInt64) ENGINE = ObjectMergeTree ORDER BY id");
		for (let i = 0; i < 8; ++i) {
			await run(a!, `INSERT INTO t VALUES (${i})`);
		}
		await settle();
		expect(merges).toEqual([{ database: "default", table: "t", ttl: false }]);
		const before = await parts("t");
		expect(await merge(b!, merges[0]!)).toBe(1);
		await settle();
		const after = await parts("t");
		expect(after.length).toBe(1);
		expect(await run(a!, "SELECT count(), sum(id) FROM t")).toBe("8\t28\n");
		/// The merged parts are deleted.
		for (const part of before) {
			expect(await env.PARTS.head(partKey(part))).toBeNull();
		}

		/// With a cap below the size of two parts, nothing merges.
		const small = await cluster(1, { mergeBytes: 1 });
		await small.run(small.databases[0]!, "CREATE TABLE t (id UInt64) ENGINE = ObjectMergeTree ORDER BY id");
		await small.run(small.databases[0]!, "INSERT INTO t VALUES (1)");
		await small.run(small.databases[0]!, "INSERT INTO t VALUES (2)");
		expect(await small.merge(small.databases[0]!, { database: "default", table: "t", ttl: false })).toBe(0);
	});

	it("fans out to shards", async () => {
		/// With `shardBytes` of 1, a table of more than one part fans out, to a shard per part, up to 3.
		const { databases, run, shards } = await cluster(4, { shardBytes: 1, maxShards: 3 });
		const [a] = databases;
		await run(a!, "CREATE TABLE t (k UInt8, v UInt64, s String) ENGINE = ObjectMergeTree ORDER BY k");
		for (let i = 0; i < 4; ++i) {
			await run(a!, `INSERT INTO t SELECT number % 5, number, toString(number) FROM numbers(${i * 1000}, 1000)`);
		}
		const reference = await cluster(1);
		await reference.run(reference.databases[0]!, "CREATE TABLE t (k UInt8, v UInt64, s String) ENGINE = Memory");
		await reference.run(reference.databases[0]!, "INSERT INTO t SELECT number % 5, number, toString(number) FROM numbers(4000)");
		for (const query of [
			"SELECT k, count(), sum(v), uniqExact(s) FROM t GROUP BY k ORDER BY k",
			"SELECT v, s FROM t ORDER BY v DESC LIMIT 3",
			"SELECT count(), avg(v), min(v), max(v) FROM t WHERE k != 2",
			"SELECT k, groupArray(v) FROM (SELECT k, v FROM t WHERE v < 12) GROUP BY k ORDER BY k",
		]) {
			expect(await run(a!, query)).toBe(await reference.run(reference.databases[0]!, query));
		}
		expect(shards.count).toBeGreaterThan(0);
		/// A query whose filter keeps no blocks reads them here.
		shards.count = 0;
		expect(await run(a!, "SELECT count() FROM t WHERE k = 7")).toBe("0\n");
		expect(shards.count).toBe(0);
		/// Neither does a query of rows, which the shards would not reduce.
		expect(await run(a!, "SELECT sum(length(s)) FROM (SELECT s FROM t WHERE k = 1)")).toBe("2978\n");
		expect(shards.count).toBe(0);
	});

	it("renames tables", async () => {
		const { databases: [a, b], run, fail, parts } = await cluster(2);
		await run(a!, "CREATE TABLE t (id UInt64) ENGINE = MergeTree ORDER BY id");
		await run(a!, "INSERT INTO t SELECT number FROM numbers(10)");
		await run(b!, "INSERT INTO t SELECT number FROM numbers(10, 5)");
		const before = await parts("t");
		/// `CREATE DATABASE` is in the catalog, so every isolate has the database.
		await run(b!, "CREATE DATABASE other");
		expect(await run(a!, "SELECT count() FROM system.databases WHERE name = 'other'")).toBe("1\n");
		expect(await fail(a!, "CREATE DATABASE other")).toContain("DATABASE_ALREADY_EXISTS");
		await run(b!, "RENAME TABLE t TO u, u TO other.v");
		expect(await parts("v")).toEqual(before);
		expect(await run(a!, "SELECT count(), sum(id) FROM other.v")).toBe("15\t105\n");
		expect(await fail(a!, "SELECT * FROM t")).toContain("UNKNOWN_TABLE");
		await run(a!, "INSERT INTO other.v VALUES (100)");
		expect(await run(b!, "SELECT count(), max(id) FROM other.v")).toBe("16\t100\n");
		expect(await run(b!, "SHOW CREATE TABLE other.v")).toContain("CREATE TABLE other.v");
	});

	it("exchanges tables", async () => {
		const { databases: [a, b], run } = await cluster(2);
		await run(a!, "CREATE TABLE x (id UInt64) ORDER BY id");
		await run(a!, "CREATE TABLE y (id UInt64, s String) ORDER BY id");
		await run(a!, "INSERT INTO x VALUES (1)");
		await run(a!, "INSERT INTO y VALUES (2, 'b'), (3, 'c')");
		await run(b!, "EXCHANGE TABLES x AND y");
		expect(await run(a!, "SELECT groupArray(id), groupArray(s) FROM x")).toBe("[2,3]\t['b','c']\n");
		expect(await run(a!, "SELECT groupArray(id) FROM y")).toBe("[1]\n");
		await run(a!, "INSERT INTO y VALUES (4)");
		expect(await run(b!, "SELECT groupArray(id) FROM (SELECT id FROM y ORDER BY id)")).toBe("[1,4]\n");
	});

	it("drops databases from the catalog", async () => {
		const { databases: [a, b], run } = await cluster(2);
		await run(a!, "CREATE DATABASE d");
		await run(a!, "CREATE TABLE d.t (id UInt64) ORDER BY id");
		await run(a!, "INSERT INTO d.t VALUES (1)");
		await run(b!, "DROP DATABASE d");
		expect(await run(a!, "SELECT count() FROM system.databases WHERE name = 'd'")).toBe("0\n");
		await run(a!, "CREATE DATABASE d");
		expect(await run(b!, "SELECT count() FROM system.tables WHERE database = 'd'")).toBe("0\n");
	});

	it("has settings, constraints, projections and column TTL", async () => {
		const { databases: [a, b], run, fail } = await cluster(2);
		await run(
			a!,
			`CREATE TABLE t (id UInt64, v UInt64, ts DateTime DEFAULT '2020-01-01 00:00:00', note String TTL ts + INTERVAL 1 DAY, CONSTRAINT positive CHECK v > 0,
			PROJECTION by_v (SELECT v, count() GROUP BY v)) ORDER BY id SETTINGS index_granularity = 1000, merge_with_ttl_timeout = 60`,
		);
		expect(await run(b!, "SHOW CREATE TABLE t")).toContain("index_granularity = 1000");
		expect(await fail(b!, "INSERT INTO t (id, v, note) VALUES (1, 0, 'x')")).toContain("VIOLATED_CONSTRAINT");
		await run(b!, "INSERT INTO t (id, v, note) SELECT number, number + 1, 'old' FROM numbers(5000)");
		expect(await run(a!, "SELECT sum(rows) FROM system.parts WHERE table = 't'")).toBe("5000\n");
		expect(await run(a!, "SELECT v, count() FROM t WHERE v < 3 GROUP BY v ORDER BY v")).toBe("1\t1\n2\t1\n");
		/// The TTL of `note` has passed, so a merge resets it to its default.
		await run(a!, "OPTIMIZE TABLE t FINAL");
		expect(await run(b!, "SELECT countIf(note = '') FROM t")).toBe("5000\n");
		expect(await fail(a!, "CREATE TABLE u (id UInt64) ORDER BY id SETTINGS no_such_setting = 1")).toContain("UNKNOWN_SETTING");
		expect(await fail(a!, "ALTER TABLE t MODIFY SETTING index_granularity = 10")).toContain("READONLY_SETTING");
		await run(a!, "ALTER TABLE t ADD PROJECTION by_id (SELECT id ORDER BY id)");
		await run(a!, "ALTER TABLE t MATERIALIZE PROJECTION by_id");
		await run(a!, "ALTER TABLE t DROP PROJECTION by_v");
		await run(a!, "ALTER TABLE t DROP CONSTRAINT positive");
		await run(b!, "INSERT INTO t (id, v, note) VALUES (9999, 0, 'z')");
		expect(await run(a!, "SELECT count() FROM t")).toBe("5001\n");
	});

	it("skips blocks with data skipping indexes", async () => {
		const { databases: [a, b], run, reads } = await cluster(2);
		await run(a!, "CREATE TABLE t (id UInt64, v UInt64, s String, INDEX v_minmax v TYPE minmax, INDEX s_set s TYPE set(10)) ORDER BY id SETTINGS index_granularity = 1000");
		/// `v` and `s` follow `id`, so each block has a narrow range of them.
		await run(a!, "INSERT INTO t SELECT number, intDiv(number, 1000), toString(intDiv(number, 1000)) FROM numbers(20000)");
		const before = reads.count;
		expect(await run(b!, "SELECT count() FROM t WHERE v = 7")).toBe("1000\n");
		/// The index of the part, and one block.
		expect(reads.count - before).toBeLessThanOrEqual(2);
		const all = reads.count;
		expect(await run(b!, "SELECT count() FROM t WHERE s = '12'")).toBe("1000\n");
		expect(reads.count - all).toBeLessThanOrEqual(2);
		/// An index added later covers the parts that `MATERIALIZE INDEX` rewrites.
		await run(a!, "ALTER TABLE t ADD INDEX v2 v * 2 TYPE minmax");
		await run(a!, "ALTER TABLE t MATERIALIZE INDEX v2");
		const later = reads.count;
		expect(await run(b!, "SELECT count() FROM t WHERE v * 2 = 10")).toBe("1000\n");
		expect(reads.count - later).toBeLessThanOrEqual(2);
		await run(a!, "ALTER TABLE t DROP INDEX v2");
		expect(await run(b!, "SELECT count() FROM t WHERE v * 2 = 10")).toBe("1000\n");
	});

	it("alters the sorting key, the TTL and the settings", async () => {
		const { databases: [a, b], run, fail } = await cluster(2);
		await run(a!, "CREATE TABLE t (ts DateTime, id UInt64) ORDER BY id");
		await run(a!, "INSERT INTO t VALUES (now() - INTERVAL 3 DAY, 1), (now(), 2)");
		await run(b!, "ALTER TABLE t ADD COLUMN k UInt8, MODIFY ORDER BY (id, k)");
		expect(await run(a!, "SHOW CREATE TABLE t")).toContain("ORDER BY (id, k)");
		expect(await fail(a!, "ALTER TABLE t MODIFY ORDER BY (id, k, ts)")).toContain("Existing column ts");
		await run(b!, "ALTER TABLE t MODIFY TTL ts + INTERVAL 1 DAY");
		expect(await run(a!, "SELECT count() FROM t")).toBe("1\n");
		await run(b!, "ALTER TABLE t REMOVE TTL");
		expect(await run(a!, "SHOW CREATE TABLE t")).not.toContain("TTL");
		await run(b!, "ALTER TABLE t MODIFY SETTING merge_with_ttl_timeout = 100");
		expect(await run(a!, "SHOW CREATE TABLE t")).toContain("merge_with_ttl_timeout = 100");
		await run(b!, "INSERT INTO t VALUES (now(), 3, 5)");
		expect(await run(a!, "SELECT groupArray((id, k)) FROM (SELECT id, k FROM t ORDER BY id)")).toBe("[(2,0),(3,5)]\n");
	});

	it("detaches, attaches, copies and moves partitions", async () => {
		const { databases: [a, b], run, fail } = await cluster(2);
		for (const table of ["p", "q"]) {
			await run(a!, `CREATE TABLE ${table} (d Date, v UInt64) PARTITION BY toYYYYMM(d) ORDER BY d`);
		}
		await run(a!, "INSERT INTO p SELECT toDate('2026-09-01') + number, number FROM numbers(60)");
		await run(b!, "ALTER TABLE p DETACH PARTITION 202609");
		expect(await run(a!, "SELECT count() FROM p")).toBe("30\n");
		expect(await run(a!, "SELECT partition_id, sum(rows) FROM system.detached_parts WHERE table = 'p' GROUP BY partition_id")).toBe("202609\t30\n");
		await run(a!, "ALTER TABLE p ATTACH PARTITION 202609");
		expect(await run(b!, "SELECT count() FROM p")).toBe("60\n");

		await run(a!, "ALTER TABLE q ATTACH PARTITION 202610 FROM p");
		expect(await run(b!, "SELECT count(), min(d) FROM q")).toBe("30\t2026-10-01\n");
		/// A table has a part once.
		expect(await fail(a!, "ALTER TABLE q ATTACH PARTITION 202610 FROM p")).toContain("has part");
		await run(a!, "ALTER TABLE q REPLACE PARTITION 202609 FROM p");
		expect(await run(b!, "SELECT count() FROM q")).toBe("60\n");
		/// `q` shares the parts of `p`: they stay when `p` gives them up.
		await run(a!, "CREATE TABLE m (d Date, v UInt64) PARTITION BY toYYYYMM(d) ORDER BY d");
		await run(b!, "ALTER TABLE p MOVE PARTITION 202610 TO TABLE m");
		expect(await run(a!, "SELECT count() FROM p")).toBe("30\n");
		expect(await run(a!, "SELECT count() FROM m")).toBe("30\n");
		await run(a!, "DROP TABLE p");
		await run(a!, "DROP TABLE m");
		expect(await run(b!, "SELECT count(), sum(v) FROM q")).toBe("60\t1770\n");

		const part = (await run(a!, "SELECT name FROM system.parts WHERE table = 'q' ORDER BY name LIMIT 1")).trim();
		await run(b!, `ALTER TABLE q DETACH PART '${part}'`);
		expect(await fail(a!, `ALTER TABLE q DROP DETACHED PART '${part}'`)).toContain("allow_drop_detached");
		await run(a!, `ALTER TABLE q DROP DETACHED PART '${part}' SETTINGS allow_drop_detached = 1`);
		expect(await run(b!, "SELECT count() FROM system.detached_parts")).toBe("0\n");
	});

	it("swaps a large partition in one commit, sharing its parts", async () => {
		const { databases: [a, b], run } = await cluster(2);
		const objects = async () => (await env.PARTS.list({ prefix: "parts/" })).objects.length;
		await run(a!, "CREATE TABLE sz_a (source LowCardinality(String), a String, g Map(String, String), h Bool) PARTITION BY source ORDER BY a");
		await run(a!, "CREATE TABLE sz_b AS sz_a");
		for (let i = 0; i < 6; ++i) {
			await run(b!, `INSERT INTO sz_b SELECT 'big', toString(number), map('k', toString(number % 5)), number % 2 = 0 FROM numbers(${i * 50000}, 50000)`);
		}
		await run(a!, "INSERT INTO sz_a VALUES ('big', 'old', {}, false), ('small', 'x', {}, true)");
		const before = await objects();
		await run(a!, "ALTER TABLE sz_a REPLACE PARTITION 'big' FROM sz_b");
		/// No part was copied: the old part of `big` goes, once nothing has it.
		expect(await objects()).toBeLessThanOrEqual(before);
		expect(await run(b!, "SELECT source, count() FROM sz_a GROUP BY source ORDER BY source")).toBe("big\t300000\nsmall\t1\n");
		/// The source may change and go: the destination keeps the parts.
		await run(b!, "OPTIMIZE TABLE sz_b FINAL");
		await run(b!, "DROP TABLE sz_b");
		expect(await run(a!, "SELECT count(), countIf(h) FROM sz_a WHERE source = 'big'")).toBe("300000\t150000\n");
		expect(await run(a!, "SELECT count() FROM system.parts WHERE table = 'sz_a'")).not.toBe("0\n");
	});

	it("reads while other partitions and tables change", async () => {
		let writes = 0;
		const writer = { run: undefined as undefined | (() => Promise<void>) };
		const { databases: [a, b], run } = await cluster(2, { shardBytes: 1, maxShards: 2 }, { beforeShard: () => writer.run?.() ?? Promise.resolve() });
		await run(a!, "CREATE TABLE r (p UInt8, v UInt64) PARTITION BY p ORDER BY v");
		await run(a!, "CREATE TABLE other (v UInt64) ORDER BY v");
		await run(a!, "INSERT INTO r SELECT 1, number FROM numbers(1000)");
		await run(a!, "INSERT INTO r SELECT 2, number FROM numbers(1000)");
		/// Before every shard runs, another isolate commits: the shards see a newer catalog than the coordinator.
		writer.run = async () => {
			writes += 1;
			await run(b!, `INSERT INTO other VALUES (${writes})`);
			await run(b!, `INSERT INTO r VALUES (3, ${writes})`);
		};
		expect(await run(a!, "SELECT count(), sum(v) FROM r WHERE p != 3")).toBe("2000\t999000\n");
		expect(writes).toBeGreaterThan(0);
	});

	it("has the virtual columns and the partitions of parts", async () => {
		const { databases: [a], run } = await cluster(1);
		await run(a!, "CREATE TABLE vp (k UInt32, s String) PARTITION BY k % 3 ORDER BY k");
		await run(a!, "INSERT INTO vp SELECT number, toString(number) FROM numbers(9)");
		expect(await run(a!, "SELECT _partition_id, count() FROM vp GROUP BY _partition_id ORDER BY _partition_id")).toBe("0\t3\n1\t3\n2\t3\n");
		expect(await run(a!, "SELECT count(DISTINCT _part) FROM vp")).toBe("3\n");
		expect(await run(a!, "SELECT groupArray(partition) FROM (SELECT partition FROM system.parts WHERE table = 'vp' ORDER BY partition)")).toBe(
			"['0','1','2']\n",
		);
		expect(await run(a!, "SELECT name = _part FROM vp, system.parts WHERE k = 4 AND table = 'vp' AND partition_id = '1'")).toBe("1\n");
	});

	it("reads only the columns that a query needs", async () => {
		const { databases: [a, b], run, reads, parts } = await cluster(2);
		await run(a!, "CREATE TABLE wide (id UInt64, k LowCardinality(String), big String, other String) ORDER BY id");
		/// Strings that do not compress, so that they are most of the part.
		await run(a!, "INSERT INTO wide SELECT number, toString(number % 7), concat(hex(sipHash128(number)), hex(sipHash128(number + 1)), hex(sipHash128(number + 2))), repeat('x', 50) FROM numbers(200000)");
		const total = (await parts("wide")).reduce((sum, part) => sum + Number(part.split("_")[2]), 0);
		const before = reads.bytes;
		expect(await run(b!, "SELECT k, sum(id) FROM wide GROUP BY k ORDER BY k LIMIT 1")).toBe("0\t2857157142\n");
		/// The index and the chunks of `id` and `k`, not the strings.
		expect(reads.bytes - before).toBeLessThan(total / 5);
		expect(await run(a!, "SELECT sum(length(big)), any(other) FROM wide")).toBe("19200000\txxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n");
	});

	it("applies the TTL in merges", async () => {
		const { databases: [a], run, merge } = await cluster(1);
		await run(a!, "CREATE TABLE e (ts DateTime, v UInt8) ENGINE = ObjectMergeTree ORDER BY ts TTL ts + INTERVAL 1 DAY");
		await run(a!, "INSERT INTO e VALUES (now() - INTERVAL 3 DAY, 1), (now(), 2), (now() - INTERVAL 2 DAY, 3)");
		expect(await merge(a!, { database: "default", table: "e", ttl: true })).toBe(1);
		expect(await run(a!, "SELECT groupArray(v) FROM e")).toBe("[2]\n");
		/// Nothing more expires, so nothing more merges.
		expect(await merge(a!, { database: "default", table: "e", ttl: true })).toBe(0);
	});

	it("runs mutations", async () => {
		const { databases: [a, b], run } = await cluster(2);
		await run(a!, "CREATE TABLE t (id UInt64, s String) ENGINE = ObjectMergeTree ORDER BY id");
		await run(a!, "INSERT INTO t SELECT number, 'a' FROM numbers(10)");
		await run(a!, "INSERT INTO t SELECT number + 10, 'b' FROM numbers(10)");
		await run(b!, "ALTER TABLE t DELETE WHERE id % 2 = 0");
		await run(a!, "ALTER TABLE t UPDATE s = 'c' WHERE id > 15");
		await run(b!, "DELETE FROM t WHERE id = 1");
		expect(await run(a!, "SELECT count(), countIf(s = 'c'), min(id) FROM t")).toBe("9\t2\t3\n");
	});

	it("splits parts by partition", async () => {
		const { databases: [a, b], run, merge } = await cluster(2);
		await run(a!, "CREATE TABLE p (d Date, v UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY d");
		await run(a!, "INSERT INTO p SELECT toDate('2026-09-01') + number, number FROM numbers(80)");
		await run(a!, "INSERT INTO p VALUES ('2026-10-15', 1000)");
		expect(await run(b!, "SELECT partition_id, count(), sum(rows) FROM system.parts WHERE table = 'p' GROUP BY partition_id ORDER BY partition_id")).toBe(
			"202609\t1\t30\n202610\t2\t32\n202611\t1\t19\n",
		);
		/// A merge stays within a partition.
		expect(await merge(b!, { database: "default", table: "p", ttl: false })).toBe(1);
		expect(await run(a!, "SELECT partition_id, count() FROM system.parts WHERE table = 'p' GROUP BY partition_id ORDER BY partition_id")).toBe(
			"202609\t1\n202610\t1\n202611\t1\n",
		);
		await run(b!, "ALTER TABLE p DROP PARTITION 202610");
		expect(await run(a!, "SELECT count(), min(d), max(d) FROM p")).toBe("49\t2026-09-01\t2026-11-19\n");
		await run(a!, "ALTER TABLE p DROP PARTITION ID '202609'");
		expect(await run(b!, "SELECT count() FROM p")).toBe("19\n");
	});

	it("skips the blocks that the filter excludes", async () => {
		const { databases: [a], run, reads } = await cluster(1);
		await run(a!, "CREATE TABLE s (id UInt64, v String) ENGINE = MergeTree ORDER BY id");
		await run(a!, "INSERT INTO s SELECT number, toString(number) FROM numbers(262144)");
		reads.count = 0;
		reads.bytes = 0;
		expect(await run(a!, "SELECT count(), sum(id), min(v) FROM s")).toBe("262144\t34359607296\t0\n");
		const all = { ...reads };
		reads.count = 0;
		reads.bytes = 0;
		/// The part has sixteen blocks of 16384 rows: the filter needs one of them, a chunk of each of its two columns.
		expect(await run(a!, "SELECT count(), min(v) FROM s WHERE id BETWEEN 70000 AND 70010")).toBe("11\t70000\n");
		expect(reads.count).toBe(2);
		expect(reads.bytes).toBeLessThan(all.bytes / 2);
	});

	it("reads more than fits in memory", async () => {
		/// Without fan-out, one engine scans 2 million rows of 8 parts, a block at a time.
		const { databases: [a], run } = await cluster(1, { shardBytes: 1024 * 1024 * 1024 });
		await run(a!, "CREATE TABLE big (id UInt64, h String) ENGINE = MergeTree ORDER BY id");
		for (let i = 0; i < 8; ++i) {
			await run(a!, `INSERT INTO big SELECT number, toString(sipHash64(number)) FROM numbers(${i * 250000}, 250000)`);
		}
		expect(await run(a!, "SELECT count(), sum(id), max(length(h)) FROM big")).toBe("2000000\t1999999000000\t20\n");
		expect(await run(a!, "SELECT id % 4 AS k, count() FROM big GROUP BY k ORDER BY k")).toBe(
			"0\t500000\n1\t500000\n2\t500000\n3\t500000\n",
		);
	});

	it("alters columns", async () => {
		const { databases: [a, b], run, fail } = await cluster(2);
		await run(a!, "CREATE TABLE t (id UInt64, v UInt32) ENGINE = MergeTree ORDER BY id");
		await run(a!, "INSERT INTO t VALUES (1, 10), (2, 20)");
		await run(b!, "ALTER TABLE t ADD COLUMN s String DEFAULT concat('x', toString(id))");
		await run(b!, "INSERT INTO t (id, v, s) VALUES (3, 30, 'new')");
		expect(await run(a!, "SELECT id, v, s FROM t ORDER BY id")).toBe("1\t10\tx1\n2\t20\tx2\n3\t30\tnew\n");
		await run(a!, "ALTER TABLE t MODIFY COLUMN v UInt64");
		await run(a!, "ALTER TABLE t DROP COLUMN s");
		expect(await run(b!, "SELECT toTypeName(v), sum(v) FROM t GROUP BY 1")).toBe("UInt64\t60\n");
		expect(await run(b!, "SELECT name FROM system.columns WHERE table = 't' ORDER BY position")).toBe("id\nv\n");
		expect(await fail(a!, "ALTER TABLE t DROP COLUMN id")).toContain("key column");
		expect(await fail(a!, "ALTER TABLE t UPDATE id = 5 WHERE 1")).toContain("Cannot UPDATE key column");
		/// A column added after the parts were written, read alone, with a default of another column.
		await run(b!, "ALTER TABLE t ADD COLUMN tag String DEFAULT concat('id', toString(id))");
		expect(await run(a!, "SELECT tag, count() FROM t GROUP BY tag ORDER BY tag")).toBe("id1\t1\nid2\t1\nid3\t1\n");
	});

	it("makes MergeTree the default engine", async () => {
		const { databases: [a, b], run } = await cluster(2);
		await run(a!, "CREATE TABLE d (x UInt8) ORDER BY x");
		await run(a!, "INSERT INTO d VALUES (1), (2)");
		expect(await run(b!, "SELECT engine, sum(x) FROM d, system.tables WHERE name = 'd' GROUP BY engine")).toBe("ObjectMergeTree\t3\n");
	});

	it("creates, truncates and drops", async () => {
		const { databases: [a, b], run, fail } = await cluster(2);
		await run(a!, "CREATE TABLE u ENGINE = ObjectMergeTree ORDER BY n AS SELECT number AS n FROM numbers(5)");
		expect(await run(b!, "SELECT sum(n) FROM u")).toBe("10\n");
		await run(b!, "TRUNCATE TABLE u");
		expect(await run(a!, "SELECT count() FROM u")).toBe("0\n");
		await run(a!, "DROP TABLE u");
		expect(await fail(b!, "SELECT * FROM u")).toContain("UNKNOWN_TABLE");
		expect(await fail(b!, "CREATE TABLE p (a UInt8) ENGINE = ObjectMergeTree ORDER BY a SAMPLE BY a")).toContain("ObjectMergeTree has no SAMPLE BY");
	});

	it("deletes the parts that the catalog does not have", async () => {
		const { databases: [a], run } = await cluster(1);
		await run(a!, "CREATE TABLE IF NOT EXISTS g (id UInt64) ENGINE = ObjectMergeTree ORDER BY id");
		await run(a!, "INSERT INTO g VALUES (1)");
		await env.PARTS.put("parts/orphan", new Uint8Array([1]));
		/// Not yet: the orphan might be the part of a commit in progress.
		await collectGarbage(env);
		expect(await env.PARTS.head("parts/orphan")).not.toBeNull();
		/// An hour later. The catalog of `collectGarbage` is the one of the Worker, which has none of these tables,
		/// so the check is only about the orphan.
		await collectGarbage(env, Date.now() + 2 * 60 * 60 * 1000);
		expect(await env.PARTS.head("parts/orphan")).toBeNull();
	});

	it("writes parts during the query, and deletes them if it fails", async () => {
		const { databases: [a], run, fail, settle } = await cluster(1);
		await run(a!, "CREATE TABLE w (id UInt64, CONSTRAINT small CHECK id < 50) ORDER BY id");
		const objects = async () => (await env.PARTS.list({ prefix: "parts/" })).objects.length;
		const before = await objects();
		/// Blocks of 10 rows: the first five become parts before the sixth breaks the constraint.
		const settings = "SETTINGS max_block_size = 10, min_insert_block_size_rows = 10, max_insert_block_size = 10";
		expect(await fail(a!, `INSERT INTO w SELECT number FROM numbers(100) ${settings}`)).toContain("VIOLATED_CONSTRAINT");
		await settle();
		expect(await objects()).toBe(before);
		await run(a!, `INSERT INTO w SELECT number FROM numbers(40) ${settings}`);
		expect(await objects()).toBe(before + 4);
		expect(await run(a!, "SELECT count(), sum(id) FROM w")).toBe("40\t780\n");
	});

	it("fails a query whose part is gone with an error, and goes on", async () => {
		const { databases: [a], run, fail, parts } = await cluster(1);
		await run(a!, "CREATE TABLE gone (id UInt64) ORDER BY id");
		await run(a!, "INSERT INTO gone VALUES (1)");
		await run(a!, "CREATE TABLE kept (id UInt64) ORDER BY id");
		await run(a!, "INSERT INTO kept VALUES (2)");
		await env.PARTS.delete(partKey((await parts("gone"))[0]!));
		expect(await fail(a!, "SELECT sum(id) FROM gone")).toContain("RECEIVED_ERROR_FROM_REMOTE_IO_SERVER");
		/// The engine is not left half way through the query.
		expect(await run(a!, "SELECT sum(id) FROM kept")).toBe("2\n");
		await run(a!, "INSERT INTO kept VALUES (3)");
		expect(await run(a!, "SELECT sum(id) FROM kept")).toBe("5\n");
	});

	it("keeps the catalog when a query fails", async () => {
		const { databases: [a, b], run, fail } = await cluster(2);
		await run(a!, "CREATE TABLE t (id UInt64) ENGINE = ObjectMergeTree ORDER BY id");
		await fail(a!, "CREATE TABLE t (id UInt64) ENGINE = ObjectMergeTree ORDER BY id");
		await fail(a!, "INSERT INTO t SELECT throwIf(number = 3) FROM numbers(5)");
		expect(await run(b!, "SELECT count() FROM t")).toBe("0\n");
		expect(await run(a!, "SELECT groupArray(name) FROM system.tables WHERE engine = 'ObjectMergeTree'")).toBe("['t']\n");
	});
});
