import { env } from "cloudflare:workers";
import { evictDurableObject, runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { ErrorCode, EventType, ListFilter } from "../src/protocol";

const bytes = (s: string) => new TextEncoder().encode(s);
const text = (b: Uint8Array | undefined) => new TextDecoder().decode(b);

async function connect() {
	const stub = env.COORDINATION.get(env.COORDINATION.newUniqueId());
	const session = await stub.createSession(60_000);
	return { stub, session };
}

describe("nodes", () => {
	it("creates, reads and updates with version checks", async () => {
		const { stub, session } = await connect();

		expect(await stub.exec(session, { type: "create", path: "/a", data: bytes("x") })).toEqual({
			error: ErrorCode.ZOK,
			pathCreated: "/a",
		});
		expect((await stub.exec(session, { type: "create", path: "/a" })).error).toBe(ErrorCode.ZNODEEXISTS);
		expect((await stub.exec(session, { type: "create", path: "/missing/b" })).error).toBe(ErrorCode.ZNONODE);

		const got = await stub.exec(session, { type: "get", path: "/a" });
		expect(text(got.data)).toBe("x");
		expect(got.stat).toMatchObject({ version: 0, dataLength: 1, numChildren: 0 });

		expect((await stub.exec(session, { type: "set", path: "/a", data: bytes("y"), version: 5 })).error).toBe(
			ErrorCode.ZBADVERSION,
		);
		const set = await stub.exec(session, { type: "set", path: "/a", data: bytes("yz"), version: 0 });
		expect(set.stat).toMatchObject({ version: 1, dataLength: 2 });

		const root = await stub.exec(session, { type: "list", path: "/" });
		expect(root.children).toEqual(["a"]);
		expect(root.stat).toMatchObject({ numChildren: 1, cversion: 1 });
	});

	it("rejects invalid paths", async () => {
		const { stub, session } = await connect();
		for (const path of ["a", "/a/", "//a", "/a/../b", "/./a", ""]) {
			expect((await stub.exec(session, { type: "create", path })).error).toBe(ErrorCode.ZBADARGUMENTS);
		}
		expect((await stub.exec(session, { type: "remove", path: "/" })).error).toBe(ErrorCode.ZBADARGUMENTS);
	});

	it("numbers sequential nodes by the parent's counter", async () => {
		const { stub, session } = await connect();
		await stub.exec(session, { type: "create", path: "/q" });
		await stub.exec(session, { type: "create", path: "/q/plain" });
		const first = await stub.exec(session, { type: "create", path: "/q/item-", sequential: true });
		const second = await stub.exec(session, { type: "create", path: "/q/item-", sequential: true });
		expect(first.pathCreated).toBe("/q/item-0000000001");
		expect(second.pathCreated).toBe("/q/item-0000000002");
	});

	it("removes only empty nodes with a matching version", async () => {
		const { stub, session } = await connect();
		await stub.exec(session, { type: "create", path: "/p" });
		await stub.exec(session, { type: "create", path: "/p/c" });

		expect((await stub.exec(session, { type: "remove", path: "/p" })).error).toBe(ErrorCode.ZNOTEMPTY);
		expect((await stub.exec(session, { type: "remove", path: "/p/c", version: 3 })).error).toBe(
			ErrorCode.ZBADVERSION,
		);
		expect((await stub.exec(session, { type: "remove", path: "/p/c", version: 0 })).error).toBe(ErrorCode.ZOK);
		expect((await stub.exec(session, { type: "remove", path: "/p/c" })).error).toBe(ErrorCode.ZNONODE);

		const parent = await stub.exec(session, { type: "exists", path: "/p" });
		expect(parent.stat).toMatchObject({ numChildren: 0, cversion: 2 });
	});
});

describe("multi", () => {
	it("rolls back a failed transaction and reports per-op errors", async () => {
		const { stub, session } = await connect();
		const result = await stub.multi(session, [
			{ type: "create", path: "/t1" },
			{ type: "create", path: "/nope/t2" },
			{ type: "create", path: "/t3" },
		]);
		expect(result.error).toBe(ErrorCode.ZNONODE);
		expect(result.results.map((r) => r.error)).toEqual([
			ErrorCode.ZOK,
			ErrorCode.ZNONODE,
			ErrorCode.ZRUNTIMEINCONSISTENCY,
		]);
		expect((await stub.exec(session, { type: "exists", path: "/t1" })).error).toBe(ErrorCode.ZNONODE);
	});

	it("applies a transaction guarded by checks atomically", async () => {
		const { stub, session } = await connect();
		await stub.exec(session, { type: "create", path: "/log" });

		const result = await stub.multi(session, [
			{ type: "check", path: "/log", version: 0 },
			{ type: "check", path: "/lock", notExists: true },
			{ type: "create", path: "/log/entry-", sequential: true, data: bytes("1") },
			{ type: "set", path: "/log", data: bytes("head"), version: 0 },
		]);
		expect(result.error).toBe(ErrorCode.ZOK);
		expect(result.results[2]?.pathCreated).toBe("/log/entry-0000000000");

		const stale = await stub.multi(session, [
			{ type: "check", path: "/log", version: 0 },
			{ type: "create", path: "/log/entry-", sequential: true },
		]);
		expect(stale.error).toBe(ErrorCode.ZBADVERSION);
		expect((await stub.exec(session, { type: "list", path: "/log" })).children).toEqual(["entry-0000000000"]);
	});

	it("runs read-only batches and rejects mixed ones", async () => {
		const { stub, session } = await connect();
		await stub.exec(session, { type: "create", path: "/r", data: bytes("v") });

		const reads = await stub.multi(session, [
			{ type: "get", path: "/r" },
			{ type: "get", path: "/absent" },
		]);
		expect(reads.error).toBe(ErrorCode.ZNONODE);
		expect(text(reads.results[0]?.data)).toBe("v");
		expect(reads.results[1]?.error).toBe(ErrorCode.ZNONODE);

		const mixed = await stub.multi(session, [
			{ type: "get", path: "/r" },
			{ type: "remove", path: "/r" },
		]);
		expect(mixed.error).toBe(ErrorCode.ZBADARGUMENTS);
		expect((await stub.exec(session, { type: "exists", path: "/r" })).error).toBe(ErrorCode.ZOK);
	});
});

describe("sessions", () => {
	it("removes ephemeral nodes when the session is closed", async () => {
		const { stub, session } = await connect();
		const other = await stub.createSession(60_000);
		await stub.exec(session, { type: "create", path: "/replicas" });
		await stub.exec(session, { type: "create", path: "/replicas/persistent" });
		await stub.exec(other, { type: "create", path: "/replicas/alive", ephemeral: true });

		expect((await stub.exec(session, { type: "create", path: "/replicas/alive/x" })).error).toBe(
			ErrorCode.ZNOCHILDRENFOREPHEMERALS,
		);
		const ephemeral = await stub.exec(session, {
			type: "list",
			path: "/replicas",
			filter: ListFilter.EPHEMERAL_ONLY,
		});
		expect(ephemeral.children).toEqual(["alive"]);

		await stub.closeSession(other);
		expect((await stub.exec(session, { type: "list", path: "/replicas" })).children).toEqual(["persistent"]);
		expect((await stub.exec(other, { type: "get", path: "/" })).error).toBe(ErrorCode.ZSESSIONEXPIRED);
	});

	it("expires idle sessions through the alarm", async () => {
		const { stub, session } = await connect();
		const idle = await stub.createSession(60_000);
		await stub.exec(idle, { type: "create", path: "/leader", ephemeral: true });

		await runInDurableObject(stub, (_instance, state) => {
			state.storage.sql.exec("UPDATE sessions SET expires_at = 0 WHERE id = ?", idle);
		});
		expect(await runDurableObjectAlarm(stub)).toBe(true);

		expect((await stub.exec(session, { type: "exists", path: "/leader" })).error).toBe(ErrorCode.ZNONODE);
		expect(await stub.heartbeat(idle)).toBe(ErrorCode.ZSESSIONEXPIRED);
		expect(await stub.heartbeat(session)).toBe(ErrorCode.ZOK);
	});
});

describe("watches", () => {
	it("fires one-shot data and child watches, delivered at least once", async () => {
		const { stub, session } = await connect();
		const writer = await stub.createSession(60_000);
		await stub.exec(writer, { type: "create", path: "/w" });

		await stub.exec(session, { type: "get", path: "/w" }, { watch: true });
		await stub.exec(session, { type: "list", path: "/w" }, { watch: true });
		await stub.exec(session, { type: "exists", path: "/w/later" }, { watch: true });

		await stub.exec(writer, { type: "set", path: "/w", data: bytes("1") });
		await stub.exec(writer, { type: "set", path: "/w", data: bytes("2") });
		await stub.exec(writer, { type: "create", path: "/w/later" });

		const first = await stub.pollEvents(session);
		expect(first.events.map(({ type, path }) => ({ type, path }))).toEqual([
			{ type: EventType.CHANGED, path: "/w" },
			{ type: EventType.CREATED, path: "/w/later" },
			{ type: EventType.CHILD, path: "/w" },
		]);
		expect((await stub.pollEvents(writer)).events).toEqual([]);

		/// Not acknowledged yet, so they are returned again.
		expect((await stub.pollEvents(session)).events).toHaveLength(3);
		const last = first.events[first.events.length - 1]!.seq;
		expect((await stub.pollEvents(session, last)).events).toEqual([]);
	});
});

describe("durability", () => {
	it("keeps the tree across eviction", async () => {
		const { stub, session } = await connect();
		await stub.exec(session, { type: "create", path: "/kept", data: bytes("v") });
		await evictDurableObject(stub);
		expect(text((await stub.exec(session, { type: "get", path: "/kept" })).data)).toBe("v");
	});
});

describe("tree", () => {
	it("reads a subtree in one call", async () => {
		const { stub, session } = await connect();
		for (const path of ["/c", "/c/a", "/c/a/x", "/c/b", "/cc"]) {
			await stub.exec(session, { type: "create", path, data: bytes(path) });
		}
		const result = await stub.tree(session, "/c");
		expect(result.error).toBe(ErrorCode.ZOK);
		expect(result.stat?.numChildren).toBe(2);
		expect(result.nodes.every((node) => node.version === 0)).toBe(true);
		expect(result.nodes.map((node) => [node.path, text(node.data)])).toEqual([
			["/c/a", "/c/a"],
			["/c/a/x", "/c/a/x"],
			["/c/b", "/c/b"],
		]);
		expect((await stub.tree(session, "/missing")).error).toBe(ErrorCode.ZNONODE);
	});
});
