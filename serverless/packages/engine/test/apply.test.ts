import { env } from "cloudflare:workers";
import { describe, expect, it } from "vitest";
import { Catalog, type Snapshot, tableKey } from "../src/catalog";
import { Database, type Io } from "../src/database";
import type { CatalogChange, Engine } from "../src/engine";

/// What `Database` tells the engine, without the engine: the parts each table was last given. `sync` and `apply` are
/// what decide them, and neither runs a query.
function fakeEngine() {
	const parts = new Map<string, string[]>();
	const engine = {
		memoryBytes: 0,
		async hostQuery() {},
		async setParts(database: string, table: string, list: readonly string[]) {
			parts.set(tableKey(database, table), [...list]);
		},
		async setFanOut() {},
		async changes() {
			return [];
		},
		async clearChanges() {},
	};
	return { engine: engine as unknown as Engine, parts };
}

/// The private steps of a query that the race is between.
interface Steps {
	state: Snapshot;
	sync(): Promise<void>;
	exclusive<T>(what: string, f: () => Promise<T>): Promise<T>;
	commitParts(snapshot: Snapshot, changes: CatalogChange[], written: string[], io: Io): Promise<boolean>;
}

const io: Io = {
	background() {},
	stats: { pages: 0, bytes: 0, hits: 0 },
	async readRange() {
		throw new Error("No part is read");
	},
	async runShard() {
		throw new Error("No query fans out");
	},
	async scheduleMerge() {},
};

const insert = (part: string): CatalogChange => ({
	kind: "replace_parts",
	database: "d",
	table: "t",
	added: part,
	removed: [],
	detached: [],
	attached: [],
	dropped_detached: [],
	shared: [],
	shared_from: ["", ""],
});

describe("applying a commit of parts", () => {
	it("lists a part once when a query syncs the engine before the insert that committed it applies it", async () => {
		/// Holds the next commit once it is in the catalog, before the engine that made it applies it.
		let hold: { landed: () => void; release: Promise<void> } | undefined;
		class HeldCatalog extends Catalog {
			override async commit(...args: Parameters<Catalog["commit"]>) {
				const version = await super.commit(...args);
				const held = hold;
				hold = undefined;
				if (held !== undefined) {
					held.landed();
					await held.release;
				}
				return version;
			}
		}
		const id = env.COORDINATION.newUniqueId();
		const catalog = new HeldCatalog(() => env.COORDINATION.get(id));
		const { engine, parts } = fakeEngine();
		const db = new Database(engine, catalog, env.PARTS) as unknown as Steps;
		const sync = () => db.exclusive("query", () => db.sync());

		await sync();
		await catalog.commit(db.state, [
			{ kind: "create_table", database: "d", table: "t", create_query: "CREATE TABLE d.t (id UInt64) ENGINE = ObjectMergeTree ORDER BY id", ttl: false },
		]);
		await sync();
		expect(await db.commitParts(db.state, [insert("p1")], [], io)).toBe(true);
		expect(parts.get(tableKey("d", "t"))).toEqual(["p1"]);

		let release!: () => void;
		const landed = new Promise<void>((resolve) => {
			hold = { landed: resolve, release: new Promise<void>((done) => (release = done)) };
		});
		const committed = db.commitParts(db.state, [insert("p2")], [], io);
		await landed;
		/// The insert has given up the lock: another query syncs the engine to its commit.
		await sync();
		expect(parts.get(tableKey("d", "t"))).toEqual(["p1", "p2"]);
		release();
		expect(await committed).toBe(true);

		/// Nothing has committed since, so no later sync would correct a part listed twice.
		expect(parts.get(tableKey("d", "t"))).toEqual(["p1", "p2"]);
		expect(db.state.tables.get(tableKey("d", "t"))?.parts).toEqual(["p1", "p2"]);
		await sync();
		expect(parts.get(tableKey("d", "t"))).toEqual(["p1", "p2"]);
	});

	it("still applies a commit when the engine has not synced past it", async () => {
		const id = env.COORDINATION.newUniqueId();
		const catalog = new Catalog(() => env.COORDINATION.get(id));
		const { engine, parts } = fakeEngine();
		const db = new Database(engine, catalog, env.PARTS) as unknown as Steps;
		await db.exclusive("query", () => db.sync());
		await catalog.commit(db.state, [
			{ kind: "create_table", database: "d", table: "t", create_query: "CREATE TABLE d.t (id UInt64) ENGINE = ObjectMergeTree ORDER BY id", ttl: false },
		]);
		await db.exclusive("query", () => db.sync());
		const before = db.state.version;
		expect(await db.commitParts(db.state, [insert("p1")], [], io)).toBe(true);
		expect(parts.get(tableKey("d", "t"))).toEqual(["p1"]);
		expect(db.state.version).toBe(before + 1);
	});
});
