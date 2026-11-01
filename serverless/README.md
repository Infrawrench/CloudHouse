# Serverless platform

This directory holds the services that let the engine of CloudHouse run on Cloudflare Workers, and later on AWS
Lambda. The code still has the names of ClickHouse; the rename is the last milestone below.

## Direction

The chosen design is:

- **Cloudflare:** the engine itself runs as WebAssembly inside Worker isolates. It does not run in
  Containers.
- **Tables:** fully writable, with `INSERT`s, background merges and replication.
- **Rename:** a full rename of the code base.

### Constraints that shape everything

Cloudflare's published limits as of 2026-10-31:

| Limit | Value | Consequence |
|---|---|---|
| Memory per isolate, including WebAssembly memory | 128 MB | The existing Emscripten build starts with 2 GiB of linear memory and ~50 MiB of static data (`cmake/target.cmake`). An isolate build must fit query state, code data and buffers well under 128 MB. |
| Worker bundle, uncompressed | 64 MiB | One module cannot hold the whole engine. It is split into roles (below), each linking only what it needs. |
| Startup, global scope | 1 s | Instantiate the module lazily inside the handler, not at the top level. |
| Threads | none | No `pthread`, no `SharedArrayBuffer` workers. Thread pools must run inline, and parallelism comes from fanning out to more isolates. |
| CPU per request | up to 5 min (`limits.cpu_ms`) | Long merges must be bounded or resumable. |
| Subrequests per request | 10,000 (paid) | This caps the fan-out of one query. |

The tree assumes a 64-bit `size_t` throughout, so the build stays on Memory64, as the existing
target does. `workerd` runs Memory64 modules with WebAssembly exceptions (verified 2026-10-31).
It also supports JavaScript Promise Integration (JSPI), verified 2026-11-01, which the engine uses to read parts.

## Architecture

```
                 ┌──────────────────────── Cloudflare ────────────────────────┐
 client ─HTTP──▶ │ gateway Worker ── auth, HTTP protocol, result cache (KV)   │
                 │     │ service bindings (RPC)                               │
                 │     ▼                                                      │
                 │ query Worker(s): engine.wasm, single-threaded, 128 MB      │
                 │     │  fan-out: one sub-isolate per range of parts         │
                 │     ├──▶ R2: part data (columns, marks, indexes)           │
                 │     ├──▶ KV: hot metadata (schemas, part lists, marks)     │
                 │     └──▶ coordination Durable Object (replaces Keeper)     │
                 │ insert Worker: engine.wasm, write path only                │
                 │ merge Worker: Queue consumer, merges and mutations         │
                 └────────────────────────────────────────────────────────────┘
```

Each Worker role is a separate WebAssembly link of the same engine sources, with only the code that
role needs. This is how the code base is broken apart: along execution roles, not along
classes. The roles talk over service bindings and RPC, and partial aggregation results travel
between isolates in `Native` format, as aggregate function states.

| Concern | Cloudflare | AWS |
|---|---|---|
| Engine compute | WebAssembly in Worker isolates | Native Linux build in Lambda (up to 10 GB memory, with real threads) |
| Table data | R2 | S3 |
| Hot metadata cache | KV, Cache API | Lambda memory, S3 |
| Coordination (Keeper) | Durable Object, `packages/coordination` | DynamoDB transactions, same protocol |
| Merges, mutations, TTL | Queues, Cron Triggers | SQS, EventBridge Scheduler |

## Packages

| Package | State |
|---|---|
| `packages/coordination` | **Done.** Keeper semantics on a SQLite-backed Durable Object. |
| `packages/engine` | **Deployed** as `clickhouse-engine-core`. The engine, its pool of `EngineHost` objects and the catalog. Milestones 1, 3 and 4 are done. |
| `packages/gateway` | **Deployed** as `clickhouse-engine`. The public HTTP interface, in front of the engines. |
| `packages/merge-worker` | Milestone 6 |
| `packages/aws` | Milestone 8 |

### `coordination`

It implements the subset of `Coordination::IKeeper` (`src/Common/ZooKeeper/IKeeper.h`) that
`ReplicatedMergeTree` and the DDL queue use, with the same error codes, `Stat` fields and watch
events. The reference semantics are `TestKeeper`:

- `create`, including ephemeral and sequential nodes; `remove`, `set` and `check` with version
  checks; and `check` against a node that must not exist.
- `get`, `exists` and `list`, with the filters `PERSISTENT_ONLY` and `EPHEMERAL_ONLY`.
- `multi`, either an atomic write transaction or a batch of reads. A failed transaction reports
  its errors the same way `TestKeeper` does.
- Sessions that expire through the Durable Object alarm and take their ephemeral nodes with them.
- One-shot data and child watches. Their events are queued per session and delivered at least once
  through `pollEvents`.

There is one object per cluster (`idFromName(cluster)`). A Durable Object processes one request at a
time, which gives the linearizability Keeper gets from Raft. If one cluster's object becomes a
bottleneck, shard it by table path prefix, because replicated tables keep separate subtrees.

Not implemented yet: ACLs, `removeRecursive`, persistent and recursive watches (`addWatch`), and
`reconfig`.

```sh
cd serverless
npm install
npm test                                            # runs inside workerd through @cloudflare/vitest-plugin
cd packages/coordination && npx wrangler deploy
```

Other Workers bind to it with:

```jsonc
"durable_objects": {
  "bindings": [{ "name": "COORDINATION", "class_name": "Coordination", "script_name": "coordination" }]
}
```

```ts
const keeper = env.COORDINATION.get(env.COORDINATION.idFromName("cluster-1"));
const session = await keeper.createSession(30_000);
await keeper.exec(session, { type: "create", path: "/tables", data: new Uint8Array() });
```

### `engine`

The engine is the CMake target `clickhouse-isolate` (`programs/isolate`), built with `-DWASM_ISOLATE=1`. It is
a module with a small C interface instead of `main`: `ch_isolate_init`, then `ch_isolate_query` for every
query. `registerIsolate.cpp` lists what it can do: the interpreters, functions, aggregate functions,
formats, storages and table functions it registers. The functions are linked from a static archive, so only
the ones on the list end up in the module.

The HTTP interface (`src/http.ts`) is a subset of the one of ClickHouse: `GET /ping`, the query in the `query`
parameter or the `POST` body, `default_format`, and exceptions as status 500 with
`X-ClickHouse-Exception-Code`. Every request except `/ping` needs `Authorization: Bearer <token>`:

- `ENGINE_TOKEN` gives full access;
- `ENGINE_READONLY_TOKEN`, if set, runs the query as the user `readonly`, with the setting `readonly` = 2: no
  writes and no DDL, but its settings can change, so that a client can tighten `max_execution_time` or
  `max_result_rows`. The Worker also refuses to commit any change to the catalog from such a query.

The profile pins what an isolate depends on: no query can raise `max_memory_usage` above 64 MiB or change the
settings of threads.

With a `query` parameter, the body is the data that follows it, and it streams to the engine, which reads it as it
parses (`ch_host_read_input`), whatever its size: an `INSERT` writes a part every 262,144 rows. Without one, the body
is the query, at most 16 MiB. A body with `Content-Encoding` (`gzip`, `deflate`, `br`, `zstd`, `lz4`, `xz`, `bz2`) is
decompressed by the engine. A query that fails in the platform rather than in the engine, for example because a
deployment reset its Durable Object, is answered with status 503 and the code 1002, and can be sent again; a query
that takes more than ten minutes, with status 504.

Every response has `Server-Timing`: `host` (the call to the engine), `startup` (a new engine reading its data),
`sync` (the catalog), `load` (parts from R2) and `commit`. A Worker's clock only moves on I/O, so the time the
engine computes is not in it. `memory` and `cache` are not durations but the MiB of memory of the engine and of
the ranges that its isolate keeps.

#### Workers

The module of the engine makes a new isolate of its Worker take about a second to start, and at low traffic
Cloudflare starts a new isolate for most requests. So the queries run elsewhere:

- `clickhouse-engine` is the gateway (`packages/gateway`), 3 KiB, with the HTTP interface. It sends every query
  to one of `ENGINE_POOL` (8) `EngineHost` objects, picked at random, or by `session_id`.
- `clickhouse-engine-core` (`packages/engine`, not public) has the engine. An `EngineHost` (`src/host.ts`) is a
  Durable Object, so it stays in one isolate on one machine and its engine stays warm. It runs queries exactly as
  the Worker would (`src/isolate.ts`), syncing with the catalog first. With `ENGINE_POOL` = 0, the Worker runs
  queries in its own isolate instead.

#### Tables

Tables in the `ObjectMergeTree` engine are durable, and every isolate sees the same ones. `MergeTree` is another
name for it, and the default engine:

```sql
CREATE TABLE events (ts DateTime, id UInt64, kind String)
ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY (kind, ts) TTL ts + INTERVAL 90 DAY
```

- **Parts.** As in `MergeTree`, an `INSERT` makes an immutable part per partition, sorted by `ORDER BY`. A part is
  in R2 as `parts/<name>`, where the name is `<uuid>_<rows>_<bytes>_<partition id>`, and its format is `CHPART03`
  (`programs/isolate/ObjectPart.h`): its columns one after another, each as a chunk per block of at most 16,384 rows,
  in `Native` and compressed with `ZSTD` on its own, then an index with the offset and size of every chunk and the
  minimum and maximum of every column of the sorting key and of the partition key per block. A query reads only the
  chunks of the columns that it needs, and the chunks close to each other in one read. Parts of the earlier format,
  `CHPART02`, whose blocks have all their columns together, are read as before, and merges rewrite them.
- **Reads.** The engine streams parts: it reads the index of a part, then the blocks that the filter of the query
  may need, with range reads that it waits for (JSPI: `ch_host_read_range` in `host.js`), so it holds a block of
  a part at a time rather than the part. Before reading, it tells the Worker what it is going to read, and the
  Worker reads up to 16 MiB ahead, several ranges at once (`src/readahead.ts`). The filter skips parts and blocks
  as `KeyCondition` does for the primary key of `MergeTree`, on the sorting key and the partition key. An isolate
  keeps the last 16 MiB of ranges it read, and the last 4096 indexes.
- **Catalog.** The definitions of the tables and the list of the parts of every table are in a `Coordination`
  object (`packages/coordination`) of the same Worker, under `/catalog`. Before a query, the isolate syncs with the
  catalog: it reads the version of `/catalog`, one round trip, and only reads the rest if the version has changed.
  A query that changes the catalog records its changes; the Worker uploads the new parts and commits them in one
  `multi` transaction:
  - a DDL query commits against the version of `/catalog` that it ran against: if the catalog has moved on, the
    query runs again, so DDL is serializable;
  - an `INSERT`, a merge or a mutation only checks the version of the definition of its table and that the parts it
    replaces are still there, so concurrent inserts do not conflict. An `ALTER` between them makes the `INSERT` run
    again against the new definition.
- **Merges.** After a commit leaves a partition with 8 parts or more, it queues a merge (`clickhouse-engine-merges`).
  The merges of a table run in one `EngineHost` (`merge-<table>`), one at a time, so that they do not choose the
  same parts and lose to each other.
  The consumer merges the smallest parts of a partition, as many as fit in 4 MiB and 10 parts (`src/parts.ts`),
  streaming them through `MergingSortedTransform`, and deletes the replaced parts.
- **TTL.** `TTL <expression>` of the table deletes rows and `TTL <expression>` of a column resets its values to the
  default, as in `MergeTree`: merges apply them, and the hourly Cron Trigger queues a merge of every part of every
  table with a TTL. `ALTER TABLE ... MATERIALIZE TTL` applies them at once. The TTL of a table can only delete rows:
  `GROUP BY`, `WHERE`, `TO DISK` and `RECOMPRESS` are refused.
- **Mutations.** `ALTER TABLE ... DELETE`, `ALTER TABLE ... UPDATE` and `DELETE FROM` rewrite the parts that they
  change one at a time, in the query, so they are synchronous. The columns of the sorting key and of the partition
  key cannot be updated.
- **`ALTER`.** `ADD COLUMN`, `DROP COLUMN`, `MODIFY COLUMN` (type, default, comment) and `COMMENT COLUMN` work
  without rewriting parts: a part written before an `ALTER` is converted when it is read, with the defaults for the
  columns it lacks. As in `MergeTree`, `MODIFY ORDER BY` can only add expressions of columns that the same `ALTER`
  adds; `MODIFY TTL`, `REMOVE TTL`, `MODIFY SETTING` and `RESET SETTING` change the definition. Columns of the keys
  and of the TTL cannot be changed.
- **Settings.** `SETTINGS` takes the settings of `MergeTree`, which are checked as there. `index_granularity` is
  the rows of a block of a part (16,384 without it, at most 262,144): smaller blocks make point queries read less,
  and it cannot change. The other settings are kept, for `SHOW CREATE TABLE`, and do nothing: a part is one object
  in R2, with nothing else to tune. `disk` and `storage_policy` are refused.
- **Data skipping indexes.** `INDEX <name> <expression> TYPE minmax`, `set`, `bloom_filter`, `ngrambf_v1` and
  `tokenbf_v1` have a granule per block of a part, made and checked by the index types of `MergeTree`, so a query
  skips the blocks that an index rules out. `ALTER TABLE ... ADD INDEX` covers the parts written after it, and
  `MATERIALIZE INDEX` rewrites the others. `text` and `vector_similarity` indexes keep more than one stream per
  granule, and are refused.
- **Constraints and projections.** `CONSTRAINT ... CHECK` is checked on `INSERT`, and `ADD`/`DROP CONSTRAINT` work.
  `PROJECTION` is accepted, with `ADD`, `DROP`, `MATERIALIZE` and `CLEAR`, so that tables of `MergeTree` can be
  created as they are, but the parts have no projections: a query reads the table, and its result is the same.
- **Partitions.** `DROP PARTITION`, `DETACH PARTITION` and `ATTACH PARTITION` (by value, by `ID`, or a `PART` by its
  name), `DROP DETACHED PARTITION` (with `allow_drop_detached`), `ATTACH PARTITION ... FROM`, `REPLACE PARTITION ...
  FROM` and `MOVE PARTITION ... TO TABLE`, as in `MergeTree`. A detached part stays in the catalog, marked
  `detached`, and in `system.detached_parts`. The parts of another table are shared rather than copied: a part
  never changes, so `ATTACH`, `REPLACE` and `MOVE PARTITION` are one commit of the catalog, which checks that the
  other table still has the parts, and take the time of a small query whatever the size of the partition (0.5 s for
  6 million rows in 37 parts, 3.3 s for 23 million rows in 112 parts). A part is deleted from R2 when no table has
  it. A table has a part once, so attaching the same parts twice is refused. `FREEZE` and `FETCH` are refused: there is no file system to freeze into, and
  no replicas to fetch from.
- **Databases.** `CREATE DATABASE` (the engine `Atomic`, the default) puts the database in the catalog, so every
  isolate has it, and `DROP DATABASE` drops its tables, then it. `ENGINE = Memory` makes a database of one engine.
- **`RENAME TABLE` and `EXCHANGE TABLES`** move tables, with their parts, to new names or other databases, in one
  commit.
- **`system.parts`** lists the parts of every table, with their partition (`partition`, for the parts written since
  2026-11-01, and `partition_id`), rows and bytes. The virtual columns `_part` and `_partition_id` give the part of
  each row. `system.settings`, `functions`, `formats`, `table_engines`, `table_functions`, `data_type_families`,
  `aggregate_function_combinators` and `time_zones` are there too.
- **Reads during writes.** A shard runs on the parts that its coordinator gives it, with the same definition of the
  table, at whatever version of the catalog it has: parts never change, so writes to other partitions and tables
  meanwhile do not make a read run again.
- **Fan-out.** A query that reads one table alone, and that needs more than 4 MiB of its blocks after the filter
  skips what it can, fans out. It is processed by shards up to `WithMergeableState`, as for a `Distributed` table,
  and finished by the engine that got it. Each shard is an `EngineHost` (`shard-<n>`) that reads about 4 MiB of the
  parts, at most 16 shards. `ObjectMergeTree` builds the query for the shards as `StorageDistributed` does, the
  shards return their blocks in `Native`, and the queries that the planner splits (aggregation, `ORDER BY ...
  LIMIT`, `DISTINCT`) come out as on a cluster. A query that joins the table with another reads it all in one
  engine, and a point query reads its few blocks in one engine.
- **Caching.** Engines read parts through `PartCache` (`src/cache.ts`), an entrypoint of the Worker behind Workers
  Caching, which works on `workers.dev` and in front of loopback calls: a part never changes, so an engine reads it
  from the cache of its data center if another engine there has read it. The catalog is one atomic read
  (`Coordination.tree`), and every query checks its version, one round trip, so it needs no cache: a cache of
  snapshots in KV would have to be keyed by the version, and every commit makes a new one, which every data center
  would miss once.
- **Memory.** A query can use up to 72 MiB of the memory of an engine, and fails with `MEMORY_LIMIT_EXCEEDED`
  beyond that. Reads and merges stream, so this limits the state of a query (a hash table of `GROUP BY`, a sort, the
  right side of a `JOIN`), not the size of a table. A query puts each part it writes in R2 as soon as it is written,
  and holds none of them until it commits. The ranges of parts that the Worker reads ahead (6 MiB) and caches (4
  MiB), and the runs of blocks that it reads at once (2 MiB), are small, because they share the 128 MB of the
  isolate with the engine, which may grow to 100 MiB. Before, a large `INSERT ... SELECT` could exceed the memory of
  the isolate, which Cloudflare then resets. `OPTIMIZE TABLE ... FINAL` of 88 MiB in 16 partitions takes 26 s.
  `uniqExact` of 2 million distinct strings is beyond it, while
  `uniq` is not.
- **Garbage.** An hourly Cron Trigger (`src/garbage.ts`) deletes the parts in R2 that no table has and that are
  older than an hour: the parts of a commit that failed and were not deleted then. A bucket belongs to one catalog.

The engine side is `programs/isolate/ObjectMergeTree.cpp` and `ObjectPart.cpp`, the Worker side `src/database.ts`,
`src/catalog.ts`, `src/readahead.ts` and `src/parts.ts`. Not supported: `SAMPLE BY`, the other engines of the family
(`ReplacingMergeTree`, `SummingMergeTree`, ...), `text` and `vector_similarity` indexes, and the uses of projections
by queries.

Tables in `Memory` belong to one engine. With a pool, a session (`session_id`) always goes to the same engine,
so its `Memory` tables and settings stay with it.

Load test, 2026-11-01, on the deployment from one client, 30 s per scenario after a warm-up, `ENGINE_POOL` = 8. `big`
is 2 million rows in 8 parts, `events` 750,000 rows in 3 partitions, and the inserts are of one row each into `writes`:

| Scenario | Concurrency | Requests/s | p50 ms | p90 ms | p99 ms | Errors |
|---|---|---|---|---|---|---|
| Point query on `big` (`WHERE id = ...`) | 10 | 124.8 | 65 | 121 | 370 | 0 |
| `GROUP BY` over one day of `events` | 10 | 131.2 | 57 | 109 | 342 | 0 |
| `GROUP BY` of all of `big`, fanned out | 5 | 12.3 | 369 | 592 | 2374 | 0 |
| `INSERT` | 5 | 23.3 | 197 | 283 | 530 | 0 |
| `INSERT` | 20 | 59.5 | 276 | 550 | 1183 | 0 |
| Mixed: points, aggregations, inserts | 25 | 78.2 | 134 | 901 | 2638 | 0 |
| Point query on `big` | 50 | 173.6 | 108 | 1024 | 2068 | 0 |

No request in these runs started an engine. The tail at high concurrency is queueing: an engine runs one query at a
time, so 50 clients on 8 engines wait for each other. Before the point queries stopped fanning out, the same
point query at concurrency 10 did 47 requests/s with a p90 of 775 ms, and before inserts stopped conflicting, 17%
of the inserts at concurrency 20 failed.

Shards, 2026-11-01: `bench`, 8 million rows (88 MiB) in 16 parts of 5.5 MiB, a `GROUP BY` of the whole table and a
filtered scan, 45 s per setting after a warm-up, from one client. Each shard reads whole parts, so a table cannot
have more shards than parts. Latencies are of the requests that did not start an engine:

| Shards | Concurrency 1: p50 / p90 ms | Concurrency 4: requests/s, p50 / p90 ms |
|---|---|---|
| none (one engine reads it all) | 1945 / 2400 | 1.3, 1962 / 4303 |
| 4 | 584 / 985 | 1.8, 1429 / 2942 |
| 8 | 455 / 821 | 3.2, 722 / 1538 |
| 16 | 436 / 1461 | 4.7, 623 / 1071 |

One query gains little past 8 shards, while 16 shards serve concurrent queries best: each reads less, so the shards
are busy for less time. The defaults, 4 MiB per shard and at most 16, give this table 16 shards; a smaller
`SHARD_BYTES` would fan out tables of a few MiB, which an engine reads in about 100 ms itself.

Formats, 2026-11-01, on `cloudhouse-instances`: the AWS price list, 7.1 million rows of 36 columns in 27 partitions
that are buckets of a hash of `instance_type`, queries as its chat writes them, on one warm engine:

| Query | `CHPART02` | `CHPART03` | `CHPART03`, `bloom_filter` on `instance_type` |
|---|---|---|---|
| Prices of one instance type in one region | 0.25 s | 0.36 s | 0.17 s |
| The cheapest arm64 instance with 16 vCPUs in a region | 0.72 s | 0.72 s | 0.40 s |
| The average price per family in a region | 0.73 s | 0.30 s | |
| The cheapest PostgreSQL RDS instance | 0.69 s | 0.36 s | |
| `count()` per platform | 0.49 s | 0.20 s | |
| The cheapest GPU instances in a region | 0.83 s | 0.39 s | 0.42 s |

A query that aggregates reads only its columns, 2 to 2.4 times faster. A query of one instance type reads a block of
every partition, since the partitions are by a hash of it, unless a data skipping index on it skips the others.

Tables in the `Memory` engine are scratch space: they last as long as the isolate does, and only it sees them.

The static data of the engine is about 12 MiB. The build moves it out of the module into a static asset,
`clickhouse-isolate.data` (`scripts/split-data.mjs`), because only the module counts against the size
limit. The engine copies it into its memory before its constructors run.

```sh
# Arrow runs `flatc` during the build, which has to run on the host: build it with the host compiler.
clang++ -std=c++17 -O2 -DFLATBUFFERS_LOCALE_INDEPENDENT=0 -Icontrib/flatbuffers/include -Icontrib/flatbuffers \
    -Icontrib/flatbuffers/grpc -o tmp/host-flatc/flatc <the sources of `flatc` in contrib/flatbuffers-cmake>
emcmake cmake -S . -B build_wasm_isolate -G Ninja -DCMAKE_BUILD_TYPE=MinSizeRel \
    "-DCMAKE_C_FLAGS_MINSIZEREL=-Oz -DNDEBUG" "-DCMAKE_CXX_FLAGS_MINSIZEREL=-Oz -DNDEBUG" \
    -DWASM_ISOLATE=1 -DENABLE_LIBRARIES=0 -DENABLE_THINLTO=0 -DSANITIZE= -DENABLE_TESTS=0 \
    -DENABLE_UTILS=0 -DENABLE_CLICKHOUSE_ALL=0 \
    -DENABLE_SIMDUTF=1 -DENABLE_AVRO=1 -DENABLE_MSGPACK=1 -DENABLE_SNAPPY=1 -DENABLE_THRIFT=1 \
    -DENABLE_BROTLI=1 -DENABLE_BZIP2=1 -DENABLE_RAPIDJSON=1 -DENABLE_PARQUET=1 -DHOST_FLATC=$PWD/tmp/host-flatc/flatc
ninja -C build_wasm_isolate clickhouse-isolate
cd serverless/packages/engine
npm run build    # links the engine without dead code, copies it into dist and checks the limits
npm test         # runs inside workerd through @cloudflare/vitest-plugin

# Deploying: the bucket once, then the engine before the gateway, which binds to its `EngineHost`.
npx wrangler r2 bucket create clickhouse-engine-parts
npx wrangler deploy
cd ../gateway && npx wrangler deploy
npx wrangler secret put ENGINE_TOKEN             # on the gateway
npx wrangler secret put ENGINE_READONLY_TOKEN    # optional

# Another deployment, with tables of its own: the environment `instances` of both `wrangler.jsonc` files has its own
# Workers, Durable Objects, bucket and queue, and a custom domain.
npx wrangler r2 bucket create cloudhouse-instances-parts && npx wrangler queues create cloudhouse-instances-merges
npx wrangler deploy --env instances && (cd ../gateway && npx wrangler deploy --env instances)
```

What the isolate build changes, under `OS_WASM_ISOLATE`:

- No `-pthread` and no stack protector. Thread pool jobs run inline on the calling thread, and creating a
  thread (`ThreadFromGlobalPool`) throws `CANNOT_SCHEDULE_TASK`, because the body of a thread may wait for
  its creator. `BackgroundSchedulePool` starts no threads.
- `MAX_CPUS` is 1, and the descriptions of settings are left out.
- The two-level variants of aggregation and of hash joins are left out: they exist to aggregate, merge and join
  in parallel. A `GROUP BY` stays single-level however many keys it has, and `join_algorithm = 'parallel_hash'` is
  `hash`, since there is one thread to run it. The macros that instantiate the variants skip them
  (`APPLY_FOR_AGGREGATED_VARIANTS`, `APPLY_FOR_JOIN_VARIANTS`), and the switches over them end with a default
  that throws. The isolate build silences the warnings about the code this leaves unused.
- The default profile runs every query on one thread, with `max_memory_usage` at 64 MiB.
- `MergeTreeBackgroundExecutor` starts no worker loops: they would run inline and never return. Projections
  create one to check their settings.
- Arrow is built without `ARROW_ENABLE_THREADING`, so it runs its tasks serially, and the reader of Parquet
  decodes inline (`max_parsing_threads` = 1) without prefetching row groups.

**Formats.** Besides the usual text formats, the engine has `Parquet`, `Arrow`, `Avro`, `MsgPack`, `BSONEachRow`,
`TSKV`, the `JSON*` family, `Template`, `CustomSeparated`, `Regexp`, `RawBLOB`, `LineAsString`, `Npy`, `XML`,
`Markdown`, `SQLInsert`, `Prometheus` and others, with schema inference. `ORC`, `Protobuf` and `CapnProto` need a
`protoc` or a `capnp` that runs on the host, and are not there. The `base64`, `base32` and `base58` functions are.

**Dead code.** The engine with these libraries is 65.5 MiB of code, over the limit of a Worker, and much of it is
code that an isolate never runs: `MergeTree` itself, `ReplicatedMergeTree`, `Distributed`, Keeper, backups and the
object storages, which the query planner and other code refer to. `programs/isolate/cut-dead-code.py` links the
module without the objects that `programs/isolate/dead-code.txt` lists, and gives every symbol that they defined and
the rest refers to a stub that throws `NOT_IMPLEMENTED` (`ch_isolate_cut_code`), or to zeroed data. That leaves
59.9 MiB. A stub that a query reaches names its number in `stubs.txt` of the build; the object that defined it then
goes off the list.

## Milestones

Baseline, measured 2026-10-31 with the existing `Build (wasm64)` configuration (emsdk 6.0.5, the
same flags as `ci/jobs/build_clickhouse.py`, target `clickhouse`, Node v24.21.0): `clickhouse.wasm`
is 258 MiB (63 MiB with `gzip -9`), and `clickhouse local` running the CI smoke query peaks at
2585 MiB of RSS with `max_threads` = 10. That is 4x the Workers bundle limit and about 20x the
isolate memory limit. Milestone 1 has to close both gaps.

Measured 2026-11-01 with `clickhouse-isolate`: 58.5 MiB of code (60,136 KiB in `wrangler deploy`, against
65,536 KiB) and 11.9 MiB of static data in the asset. With Parquet, Arrow, Avro and the other formats, and without
the dead code: 59.9 MiB of code (61,509 KiB) and 13.9 MiB of static data. The linear memory starts at 48 MiB, and stops at 100 MiB
so that the isolate stays under 128 MB. In Node, compiling the module takes about 80 ms and starting the engine
about 90 ms. A warm `SELECT 1` takes 0.3 ms, and summing a million numbers takes 2 ms.

Through the gateway, from a client in the same region: about 1 s for the first query of an `EngineHost`, then
70 to 150 ms per query, of which about 15 to 25 ms is the sync with the catalog. Before the gateway, about half
of the queries started a new isolate and took 0.5 to 1.3 s.

1. **Isolate build profile.** Add an Emscripten profile that has no `-pthread`, no
   `PROXY_TO_PTHREAD`, a small initial memory and a hard maximum under 128 MB, and a minimal feature
   set. Build it in CI or a remote builder, because a full build does not fit on a laptop with
   little free disk. Measure the module size, idle memory and instantiation time against the
   limits above.
2. **Host I/O interface.** **Done** with JSPI rather than an `IObjectStorage`: the engine asks the Worker for
   ranges of parts and waits for them (`ch_host_read_range`), and the Worker commits the changes that a query
   records (see `ObjectMergeTree`). `workerd` has JSPI (`WebAssembly.Suspending` and `WebAssembly.promising`), with
   memory64 and exceptions. The plan was:
   - `HostObjectStorage`, an `IObjectStorage` implementation backed by R2 bindings.
   - `HostKeeper`, an `IKeeper` implementation that speaks the `coordination` protocol.
   - KV reads and writes.
3. **Single-threaded execution.** **Done.** Run thread pools inline. Drive the background schedule pool from
   host calls, such as alarms and queue messages, instead of threads.
4. **MergeTree on shared storage.** Keep part data in R2 and the part list in `coordination`, so
   that disposable isolates are not replicas of their own. **Done** as `ObjectMergeTree` (see above), with
   partitions, a min-max index per block and `ALTER` of columns, without secondary indexes.
5. **Fan-out queries and the gateway.** **Done** (see `ObjectMergeTree`). A coordinator isolate splits the parts among sub-isolates
   and combines their aggregation states, reusing the distributed query machinery. The gateway
   Worker serves the HTTP interface.
6. **Background work.** **Done** for merges and TTL; mutations run in the query. Merges and mutations run as Queue consumers, with part size capped so that
   each merge fits one isolate's memory and CPU limits. TTL runs from Cron Triggers.
7. **Caching.** **Done** with Workers Caching for parts, and one atomic read of the catalog instead of KV (see
   `ObjectMergeTree`). Keep schemas, part lists, marks and primary indexes in KV, and hot column ranges in
   the Cache API.
8. **AWS Lambda.** Deploy the native build as a container image behind a function URL, with S3
   storage and a DynamoDB backend for the `coordination` protocol. The DynamoDB backend is limited
   to 100 items per `TransactWriteItems`, which bounds the size of a `multi`. Deploy with CDK.
9. **Rename.** Do the full rename last, as a scripted, mechanical change, so upstream fixes can
   still be merged while the port is in progress.
