# CloudHouse

CloudHouse is ClickHouse® running on Cloudflare Workers: the engine itself, compiled to WebAssembly, runs inside
Worker isolates, and its tables live in R2. There are no servers, containers or clusters to run. A query is an HTTP
request, tables are durable and writable, and large queries fan out over many isolates.

It is a fork of [ClickHouse](https://github.com/ClickHouse/ClickHouse). The SQL, the functions, the formats and the
query engine are those of ClickHouse; what changes is where it runs and how tables are stored.

## What works

- **SQL.** The analyzer and the planner of ClickHouse, with the common functions and aggregate functions, joins,
  subqueries, window functions, `WITH`, `ARRAY JOIN`, and `SET`/`SETTINGS`.
- **Durable tables.** `CREATE TABLE ... ENGINE = MergeTree` (the default engine) stores parts in R2 and a catalog in
  a Durable Object, which every isolate shares:
  - `ORDER BY`, `PARTITION BY`, `PRIMARY KEY`, `TTL` of rows and of columns, `SETTINGS`, `CONSTRAINT`;
  - data skipping indexes (`minmax`, `set`, `bloom_filter`, `ngrambf_v1`, `tokenbf_v1`);
  - `INSERT`, background merges, `ALTER TABLE ... DELETE` and `UPDATE`, `DELETE FROM`;
  - `ALTER` of columns, of the sorting key, of the TTL and of settings;
  - `DROP`, `DETACH`, `ATTACH`, `REPLACE` and `MOVE` of partitions;
  - `RENAME TABLE`, `EXCHANGE TABLES`, `TRUNCATE`, databases;
  - `system.parts`, `system.detached_parts`.

  Queries skip parts and blocks by the sorting key, the partition key and the indexes, and stream the rest from R2.
- **Fan-out.** A query that reads more than a few MiB of a table is processed by up to 16 shards, each an isolate
  that reads part of the table, as a `Distributed` table would, and finished by the isolate that got it.
- **Formats.** `Parquet`, `Arrow`, `Avro`, `MsgPack`, `Native`, `RowBinary`, `CSV`, `TabSeparated`, the `JSON`
  family, `Values`, `Pretty`, `Markdown`, `XML` and others, in and out, with schema inference.
- **HTTP interface.** The query in the URL or the body, `default_format`, `session_id`, and errors as in ClickHouse.
  Requests need a bearer token, with an optional read-only token.

What is not there: the other engines of the `MergeTree` family (`ReplacingMergeTree`, `SummingMergeTree`, ...),
`SAMPLE BY`, the use of projections by queries, `text` and `vector_similarity` indexes, `ORC`, `Protobuf` and
`CapnProto`, table functions that read from the network or from files, and dictionaries.

## Limits

A Worker isolate has 128 MB of memory and no threads, and a Worker is at most 64 MiB of code, so:

- a query can use up to 72 MiB of memory for its state: a hash table of `GROUP BY`, a sort, the right side of a
  `JOIN`. Reads, merges and fan-out stream, so this does not limit the size of a table;
- every isolate runs one query at a time, on one thread: concurrency comes from more isolates;
- the engine is linked without what an isolate cannot use: `MergeTree` on disks, replication, Keeper, clusters,
  object storages, backups.

## Performance

Measured on Cloudflare on 2026-11-01, from one client, with warm isolates:

| Query | Throughput | Latency |
|---|---|---|
| Point query on 2 million rows | 125 requests/s at 10 clients | p50 65 ms, p90 121 ms |
| `GROUP BY` over one day of 750,000 rows | 131 requests/s at 10 clients | p50 57 ms |
| `INSERT` of one row | 60 requests/s at 20 clients | p50 276 ms |
| `GROUP BY` over 8 million rows (88 MiB), 16 shards | 4.7 requests/s at 4 clients | p50 623 ms |

## Using it

```sh
curl -H "Authorization: Bearer $TOKEN" https://<your deployment>/ \
    --data-binary "CREATE TABLE events (ts DateTime, kind String, v Float64) ORDER BY (kind, ts)"
curl -H "Authorization: Bearer $TOKEN" "https://<your deployment>/?query=INSERT%20INTO%20events%20FORMAT%20Parquet" \
    --data-binary @events.parquet
curl -H "Authorization: Bearer $TOKEN" https://<your deployment>/ \
    --data-binary "SELECT kind, count(), avg(v) FROM events GROUP BY kind FORMAT JSON"
```

Any ClickHouse client that speaks the HTTP interface works the same way.

## Building and deploying

The engine is the CMake target `clickhouse-isolate` (`programs/isolate`), built with Emscripten for 64-bit
WebAssembly. The Workers are in `serverless/`: `packages/gateway` (the HTTP interface), `packages/engine` (the
engine, the catalog, the shards and the merges) and `packages/coordination` (a Keeper replacement on a Durable
Object). [`serverless/README.md`](serverless/README.md) has the commands to build, test and deploy, the design, and
what the WebAssembly build changes in ClickHouse.

## Status

CloudHouse runs on Cloudflare today. An AWS Lambda target, with the native build, S3 and DynamoDB, is planned. The
code still has the names of ClickHouse; the rename comes last, so that the fork can keep merging from ClickHouse.

## License

Apache License 2.0, as ClickHouse. ClickHouse® is a registered trademark of ClickHouse, Inc. CloudHouse is not
affiliated with or endorsed by ClickHouse, Inc.
