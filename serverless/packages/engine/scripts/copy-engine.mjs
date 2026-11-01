/// Copies the engine from the build directory of the `clickhouse-isolate` target into `dist`, moves its static
/// data into a static asset (see `split-data.mjs`), and checks the result against the limits of a Worker. The engine
/// is the one that `programs/isolate/cut-dead-code.py` links, without the code that an isolate never runs.
///
///   node scripts/copy-engine.mjs [build directory]
///
/// The build directory defaults to `$CLICKHOUSE_ISOLATE_BUILD_DIR`, then to `build_wasm_isolate` at the root of
/// the repository.

import { copyFileSync, mkdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { splitData } from "./split-data.mjs";

const package_dir = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const build_dir = resolve(
	process.argv[2] ?? process.env.CLICKHOUSE_ISOLATE_BUILD_DIR ?? join(package_dir, "../../../build_wasm_isolate"),
);
const dist_dir = join(package_dir, "dist");
const assets_dir = join(dist_dir, "assets");

/// Workers counts the uncompressed size of everything that is uploaded with the script, but not static assets,
/// which have a limit per file.
const worker_size_limit = 64 * 1024 * 1024;
const asset_size_limit = 25 * 1024 * 1024;

const mib = (size) => `${(size / 1024 / 1024).toFixed(1)} MiB`;

mkdirSync(assets_dir, { recursive: true });
const engine_dir = join(build_dir, "programs/isolate/cut");

copyFileSync(join(engine_dir, "clickhouse-isolate.mjs"), join(dist_dir, "clickhouse-isolate.mjs"));
const { module, data, segments } = splitData(readFileSync(join(engine_dir, "clickhouse-isolate.wasm")));
writeFileSync(join(dist_dir, "clickhouse-isolate.wasm"), module);
writeFileSync(join(assets_dir, "clickhouse-isolate.data"), data);

const worker_size = statSync(join(dist_dir, "clickhouse-isolate.mjs")).size + module.length;
console.log(`clickhouse-isolate.wasm: ${mib(module.length)} of code`);
console.log(`clickhouse-isolate.data: ${mib(data.length)} in ${segments} segments, limit ${mib(asset_size_limit)}`);
console.log(`worker: ${mib(worker_size)}, limit ${mib(worker_size_limit)}`);
if (worker_size > worker_size_limit || data.length > asset_size_limit) {
	console.error("The engine does not fit in the limits of a Worker");
	process.exitCode = 1;
}
