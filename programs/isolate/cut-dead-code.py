#!/usr/bin/env python3
"""Links `clickhouse-isolate` without the code that an isolate never runs.

The isolate registers few storages, but the code it keeps refers to much more: the query planner refers to
`ReadFromMergeTree`, `executeQuery` to `QueryFuzzer`, the backups to `StorageMergeTree`, and so on, so the linker
keeps `MergeTree`, `ReplicatedMergeTree`, `Distributed`, Keeper and the object storages, which an isolate cannot
have. This links the module from the archives without the objects that `dead-code.txt` lists, and gives every symbol
that they defined and the rest refers to a stub: a function that throws `NOT_IMPLEMENTED` (`ch_isolate_cut_code` in
`isolate.cpp`), or zeroed data. A stub is never reached, because what refers to it is about tables that an isolate
does not have; if it is, the query fails with that exception.

    cut-dead-code.py <build directory>

writes `programs/isolate/cut/clickhouse-isolate.{mjs,wasm}` in the build directory.
"""

import os
import re
import shlex
import struct
import subprocess
import sys
from pathlib import Path

SOURCE_DIR = Path(__file__).resolve().parent
DATA_STUB_SIZE = 512


def leb(data, i):
    result = shift = 0
    while True:
        byte = data[i]
        i += 1
        result |= (byte & 0x7F) << shift
        shift += 7
        if byte < 0x80:
            return result, i


VALUE_TYPES = {0x7F: "i32", 0x7E: "i64", 0x7D: "f32", 0x7C: "f64", 0x7B: "v128", 0x70: "funcref", 0x6F: "externref", 0x69: "exnref"}


def imported_functions(data):
    """The functions that a WebAssembly object imports, which are its undefined functions: name -> (params, results)."""
    if data[:4] != b"\0asm":
        return {}
    types = []
    result = {}
    i = 8
    while i < len(data):
        section = data[i]
        size, i = leb(data, i + 1)
        end = i + size
        if section == 1:
            count, j = leb(data, i)
            for _ in range(count):
                assert data[j] == 0x60
                params_count, j = leb(data, j + 1)
                params = [VALUE_TYPES[t] for t in data[j : j + params_count]]
                j += params_count
                results_count, j = leb(data, j)
                results = [VALUE_TYPES[t] for t in data[j : j + results_count]]
                j += results_count
                types.append((params, results))
        elif section == 2:
            count, j = leb(data, i)
            for _ in range(count):
                length, j = leb(data, j)
                j += length
                length, j = leb(data, j)
                field = data[j : j + length].decode()
                j += length
                kind = data[j]
                j += 1
                if kind == 0:
                    index, j = leb(data, j)
                    result[field] = types[index]
                elif kind == 1:
                    j += 1
                    flags, j = leb(data, j)
                    _, j = leb(data, j)
                    if flags & 1:
                        _, j = leb(data, j)
                elif kind == 2:
                    flags, j = leb(data, j)
                    _, j = leb(data, j)
                    if flags & 1:
                        _, j = leb(data, j)
                elif kind == 3:
                    j += 2
                elif kind == 4:
                    j += 1
                    _, j = leb(data, j)
        i = end
    return result


def archive_members(path):
    """The contents of the members of a GNU archive."""
    data = Path(path).read_bytes()
    assert data[:8] == b"!<arch>\n", path
    i = 8
    while i < len(data):
        header = data[i : i + 60]
        size = int(header[48:58].decode().strip())
        yield data[i + 60 : i + 60 + size]
        i += 60 + size + (size & 1)


def run(command, **kwargs):
    return subprocess.run(command, check=True, text=True, capture_output=True, **kwargs).stdout


def main():
    build = Path(sys.argv[1]).resolve()
    out = build / "programs/isolate/cut"
    out.mkdir(parents=True, exist_ok=True)
    patterns = [
        re.compile(line.strip())
        for line in (SOURCE_DIR / "dead-code.txt").read_text().splitlines()
        if line.strip() and not line.startswith("#")
    ]
    bin_dir = Path(os.environ["HOME"]) / "emsdk/upstream/bin"

    link = run(["ninja", "-C", str(build), "-t", "commands", "clickhouse-isolate"]).strip().splitlines()[-1]
    # CMake wraps the command of a link as `: && <command> && :`.
    arguments = shlex.split(next(piece for piece in link.split("&&") if " -o " in piece))

    # The archives of the build without the dead objects.
    cut_arguments = []
    kept_archives = []
    removed = 0
    for argument in arguments:
        if argument.endswith(".a") and not argument.startswith("/") and (build / argument).exists():
            query = run(["ninja", "-C", str(build), "-t", "query", argument])
            objects = [line.strip() for line in query.splitlines() if line.strip().endswith(".o")]
            kept = [o for o in objects if not any(p.search(o) for p in patterns)]
            removed += len(objects) - len(kept)
            if len(kept) != len(objects):
                archive = out / argument.replace("/", "_")
                archive.unlink(missing_ok=True)
                run([str(bin_dir / "llvm-ar"), "qcs", str(archive), *kept], cwd=build)
                argument = str(archive)
            kept_archives.append(argument)
        cut_arguments.append(argument)
    output = cut_arguments.index("-o") + 1
    cut_arguments[output] = str(out / "clickhouse-isolate.mjs")

    # The symbols that only the dead objects defined.
    attempt = subprocess.run(
        [*cut_arguments, "-Wl,--no-demangle", "-Wl,--error-limit=0"], cwd=build, text=True, capture_output=True
    )
    undefined = sorted(set(re.findall(r"undefined symbol: (\S+)", attempt.stderr)))
    if attempt.returncode != 0 and not undefined:
        sys.exit(attempt.stderr)

    signatures = {}
    for archive in kept_archives:
        for member in archive_members(build / archive if not archive.startswith("/") else archive):
            for name, signature in imported_functions(member).items():
                signatures.setdefault(name, signature)

    stubs = ["\t.functype ch_isolate_cut_code (i32) -> ()"]
    functions = data = 0
    for index, name in enumerate(undefined):
        if name in signatures:
            params, results = signatures[name]
            stubs += [
                f"\t.section .text.{name},\"\",@",
                f"\t.globl {name}",
                f"\t.type {name},@function",
                f"{name}:",
                f"\t.functype {name} ({', '.join(params)}) -> ({', '.join(results)})",
                f"\ti32.const {index}",
                "\tcall ch_isolate_cut_code",
                "\tunreachable",
                "\tend_function",
            ]
            functions += 1
        else:
            stubs += [
                f"\t.section .bss.{name},\"\",@",
                f"\t.globl {name}",
                "\t.p2align 4",
                f"{name}:",
                f"\t.skip {DATA_STUB_SIZE}",
                f"\t.size {name}, {DATA_STUB_SIZE}",
            ]
            data += 1
    (out / "stubs.s").write_text("\n".join(stubs) + "\n")
    (out / "stubs.txt").write_text("".join(f"{index}\t{name}\n" for index, name in enumerate(undefined)))
    run([str(bin_dir / "clang"), "--target=wasm64-unknown-emscripten", "-c", str(out / "stubs.s"), "-o", str(out / "stubs.o")])

    run([*cut_arguments[:output + 1], str(out / "stubs.o"), *cut_arguments[output + 1 :]], cwd=build)
    wasm = out / "clickhouse-isolate.wasm"
    print(f"removed {removed} objects, stubbed {functions} functions and {data} data symbols: {wasm.stat().st_size / 2**20:.1f} MiB")


if __name__ == "__main__":
    main()
