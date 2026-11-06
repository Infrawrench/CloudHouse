# The engine for single-threaded WebAssembly isolates, such as Cloudflare Workers: a module with a small C
# interface (`isolate.cpp`) instead of `main`, and with only the parts that `registerIsolate.cpp` registers.

# The functions are an OBJECT library, so that every function they register ends up in the server. Here they go
# into a static archive instead, so that only the objects that `registerIsolate.cpp` references are linked.
add_library(clickhouse_functions_isolate STATIC
    $<TARGET_OBJECTS:clickhouse_functions_obj>
    $<TARGET_OBJECTS:clickhouse_functions_url>
    $<TARGET_OBJECTS:clickhouse_functions_array>
    $<TARGET_OBJECTS:clickhouse_functions_time_series>)
get_target_property(clickhouse_functions_isolate_link clickhouse_functions_obj LINK_LIBRARIES)
target_link_libraries(clickhouse_functions_isolate PUBLIC ${clickhouse_functions_isolate_link})

clickhouse_add_executable(clickhouse-isolate isolate.cpp registerIsolate.cpp ObjectMergeTree.cpp ObjectPart.cpp)
target_link_libraries(clickhouse-isolate PRIVATE
    clickhouse_functions_isolate
    clickhouse_aggregate_functions
    clickhouse_table_functions
    clickhouse_storages_system
    clickhouse_parsers
    clickhouse_common_io
    dbms)

# An ES module whose default export instantiates the engine, for a host that hands it the compiled module
# (`instantiateWasm`): a Worker cannot compile WebAssembly at run time, it imports the `.wasm` file instead.
set_target_properties(clickhouse-isolate PROPERTIES SUFFIX ".mjs" LINK_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/host.js")
target_link_options(clickhouse-isolate PRIVATE
    --no-entry
    -sMODULARIZE=1
    -sEXPORT_ES6=1
    -sEXPORT_NAME=createClickHouse
    -sENVIRONMENT=web
    -sDYNAMIC_EXECUTION=0
    # The engine reads the parts of `ObjectMergeTree` from the host while it runs a query (`host.js`): JSPI suspends it
    # in the import until the bytes are there. The functions that run queries return promises.
    -sJSPI=1
    "-sJSPI_EXPORTS=ch_isolate_query,ch_isolate_shard_query"
    "--js-library=${CMAKE_CURRENT_SOURCE_DIR}/host.js"
    "-sEXPORTED_FUNCTIONS=_ch_isolate_init,_ch_isolate_query,_ch_isolate_host_query,_ch_isolate_set_parts,_ch_isolate_changes,_ch_isolate_clear_changes,_ch_isolate_set_fanout,_ch_isolate_fanout_requests,_ch_isolate_fanout_broadcast,_ch_isolate_add_fanout_result,_ch_isolate_clear_fanout,_ch_isolate_shard_query,_ch_isolate_result_data,_ch_isolate_result_size,_malloc,_free"
    "-sEXPORTED_RUNTIME_METHODS=HEAPU8"
    # The memory of an isolate, 128 MB, holds the JavaScript heap as well as the linear memory, so the linear memory
    # stops at 88 MiB: past that, allocations fail with an exception instead of the isolate being killed. At 100 MiB,
    # the JavaScript heap of the Worker (the cached and read-ahead ranges of parts, the responses) did not fit beside
    # it, and Cloudflare reset the isolate (2026-11-02). The parser
    # and the analyzer recurse deeply, so the stack stays large.
    -sSTACK_SIZE=2097152
    -sINITIAL_MEMORY=50331648
    -sALLOW_MEMORY_GROWTH=1
    -sMAXIMUM_MEMORY=92274688)
