/// The engine as a library for a single-threaded WebAssembly isolate, such as a Cloudflare Worker.
///
/// The host instantiates the module once per isolate, calls `ch_isolate_init`, and then calls
/// `ch_isolate_query` for every query. All state (databases, tables in the `Memory` engine) lives
/// in the linear memory of the module, so it lasts as long as the isolate does. There is no
/// `main`, no threads, no sockets, and the file system is Emscripten's in-memory one.
///
/// The functions, formats and storages compiled in are the ones of the "query" role, see
/// `registerIsolate.cpp`.

#include <Access/AccessControl.h>
#include <Access/MemoryAccessStorage.h>
#include <Core/Defines.h>
#include <Databases/DatabaseMemory.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/CompressionMethod.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteHelpers.h>
#include <IO/SharedThreadPools.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/SecretArgumentsRegistry.h>
#include <Interpreters/Session.h>
#include <Interpreters/executeQuery.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Storages/System/StorageSystemAggregateFunctionCombinators.h>
#include <Storages/System/StorageSystemColumns.h>
#include <Storages/System/StorageSystemDataTypeFamilies.h>
#include <Storages/System/StorageSystemDatabases.h>
#include <Storages/System/StorageSystemFormats.h>
#include <Storages/System/StorageSystemFunctions.h>
#include <Storages/System/StorageSystemNumbers.h>
#include <Storages/System/StorageSystemOne.h>
#include <Storages/System/StorageSystemSettings.h>
#include <Storages/System/StorageSystemTableEngines.h>
#include <Storages/System/StorageSystemTableFunctions.h>
#include <Storages/System/StorageSystemTables.h>
#include <Storages/System/StorageSystemTimeZones.h>
#include <Storages/System/StorageSystemZeros.h>
#include <Storages/System/attachSystemTablesImpl.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/MemoryTracker.h>
#include <Common/QueryScope.h>
#include <base/scope_guard.h>
#include <Common/ThreadPool.h>
#include <Common/ThreadStatus.h>

#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/Logger.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/SAX/InputSource.h>
#include <Poco/Util/Application.h>
#include <Poco/Util/XMLConfiguration.h>

#include <emscripten.h>

#include "ObjectMergeTree.h"
#include "ObjectPart.h"

#include <filesystem>
#include <map>
#include <sstream>

namespace fs = std::filesystem;

/// Imported from the host (`host.js`): copies up to `size` bytes of the body of the request of the query to `dest`,
/// and returns how many, 0 at its end. The engine waits for them (JSPI).
extern "C" size_t ch_host_read_input(char * dest, size_t size);

namespace DB
{

void registerIsolate();

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_READ_FROM_ISTREAM;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int TOO_MANY_ROWS_OR_BYTES;
}

namespace
{

/// Everything is in Emscripten's in-memory file system, which is gone with the isolate.
constexpr auto data_path = "/clickhouse/";

/// The settings make every query run on the calling thread. The rest of the profile is about memory: an
/// isolate has 128 MB in all, for the code, the static data and the heap.
constexpr auto config_xml = R"XML(<clickhouse>
    <path>/clickhouse/</path>
    <default_profile>default</default_profile>
    <default_database>default</default_database>
    <!-- The exceptions of queries go back to the host with their messages, so the engine logs only fatal errors. It
         would log every `NOT_FOUND_EXPECTED_DATA_PART`, which the host expects, with a stack trace. -->
    <logger><level>fatal</level></logger>
</clickhouse>)XML";

constexpr auto users_xml = R"XML(<clickhouse>
    <profiles>
        <default>
            <max_threads>1</max_threads>
            <max_insert_threads>1</max_insert_threads>
            <max_final_threads>1</max_final_threads>
            <max_parsing_threads>1</max_parsing_threads>
            <max_download_threads>1</max_download_threads>
            <input_format_parallel_parsing>0</input_format_parallel_parsing>
            <!-- The reader of Parquet prefetches row groups on threads; with `max_parsing_threads` = 1 it decodes inline. -->
            <input_format_parquet_enable_row_group_prefetch>0</input_format_parquet_enable_row_group_prefetch>
            <output_format_parallel_formatting>0</output_format_parallel_formatting>
            <use_concurrency_control>0</use_concurrency_control>
            <async_insert>0</async_insert>
            <!-- The squashing of `Pretty` formats waits for the next block on a thread of its own. -->
            <output_format_pretty_squash_consecutive_ms>0</output_format_pretty_squash_consecutive_ms>
            <log_queries>0</log_queries>
            <max_memory_usage>58720256</max_memory_usage>
            <max_bytes_before_external_group_by>0</max_bytes_before_external_group_by>
            <max_bytes_before_external_sort>0</max_bytes_before_external_sort>
            <!-- An `INSERT` makes a part of each block of this many rows, which it holds in memory. -->
            <min_insert_block_size_rows>262144</min_insert_block_size_rows>
            <max_insert_block_size>262144</max_insert_block_size>
            <!-- Shards send single-level aggregation states: there are no two-level ones in an isolate. -->
            <distributed_aggregation_memory_efficient>0</distributed_aggregation_memory_efficient>
            <!-- What an isolate depends on: a query may lower the memory it uses, but not raise it, nor use threads. -->
            <constraints>
                <max_memory_usage><max>58720256</max></max_memory_usage>
                <max_threads><const/></max_threads>
                <max_insert_threads><const/></max_insert_threads>
                <max_final_threads><const/></max_final_threads>
                <max_parsing_threads><const/></max_parsing_threads>
                <max_download_threads><const/></max_download_threads>
                <input_format_parallel_parsing><const/></input_format_parallel_parsing>
                <output_format_parallel_formatting><const/></output_format_parallel_formatting>
                <input_format_parquet_enable_row_group_prefetch><const/></input_format_parquet_enable_row_group_prefetch>
                <use_concurrency_control><const/></use_concurrency_control>
                <max_bytes_before_external_group_by><const/></max_bytes_before_external_group_by>
                <max_bytes_before_external_sort><const/></max_bytes_before_external_sort>
                <distributed_aggregation_memory_efficient><const/></distributed_aggregation_memory_efficient>
            </constraints>
        </default>
        <!-- `readonly` = 2: no writes and no DDL, but settings, such as limits, can change within the constraints. -->
        <readonly>
            <profile>default</profile>
            <readonly>2</readonly>
        </readonly>
    </profiles>
    <users>
        <default>
            <password></password>
            <networks><ip>::/0</ip></networks>
            <profile>default</profile>
            <quota>default</quota>
            <access_management>1</access_management>
        </default>
        <!-- Queries that may only read: no writes and no DDL. -->
        <readonly>
            <password></password>
            <networks><ip>::/0</ip></networks>
            <profile>readonly</profile>
            <quota>default</quota>
        </readonly>
    </users>
    <quotas><default></default></quotas>
</clickhouse>)XML";

Poco::AutoPtr<Poco::Util::XMLConfiguration> parseXML(const char * xml)
{
    std::istringstream stream{std::string{xml}}; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::XML::InputSource source{stream};
    return new Poco::Util::XMLConfiguration{&source};
}

/// Some of the code reads the configuration from `Poco::Util::Application::instance`, as in `clickhouse-local`.
class Application : public Poco::Util::Application
{
};

struct Isolate
{
    Application * application = nullptr;
    SharedContextHolder shared_context;
    ContextMutablePtr global_context;
    /// One session per user, authenticated once, as the TCP interface keeps one for its queries. Every query has a
    /// query context of its own, so a `SET` in one does not reach the next.
    std::map<String, std::unique_ptr<Session>> sessions;
};

Isolate * isolate = nullptr;

/// The result of the last call, for the host to copy out: the query output, or the error message.
std::string result;

void attachSystemTable(ContextMutablePtr context, IDatabase & system_database, StoragePtr storage)
{
    system_database.attachTable(context, storage->getStorageID().table_name, storage, {});
}

void initialize()
{
    MainThreadStatus::getInstance();

    auto config = parseXML(config_xml);

    /// The isolate is never destroyed. It is published at the end, so that queries never see it half-initialized.
    auto * state = new Isolate;
    state->application = new Application;
    state->application->config().add(config.get(), Poco::Util::Application::PRIO_DEFAULT, /* writeable= */ false, /* shared= */ true);

    Poco::AutoPtr<Poco::ConsoleChannel> channel(new Poco::ConsoleChannel(std::cerr));
    Poco::Logger::root().setChannel(channel);
    Poco::Logger::root().setLevel(config->getString("logger.level"));

    registerIsolate();
    setSecretArgumentsFinder(&SecretArgumentsRegistry::instance());

    /// Well below the 88 MiB of linear memory (`MAXIMUM_MEMORY`), which also holds the static data (14 MiB) and the
    /// stack (2 MiB), and loses some to fragmentation: an engine that runs out of memory throws `MEMORY_LIMIT_EXCEEDED`
    /// for the query, rather than failing an allocation anywhere. The cached indexes of parts count too.
    total_memory_tracker.setHardLimit(60 * 1024 * 1024);

    /// The pools run their jobs inline (see `ThreadPoolImpl::scheduleImpl`), so the sizes only need to be positive.
    GlobalThreadPool::initialize(1, 0, 0);
    getIOThreadPool().initialize(1, 0, 0);
    getFormatParsingThreadPool().initialize(1, 0, 0);
    getActivePartsLoadingThreadPool().initialize(1, 0, 0);
    getOutdatedPartsLoadingThreadPool().initialize(1, 0, 0);
    getUnexpectedPartsLoadingThreadPool().initialize(1, 0, 0);
    getPartsCleaningThreadPool().initialize(1, 0, 0);
    getDatabaseCatalogDropTablesThreadPool().initialize(1, 0, 0);
    getDatabaseCatalogShutdownTablesThreadPool().initialize(1, 0, 0);
    getMergeTreePrefixesDeserializationThreadPool().initialize(1, 0, 0);

    state->shared_context = Context::createShared();
    auto & context = state->global_context;
    context = Context::createGlobal(state->shared_context.get());
    context->makeGlobalContext();
    context->setApplicationType(Context::ApplicationType::LOCAL);
    context->setConfig(config);

    fs::create_directories(data_path);
    context->setPath(data_path);
    context->setTemporaryStoragePath(fs::path(data_path) / "tmp" / "", 0);
    context->setFlagsPath(fs::path(data_path) / "flags" / "");
    context->setUserFilesPath(fs::path(data_path) / "user_files" / "");

    auto & access_control = context->getAccessControl();
    access_control.setNoPasswordAllowed(true);
    access_control.setPlaintextPasswordAllowed(true);
    context->setUsersConfig(parseXML(users_xml));
    access_control.addMemoryStorage(MemoryAccessStorage::STORAGE_TYPE, /* allow_backup= */ false);

    context->setMarkCache(DEFAULT_MARK_CACHE_POLICY, 4 << 20, 0.5);
    context->setUncompressedCache(DEFAULT_UNCOMPRESSED_CACHE_POLICY, 0, 0.5);
    context->setIndexUncompressedCache(DEFAULT_INDEX_UNCOMPRESSED_CACHE_POLICY, 0, DEFAULT_INDEX_UNCOMPRESSED_CACHE_SIZE_RATIO);
    context->setIndexMarkCache(DEFAULT_INDEX_MARK_CACHE_POLICY, 0, DEFAULT_INDEX_MARK_CACHE_SIZE_RATIO);
    context->setMMappedFileCache(0);
    context->setQueryConditionCache(DEFAULT_QUERY_CONDITION_CACHE_POLICY, 0, 0);
    context->setQueryResultCache(0, 0, 0, 0);

    context->setDefaultProfiles(*config);

    DatabaseCatalog::instance().initializeAndLoadTemporaryDatabase();

    auto system_database = std::make_shared<DatabaseMemory>(DatabaseCatalog::SYSTEM_DATABASE, context);
    DatabaseCatalog::instance().attachDatabase(DatabaseCatalog::SYSTEM_DATABASE, system_database);
    attachSystemTable(context, *system_database, std::make_shared<StorageSystemOne>(StorageID(DatabaseCatalog::SYSTEM_DATABASE, "one")));
    attachSystemTable(context, *system_database,
        std::make_shared<StorageSystemNumbers>(StorageID(DatabaseCatalog::SYSTEM_DATABASE, "numbers"), /* multithreaded= */ false, "number"));
    attachSystemTable(context, *system_database,
        std::make_shared<StorageSystemZeros>(StorageID(DatabaseCatalog::SYSTEM_DATABASE, "zeros"), /* multithreaded= */ false));
    /// `SHOW TABLES`, `SHOW DATABASES` and `SHOW COLUMNS` read these.
    attachSystemTable(context, *system_database, std::make_shared<StorageSystemDatabases>(
        StorageID(DatabaseCatalog::SYSTEM_DATABASE, "databases"), StorageSystemDatabases::getColumnsDescription()));
    attachSystemTable(context, *system_database, std::make_shared<StorageSystemTables>(StorageID(DatabaseCatalog::SYSTEM_DATABASE, "tables")));
    attachSystemTable(context, *system_database, std::make_shared<StorageSystemColumns>(StorageID(DatabaseCatalog::SYSTEM_DATABASE, "columns")));
    attachSystemParts(context, *system_database);
    attachSystemDetachedParts(context, *system_database);
    /// What the engine has, for clients that look.
    attach<StorageSystemSettings>(context, *system_database, "settings", "The settings of the session and their values.");
    attach<StorageSystemFunctions>(context, *system_database, "functions", "The functions that the engine has.");
    attach<StorageSystemFormats>(context, *system_database, "formats", "The formats of input and output.");
    attach<StorageSystemTableEngines>(context, *system_database, "table_engines", "The engines of tables.");
    attach<StorageSystemTableFunctions>(context, *system_database, "table_functions", "The table functions.");
    attach<StorageSystemDataTypeFamilies>(context, *system_database, "data_type_families", "The data types.");
    attach<StorageSystemAggregateFunctionCombinators>(context, *system_database, "aggregate_function_combinators", "The combinators of aggregate functions.");
    attach<StorageSystemTimeZones>(context, *system_database, "time_zones", "The time zones that the engine knows.");

    const String default_database = config->getString("default_database");
    DatabaseCatalog::instance().attachDatabase(default_database, std::make_shared<DatabaseMemory>(default_database, context));
    context->setCurrentDatabase(default_database);

    isolate = state;
}

ContextMutablePtr makeQueryContext(bool readonly)
{
    const String user = readonly ? "readonly" : "default";
    auto & session = isolate->sessions[user];
    if (!session)
    {
        auto new_session = std::make_unique<Session>(isolate->global_context, ClientInfo::Interface::LOCAL);
        new_session->authenticate(user, "", Poco::Net::SocketAddress{});
        session = std::move(new_session);
    }
    auto context = session->makeQueryContext();
    context->setCurrentQueryId("");
    return context;
}

/// The body of the request of a query, which the engine reads from the host as it goes (JSPI), so that it holds a
/// buffer of it at a time, whatever its size.
class HostInputReadBuffer final : public BufferWithOwnMemory<ReadBuffer>
{
public:
    HostInputReadBuffer() : BufferWithOwnMemory<ReadBuffer>(1024 * 1024) {}

private:
    bool nextImpl() override
    {
        const size_t size = ch_host_read_input(internal_buffer.begin(), internal_buffer.size());
        if (size == static_cast<size_t>(-1))
            throw Exception(ErrorCodes::CANNOT_READ_FROM_ISTREAM, "Cannot read the body of the request: {}", takeHostError());
        if (size == 0)
            return false;
        working_buffer = Buffer(internal_buffer.begin(), internal_buffer.begin() + size);
        return true;
    }
};

/// Runs a query, as the HTTP interface does: its text is `query`, then the body of the request if `with_input`,
/// which is compressed with `input_encoding` (`Content-Encoding`: `gzip`, `zstd`, ...), or not if it is empty.
void runQuery(std::string_view query, std::string_view default_format, bool readonly, bool with_input, std::string_view input_encoding)
{
    auto context = makeQueryContext(readonly);
    if (!default_format.empty())
        context->setDefaultFormat(String{default_format});

    auto query_scope = QueryScope::create(context);

    ConcatReadBuffer::Buffers buffers;
    buffers.push_back(std::make_unique<ReadBufferFromMemory>(query.data(), query.size()));
    if (with_input)
        buffers.push_back(wrapReadBufferWithCompressionMethod(
            std::make_unique<HostInputReadBuffer>(), chooseCompressionMethod({}, String{input_encoding})));
    ConcatReadBuffer in(std::move(buffers));
    WriteBufferFromString out(result);
    executeQuery(in, out, context, /* set_result_details= */ {});
    out.finalize();
}

/// Runs the query of a shard up to `WithMergeableState` on `parts` of a table, and makes its result the blocks it
/// produces.
/// The most that a shard sends back, in the bytes of its blocks. The result of a shard is held whole, here, in the
/// Worker, and in the coordinator, which holds those of all its shards: past this, the query fails, rather than the
/// isolate of the shard or of the coordinator running out of memory. A query that aggregates, or with a `LIMIT`, sends
/// back much less than this; a query of rows without one, as much as its filter keeps.
constexpr size_t max_shard_result_bytes = 4 * 1024 * 1024;

void runShardQuery(std::string_view query, const String & database, const String & table, Strings parts)
{
    auto context = makeQueryContext(/* readonly= */ false);
    auto query_scope = QueryScope::create(context);
    auto & store = ObjectPartStore::instance();
    store.beginOverride(database, table, std::move(parts));
    store.setShardQuery(true);
    SCOPE_EXIT(store.setShardQuery(false); store.endOverride());

    auto [ast, io] = executeQuery(query, context, QueryFlags{}, QueryProcessingStage::WithMergeableState);
    Blocks blocks;
    {
        PullingPipelineExecutor executor(io.pipeline);
        /// The header goes first, so that a shard without rows still gives the structure of its result.
        blocks.push_back(executor.getHeader().cloneEmpty());
        Block block;
        size_t bytes = 0;
        while (executor.pull(block))
        {
            if (!block.rows())
                continue;
            bytes += block.bytes();
            if (bytes > max_shard_result_bytes)
                throw Exception(
                    ErrorCodes::TOO_MANY_ROWS_OR_BYTES,
                    "A shard of the query has more than {} MiB of results: aggregate the rows, filter them more, or add a LIMIT",
                    max_shard_result_bytes / 1024 / 1024);
            blocks.push_back(std::move(block));
        }
    }
    io.onFinish();
    result = ObjectPartStore::encodeBlocks(blocks);
}

/// The changes of a query to the catalog, one JSON object per line, see `CatalogChange`.
String formatChanges()
{
    String text;
    WriteBufferFromString out(text);
    for (const auto & change : ObjectPartStore::instance().getChanges())
    {
        writeCString("{\"database\":", out);
        writeJSONString(change.database, out, {});
        writeCString(",\"table\":", out);
        writeJSONString(change.table, out, {});
        switch (change.kind)
        {
            case CatalogChange::Kind::CreateTable:
            case CatalogChange::Kind::AlterTable:
                writeCString(change.kind == CatalogChange::Kind::CreateTable ? ",\"kind\":\"create_table\"" : ",\"kind\":\"alter_table\"", out);
                writeCString(",\"create_query\":", out);
                writeJSONString(change.create_query, out, {});
                writeCString(change.has_ttl ? ",\"ttl\":true" : ",\"ttl\":false", out);
                break;
            case CatalogChange::Kind::CreateDatabase:
                writeCString(",\"kind\":\"create_database\"", out);
                break;
            case CatalogChange::Kind::DropDatabase:
                writeCString(",\"kind\":\"drop_database\"", out);
                break;
            case CatalogChange::Kind::ExchangeTables:
                writeCString(",\"kind\":\"exchange_tables\",\"to_database\":", out);
                writeJSONString(change.to_database, out, {});
                writeCString(",\"to_table\":", out);
                writeJSONString(change.to_table, out, {});
                writeCString(",\"create_query\":", out);
                writeJSONString(change.create_query, out, {});
                writeCString(change.has_ttl ? ",\"ttl\":true" : ",\"ttl\":false", out);
                writeCString(",\"to_create_query\":", out);
                writeJSONString(change.to_create_query, out, {});
                writeCString(change.to_has_ttl ? ",\"to_ttl\":true" : ",\"to_ttl\":false", out);
                break;
            case CatalogChange::Kind::DropTable:
                writeCString(",\"kind\":\"drop_table\"", out);
                break;
            case CatalogChange::Kind::RenameTable:
                writeCString(",\"kind\":\"rename_table\",\"to_database\":", out);
                writeJSONString(change.to_database, out, {});
                writeCString(",\"to_table\":", out);
                writeJSONString(change.to_table, out, {});
                writeCString(",\"create_query\":", out);
                writeJSONString(change.create_query, out, {});
                writeCString(change.has_ttl ? ",\"ttl\":true" : ",\"ttl\":false", out);
                break;
            case CatalogChange::Kind::ReplaceParts:
                writeCString(",\"kind\":\"replace_parts\",\"added\":", out);
                if (!change.added.empty())
                {
                    writeJSONString(change.added, out, {});
                    writeCString(",\"rows\":", out);
                    writeIntText(change.added_rows, out);
                }
                else
                    writeCString("null", out);
                for (const auto & [key, parts] : {
                         std::pair{"removed", &change.removed},
                         std::pair{"detached", &change.detached},
                         std::pair{"attached", &change.attached},
                         std::pair{"dropped_detached", &change.dropped_detached}})
                {
                    writeCString(",\"", out);
                    writeCString(key, out);
                    writeCString("\":[", out);
                    for (size_t i = 0; i < parts->size(); ++i)
                    {
                        if (i)
                            writeChar(',', out);
                        writeJSONString((*parts)[i], out, {});
                    }
                    writeChar(']', out);
                }
                writeCString(",\"shared\":[", out);
                for (size_t i = 0; i < change.shared.size(); ++i)
                {
                    if (i)
                        writeChar(',', out);
                    writeJSONString(change.shared[i], out, {});
                }
                writeCString("],\"shared_from\":[", out);
                writeJSONString(change.shared_from.first, out, {});
                writeChar(',', out);
                writeJSONString(change.shared_from.second, out, {});
                writeChar(']', out);
                break;
        }
        writeCString("}\n", out);
    }
    out.finalize();
    return text;
}

/// Splits `text` into lines, without the empty last one.
Strings splitLines(std::string_view text)
{
    Strings lines;
    size_t begin = 0;
    while (begin < text.size())
    {
        size_t end = text.find('\n', begin);
        if (end == std::string_view::npos)
            end = text.size();
        lines.emplace_back(text.substr(begin, end - begin));
        begin = end + 1;
    }
    return lines;
}

int run(auto && function)
{
    result.clear();
    try
    {
        function();
        return 0;
    }
    catch (...)
    {
        result = getCurrentExceptionMessage(/* with_stacktrace= */ false);
        int code = getCurrentExceptionCode();
        return code ? code : ErrorCodes::LOGICAL_ERROR;
    }
}

}

}

/// The interface for the host. Every function returns 0 on success, or the code of the exception, whose
/// message is then the result.
extern "C"
{

int ch_isolate_init();
int ch_isolate_query(
    const char * query, size_t query_size, const char * default_format, size_t default_format_size, int readonly, int with_input,
    const char * input_encoding, size_t input_encoding_size);
int ch_isolate_host_query(const char * query, size_t query_size);
int ch_isolate_set_parts(const char * spec, size_t spec_size);
int ch_isolate_changes();
int ch_isolate_clear_changes();
int ch_isolate_set_fanout(const char * spec, size_t spec_size);
int ch_isolate_fanout_requests();
int ch_isolate_add_fanout_result(const char * spec, size_t spec_size, const char * data, size_t data_size);
int ch_isolate_clear_fanout();
int ch_isolate_shard_query(const char * query, size_t query_size, const char * spec, size_t spec_size);
const char * ch_isolate_result_data();
size_t ch_isolate_result_size();
[[noreturn]] void ch_isolate_cut_code(int index);

/// What the stubs of `cut-dead-code.py` call: the code of a storage that an isolate does not have, number `index` in
/// `stubs.txt` of the build.
void ch_isolate_cut_code(int index)
{
    throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "The isolate does not have this code (stub {})", index);
}

EMSCRIPTEN_KEEPALIVE int ch_isolate_init()
{
    return DB::run([] { if (!DB::isolate) DB::initialize(); });
}

/// Runs one query, with the data of an `INSERT` following it, like the HTTP interface: in the same text, or, if
/// `with_input`, in the body of the request, which the engine reads from the host (`ch_host_read_input`), compressed
/// with `input_encoding` if it is not empty. `default_format` applies when the query has no `FORMAT` clause; empty
/// means `TabSeparated`. A `readonly` query runs as the user `readonly`, with the setting `readonly` = 2.
EMSCRIPTEN_KEEPALIVE int ch_isolate_query(
    const char * query, size_t query_size, const char * default_format, size_t default_format_size, int readonly, int with_input,
    const char * input_encoding, size_t input_encoding_size)
{
    return DB::run([&]
    {
        if (!DB::isolate)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "ch_isolate_init has not been called");
        auto & store = DB::ObjectPartStore::instance();
        /// The changes of one query are committed together, so the host clears them before the next query.
        if (!store.getChanges().empty())
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "The changes of the previous query are neither committed nor cleared");
        DB::runQuery(
            {query, query_size}, {default_format, default_format_size}, readonly != 0, with_input != 0, {input_encoding, input_encoding_size});
    });
}

/// Runs a query of the host, which brings the isolate up to date with the catalog: it may attach and drop tables
/// of `ObjectMergeTree`, and it changes nothing in the catalog.
EMSCRIPTEN_KEEPALIVE int ch_isolate_host_query(const char * query, size_t query_size)
{
    return DB::run([&]
    {
        auto & store = DB::ObjectPartStore::instance();
        store.setHostQuery(true);
        SCOPE_EXIT(store.setHostQuery(false));
        DB::runQuery({query, query_size}, {}, /* readonly= */ false, /* with_input= */ false, {});
    });
}

/// Sets the parts of a table: `spec` is its database, its name, then the parts, one per line, the detached ones as
/// `detached/<part>`.
EMSCRIPTEN_KEEPALIVE int ch_isolate_set_parts(const char * spec, size_t spec_size)
{
    return DB::run([&]
    {
        auto lines = DB::splitLines({spec, spec_size});
        if (lines.size() < 2)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "The parts of a table need its database and its name");
        DB::Strings parts;
        DB::Strings detached;
        for (size_t i = 2; i < lines.size(); ++i)
        {
            if (lines[i].starts_with("detached/"))
                detached.push_back(lines[i].substr(std::string_view("detached/").size()));
            else
                parts.push_back(std::move(lines[i]));
        }
        auto & store = DB::ObjectPartStore::instance();
        store.setVisibleParts(lines[0], lines[1], std::move(parts));
        store.setDetachedParts(lines[0], lines[1], std::move(detached));
    });
}

/// After a query succeeded: its changes to the catalog, one JSON object per line.
EMSCRIPTEN_KEEPALIVE int ch_isolate_changes()
{
    return DB::run([] { DB::result = DB::formatChanges(); });
}

/// The host has committed the changes of the last query, or given up on them.
EMSCRIPTEN_KEEPALIVE int ch_isolate_clear_changes()
{
    return DB::run([] { DB::ObjectPartStore::instance().clearChanges(); });
}

/// Makes a table fan out: `spec` is its database, its name, then the bytes of blocks above which a query fans out (0 for
/// never), one per line.
EMSCRIPTEN_KEEPALIVE int ch_isolate_set_fanout(const char * spec, size_t spec_size)
{
    return DB::run([&]
    {
        auto lines = DB::splitLines({spec, spec_size});
        if (lines.size() != 3)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Fan-out needs a database, a table and a number of bytes");
        DB::ObjectPartStore::instance().setFanOut(lines[0], lines[1], DB::parse<UInt64>(lines[2]));
    });
}

/// After a query failed with `NOT_FOUND_EXPECTED_DATA_PART`: the queries for the shards of the tables that fan out,
/// one JSON object per line, with `database`, `table` and `query`.
EMSCRIPTEN_KEEPALIVE int ch_isolate_fanout_requests()
{
    return DB::run([]
    {
        DB::WriteBufferFromString out(DB::result);
        for (const auto & request : DB::ObjectPartStore::instance().takeFanOutRequests())
        {
            writeCString("{\"database\":", out);
            DB::writeJSONString(request.database, out, {});
            writeCString(",\"table\":", out);
            DB::writeJSONString(request.table, out, {});
            writeCString(",\"query\":", out);
            DB::writeJSONString(request.query, out, {});
            writeCString(request.joins ? ",\"joins\":true}\n" : ",\"joins\":false}\n", out);
        }
        out.finalize();
    });
}

/// The result of one shard for a fan-out request: `spec` is the database, the table, then the query of the request.
EMSCRIPTEN_KEEPALIVE int ch_isolate_add_fanout_result(const char * spec, size_t spec_size, const char * data, size_t data_size)
{
    return DB::run([&]
    {
        std::string_view text(spec, spec_size);
        const size_t first = text.find('\n');
        const size_t second = first == std::string_view::npos ? first : text.find('\n', first + 1);
        if (second == std::string_view::npos)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "A fan-out result needs a database, a table and a query");
        DB::ObjectPartStore::instance().addFanOutResult(
            std::string(text.substr(0, first)), std::string(text.substr(first + 1, second - first - 1)), std::string(text.substr(second + 1)),
            std::string(data, data_size));
    });
}

/// After the query that fanned out is done.
EMSCRIPTEN_KEEPALIVE int ch_isolate_clear_fanout()
{
    return DB::run([] { DB::ObjectPartStore::instance().clearFanOutResults(); });
}

/// Runs the query of a shard, which the coordinator asked for in a fan-out request: `spec` is the database, the
/// table, then the parts of the shard, one per line.
EMSCRIPTEN_KEEPALIVE int ch_isolate_shard_query(const char * query, size_t query_size, const char * spec, size_t spec_size)
{
    return DB::run([&]
    {
        auto lines = DB::splitLines({spec, spec_size});
        if (lines.size() < 2)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "A shard needs a database and a table");
        DB::Strings parts(std::make_move_iterator(lines.begin() + 2), std::make_move_iterator(lines.end()));
        DB::runShardQuery({query, query_size}, lines[0], lines[1], std::move(parts));
    });
}

EMSCRIPTEN_KEEPALIVE const char * ch_isolate_result_data()
{
    return DB::result.data();
}

EMSCRIPTEN_KEEPALIVE size_t ch_isolate_result_size()
{
    return DB::result.size();
}

}
