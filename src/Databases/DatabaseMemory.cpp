#include <Databases/DDLDependencyVisitor.h>
#include <Databases/DDLLoadingDependencyVisitor.h>
#include <Databases/DatabaseFactory.h>
#include <Databases/DatabaseMemory.h>
#include <Databases/DatabasesCommon.h>
#include <Disks/IDisk.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/quoteString.h>
#include <Storages/IStorage.h>
#include <Core/Settings.h>
#include <Core/UUID.h>

namespace DB
{

namespace Setting
{
    extern const SettingsSeconds lock_acquire_timeout;
}

namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int TABLE_ALREADY_EXISTS;
}

DatabaseMemory::DatabaseMemory(const String & name_, ContextPtr context_)
    : DatabaseWithOwnTablesBase(name_, "DatabaseMemory(" + name_ + ")", context_)
    , data_path(DatabaseCatalog::getDataDirPath(name_) / "")
{
    auto component_guard = Coordination::setCurrentComponent("DatabaseMemory::DatabaseMemory");
    /// Temporary database should not have any data at the moment of its creation.
    /// In case of starting up after sudden server shutdown, remove the database folder of the temporary database.
    if (name_ == DatabaseCatalog::TEMPORARY_DATABASE)
        removeDataPath(context_);
}

void DatabaseMemory::createTable(
    ContextPtr /*context*/,
    const String & table_name,
    const StoragePtr & table,
    const ASTPtr & query)
{
    ensurePopulated();
    std::lock_guard lock{mutex};

    /// A detached table keeps its definition and dependencies under its name, only a short `ATTACH` may reuse it.
    const auto * create_query = query ? query->as<ASTCreateQuery>() : nullptr;
    if (!create_query || !create_query->attach_short_syntax)
        checkMetadataFilenameAvailabilityUnlocked(table_name);

    attachTableUnlocked(table_name, table);

    /// Clean the query from temporary flags.
    ASTPtr query_to_store = query;
    if (query)
    {
        query_to_store = query->clone();
        auto * create = query_to_store->as<ASTCreateQuery>();
        if (!create)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Query '{}' is not CREATE query", query->formatForErrorMessage());
        cleanupObjectDefinitionFromTemporaryFlags(*create);
    }

    create_queries.emplace(table_name, query_to_store);
}

void DatabaseMemory::dropTable(
    ContextPtr /*context*/,
    const String & table_name,
    bool /*sync*/)
{
    ensurePopulated();
    StoragePtr table;
    {
        std::lock_guard lock{mutex};
        table = detachTableUnlocked(table_name);
    }
    try
    {
        /// Remove table without lock since
        /// - it does not require it
        /// - it may cause lock-order-inversion if underlying storage need to resolve tables
        table->drop();

        if (table->storesDataOnDisk())
        {
            auto metdata_disk = getDisk();
            metdata_disk->removeRecursive(getTableDataPath(table_name));
        }
    }
    catch (...)
    {
        std::lock_guard lock{mutex};
        attachTableUnlocked(table_name, table);
        throw;
    }

    std::lock_guard lock{mutex};
    table->is_dropped = true;
    create_queries.erase(table_name);
    snapshot_detached_tables.erase(table_name);
    UUID table_uuid = table->getStorageID().uuid;
    if (table_uuid != UUIDHelpers::Nil)
        DatabaseCatalog::instance().removeUUIDMappingFinally(table_uuid);
}

void DatabaseMemory::renameTable(
    ContextPtr local_context,
    const String & table_name,
    IDatabase & to_database,
    const String & to_table_name,
    bool exchange,
    bool dictionary)
{
    if (dictionary)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Database {} does not support renaming dictionaries", getEngineName());
    auto * to_memory = typeid_cast<DatabaseMemory *>(&to_database);
    if (!to_memory)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot move a table from a database of engine {} to one of engine {}",
            getEngineName(), to_database.getEngineName());

    ensurePopulated();
    to_memory->ensurePopulated();
    auto get_table = [&](DatabaseMemory & database, const String & name)
    {
        StoragePtr table = database.getTable(name, local_context);
        if (table->storesDataOnDisk())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Database {} cannot rename table {}, which stores data on disk",
                getEngineName(), backQuote(name));
        return table;
    };

    StoragePtr table = get_table(*this, table_name);
    const StorageID new_id{to_database.getDatabaseName(), to_table_name, table->getStorageID().uuid};
    table->checkTableCanBeRenamed(new_id);
    StoragePtr other_table;
    StorageID other_new_id = StorageID::createEmpty();
    if (exchange)
    {
        other_table = get_table(*to_memory, to_table_name);
        other_new_id = StorageID{getDatabaseName(), table_name, other_table->getStorageID().uuid};
        other_table->checkTableCanBeRenamed(other_new_id);
    }
    else
    {
        if (to_memory->isTableExist(to_table_name, local_context))
            throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS, "Table {}.{} already exists",
                backQuote(new_id.database_name), backQuote(new_id.table_name));
        to_memory->checkMetadataFilenameAvailability(to_table_name);
    }

    const auto lock_timeout = local_context->getSettingsRef()[Setting::lock_acquire_timeout];
    auto table_lock = table->lockExclusively(local_context->getCurrentQueryId(), lock_timeout);
    TableExclusiveLockHolder other_table_lock;
    if (other_table)
        other_table_lock = other_table->lockExclusively(local_context->getCurrentQueryId(), lock_timeout);

    ASTPtr create_query = detachTableForRename(table_name, new_id);
    ASTPtr other_create_query = other_table ? to_memory->detachTableForRename(to_table_name, other_new_id) : nullptr;
    table->renameInMemory(new_id);
    if (other_table)
        other_table->renameInMemory(other_new_id);
    to_memory->createTable(local_context, new_id.table_name, table, create_query);
    if (other_table)
        createTable(local_context, other_new_id.table_name, other_table, other_create_query);
}

ASTPtr DatabaseMemory::detachTableForRename(const String & table_name, const StorageID & new_id)
{
    ASTPtr create_query;
    {
        std::lock_guard lock{mutex};
        auto it = create_queries.find(table_name);
        if (it == create_queries.end() || !it->second)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot rename: There is no metadata of table {}", backQuote(table_name));
        create_query = it->second->clone();
        detachTableUnlocked(table_name);
        /// Renamed, not detached: the name is free.
        snapshot_detached_tables.erase(table_name);
        create_queries.erase(it);
    }
    auto & create = create_query->as<ASTCreateQuery &>();
    create.setDatabase(new_id.database_name);
    create.setTable(new_id.table_name);
    return create_query;
}

ASTPtr DatabaseMemory::getCreateDatabaseQueryImpl() const
{
    auto create_query = make_intrusive<ASTCreateQuery>();
    create_query->setDatabase(database_name);
    create_query->set(create_query->storage, make_intrusive<ASTStorage>());
    auto engine = makeASTFunction(getEngineName());
    engine->setNoEmptyArgs(true);
    create_query->storage->set(create_query->storage->engine, engine);

    if (!comment.empty())
        create_query->set(create_query->comment, make_intrusive<ASTLiteral>(comment));

    return create_query;
}

ASTPtr DatabaseMemory::getCreateTableQueryImpl(const String & table_name, ContextPtr, bool throw_on_error) const
{
    ensurePopulated();
    std::lock_guard lock{mutex};
    auto it = create_queries.find(table_name);
    if (it == create_queries.end() || !it->second)
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "There is no metadata of table {} in database {}", table_name, database_name);
        return {};
    }
    return it->second->clone();
}

void DatabaseMemory::checkMetadataFilenameAvailability(const String & table_name) const
{
    ensurePopulated();
    std::lock_guard lock{mutex};
    checkMetadataFilenameAvailabilityUnlocked(table_name);
}

void DatabaseMemory::checkMetadataFilenameAvailabilityUnlocked(const String & table_name) const
{
    if (snapshot_detached_tables.contains(table_name))
        throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS, "Table {}.{} already exists (detached)",
                        backQuote(database_name), backQuote(table_name));
}

UUID DatabaseMemory::tryGetTableUUID(const String & table_name) const
{
    if (auto table = tryGetTable(table_name, getContext()))
        return table->getStorageID().uuid;
    return UUIDHelpers::Nil;
}

void DatabaseMemory::removeDataPath(ContextPtr)
{
    /// This method is called in two cases:
    /// 1. During startup for the temporary database (_temporary_and_external_tables) to clean up
    ///    stale directories from previous server sessions (e.g., after crash or Ctrl+C).
    ///    Temporary tables with disk-based engines (like MergeTree) may leave behind files that
    ///    need to be removed.
    /// 2. On explicit DROP DATABASE to remove all data.
    ///
    /// We must use removeRecursive() instead of removeDirectoryIfExists() because the directory
    /// may contain files from temporary tables. Using removeDirectoryIfExists()
    /// would fail or throw an exception if the directory is not empty.
    auto db_disk = getDisk();
    db_disk->removeRecursive(data_path);
}

void DatabaseMemory::drop(ContextPtr local_context)
{
    /// Remove data on explicit DROP DATABASE
    removeDataPath(local_context);
}

void DatabaseMemory::alterTable(ContextPtr local_context, const StorageID & table_id, const StorageInMemoryMetadata & metadata, const bool validate_new_create_query)
{
    ensurePopulated();
    ASTPtr create_query;
    {
        std::lock_guard lock{mutex};
        auto it = tables.find(table_id.table_name);
        if (it == tables.end() || (table_id.uuid != UUIDHelpers::Nil && it->second->getStorageID().uuid != table_id.uuid))
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {} doesn't exist", table_id.getNameForLogs());

        auto it_query = create_queries.find(table_id.table_name);
        if (it_query == create_queries.end() || !it_query->second)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot alter: There is no metadata of table {}", table_id.getNameForLogs());

        create_query = it_query->second->clone();
    }

    /// Apply metadata changes to the cloned AST without holding a lock to avoid possible deadlock
    /// (i.e. when ALTER contains IN (table)).
    applyMetadataChangesToCreateQuery(create_query, metadata, local_context, validate_new_create_query);

    /// The create query of the table has been just changed, we need to update dependencies too.
    auto ref_dependencies = getDependenciesFromCreateQuery(local_context->getGlobalContext(), table_id.getQualifiedName(), create_query, local_context->getCurrentDatabase());
    auto loading_dependencies = getLoadingDependenciesFromCreateQuery(local_context->getGlobalContext(), table_id.getQualifiedName(), create_query, local_context->getCurrentDatabase());
    DatabaseCatalog::instance().checkTableCanBeAddedWithNoCyclicDependencies(table_id.getQualifiedName(), ref_dependencies.dependencies, loading_dependencies);

    {
        std::lock_guard lock{mutex};
        create_queries[table_id.table_name] = create_query;
    }

    DatabaseCatalog::instance().updateDependencies(table_id, ref_dependencies.dependencies, loading_dependencies, ref_dependencies.mv_from_dependency ? TableNamesSet{ref_dependencies.mv_from_dependency->getQualifiedName()} : TableNamesSet{});
}

std::vector<std::pair<ASTPtr, StoragePtr>> DatabaseMemory::getTablesForBackup(const FilterByNameFunction & filter, const ContextPtr & local_context) const
{
    /// We need a special processing for the temporary database.
    if (getDatabaseName() != DatabaseCatalog::TEMPORARY_DATABASE)
        return DatabaseWithOwnTablesBase::getTablesForBackup(filter, local_context);

    std::vector<std::pair<ASTPtr, StoragePtr>> res;

    /// `this->tables` for the temporary database doesn't contain real names of tables.
    /// That's why we need to call Context::getExternalTables() and then resolve those names using tryResolveStorageID() below.
    auto external_tables = local_context->getExternalTables();

    for (const auto & [table_name, storage] : external_tables)
    {
        if (!filter(table_name))
            continue;

        auto storage_id = local_context->tryResolveStorageID(StorageID{"", table_name}, Context::ResolveExternal);
        if (!storage_id)
        {
            LOG_WARNING(log, "Couldn't resolve the name of temporary table {}", backQuoteIfNeed(table_name));
            continue;
        }

        /// Here `storage_id.table_name` looks like looks like "_tmp_ab9b15a3-fb43-4670-abec-14a0e9eb70f1"
        /// it's not the real name of the table.
        auto create_table_query = tryGetCreateTableQuery(storage_id.table_name, local_context);
        if (!create_table_query)
        {
            LOG_WARNING(log, "Couldn't get a create query for temporary table {}", backQuoteIfNeed(table_name));
            continue;
        }

        auto * create = create_table_query->as<ASTCreateQuery>();
        if (create->getTable() != table_name)
        {
            /// Probably the database has been just renamed. Use the older name for backup to keep the backup consistent.
            LOG_WARNING(log, "Got a create query with unexpected name {} for temporary table {}",
                        backQuoteIfNeed(create->getTable()), backQuoteIfNeed(table_name));
            create_table_query = create_table_query->clone();
            create = create_table_query->as<ASTCreateQuery>();
            create->setTable(table_name);
        }

        chassert(storage);
        storage->applyMetadataChangesToCreateQueryForBackup(create_table_query);
        res.emplace_back(create_table_query, storage);
    }

    return res;
}

void registerDatabaseMemory(DatabaseFactory & factory);
void registerDatabaseMemory(DatabaseFactory & factory)
{
    auto create_fn = [](const DatabaseFactory::Arguments & args)
    {
        return make_shared<DatabaseMemory>(
            args.database_name,
            args.context);
    };
    factory.registerDatabase("Memory", create_fn, SecretArgumentsSpec{}, {}, Documentation{
        .description = R"DOCS_MD(
The `Memory` database engine keeps its metadata and table definitions only in memory. It is intended for temporary databases: the database and its tables are lost when the server stops or restarts.

## Creating a database {#creating-a-database}

```sql
CREATE DATABASE temporary_data
ENGINE = Memory;
```

## Usage {#usage}

Create and use tables as in an [`Atomic`](/reference/engines/database-engines/atomic) database:

```sql
CREATE TABLE temporary_data.events
(
    id UInt64,
    name String
)
ENGINE = Memory;

INSERT INTO temporary_data.events VALUES (1, 'started');
```

Do not use this engine for data or definitions that must survive a restart. Use `Atomic`, the default open-source database engine, for persistent database metadata.

## See also {#see-also}

- [Atomic database engine](/reference/engines/database-engines/atomic)
- [`Memory` table engine](/reference/engines/table-engines/special/memory)
)DOCS_MD",
        .syntax = "ENGINE = Memory",
        .related = {"Atomic"}});
}

}
