#pragma once

#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>

#include <map>
#include <memory>
#include <optional>
#include <set>
#include <tuple>


namespace DB
{

class IDatabase;

/// The tables of the `ObjectMergeTree` engine (also registered as `MergeTree`) keep their rows in immutable parts,
/// sorted by the sorting key and split by the partition key, as `MergeTree` does. An `INSERT` adds a part per
/// partition, `OPTIMIZE` merges the parts of a partition, and mutations rewrite parts.
///
/// The engine does not keep the parts. The host (the Worker) keeps every part in object storage and the list of the
/// parts of every table in the catalog, and it drives the engine with the functions of `isolate.cpp`:
/// - before a query, it brings the engine up to date with the catalog: it creates, alters and drops the tables, and
///   sets which parts of each table are visible;
/// - a query reads the parts it needs while it runs, block by block, from the host (`ObjectPart.h`);
/// - a query that changes the catalog (creates, alters or drops a table, adds or replaces parts) records the change,
///   and the host commits the changes when the query succeeds, all of them or none.
///
/// A table can also fan out: the host marks it so when it is too large for one isolate. A query that reads only
/// that table is then processed up to `WithMergeableState` by shards, other isolates that each see a part of its
/// parts, as for a `Distributed` table. The first time, the query fails with `NOT_FOUND_EXPECTED_DATA_PART` and
/// leaves a fan-out request: the query for the shards. The host runs it on the shards (`isShardQuery`) and gives
/// their results back, and the query runs again on them.

/// The name of a part, `<uuid>_<rows>_<bytes>_<partition id>`, so that the host knows its size and its partition from
/// the catalog.
struct ObjectPartName
{
    String uuid;
    UInt64 rows = 0;
    UInt64 bytes = 0;
    String partition_id;

    static ObjectPartName parse(const String & name);
    String toString() const;
};

/// A change to the catalog, for the host to commit.
struct CatalogChange
{
    enum class Kind : uint8_t
    {
        /// Creates the database `database`, which every isolate then has.
        CreateDatabase,
        CreateTable,
        /// Changes the definition of a table: `create_query` is the new one.
        AlterTable,
        DropTable,
        /// Renames the table to `to_database`.`to_table`, with its parts: `create_query` is the new statement.
        RenameTable,
        /// Exchanges the table with `to_database`.`to_table`: `create_query` is the new statement of the table that is
        /// now `to_database`.`to_table`, `to_create_query` of the one that is now `database`.`table`.
        ExchangeTables,
        DropDatabase,
        /// Adds the part `added`, if any, in place of the parts `removed`.
        ReplaceParts,
    };

    Kind kind = Kind::ReplaceParts;
    String database = {};
    String table = {};
    /// `RenameTable`.
    String to_database = {};
    String to_table = {};
    /// `CreateTable`, `AlterTable` and `RenameTable`: the statement that creates the table, and whether the table has a TTL, which
    /// the host applies with merges from time to time.
    String create_query = {};
    bool has_ttl = false;
    /// `ExchangeTables`.
    String to_create_query = {};
    bool to_has_ttl = false;
    /// `ReplaceParts`.
    String added = {};
    UInt64 added_rows = 0;
    Strings removed = {};
    /// `ReplaceParts`: the setting `insert_deduplication_token` of the `INSERT`, which the host commits once.
    String deduplication_token = {};
    /// `ReplaceParts`, for the commands on partitions: parts that are detached, detached parts that are attached again,
    /// detached parts that are deleted, and parts of the table `shared_from` that the table gets too. A part never
    /// changes, so tables share it rather than copy it: it is deleted when no table has it.
    Strings detached = {};
    Strings attached = {};
    Strings dropped_detached = {};
    Strings shared = {};
    std::pair<String, String> shared_from = {};
};

/// The query for the shards of a table that fans out.
struct FanOutRequest
{
    String database;
    String table;
    String query;
    /// The other sides of the joins and `IN`s of the query, which the coordinator has read, for the shards. See
    /// `ObjectPartStore::encodeBroadcast`.
    String broadcast;
};

class ObjectPartStore
{
public:
    static ObjectPartStore & instance();

    /// A query of the host, which brings the isolate up to date with the catalog, changes nothing in the catalog.
    bool isHostQuery() const { return host_query; }
    void setHostQuery(bool value) { host_query = value; }

    void setVisibleParts(const String & database, const String & table, Strings parts);
    Strings getVisibleParts(const StorageID & table_id) const;
    /// The detached parts of a table (`ALTER TABLE ... DETACH PARTITION`), which only `ATTACH` and `DROP DETACHED` see.
    void setDetachedParts(const String & database, const String & table, Strings parts);
    Strings getDetachedParts(const StorageID & table_id) const;
    const std::map<std::pair<String, String>, Strings> & allDetachedParts() const { return detached_parts; }
    /// Moves the parts and the fan-out of a table to its new name.
    void renameTable(const StorageID & from, const StorageID & to);
    const std::map<std::pair<String, String>, Strings> & allVisibleParts() const { return visible_parts; }

    /// For the duration of one piece of work, only `parts` of a table are visible: the part of a shard, or the part
    /// that a mutation rewrites. Other work keeps seeing all of them.
    void beginOverride(const String & database, const String & table, Strings parts);
    void endOverride();

    /// A query of a shard, which reads its part of a table that fans out on the coordinator, and does not fan out.
    bool isShardQuery() const { return shard_query; }
    void setShardQuery(bool value) { shard_query = value; }

    void addChange(CatalogChange change);
    const std::vector<CatalogChange> & getChanges() const { return changes; }
    std::vector<CatalogChange> & getChanges() { return changes; }
    /// The host has committed the changes, or given up on them.
    void clearChanges() { changes.clear(); }

    /// A query on the table fans out if the blocks that its filter needs are more than `bytes`; 0 for never.
    void setFanOut(const String & database, const String & table, UInt64 bytes);
    UInt64 fanOutBytes(const StorageID & table_id) const;
    void addFanOutRequest(FanOutRequest request);
    std::vector<FanOutRequest> takeFanOutRequests();
    /// The results of the shards for the query `query` on a table, in the `Native` format, one per shard.
    void addFanOutResult(const String & database, const String & table, const String & query, String data);
    const Strings * getFanOutResults(const StorageID & table_id, const String & query) const;
    void clearFanOutResults();

    /// Encodes blocks for the host, in the `Native` format, not compressed: the results of shards.
    static String encodeBlocks(const Blocks & blocks);
    /// The tables of the query context of the coordinator that `query` for the shards reads, and the same tables in the
    /// query context of a shard.
    static String encodeBroadcast(const ContextPtr & context, std::string_view query);
    static void addBroadcast(const ContextMutablePtr & context, std::string_view data);
    static Blocks decodeBlocks(std::string_view data);

private:
    bool host_query = false;
    bool shard_query = false;
    std::map<std::pair<String, String>, Strings> visible_parts;
    std::map<std::pair<String, String>, Strings> detached_parts;
    std::optional<std::pair<std::pair<String, String>, Strings>> visible_override;
    std::vector<CatalogChange> changes;
    std::map<std::pair<String, String>, UInt64> fan_out_tables;
    std::vector<FanOutRequest> fan_out_requests;
    std::map<std::tuple<String, String, String>, Strings> fan_out_results;
};

class StorageFactory;
void registerStorageObjectMergeTree(StorageFactory & factory);

/// `Atomic`, the default engine of `CREATE DATABASE`, makes a database of the catalog, which every isolate has. Its
/// tables are kept as in `Memory`, and those of `ObjectMergeTree` are in the catalog. `ENGINE = Memory` makes a
/// database of this isolate alone.
class DatabaseFactory;
void registerDatabaseCatalog(DatabaseFactory & factory);

/// `DROP`, `DETACH` and `TRUNCATE`, as `InterpreterDropQuery`, and `DROP DATABASE` removes the database from the catalog.
class InterpreterFactory;
void registerInterpreterCatalogDropQuery(InterpreterFactory & factory);

/// `system.detached_parts`: the detached parts of the tables of `ObjectMergeTree`.
void attachSystemDetachedParts(const ContextMutablePtr & context, IDatabase & system_database);

/// `system.parts`: the visible parts of the tables of `ObjectMergeTree`.
void attachSystemParts(const ContextMutablePtr & context, IDatabase & system_database);

}
