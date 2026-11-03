#include "ObjectMergeTree.h"
#include "ObjectPart.h"

#include <Analyzer/AggregationUtils.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/Utils.h>
#include <Analyzer/QueryNode.h>
#include <Columns/ColumnConst.h>
#include <Analyzer/TableNode.h>
#include <Core/ProtocolDefines.h>
#include <Core/Settings.h>
#include <Core/SortDescription.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/DatabaseFactory.h>
#include <Databases/DatabaseMemory.h>
#include <Databases/IDatabase.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Interpreters/sortBlock.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTPartition.h>
#include <Parsers/ASTSetQuery.h>
#include <Planner/PlannerContext.h>
#include <Planner/Utils.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <Processors/TTL/ITTLAlgorithm.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/AlterCommands.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/MergeTreePartition.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MutationCommands.h>
#include <Storages/PartitionCommands.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageDummy.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/System/IStorageSystemOneBlock.h>
#include <Storages/TTLDescription.h>
#include <Storages/buildQueryTreeForShard.h>
#include <Storages/removeGroupingFunctionSpecializations.h>
#include <Common/CurrentThread.h>
#include <Common/DateLUT.h>
#include <Common/Exception.h>
#include <base/scope_guard.h>


namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_drop_detached;
    extern const SettingsBool materialize_ttl_after_modify;
    extern const SettingsUInt64 max_partitions_per_insert_block;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 index_granularity;
}

namespace ErrorCodes
{
    extern const int ALTER_OF_COLUMN_IS_FORBIDDEN;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_FORGET_PARTITION;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    extern const int CANNOT_UPDATE_COLUMN;
    extern const int INCOMPATIBLE_COLUMNS;
    extern const int NO_SUCH_DATA_PART;
    extern const int NOT_FOUND_EXPECTED_DATA_PART;
    extern const int NOT_IMPLEMENTED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int READONLY_SETTING;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

/// How many times `tree` reads the table `table_id`, anywhere: in its joins, its subqueries, its `IN`s.
size_t countTableReferences(const QueryTreeNodePtr & tree, const StorageID & table_id)
{
    size_t count = 0;
    std::vector<const IQueryTreeNode *> stack{tree.get()};
    while (!stack.empty())
    {
        const auto * node = stack.back();
        stack.pop_back();
        if (!node)
            continue;
        if (const auto * table = node->as<TableNode>(); table && table->getStorageID() == table_id)
            ++count;
        for (const auto & child : node->getChildren())
            stack.push_back(child.get());
    }
    return count;
}

/// Whether the joins of `join_tree` give the same rows when its leftmost table is split among shards, each of which
/// joins its rows with the whole of the other side: `INNER`, `LEFT` and cross joins do; `RIGHT` and `FULL` would add
/// the rows of the other side that no row of a shard matches once per shard, and `PASTE` pairs rows by position.
bool joinsSplitByLeftTable(const QueryTreeNodePtr & join_tree)
{
    const IQueryTreeNode * node = join_tree.get();
    while (const auto * join = node->as<JoinNode>())
    {
        const auto kind = join->getKind();
        if (kind != JoinKind::Inner && kind != JoinKind::Left && kind != JoinKind::Cross && kind != JoinKind::Comma)
            return false;
        node = join->getLeftTableExpressionNode().get();
    }
    /// An `ARRAY JOIN` on the way to the leftmost table is not split.
    return node->getNodeType() == QueryTreeNodeType::TABLE;
}

/// The results of shards are this, then their blocks in the `Native` format of `revision`, not compressed: they are
/// small, and they only travel between isolates.
constexpr std::string_view blocks_magic = "CHBLKS01";
constexpr UInt64 blocks_revision = DBMS_TCP_PROTOCOL_VERSION;
/// The most rows of a block of a part, so that a block fits in the memory of a query.
constexpr UInt64 max_rows_per_block = 262144;

/// Whether each row has expired by `ttl`: as in `MergeTree`, once its TTL is not in the future.
IColumn::Filter expiredRows(const Block & block, const TTLDescription & ttl, const ContextPtr & context)
{
    /// As `TTLDeleteAlgorithm`: the analysis of the TTL widens its `Date` and `DateTime` sources, and the helper
    /// casts the columns of the block to the types the expression expects.
    auto expires = ITTLAlgorithm::executeExpressionAndGetColumn(ttl.buildExpression(context).expression, block, ttl.result_column);
    PaddedPODArray<Int64> expires_at;
    ITTLAlgorithm::extractTimestamps(expires.get(), DateLUT::instance(), expires_at);
    const auto now = time(nullptr);
    IColumn::Filter expired(block.rows());
    for (size_t i = 0; i < block.rows(); ++i)
        expired[i] = expires_at[i] <= now;
    return expired;
}

/// Applies the TTL: removes the rows whose TTL has passed, and resets the values of a column whose TTL has passed to
/// its default, as `TTLColumnAlgorithm`. Returns whether it changed anything.
bool applyTTL(Block & block, const StorageInMemoryMetadata & metadata, const ContextPtr & context)
{
    if (block.rows() == 0)
        return false;
    bool changed = false;
    if (metadata.hasRowsTTL())
    {
        const auto expired = expiredRows(block, metadata.getRowsTTL(), context);
        IColumn::Filter keep(expired.size());
        size_t kept = 0;
        for (size_t i = 0; i < expired.size(); ++i)
        {
            keep[i] = !expired[i];
            kept += keep[i];
        }
        if (kept != block.rows())
        {
            for (auto & column : block)
                column.column = column.column->filter(keep, kept);
            changed = true;
        }
    }
    if (block.rows() == 0)
        return changed;
    for (const auto & [name, ttl] : metadata.getColumnTTLs())
    {
        if (!block.has(name))
            continue;
        const auto expired = expiredRows(block, ttl, context);
        if (std::find(expired.begin(), expired.end(), 1) == expired.end())
            continue;
        /// The defaults of the column, for every row: of its `DEFAULT` expression, or of its type.
        Block defaults;
        for (const auto & column : block)
            if (column.name != name)
                defaults.insert(column);
        if (auto dag = evaluateMissingDefaults(defaults, NamesAndTypesList{{name, block.getByName(name).type}}, metadata.getColumns(), context))
            ExpressionActions(std::move(*dag)).execute(defaults);
        auto & column = block.getByName(name);
        ColumnPtr default_column = defaults.has(name)
            ? defaults.getByName(name).column->convertToFullColumnIfConst()
            : column.type->createColumnConstWithDefaultValue(block.rows())->convertToFullColumnIfConst();
        auto result = column.type->createColumn();
        result->reserve(block.rows());
        for (size_t i = 0; i < block.rows(); ++i)
            result->insertFrom(expired[i] ? *default_column : *column.column, i);
        column.column = std::move(result);
        changed = true;
    }
    return changed;
}

SortDescription sortDescription(const StorageInMemoryMetadata & metadata)
{
    const auto & key = metadata.getSortingKey();
    SortDescription description;
    for (size_t i = 0; i < key.column_names.size(); ++i)
        description.emplace_back(key.column_names[i], !key.reverse_flags.empty() && key.reverse_flags[i] ? -1 : 1);
    return description;
}

/// Sorts the rows by the sorting key, which may be an expression of the columns.
void sortBySortingKey(Block & block, const StorageInMemoryMetadata & metadata)
{
    const auto & key = metadata.getSortingKey();
    if (key.column_names.empty())
        return;
    Block with_key = block;
    key.expression->execute(with_key);
    IColumn::Permutation permutation;
    stableGetPermutation(with_key, sortDescription(metadata), permutation);
    for (auto & column : block)
        column.column = column.column->permute(permutation, 0);
}

/// The columns of the min-max index of the parts of a table.
NamesAndTypesList tableMinmaxColumns(const StorageInMemoryMetadata & metadata)
{
    NamesAndTypesList required;
    const auto & columns = metadata.getColumns();
    for (const auto & names : {metadata.getColumnsRequiredForSortingKey(), metadata.getColumnsRequiredForPartitionKey()})
        for (const auto & name : names)
            if (columns.hasPhysical(name))
                required.emplace_back(name, columns.getPhysical(name).type);
    return minmaxColumns(required, columns);
}

/// The columns that a mutation or an `ALTER` must not change: sorting a part and splitting it into partitions depends
/// on them, as do the TTL and the min-max index.
NameSet keyColumns(const StorageInMemoryMetadata & metadata)
{
    NameSet result;
    for (const auto & names : {metadata.getColumnsRequiredForSortingKey(), metadata.getColumnsRequiredForPartitionKey(),
                               metadata.getColumnsRequiredForPrimaryKey()})
        result.insert(names.begin(), names.end());
    if (metadata.hasRowsTTL())
        for (const auto & column : metadata.getRowsTTL().expression_source_columns)
            result.insert(column.name);
    return result;
}

String formatCreateQuery(const StorageID & table_id, const StorageInMemoryMetadata & metadata, const ASTStorage & storage_def)
{
    auto create = make_intrusive<ASTCreateQuery>();
    create->setDatabase(table_id.database_name);
    create->setTable(table_id.table_name);
    auto columns_list = make_intrusive<ASTColumns>();
    columns_list->set(columns_list->columns, InterpreterCreateQuery::formatColumns(metadata.getColumns()));
    if (!metadata.secondary_indices.empty())
        columns_list->set(columns_list->indices, InterpreterCreateQuery::formatIndices(metadata.secondary_indices));
    if (!metadata.constraints.empty())
        columns_list->set(columns_list->constraints, InterpreterCreateQuery::formatConstraints(metadata.constraints));
    if (!metadata.projections.empty())
        columns_list->set(columns_list->projections, InterpreterCreateQuery::formatProjections(metadata.projections));
    create->set(create->columns_list, columns_list);
    create->set(create->storage, storage_def.clone());
    if (!metadata.comment.empty())
        create->set(create->comment, make_intrusive<ASTLiteral>(metadata.comment));
    return create->formatWithSecretsOneLine();
}

/// The settings of a table, `SETTINGS` of `MergeTree`: they are checked as in `MergeTree`, and `index_granularity` is
/// the rows of a block of a part. The others are kept for `SHOW CREATE TABLE` and have no effect, because a part of
/// `ObjectMergeTree` has nothing else to tune: it is one object, with a block per granule.
std::shared_ptr<const MergeTreeSettings> tableSettings(const SettingsChanges & changes, const ContextPtr & context)
{
    for (const auto & change : changes)
        if (change.name == "disk" || change.name == "storage_policy")
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The parts of ObjectMergeTree are in object storage, without {}", change.name);
    auto settings = std::make_shared<MergeTreeSettings>();
    settings->applyChanges(changes, context, /* is_loading_from_existing_metadata= */ false);
    if (settings->isChanged("index_granularity"))
    {
        const UInt64 rows = (*settings)[MergeTreeSetting::index_granularity];
        if (rows == 0 || rows > max_rows_per_block)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "index_granularity of ObjectMergeTree is from 1 to {}, not {}", max_rows_per_block, rows);
    }
    return settings;
}

size_t rowsPerBlock(const MergeTreeSettings & settings)
{
    return settings.isChanged("index_granularity") ? (settings)[MergeTreeSetting::index_granularity] : ObjectPartWriter::default_rows_per_block;
}

SettingsChanges settingsChanges(const ASTStorage & storage_def)
{
    return storage_def.settings ? storage_def.settings->changes : SettingsChanges{};
}

/// The TTL of a table can only delete rows, as the merges apply it.
void checkTTL(const TTLTableDescription & ttl)
{
    if (ttl.rows_ttl.mode != TTLMode::DELETE || ttl.rows_ttl.where_expression_ast || !ttl.rows_where_ttl.empty() || !ttl.move_ttl.empty()
        || !ttl.recompression_ttl.empty() || !ttl.group_by_ttl.empty())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The TTL of ObjectMergeTree can only delete rows, without WHERE");
}

/// The value of the partition of the rows of `block`, which are all in one partition, as `system.parts` shows it.
String partitionText(const StorageMetadataPtr & metadata, const Block & block, const ContextPtr & context)
{
    if (!metadata->hasPartitionKey())
        return "tuple()";
    MergeTreePartition partition;
    partition.create(metadata, block, 0, context);
    WriteBufferFromOwnString out;
    partition.serializeText(metadata->getPartitionKey().sample_block, out, FormatSettings{});
    return out.str();
}

/// The id of the partition that `ALTER TABLE ... PARTITION <value>` names, as `MergeTreePartition::getID`.
String partitionIdFromAST(const ASTPartition & partition, const StorageInMemoryMetadata & metadata, const ContextPtr & context)
{
    if (partition.all)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ObjectMergeTree has no PARTITION ALL");
    if (partition.id)
        return partition.id->as<const ASTLiteral &>().value.safeGet<String>();

    const auto & key = metadata.getPartitionKey();
    const auto & types = key.data_types;
    auto [value, type] = evaluateConstantExpression(partition.value->ptr(), context);
    Row row;
    if (types.size() == 1)
    {
        row.push_back(convertFieldToTypeOrThrow(value, *types[0]));
    }
    else
    {
        const auto & tuple = value.safeGet<Tuple>();
        if (tuple.size() != types.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The partition key has {} columns, not {}", types.size(), tuple.size());
        for (size_t i = 0; i < types.size(); ++i)
            row.push_back(convertFieldToTypeOrThrow(tuple[i], *types[i]));
    }
    return MergeTreePartition(std::move(row)).getID(key.sample_block);
}

/// Adds the columns of the sorting key, which may be expressions, to the columns of a stream, for a merge.
ActionsDAG withSortingKey(const StorageInMemoryMetadata & metadata, const Block & header)
{
    ActionsDAG dag = metadata.getSortingKey().expression->getActionsDAG().clone();
    for (const auto & column : header)
    {
        if (dag.tryFindInOutputs(column.name))
            continue;
        const ActionsDAG::Node * input = nullptr;
        for (const auto * node : dag.getInputs())
            if (node->result_name == column.name)
                input = node;
        if (!input)
            input = &dag.addInput(column.name, column.type);
        dag.getOutputs().push_back(input);
    }
    return dag;
}

Block physicalColumns(const Block & block, const Block & header)
{
    Block result;
    for (const auto & column : header)
        result.insert(block.getByName(column.name));
    return result;
}


/// Reads the visible parts of a table, block by block. The filter of the query, pushed down to the step, skips the
/// parts and the blocks whose ranges of the min-max columns cannot satisfy it.
class ReadFromObjectMergeTree final : public SourceStepWithFilter
{
public:
    ReadFromObjectMergeTree(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        Strings parts_,
        ObjectPartSkipIndices skip_indices_)
        : SourceStepWithFilter(
              std::make_shared<const Block>(storage_snapshot_->getSampleBlockForColumns(column_names_)),
              column_names_,
              query_info_,
              storage_snapshot_,
              context_)
        , parts(std::move(parts_))
        , skip_indices(std::move(skip_indices_))
    {
    }

    String getName() const override { return "ReadFromObjectMergeTree"; }

    QueryPlanStepPtr clone() const override { return std::make_unique<ReadFromObjectMergeTree>(*this); }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override
    {
        const auto & metadata = storage_snapshot->metadata;
        const auto minmax = tableMinmaxColumns(*metadata);
        std::optional<KeyCondition> condition;
        if (filter_actions_dag && !minmax.empty())
        {
            ActionsDAGWithInversionPushDown inverted(filter_actions_dag->getOutputs().front(), context, /* boolean_context= */ true);
            condition.emplace(inverted, context, minmax.getNames(), std::make_shared<ExpressionActions>(ActionsDAG(minmax)));
        }
        const auto skip = skipConditions(skip_indices, filter_actions_dag ? filter_actions_dag->getOutputs().front() : nullptr, context);

        auto columns = storage_snapshot->getColumnsByNames(
            GetColumnsOptions(GetColumnsOptions::All).withSubcolumns().withVirtuals(VirtualsKind::All, VirtualsMaterializationPlace::Reader),
            required_source_columns);
        for (const auto & part : parts)
            prefetchPartIndex(part, ObjectPartName::parse(part).bytes);
        std::vector<std::tuple<String, ObjectPartIndexPtr, std::vector<size_t>>> chosen;
        for (const auto & part : parts)
        {
            auto index = readPartIndex(part, ObjectPartName::parse(part).bytes);
            std::vector<size_t> blocks;
            for (size_t i = 0; i < index->blocks.size(); ++i)
                if ((!condition || mayContainRows(*condition, minmax, *index, index->blocks[i].ranges))
                    && skipIndicesMayContainRows(skip, *index, i))
                    blocks.push_back(i);
            if (!blocks.empty())
                chosen.emplace_back(part, std::move(index), std::move(blocks));
        }
        /// The columns that the parts have, which the source reads, without the virtual ones.
        Names physical;
        for (const auto & column : columns)
            if (column.name != "_part" && column.name != "_partition_id"
                && std::find(physical.begin(), physical.end(), column.getNameInStorage()) == physical.end())
                physical.push_back(column.getNameInStorage());
        Pipes pipes;
        for (auto & [part, index, blocks] : chosen)
        {
            prefetchPartBlocks(part, *index, blocks, physical);
            pipes.emplace_back(std::make_shared<ObjectPartSource>(output_header, part, index, std::move(blocks), columns, metadata->getColumns(), context));
        }
        if (pipes.empty())
            pipes.emplace_back(std::make_shared<NullSource>(output_header));
        pipeline.init(Pipe::unitePipes(std::move(pipes)));
    }

private:
    Strings parts;
    ObjectPartSkipIndices skip_indices;
};


class StorageObjectMergeTree final : public IStorage
{
public:
    StorageObjectMergeTree(
        const StorageID & table_id_, StorageInMemoryMetadata metadata, ASTPtr storage_def_, std::shared_ptr<const MergeTreeSettings> storage_settings_)
        : IStorage(table_id_)
        , storage_def(std::move(storage_def_))
        , storage_settings(std::move(storage_settings_))
    {
        setInMemoryMetadata(metadata);
    }

    std::string getName() const override { return "ObjectMergeTree"; }

    bool supportsSubcolumns() const override { return true; }
    bool supportsTTL() const override { return true; }
    /// `DELETE FROM` is a mutation, as `ALTER TABLE ... DELETE`.
    bool supportsDelete() const override { return true; }
    bool parallelizeOutputAfterReading(ContextPtr) const override { return false; }

    String createQuery(const StorageInMemoryMetadata & metadata) const
    {
        return formatCreateQuery(getStorageID(), metadata, storage_def->as<const ASTStorage &>());
    }

    /// A writer of a part of the table as `metadata` defines it.
    ObjectPartWriter partWriter(const StorageMetadataPtr & metadata) const
    {
        return ObjectPartWriter(
            metadata->getSampleBlock(), tableMinmaxColumns(*metadata), skipIndices(metadata, *storage_settings), rowsPerBlock(*storage_settings));
    }

    /// Whether the parts of `source` can be parts of this table: as `MergeTreeData::checkStructureAndGetMergeTreeData`,
    /// the same columns, sorting key and partition key.
    const StorageObjectMergeTree & checkSameStructure(const StoragePtr & source, const StorageMetadataPtr & metadata, const ContextPtr & context) const
    {
        const auto * other = dynamic_cast<const StorageObjectMergeTree *>(source.get());
        if (!other)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table {} is not of ObjectMergeTree", source->getStorageID().getNameForLogs());
        const auto other_metadata = other->getInMemoryMetadataPtr(context, false);
        auto format = [](const ASTPtr & ast) { return ast ? ast->formatWithSecretsOneLine() : String{}; };
        if (metadata->getColumns().getAllPhysical().toString() != other_metadata->getColumns().getAllPhysical().toString())
            throw Exception(ErrorCodes::INCOMPATIBLE_COLUMNS, "Tables {} and {} have different columns", getStorageID().getNameForLogs(), other->getStorageID().getNameForLogs());
        if (format(metadata->getSortingKeyAST()) != format(other_metadata->getSortingKeyAST()))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Tables {} and {} have different sorting keys", getStorageID().getNameForLogs(), other->getStorageID().getNameForLogs());
        if (format(metadata->getPartitionKeyAST()) != format(other_metadata->getPartitionKeyAST()))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Tables {} and {} have different partition keys", getStorageID().getNameForLogs(), other->getStorageID().getNameForLogs());
        return *other;
    }

    /// A query that reads only this table, when it fans out, is processed up to `WithMergeableState` by the shards.
    bool needsFiltersForQueryProcessingStage() const override { return true; }

    QueryProcessingStage::Enum getQueryProcessingStage(
        ContextPtr context,
        QueryProcessingStage::Enum to_stage,
        const StorageSnapshotPtr & /*storage_snapshot*/,
        SelectQueryInfo & query_info) const override
    {
        const auto & store = ObjectPartStore::instance();
        const UInt64 fan_out_bytes = store.fanOutBytes(getStorageID());
        if (to_stage < QueryProcessingStage::WithMergeableState || store.isShardQuery() || fan_out_bytes == 0)
            return QueryProcessingStage::FetchColumns;
        const auto * query_node = query_info.query_tree ? query_info.query_tree->as<QueryNode>() : nullptr;
        if (!query_node || !query_info.table_expression)
            return QueryProcessingStage::FetchColumns;
        /// A query that joins this table with others fans out when this table is the leftmost of its joins: the planner
        /// then gives the whole join to this table, as it does to `Distributed`, and each shard joins its parts with the
        /// whole of the other tables, which it reads itself. `instance_types` and the like are small; `prices` is not.
        const auto & join_tree = query_node->getJoinTreeNode();
        if (join_tree.get() != query_info.table_expression.get()
            && (extractLeftTableExpression(query_node->getJoinTreeNodeTyped()).get() != query_info.table_expression.get() || !joinsSplitByLeftTable(join_tree)))
            return QueryProcessingStage::FetchColumns;
        /// A shard sees only its parts of this table, so a query that reads it again, in a subquery, an `IN` or on the
        /// other side of a join, would see only those there too.
        if (countTableReferences(query_info.query_tree, getStorageID()) != 1)
            return QueryProcessingStage::FetchColumns;
        /// A query of rows fans out too: read here, a `SELECT *` with a filter on columns outside the key read 200 MiB of
        /// parts in one engine, which took tens of seconds and could exceed the memory of the isolate. A shard sends back
        /// at most `max_shard_result_bytes` (`isolate.cpp`), so one that keeps too many rows fails the query.
        /// A query that its filter keeps to a few blocks, such as a point query, reads them here.
        if (selectedBytes(query_info, context) <= fan_out_bytes)
            return QueryProcessingStage::FetchColumns;
        return QueryProcessingStage::WithMergeableState;
    }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t /*max_block_size*/,
        size_t /*num_streams*/) override
    {
        if (processed_stage != QueryProcessingStage::FetchColumns)
        {
            readFromShards(query_plan, storage_snapshot, query_info, context, processed_stage);
            return;
        }
        storage_snapshot->check(column_names);
        query_plan.addStep(std::make_unique<ReadFromObjectMergeTree>(
            column_names, query_info, storage_snapshot, context, ObjectPartStore::instance().getVisibleParts(getStorageID()),
            skipIndices(storage_snapshot->metadata, *storage_settings)));
    }

    SinkToStoragePtr write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr context, bool /*async_insert*/) override;

    /// Merges the visible parts of each partition into one: the host shows the engine the parts it chose to merge.
    bool optimize(
        const ASTPtr & /*query*/,
        const StorageMetadataPtr & metadata_snapshot,
        const ASTPtr & partition,
        bool /*final*/,
        bool deduplicate,
        const Names & /*deduplicate_by_columns*/,
        bool /*cleanup*/,
        ContextPtr context) override
    {
        if (deduplicate)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ObjectMergeTree does not deduplicate");
        std::optional<String> only;
        if (partition)
            only = partitionIdFromAST(partition->as<const ASTPartition &>(), *metadata_snapshot, context);

        std::map<String, Strings> by_partition;
        for (const auto & part : ObjectPartStore::instance().getVisibleParts(getStorageID()))
            by_partition[ObjectPartName::parse(part).partition_id].push_back(part);

        bool merged = false;
        for (const auto & [partition_id, parts] : by_partition)
        {
            if (only && *only != partition_id)
                continue;
            if (parts.size() > 1 || metadata_snapshot->hasAnyTTL())
                merged |= mergeParts(parts, partition_id, metadata_snapshot, context, /* force= */ false);
        }
        return merged;
    }

    void checkMutationIsPossible(const MutationCommands & commands, const Settings & /*settings*/) const override
    {
        const auto metadata = getInMemoryMetadataPtr(CurrentThread::tryGetQueryContext(), false);
        const auto key_columns = keyColumns(*metadata);
        for (const auto & command : commands)
        {
            if (command.type != MutationCommand::UPDATE)
                continue;
            for (const auto & [column, expression] : getColumnToUpdateExpression(*command.ast()))
                if (key_columns.contains(column))
                    throw Exception(ErrorCodes::CANNOT_UPDATE_COLUMN, "Cannot UPDATE key column {}", column);
        }
    }

    /// A mutation (`ALTER TABLE ... DELETE` or `UPDATE`, `DELETE FROM`) rewrites the parts one at a time, so that it
    /// holds one part, compressed, at a time. It does not change the key columns, so the rows stay sorted.
    ///
    /// `MATERIALIZE INDEX` and `MATERIALIZE TTL` rewrite every part, which makes the granules of the indexes of now and
    /// applies the TTL of now. A dropped or cleared index, or a projection, needs nothing: a granule that is not of an
    /// index of now is not used, and the parts have no projections.
    void mutate(const MutationCommands & commands, ContextPtr context) override
    {
        MutationCommands rewrites;
        bool rewrite_all = false;
        bool materialize_ttl = false;
        for (const auto & command : commands)
        {
            switch (command.type)
            {
                case MutationCommand::MATERIALIZE_INDEX:
                    rewrite_all = true;
                    break;
                case MutationCommand::MATERIALIZE_TTL:
                    rewrite_all = materialize_ttl = true;
                    break;
                case MutationCommand::DROP_INDEX:
                case MutationCommand::DROP_PROJECTION:
                case MutationCommand::MATERIALIZE_PROJECTION:
                    break;
                default:
                    rewrites.push_back(command);
            }
        }
        if (rewrites.empty() && !rewrite_all)
            return;

        auto & store = ObjectPartStore::instance();
        const auto table_id = getStorageID();
        auto metadata_snapshot = getInMemoryMetadataPtr(context, false);
        auto storage = DatabaseCatalog::instance().getTable(table_id, context);
        const Block header = metadata_snapshot->getSampleBlock();
        for (const auto & part : store.getVisibleParts(table_id))
        {
            store.beginOverride(table_id.database_name, table_id.table_name, {part});
            SCOPE_EXIT(store.endOverride());
            std::optional<MutationsInterpreter> interpreter;
            QueryPipeline pipeline;
            if (rewrites.empty())
            {
                pipeline = QueryPipeline(readWholePart(part, metadata_snapshot, context));
            }
            else
            {
                MutationsInterpreter::Settings settings(/* can_execute_= */ true);
                settings.return_all_columns = true;
                interpreter.emplace(storage, metadata_snapshot, rewrites, context, settings);
                pipeline = QueryPipelineBuilder::getPipeline(interpreter->execute());
            }
            PullingPipelineExecutor executor(pipeline);
            ObjectPartWriter writer = partWriter(metadata_snapshot);
            Block block;
            while (executor.pull(block))
            {
                Block physical = physicalColumns(block, header);
                if (materialize_ttl)
                    applyTTL(physical, *metadata_snapshot, context);
                if (writer.rows() == 0 && physical.rows() > 0)
                    writer.setPartition(partitionText(metadata_snapshot, physical, context));
                writer.write(physical);
            }
            addPart(writer, ObjectPartName::parse(part).partition_id, {part});
        }
    }

    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const override
    {
        const auto metadata = getInMemoryMetadataPtr(context, false);
        const auto key_columns = keyColumns(*metadata);
        NameSet added_columns;
        for (const auto & command : commands)
        {
            switch (command.type)
            {
                case AlterCommand::Type::ADD_COLUMN:
                    added_columns.insert(command.column_name);
                    break;
                case AlterCommand::Type::COMMENT_COLUMN:
                case AlterCommand::Type::COMMENT_TABLE:
                case AlterCommand::Type::ADD_INDEX:
                case AlterCommand::Type::DROP_INDEX:
                case AlterCommand::Type::ADD_CONSTRAINT:
                case AlterCommand::Type::DROP_CONSTRAINT:
                case AlterCommand::Type::ADD_PROJECTION:
                case AlterCommand::Type::DROP_PROJECTION:
                case AlterCommand::Type::MODIFY_ORDER_BY:
                case AlterCommand::Type::MODIFY_TTL:
                case AlterCommand::Type::REMOVE_TTL:
                case AlterCommand::Type::MODIFY_SETTING:
                case AlterCommand::Type::RESET_SETTING:
                    break;
                case AlterCommand::Type::DROP_COLUMN:
                case AlterCommand::Type::MODIFY_COLUMN:
                    if (key_columns.contains(command.column_name))
                        throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN, "Cannot {} key column {}", command.type, command.column_name);
                    break;
                default:
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ALTER of type {} is not supported by ObjectMergeTree", command.type);
            }
        }

        StorageInMemoryMetadata new_metadata = *metadata;
        commands.apply(new_metadata, context);
        /// As `MergeTreeData::checkAlterIsPossible`: the parts stay sorted only if the new sorting key is the old one
        /// with expressions of columns that this `ALTER` adds after it, which have the same value in every row of a part.
        const auto & old_key = metadata->getSortingKey();
        const auto & new_key = new_metadata.getSortingKey();
        const auto old_expressions = old_key.expression_list_ast ? old_key.expression_list_ast->children : ASTs{};
        const auto new_expressions = new_key.expression_list_ast ? new_key.expression_list_ast->children : ASTs{};
        if (new_expressions.size() < old_expressions.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The sorting key of ObjectMergeTree can only get expressions at its end");
        for (size_t i = 0; i < old_expressions.size(); ++i)
            if (old_expressions[i]->formatWithSecretsOneLine() != new_expressions[i]->formatWithSecretsOneLine())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The sorting key of ObjectMergeTree can only get expressions at its end");
        if (new_expressions.size() > old_expressions.size())
        {
            const auto old_columns = old_key.expression ? old_key.expression->getRequiredColumns() : Names{};
            for (const auto & column : new_key.expression->getRequiredColumns())
                if (std::find(old_columns.begin(), old_columns.end(), column) == old_columns.end() && !added_columns.contains(column))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Existing column {} is used in the expression that was added to the sorting key. "
                        "You can add expressions that use only the newly added columns", column);
        }
        checkTTL(new_metadata.table_ttl);
        const auto settings_changes = new_metadata.settings_changes ? new_metadata.settings_changes->as<const ASTSetQuery &>().changes : SettingsChanges{};
        const auto new_settings = tableSettings(settings_changes, context);
        if (rowsPerBlock(*new_settings) != rowsPerBlock(*storage_settings))
            throw Exception(ErrorCodes::READONLY_SETTING, "index_granularity of a table of ObjectMergeTree cannot change");
        skipIndices(std::make_shared<const StorageInMemoryMetadata>(new_metadata), *new_settings);
    }

    /// Changes the definition only: the parts keep their columns, and a block is converted when it is read
    /// (`readPartBlock`), which fills the columns added since with their defaults and casts the modified ones.
    void alter(const AlterCommands & commands, ContextPtr context, AlterLockHolder & /*alter_lock_holder*/, DDLGuardPtr & /*ddl_guard*/) override
    {
        const auto table_id = getStorageID();
        const auto metadata = getInMemoryMetadataPtr(context, false);
        StorageInMemoryMetadata new_metadata = *metadata;
        commands.apply(new_metadata, context);
        auto database = DatabaseCatalog::instance().getDatabase(table_id.database_name);
        database->alterTable(context, table_id, new_metadata, /* validate_new_create_query= */ true);
        setInMemoryMetadata(new_metadata);
        /// The `ENGINE` clause has the sorting key, the TTL and the settings, which the database has updated.
        storage_def = database->getCreateTableQuery(table_id.table_name, context)->as<const ASTCreateQuery &>().storage->clone();
        storage_settings = tableSettings(settingsChanges(storage_def->as<const ASTStorage &>()), context);
        /// As `StorageMergeTree::alter`: a new TTL applies to the parts there are, unless `materialize_ttl_after_modify` = 0.
        MutationCommands materialize;
        for (const auto & command : commands.getMutationCommands(*metadata, context->getSettingsRef()[Setting::materialize_ttl_after_modify], context))
            if (command.type == MutationCommand::MATERIALIZE_TTL)
                materialize.push_back(command);
        auto & store = ObjectPartStore::instance();
        if (!store.isHostQuery())
            store.addChange({
                .kind = CatalogChange::Kind::AlterTable,
                .database = table_id.database_name,
                .table = table_id.table_name,
                .create_query = createQuery(new_metadata),
                .has_ttl = new_metadata.hasRowsTTL(),
            });
        if (!materialize.empty())
            mutate(materialize, context);
    }

    void checkAlterPartitionIsPossible(
        const PartitionCommands & commands, const StorageMetadataPtr & /*metadata_snapshot*/, const Settings & settings, ContextPtr /*context*/) const override
    {
        for (const auto & command : commands)
        {
            switch (command.type)
            {
                case PartitionCommand::DROP_PARTITION:
                case PartitionCommand::ATTACH_PARTITION:
                case PartitionCommand::REPLACE_PARTITION:
                case PartitionCommand::FORGET_PARTITION:
                    break;
                case PartitionCommand::DROP_DETACHED_PARTITION:
                    if (!settings[Setting::allow_drop_detached])
                        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                            "Cannot execute query: DROP DETACHED PART is disabled (see allow_drop_detached setting)");
                    break;
                case PartitionCommand::MOVE_PARTITION:
                    if (command.move_destination_type != PartitionCommand::TABLE)
                        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The parts of ObjectMergeTree are in object storage, without disks or volumes");
                    break;
                default:
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ObjectMergeTree has no {}", command.typeToString());
            }
        }
    }

    /// The commands on partitions, as in `MergeTree`. They only change which parts the table and its detached parts
    /// have. The parts of another table are shared, not copied, so that `REPLACE PARTITION ... FROM` and `MOVE PARTITION`
    /// are one commit of the catalog, whatever the size of the partition.
    Pipe alterPartition(const StorageMetadataPtr & metadata_snapshot, const PartitionCommands & commands, ContextPtr context) override
    {
        auto & store = ObjectPartStore::instance();
        const auto table_id = getStorageID();
        for (const auto & command : commands)
        {
            /// The parts that the command is about: a part, by its name, or the parts of a partition.
            auto partition_id = [&] { return partitionIdFromAST(command.partition->as<const ASTPartition &>(), *metadata_snapshot, context); };
            auto select = [&](const Strings & parts)
            {
                Strings result;
                if (command.part)
                {
                    const String name = command.partition->as<const ASTLiteral &>().value.safeGet<String>();
                    if (std::find(parts.begin(), parts.end(), name) == parts.end())
                        throw Exception(ErrorCodes::NO_SUCH_DATA_PART, "No part {} in table {}", name, table_id.getNameForLogs());
                    result.push_back(name);
                    return result;
                }
                const String id = partition_id();
                for (const auto & part : parts)
                    if (ObjectPartName::parse(part).partition_id == id)
                        result.push_back(part);
                return result;
            };
            CatalogChange change{.kind = CatalogChange::Kind::ReplaceParts, .database = table_id.database_name, .table = table_id.table_name};
            switch (command.type)
            {
                case PartitionCommand::DROP_PARTITION:
                    (command.detach ? change.detached : change.removed) = select(store.getVisibleParts(table_id));
                    break;
                case PartitionCommand::ATTACH_PARTITION:
                    change.attached = select(store.getDetachedParts(table_id));
                    break;
                case PartitionCommand::DROP_DETACHED_PARTITION:
                    change.dropped_detached = select(store.getDetachedParts(table_id));
                    break;
                case PartitionCommand::FORGET_PARTITION:
                    if (!select(store.getVisibleParts(table_id)).empty())
                        throw Exception(ErrorCodes::CANNOT_FORGET_PARTITION, "Partition is not empty");
                    break;
                case PartitionCommand::REPLACE_PARTITION:
                {
                    /// `ATTACH PARTITION ... FROM` adds the parts of the partition of the other table, and `REPLACE
                    /// PARTITION ... FROM` also removes the parts of the partition of this one.
                    const auto source = DatabaseCatalog::instance().getTable({context->resolveDatabase(command.from_database), command.from_table}, context);
                    const auto & other = checkSameStructure(source, metadata_snapshot, context);
                    const auto other_id = other.getStorageID();
                    const String id = partition_id();
                    for (const auto & part : store.getVisibleParts(other_id))
                        if (ObjectPartName::parse(part).partition_id == id)
                            change.shared.push_back(part);
                    change.shared_from = {other_id.database_name, other_id.table_name};
                    if (command.replace)
                        change.removed = select(store.getVisibleParts(table_id));
                    break;
                }
                case PartitionCommand::MOVE_PARTITION:
                {
                    const auto destination = DatabaseCatalog::instance().getTable({context->resolveDatabase(command.to_database), command.to_table}, context);
                    const auto & other = checkSameStructure(destination, metadata_snapshot, context);
                    const auto other_id = other.getStorageID();
                    /// The destination gets the parts before this table gives them up, in the same commit.
                    CatalogChange moved{.kind = CatalogChange::Kind::ReplaceParts, .database = other_id.database_name, .table = other_id.table_name};
                    change.removed = select(store.getVisibleParts(table_id));
                    moved.shared = change.removed;
                    moved.shared_from = {table_id.database_name, table_id.table_name};
                    const auto destination_parts = store.getVisibleParts(other_id);
                    for (const auto & part : moved.shared)
                        if (std::find(destination_parts.begin(), destination_parts.end(), part) != destination_parts.end())
                            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table {} has part {} already", other_id.getNameForLogs(), part);
                    if (!moved.shared.empty())
                        store.addChange(std::move(moved));
                    break;
                }
                default:
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ObjectMergeTree has no {}", command.typeToString());
            }
            /// A table has a part once: the parts of another table can be attached to it once.
            for (const auto & part : change.shared)
            {
                const auto visible = store.getVisibleParts(table_id);
                if (std::find(visible.begin(), visible.end(), part) != visible.end()
                    && std::find(change.removed.begin(), change.removed.end(), part) == change.removed.end())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table {} has part {} already, from {}.{}",
                        table_id.getNameForLogs(), part, change.shared_from.first, change.shared_from.second);
            }
            if (!change.removed.empty() || !change.detached.empty() || !change.attached.empty() || !change.dropped_detached.empty()
                || !change.shared.empty())
                store.addChange(std::move(change));
        }
        return {};
    }

    void truncate(const ASTPtr & /*query*/, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr /*context*/, TableExclusiveLockHolder &) override
    {
        auto & store = ObjectPartStore::instance();
        auto removed = store.getVisibleParts(getStorageID());
        if (removed.empty())
            return;
        const auto table_id = getStorageID();
        store.addChange({.kind = CatalogChange::Kind::ReplaceParts, .database = table_id.database_name, .table = table_id.table_name, .removed = std::move(removed)});
    }

    void drop() override
    {
        auto & store = ObjectPartStore::instance();
        if (store.isHostQuery())
            return;
        const auto table_id = getStorageID();
        store.addChange({.kind = CatalogChange::Kind::DropTable, .database = table_id.database_name, .table = table_id.table_name});
    }

    /// The parts keep their names: only the catalog moves them to the new name of the table.
    void renameInMemory(const StorageID & new_table_id) override
    {
        const auto old_table_id = getStorageID();
        IStorage::renameInMemory(new_table_id);
        auto & store = ObjectPartStore::instance();
        store.renameTable(old_table_id, new_table_id);
        if (store.isHostQuery())
            return;
        const auto metadata = getInMemoryMetadataPtr(CurrentThread::tryGetQueryContext(), false);
        /// `EXCHANGE TABLES a AND b` renames `a` to `b`, then `b` to `a`, which the catalog does at once.
        auto & changes = store.getChanges();
        if (!changes.empty() && changes.back().kind == CatalogChange::Kind::RenameTable
            && changes.back().database == new_table_id.database_name && changes.back().table == new_table_id.table_name
            && changes.back().to_database == old_table_id.database_name && changes.back().to_table == old_table_id.table_name)
        {
            auto & exchange = changes.back();
            exchange.kind = CatalogChange::Kind::ExchangeTables;
            exchange.to_create_query = createQuery(*metadata);
            exchange.to_has_ttl = metadata->hasRowsTTL();
            return;
        }
        store.addChange({
            .kind = CatalogChange::Kind::RenameTable,
            .database = old_table_id.database_name,
            .table = old_table_id.table_name,
            .to_database = new_table_id.database_name,
            .to_table = new_table_id.table_name,
            .create_query = createQuery(*metadata),
            .has_ttl = metadata->hasRowsTTL(),
        });
    }

    /// Adds the part that `writer` wrote in place of the parts `removed`. A part without rows only removes them. The
    /// host puts the part in object storage now, so that a query that writes many parts holds one at a time.
    void addPart(ObjectPartWriter & writer, const String & partition_id, Strings removed) const
    {
        auto & store = ObjectPartStore::instance();
        const auto table_id = getStorageID();
        CatalogChange change{.kind = CatalogChange::Kind::ReplaceParts, .database = table_id.database_name, .table = table_id.table_name, .removed = std::move(removed)};
        if (writer.rows() > 0)
        {
            change.added_rows = writer.rows();
            const String data = writer.finish();
            change.added = ObjectPartName{
                .uuid = toString(UUIDHelpers::generateV4()), .rows = change.added_rows, .bytes = data.size(), .partition_id = partition_id}
                               .toString();
            writePart(change.added, data);
        }
        else if (change.removed.empty())
        {
            return;
        }
        store.addChange(std::move(change));
    }

private:
    /// The bytes that the query reads: of the columns that it needs, of the blocks of the visible parts that its filter
    /// may need.
    UInt64 selectedBytes(const SelectQueryInfo & query_info, const ContextPtr & context) const
    {
        const auto metadata = getInMemoryMetadataPtr(context, false);
        const auto parts = ObjectPartStore::instance().getVisibleParts(getStorageID());
        const auto minmax = tableMinmaxColumns(*metadata);
        Names needed;
        for (const auto & name : query_info.planner_context->getTableExpressionDataOrThrow(query_info.table_expression).getColumnNames())
            if (auto column = metadata->getColumns().tryGetColumnOrSubcolumn(GetColumnsOptions::All, name))
                needed.push_back(column->getNameInStorage());
        UInt64 bytes = 0;
        if (!query_info.filter_actions_dag)
        {
            for (const auto & part : parts)
                prefetchPartIndex(part, ObjectPartName::parse(part).bytes);
            for (const auto & part : parts)
            {
                const auto index = readPartIndex(part, ObjectPartName::parse(part).bytes);
                for (size_t i = 0; i < index->blocks.size(); ++i)
                    bytes += index->bytesToRead(i, needed);
            }
            return bytes;
        }
        ActionsDAGWithInversionPushDown inverted(query_info.filter_actions_dag->getOutputs().front(), context, /* boolean_context= */ true);
        KeyCondition condition(inverted, context, minmax.getNames(), std::make_shared<ExpressionActions>(ActionsDAG(minmax)));
        const auto indices = skipIndices(metadata, *storage_settings);
        const auto skip = skipConditions(indices, query_info.filter_actions_dag->getOutputs().front(), context);
        for (const auto & part : parts)
            prefetchPartIndex(part, ObjectPartName::parse(part).bytes);
        for (const auto & part : parts)
        {
            const auto index = readPartIndex(part, ObjectPartName::parse(part).bytes);
            for (size_t i = 0; i < index->blocks.size(); ++i)
                if (mayContainRows(condition, minmax, *index, index->blocks[i].ranges) && skipIndicesMayContainRows(skip, *index, i))
                    bytes += index->bytesToRead(i, needed);
        }
        return bytes;
    }

    /// Streams all the rows of a part, with the columns of the table as `metadata` defines it.
    static Pipe readWholePart(const String & part, const StorageMetadataPtr & metadata, const ContextPtr & context)
    {
        auto index = readPartIndex(part, ObjectPartName::parse(part).bytes);
        std::vector<size_t> blocks(index->blocks.size());
        for (size_t i = 0; i < blocks.size(); ++i)
            blocks[i] = i;
        prefetchPartBlocks(part, *index, blocks, {});
        return Pipe(std::make_shared<ObjectPartSource>(
            std::make_shared<const Block>(metadata->getSampleBlock()), part, std::move(index), std::move(blocks),
            metadata->getColumns().getAllPhysical(), metadata->getColumns(), context));
    }

    /// Merges parts of one partition into one, streaming: each part is sorted, so a merge sort of their blocks makes
    /// the merged part, and the TTL removes the rows that have expired. A single part is rewritten only for the TTL,
    /// unless `force`.
    bool mergeParts(const Strings & parts, const String & partition_id, const StorageMetadataPtr & metadata, const ContextPtr & context, bool force) const
    {
        const Block header = metadata->getSampleBlock();
        for (const auto & part : parts)
            prefetchPartIndex(part, ObjectPartName::parse(part).bytes);
        Pipes pipes;
        for (const auto & part : parts)
            pipes.emplace_back(readWholePart(part, metadata, context));
        Pipe pipe = Pipe::unitePipes(std::move(pipes));
        if (!metadata->getSortingKey().column_names.empty() && pipe.numOutputPorts() > 1)
        {
            auto expression = std::make_shared<ExpressionActions>(withSortingKey(*metadata, header));
            pipe.addSimpleTransform([&](const SharedHeader & stream_header) { return std::make_shared<ExpressionTransform>(stream_header, expression); });
            pipe.addTransform(std::make_shared<MergingSortedTransform>(
                pipe.getSharedHeader(), pipe.numOutputPorts(), sortDescription(*metadata), rowsPerBlock(*storage_settings), 0, std::nullopt,
                SortingQueueStrategy::Batch));
        }

        QueryPipeline pipeline(std::move(pipe));
        PullingPipelineExecutor executor(pipeline);
        ObjectPartWriter writer = partWriter(metadata);
        bool expired = false;
        Block block;
        while (executor.pull(block))
        {
            Block physical_block = physicalColumns(block, header);
            expired |= applyTTL(physical_block, *metadata, context);
            if (writer.rows() == 0 && physical_block.rows() > 0)
                writer.setPartition(partitionText(metadata, physical_block, context));
            writer.write(physical_block);
        }
        if (parts.size() == 1 && !expired && !force)
            return false;
        addPart(writer, partition_id, parts);
        return true;
    }

    /// As `StorageDistributed::read`: the query for the shards is the query tree with the table in it, and their
    /// results come back at `processed_stage`, one stream per shard, for the planner to merge.
    void readFromShards(
        QueryPlan & query_plan,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage) const
    {
        const auto & planner_context = query_info.planner_context;
        auto shard_table = std::make_shared<TableNode>(
            std::make_shared<StorageDummy>(getStorageID(), ColumnsDescription{storage_snapshot->getColumns(GetColumnsOptions(GetColumnsOptions::All))}),
            planner_context->getQueryContext());
        if (const auto * table_node = query_info.table_expression->as<TableNode>())
            if (const auto & modifiers = table_node->getTableExpressionModifiers())
                shard_table->setTableExpressionModifiers(*modifiers);
        shard_table->setAlias(query_info.table_expression->getAlias());
        auto shard_tree = buildQueryTreeForShard(
            planner_context, query_info.query_tree->cloneAndReplace(query_info.table_expression, std::move(shard_table)), false);

        Block header = *InterpreterSelectQueryAnalyzer::getSampleBlock(shard_tree, context, SelectQueryOptions(processed_stage).analyze());
        for (auto & column : header)
            column.column = column.column->convertToFullColumnIfConst();
        auto shared_header = std::make_shared<const Block>(header);

        auto tree_for_ast = shard_tree->clone();
        removeGroupingFunctionSpecializations(tree_for_ast);
        const String shard_query = queryNodeToDistributedSelectQuery(tree_for_ast)->formatWithSecretsOneLine();

        auto & store = ObjectPartStore::instance();
        const auto table_id = getStorageID();
        const auto * results = store.getFanOutResults(table_id, shard_query);
        if (!results)
        {
            const auto * query_node = query_info.query_tree ? query_info.query_tree->as<QueryNode>() : nullptr;
            const bool joins = query_node && query_info.table_expression
                && query_node->getJoinTreeNode().get() != query_info.table_expression.get();
            store.addFanOutRequest({.database = table_id.database_name, .table = table_id.table_name, .query = shard_query, .joins = joins});
            throw Exception(ErrorCodes::NOT_FOUND_EXPECTED_DATA_PART, "The query on table {} fans out to shards", table_id.getNameForLogs());
        }

        /// As `RemoteSource`: aggregation states are merged by bucket, which the block info of a shard carries.
        bool aggregated = false;
        for (const auto & type : header.getDataTypes())
            aggregated |= typeid_cast<const DataTypeAggregateFunction *>(type.get()) != nullptr;

        Pipes pipes;
        for (const auto & data : *results)
        {
            Chunks chunks;
            for (auto & block : ObjectPartStore::decodeBlocks(data))
            {
                if (block.rows() == 0)
                    continue;
                const BlockInfo info = block.info;
                auto converting = ActionsDAG::makeConvertingActions(
                    block.getColumnsWithTypeAndName(), header.getColumnsWithTypeAndName(), ActionsDAG::MatchColumnsMode::Name, context);
                ExpressionActions(std::move(converting)).execute(block);
                const size_t rows = block.rows();
                Chunk chunk(block.getColumns(), rows);
                if (aggregated)
                {
                    auto chunk_info = std::make_shared<AggregatedChunkInfo>();
                    chunk_info->bucket_num = info.bucket_num;
                    chunk_info->is_overflows = info.is_overflows;
                    chunk_info->out_of_order_buckets = info.out_of_order_buckets;
                    chunk.getChunkInfos().add(std::move(chunk_info));
                }
                chunks.push_back(std::move(chunk));
            }
            pipes.emplace_back(std::make_shared<SourceFromChunks>(shared_header, std::move(chunks)));
        }
        if (pipes.empty())
            pipes.emplace_back(std::make_shared<NullSource>(shared_header));
        auto step = std::make_unique<ReadFromPreparedSource>(Pipe::unitePipes(std::move(pipes)));
        step->setStepDescription("Read from the shards of ObjectMergeTree");
        query_plan.addStep(std::move(step));
    }

    /// The `ENGINE` clause, for the statement that creates the table after an `ALTER`.
    ASTPtr storage_def;
    std::shared_ptr<const MergeTreeSettings> storage_settings;
};


/// An `INSERT` makes a part of each partition of each block that it receives, sorted by the sorting key.
class ObjectMergeTreeSink final : public SinkToStorage
{
public:
    ObjectMergeTreeSink(const StorageObjectMergeTree & storage_, const StorageMetadataPtr & metadata_snapshot_, ContextPtr context_)
        : SinkToStorage(std::make_shared<const Block>(metadata_snapshot_->getSampleBlock()))
        , storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
        , context(std::move(context_))
    {
    }

    String getName() const override { return "ObjectMergeTreeSink"; }

    void consume(Chunk & chunk) override
    {
        auto block = getHeader().cloneWithColumns(chunk.getColumns());
        metadata_snapshot->check(block, true);
        if (block.rows() == 0)
            return;
        const size_t max_parts = context->getSettingsRef()[Setting::max_partitions_per_insert_block];
        for (auto & part : MergeTreeDataWriter::splitBlockIntoParts(std::move(block), max_parts, metadata_snapshot, context))
        {
            const String partition_id
                = part.partition_id.empty() ? part.partition.getID(metadata_snapshot->getPartitionKey().sample_block) : part.partition_id;
            Block rows = std::move(*part.block);
            sortBySortingKey(rows, *metadata_snapshot);
            ObjectPartWriter writer = storage.partWriter(metadata_snapshot);
            writer.setPartition(partitionText(metadata_snapshot, rows, context));
            writer.write(rows);
            storage.addPart(writer, partition_id, {});
        }
    }

private:
    const StorageObjectMergeTree & storage;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr context;
};

SinkToStoragePtr StorageObjectMergeTree::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr context, bool /*async_insert*/)
{
    return std::make_shared<ObjectMergeTreeSink>(*this, metadata_snapshot, context);
}


class StorageSystemObjectParts final : public IStorageSystemOneBlock
{
public:
    explicit StorageSystemObjectParts(const StorageID & table_id_) : IStorageSystemOneBlock(table_id_, getColumnsDescription()) {}

    std::string getName() const override { return "SystemObjectParts"; }

protected:
    bool supportsColumnsMask() const override { return true; }

public:

    static ColumnsDescription getColumnsDescription()
    {
        auto string = std::make_shared<DataTypeString>();
        auto uint64 = std::make_shared<DataTypeUInt64>();
        return ColumnsDescription{
            {"database", string, "The database of the table."},
            {"table", string, "The table."},
            {"name", string, "The name of the part: its UUID, rows, bytes and partition."},
            {"partition", string, "The value of the partition of the part. Empty for the parts written before it was kept."},
            {"partition_id", string, "The id of the partition of the part."},
            {"rows", uint64, "The number of rows of the part."},
            {"bytes_on_disk", uint64, "The size of the part in object storage."},
            {"active", std::make_shared<DataTypeUInt8>(), "1: every part listed is in the catalog."},
        };
    }

protected:
    void fillData(MutableColumns & res_columns, ContextPtr /*context*/, const ActionsDAG::Node * /*predicate*/, std::vector<UInt8> columns_mask) const override
    {
        const auto & all = ObjectPartStore::instance().allVisibleParts();
        /// The value of the partition is in the index of the part, which is read only if the query needs it.
        const bool with_partition = columns_mask.at(3) != 0;
        if (with_partition)
            for (const auto & [table, parts] : all)
                for (const auto & part : parts)
                    prefetchPartIndex(part, ObjectPartName::parse(part).bytes);
        for (const auto & [table, parts] : all)
        {
            for (const auto & part : parts)
            {
                const auto name = ObjectPartName::parse(part);
                size_t source = 0;
                size_t result = 0;
                auto insert = [&](const Field & value)
                {
                    if (columns_mask[source++])
                        res_columns[result++]->insert(value);
                };
                insert(table.first);
                insert(table.second);
                insert(part);
                insert(with_partition ? readPartIndex(part, name.bytes)->partition : String{});
                insert(name.partition_id);
                insert(name.rows);
                insert(name.bytes);
                insert(1);
            }
        }
    }
};

class StorageSystemObjectDetachedParts final : public IStorageSystemOneBlock
{
public:
    explicit StorageSystemObjectDetachedParts(const StorageID & table_id_) : IStorageSystemOneBlock(table_id_, getColumnsDescription()) {}

    std::string getName() const override { return "SystemObjectDetachedParts"; }

    static ColumnsDescription getColumnsDescription()
    {
        auto string = std::make_shared<DataTypeString>();
        auto uint64 = std::make_shared<DataTypeUInt64>();
        return ColumnsDescription{
            {"database", string, "The database of the table."},
            {"table", string, "The table."},
            {"name", string, "The name of the part: its UUID, rows, bytes and partition."},
            {"partition_id", string, "The id of the partition of the part."},
            {"rows", uint64, "The number of rows of the part."},
            {"bytes_on_disk", uint64, "The size of the part in object storage."},
        };
    }

protected:
    void fillData(MutableColumns & res_columns, ContextPtr /*context*/, const ActionsDAG::Node * /*predicate*/, std::vector<UInt8> /*columns_mask*/) const override
    {
        for (const auto & [table, parts] : ObjectPartStore::instance().allDetachedParts())
        {
            for (const auto & part : parts)
            {
                const auto name = ObjectPartName::parse(part);
                size_t i = 0;
                res_columns[i++]->insert(table.first);
                res_columns[i++]->insert(table.second);
                res_columns[i++]->insert(part);
                res_columns[i++]->insert(name.partition_id);
                res_columns[i++]->insert(name.rows);
                res_columns[i++]->insert(name.bytes);
            }
        }
    }
};

}


ObjectPartName ObjectPartName::parse(const String & name)
{
    /// The partition id may contain `_` but the rest may not.
    size_t positions[3];
    size_t from = 0;
    for (auto & position : positions)
    {
        position = name.find('_', from);
        if (position == String::npos)
            throw Exception(ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED, "Not the name of a part: {}", name);
        from = position + 1;
    }
    ObjectPartName result;
    result.uuid = name.substr(0, positions[0]);
    result.rows = DB::parse<UInt64>(name.substr(positions[0] + 1, positions[1] - positions[0] - 1));
    result.bytes = DB::parse<UInt64>(name.substr(positions[1] + 1, positions[2] - positions[1] - 1));
    result.partition_id = name.substr(positions[2] + 1);
    if (result.uuid.size() != 36 || result.partition_id.empty())
        throw Exception(ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED, "Not the name of a part: {}", name);
    return result;
}

String ObjectPartName::toString() const
{
    return fmt::format("{}_{}_{}_{}", uuid, rows, bytes, partition_id);
}


ObjectPartStore & ObjectPartStore::instance()
{
    static ObjectPartStore store;
    return store;
}

void ObjectPartStore::setVisibleParts(const String & database, const String & table, Strings parts)
{
    if (parts.empty())
        visible_parts.erase({database, table});
    else
        visible_parts[{database, table}] = std::move(parts);
}

void ObjectPartStore::setDetachedParts(const String & database, const String & table, Strings parts)
{
    if (parts.empty())
        detached_parts.erase({database, table});
    else
        detached_parts[{database, table}] = std::move(parts);
}

Strings ObjectPartStore::getDetachedParts(const StorageID & table_id) const
{
    auto it = detached_parts.find({table_id.database_name, table_id.table_name});
    return it == detached_parts.end() ? Strings{} : it->second;
}

void ObjectPartStore::renameTable(const StorageID & from, const StorageID & to)
{
    const std::pair<String, String> from_key{from.database_name, from.table_name};
    const std::pair<String, String> to_key{to.database_name, to.table_name};
    for (auto * parts_of_tables : {&visible_parts, &detached_parts})
    {
        if (auto parts = parts_of_tables->extract(from_key))
        {
            parts.key() = to_key;
            parts_of_tables->insert(std::move(parts));
        }
    }
    if (auto fan_out = fan_out_tables.extract(from_key))
    {
        fan_out.key() = to_key;
        fan_out_tables.insert(std::move(fan_out));
    }
}

Strings ObjectPartStore::getVisibleParts(const StorageID & table_id) const
{
    const std::pair<String, String> key{table_id.database_name, table_id.table_name};
    if (visible_override && visible_override->first == key)
        return visible_override->second;
    auto it = visible_parts.find(key);
    return it == visible_parts.end() ? Strings{} : it->second;
}

void ObjectPartStore::beginOverride(const String & database, const String & table, Strings parts)
{
    visible_override.emplace(std::pair{database, table}, std::move(parts));
}

void ObjectPartStore::endOverride()
{
    visible_override.reset();
}

void ObjectPartStore::addChange(CatalogChange change)
{
    changes.push_back(std::move(change));
}

void ObjectPartStore::setFanOut(const String & database, const String & table, UInt64 bytes)
{
    if (bytes)
        fan_out_tables[{database, table}] = bytes;
    else
        fan_out_tables.erase({database, table});
}

UInt64 ObjectPartStore::fanOutBytes(const StorageID & table_id) const
{
    auto it = fan_out_tables.find({table_id.database_name, table_id.table_name});
    return it == fan_out_tables.end() ? 0 : it->second;
}

void ObjectPartStore::addFanOutRequest(FanOutRequest request)
{
    fan_out_requests.push_back(std::move(request));
}

std::vector<FanOutRequest> ObjectPartStore::takeFanOutRequests()
{
    return std::exchange(fan_out_requests, {});
}

void ObjectPartStore::addFanOutResult(const String & database, const String & table, const String & query, String data)
{
    fan_out_results[{database, table, query}].push_back(std::move(data));
}

const Strings * ObjectPartStore::getFanOutResults(const StorageID & table_id, const String & query) const
{
    auto it = fan_out_results.find({table_id.database_name, table_id.table_name, query});
    return it == fan_out_results.end() ? nullptr : &it->second;
}

void ObjectPartStore::clearFanOutResults()
{
    fan_out_results.clear();
    fan_out_requests.clear();
}

String ObjectPartStore::encodeBlocks(const Blocks & blocks)
{
    String data;
    WriteBufferFromString out(data);
    out.write(blocks_magic.data(), blocks_magic.size());
    writeBinaryLittleEndian(blocks_revision, out);
    if (!blocks.empty())
    {
        NativeWriter writer(out, blocks_revision, std::make_shared<const Block>(blocks.front().cloneEmpty()));
        for (const auto & block : blocks)
            writer.write(block);
        writer.flush();
    }
    out.finalize();
    return data;
}

Blocks ObjectPartStore::decodeBlocks(std::string_view data)
{
    ReadBufferFromMemory in(data.data(), data.size());
    String magic(blocks_magic.size(), '\0');
    in.readStrict(magic.data(), magic.size());
    if (magic != blocks_magic)
        throw Exception(ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED, "Not the results of a shard of ObjectMergeTree");
    UInt64 revision;
    readBinaryLittleEndian(revision, in);
    NativeReader reader(in, revision);
    Blocks blocks;
    while (true)
    {
        Block block = reader.read();
        if (block.empty())
            return blocks;
        blocks.push_back(std::move(block));
    }
}


void attachSystemParts(const ContextMutablePtr & context, IDatabase & system_database)
{
    auto storage = std::make_shared<StorageSystemObjectParts>(StorageID(DatabaseCatalog::SYSTEM_DATABASE, "parts"));
    system_database.attachTable(context, "parts", storage, {});
}

void attachSystemDetachedParts(const ContextMutablePtr & context, IDatabase & system_database)
{
    auto storage = std::make_shared<StorageSystemObjectDetachedParts>(StorageID(DatabaseCatalog::SYSTEM_DATABASE, "detached_parts"));
    system_database.attachTable(context, "detached_parts", storage, {});
}


void registerDatabaseCatalog(DatabaseFactory & factory)
{
    auto create = [](const DatabaseFactory::Arguments & args) -> DatabasePtr
    {
        auto & store = ObjectPartStore::instance();
        if (!store.isHostQuery())
            store.addChange({.kind = CatalogChange::Kind::CreateDatabase, .database = args.database_name});
        return std::make_shared<DatabaseMemory>(args.database_name, args.context);
    };
    factory.registerDatabase("Atomic", create, SecretArgumentsSpec{});
}

namespace
{

/// `DROP DATABASE` of a database of the catalog also removes it from the catalog, after the tables in it.
class InterpreterCatalogDropQuery final : public IInterpreter
{
public:
    InterpreterCatalogDropQuery(const ASTPtr & query_, ContextMutablePtr context_)
        : query(query_)
        , inner(query_, std::move(context_))
    {
    }

    BlockIO execute() override
    {
        const auto & drop = query->as<const ASTDropQuery &>();
        const bool drops_database = drop.kind == ASTDropQuery::Drop && drop.database && !drop.table && !drop.database_and_tables;
        const String database = drops_database ? drop.getDatabase() : String{};
        const bool existed = drops_database && DatabaseCatalog::instance().isDatabaseExist(database);
        BlockIO result = inner.execute();
        auto & store = ObjectPartStore::instance();
        if (existed && !store.isHostQuery())
            store.addChange({.kind = CatalogChange::Kind::DropDatabase, .database = database});
        return result;
    }

private:
    ASTPtr query;
    InterpreterDropQuery inner;
};

}

void registerInterpreterCatalogDropQuery(InterpreterFactory & factory)
{
    auto create = [](const InterpreterFactory::Arguments & args) { return std::make_unique<InterpreterCatalogDropQuery>(args.query, args.context); };
    factory.registerInterpreter("InterpreterDropQuery", create);
}

void registerStorageObjectMergeTree(StorageFactory & factory)
{
    auto create = [](const StorageFactory::Arguments & args) -> StoragePtr
    {
        if (!args.engine_args.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "{} has no arguments", args.engine_name);
        const auto & storage_def = *args.storage_def;
        if (storage_def.sample_by)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ObjectMergeTree has no SAMPLE BY");
        if (args.table_id.database_name == DatabaseCatalog::TEMPORARY_DATABASE)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Temporary tables cannot be in ObjectMergeTree, use Memory");

        auto & store = ObjectPartStore::instance();
        /// The tables are in the catalog: an isolate only attaches them from there, when the host tells it to.
        if (args.mode != LoadingStrictnessLevel::CREATE && !store.isHostQuery())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Tables of ObjectMergeTree cannot be attached");

        auto context = args.getContext();
        auto storage_settings = tableSettings(settingsChanges(storage_def), context);
        StorageInMemoryMetadata metadata;
        metadata.setColumns(args.columns);
        metadata.setVirtuals(objectPartVirtuals());
        metadata.setComment(args.comment);
        if (storage_def.settings)
            metadata.settings_changes = storage_def.settings->ptr();
        metadata.sorting_key = storage_def.order_by
            ? KeyDescription::getKeyFromAST(storage_def.order_by->ptr(), args.columns, {}, context)
            : KeyDescription::buildEmptyKey();
        if (storage_def.primary_key)
        {
            metadata.primary_key = KeyDescription::getPrimaryKeyFromAST(storage_def.primary_key->ptr(), metadata.sorting_key, args.columns, {}, context);
            const auto & primary = metadata.primary_key.column_names;
            const auto & sorting = metadata.sorting_key.column_names;
            if (primary.size() > sorting.size() || !std::equal(primary.begin(), primary.end(), sorting.begin()))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The PRIMARY KEY of a table has to be a prefix of its ORDER BY");
        }
        else
        {
            metadata.primary_key = metadata.sorting_key;
        }
        if (storage_def.partition_by)
            metadata.partition_key = KeyDescription::getKeyFromAST(storage_def.partition_by->ptr(), args.columns, {}, context);
        if (storage_def.ttl_table)
        {
            /// The host attaches a table with a TTL that was validated when it was created.
            const auto validation = store.isHostQuery() ? TTLValidationMode::Attach : TTLValidationMode::Validate;
            metadata.table_ttl = TTLTableDescription::getTTLForTableFromAST(storage_def.ttl_table->ptr(), args.columns, context, metadata.primary_key, validation);
            checkTTL(metadata.table_ttl);
        }
        const auto validation = store.isHostQuery() ? TTLValidationMode::Attach : TTLValidationMode::Validate;
        for (const auto & [name, ast] : args.columns.getColumnTTLs())
            metadata.column_ttls_by_name[name] = TTLDescription::getTTLFromAST(ast, args.columns, context, metadata.primary_key, validation);

        /// As `registerStorageMergeTree`, without the implicit indexes: the min-max index of a part is on its keys.
        const auto * columns_list = args.query.columns_list;
        if (columns_list && columns_list->indices)
            for (const auto & index : columns_list->indices->children)
                metadata.secondary_indices.push_back(
                    IndexDescription::getIndexFromAST(index, args.columns, /* is_implicitly_created= */ false, metadata.escape_index_filenames, context));
        if (columns_list && columns_list->projections)
            for (const auto & projection : columns_list->projections->children)
                metadata.projections.add(ProjectionDescription::getProjectionFromAST(
                    projection, args.columns, &metadata.partition_key, context, args.mode, args.query.attach_short_syntax));
        metadata.constraints = args.constraints;
        for (const auto & index : metadata.secondary_indices)
            MergeTreeIndexFactory::instance().validate(index, args.mode > LoadingStrictnessLevel::CREATE, *storage_settings);
        skipIndices(std::make_shared<const StorageInMemoryMetadata>(metadata), *storage_settings);

        auto storage = std::make_shared<StorageObjectMergeTree>(args.table_id, metadata, storage_def.clone(), storage_settings);
        if (!store.isHostQuery())
            store.addChange({
                .kind = CatalogChange::Kind::CreateTable,
                .database = args.table_id.database_name,
                .table = args.table_id.table_name,
                .create_query = storage->createQuery(metadata),
                .has_ttl = metadata.hasRowsTTL(),
            });
        return storage;
    };

    StorageFactory::StorageFeatures features{
        .supports_settings = true,
        .supports_skipping_indices = true,
        .supports_projections = true,
        .supports_sort_order = true,
        .supports_ttl = true,
        .has_builtin_setting_fn = MergeTreeSettings::hasBuiltin};
    factory.registerStorage("ObjectMergeTree", create, SecretArgumentsSpec{}, features);
    /// So that `ENGINE = MergeTree`, and the default engine of `CREATE TABLE`, make tables of `ObjectMergeTree`.
    factory.registerStorage("MergeTree", create, SecretArgumentsSpec{}, features);
}

}
