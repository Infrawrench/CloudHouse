#pragma once

#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <Core/Range.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/ISource.h>
#include <Storages/MergeTree/MergeTreeIndices.h>

#include <memory>
#include <unordered_map>


namespace DB
{

class ColumnsDescription;
class KeyCondition;

/// A part of `ObjectMergeTree`, as it is in object storage:
///
///     "CHPART03" UInt64 revision
///     the columns, one after the other: of each, its chunk of each block, in the `Native` format of `revision`,
///         compressed on its own; a block has at most `rows_per_block` rows
///     the index: one `Native` block, not compressed, with a row per block: `_rows`, the offset and the size of the
///         chunk of each column, `_col_offset.<column>` and `_col_size.<column>`, then the
///         minimum and the maximum of each column of the min-max index, `_min.<column>` and `_max.<column>`, then the
///         granule of each data skipping index of the table, `_skip.<index>`, and the value of the partition of the
///         part, as `system.parts` shows it, `_partition`
///     UInt64 offset of the index, UInt64 size of the index, "CHPART03"
///
/// A query reads the index, then the chunks of the columns that it needs of the blocks that its filter may need, with
/// range reads: the engine asks the host for the bytes and waits for them (JSPI, see `host.js`), so that it holds one
/// block of a part at a time, not the part. The chunks of a column are together, so reading a column of many blocks
/// is one range.
///
/// A part of the earlier format, "CHPART02", has the blocks with all their columns compressed together, and `_offset`
/// and `_size` of each block in its index; it is read whole.
///
/// The min-max index covers the columns of the sorting key and of the partition key that can be compared: a part
/// and a block are skipped when their ranges of these columns cannot satisfy the filter (`KeyCondition`). A data
/// skipping index (`INDEX ... TYPE minmax`, `set`, `bloom_filter`, ...) has a granule per block, made and checked by
/// the index types of `MergeTree`, so a block is also skipped when the granule of an index rules it out.

/// A data skipping index of a table, as the parts store it.
struct ObjectPartSkipIndex
{
    String name;
    /// Of the definition of the index: a granule written for another definition of it is not used.
    UInt64 fingerprint = 0;
    MergeTreeIndexVersion version = 0;
    MergeTreeIndexPtr index;
};

using ObjectPartSkipIndices = std::vector<ObjectPartSkipIndex>;

/// The data skipping indexes of a table. The index types that keep their granules in more than one stream (`text`,
/// `vector_similarity`) are refused.
ObjectPartSkipIndices skipIndices(const StorageMetadataPtr & metadata, const MergeTreeSettings & settings);

struct ObjectPartIndex
{
    struct Entry
    {
        /// Of the block, in a part of "CHPART02"; `size` is the size of all the chunks of the block in "CHPART03".
        UInt64 offset = 0;
        UInt64 size = 0;
        UInt64 rows = 0;
        Hyperrectangle ranges;
        /// The offset and the size of the chunk of each of `columns`, in a part of "CHPART03".
        std::vector<std::pair<UInt64, UInt64>> chunks;
    };

    /// 2 or 3, of "CHPART02" or "CHPART03".
    UInt8 format = 3;
    UInt64 revision = 0;
    /// The columns of the part, in a part of "CHPART03".
    Names columns;
    /// The columns of the min-max index, as they were when the part was written.
    NamesAndTypesList minmax_columns;
    std::vector<Entry> blocks;
    /// The granules of the data skipping indexes, by the name of the index: one per block.
    std::unordered_map<String, Strings> skip_granules;
    /// The value of the partition, as `system.parts` shows it. Empty in the parts written before it was kept.
    String partition;

    UInt64 rows() const;
    /// The bytes that reading `names` of block `block` reads: of their chunks, or of the whole block.
    UInt64 bytesToRead(size_t block, const Names & names) const;
};

using ObjectPartIndexPtr = std::shared_ptr<const ObjectPartIndex>;

/// Writes a part, a block at a time: it holds the compressed part, not its blocks.
class ObjectPartWriter
{
public:
    /// The rows of a block, unless the table sets `index_granularity`. A merge holds a block of every column of every
    /// part that it merges, so a block of a wide table has to be small.
    static constexpr size_t default_rows_per_block = 16384;

    ObjectPartWriter(const Block & header, NamesAndTypesList minmax_columns, ObjectPartSkipIndices skip_indices, size_t rows_per_block);
    ~ObjectPartWriter();

    void write(const Block & block);
    /// The value of the partition of the part, for `system.parts`.
    void setPartition(String partition);
    /// Returns the part.
    String finish();

    UInt64 rows() const { return total_rows; }

private:
    void writeBlock(const Block & block);

    struct State;
    std::unique_ptr<State> state;
    size_t rows_per_block;
    UInt64 total_rows = 0;
};

/// Reads the bytes `[offset, offset + size)` of a part from the host, waiting for them.
String readPartRange(const String & part, UInt64 offset, UInt64 size);

/// Gives a part that a query wrote to the host, which puts it in object storage before it returns.
void writePart(const String & part, const String & data);

/// The message of the error of the last import of the host that failed, which it returned -1 for.
String takeHostError();

/// The index of a part of `bytes` bytes, read from the host, or from a cache of the indexes that the isolate read.
ObjectPartIndexPtr readPartIndex(const String & part, UInt64 bytes);
ObjectPartIndexPtr cachedPartIndex(const String & part);

/// Tell the host what a query is going to read, so that it reads ahead, several ranges at once: the index of a part,
/// and the chosen blocks, a run of consecutive ones in one range.
void prefetchPartRange(const String & part, UInt64 offset, UInt64 size);
void prefetchPartIndex(const String & part, UInt64 bytes);
void prefetchPartBlocks(const String & part, const ObjectPartIndex & index, const std::vector<size_t> & blocks, const Names & columns);

/// Reads a block of a part and converts it to the columns of the table now: the part may predate an `ALTER`, so a
/// column that it lacks gets its default, and a column of another type is cast.
Block readPartBlock(
    const String & part,
    const ObjectPartIndex & index,
    size_t block,
    const NamesAndTypesList & columns,
    const ColumnsDescription & table_columns,
    const ContextPtr & context);

/// The virtual columns of the tables of `ObjectMergeTree`, which a source fills for each part: `_part`, its name, and
/// `_partition_id`.
VirtualColumnsDescription objectPartVirtuals();

/// Streams the chosen blocks of a part, with the columns `columns` (which may include subcolumns and the virtual
/// columns) of the table.
class ObjectPartSource final : public ISource
{
public:
    ObjectPartSource(
        SharedHeader header,
        String part,
        ObjectPartIndexPtr index,
        std::vector<size_t> blocks,
        NamesAndTypesList columns,
        const ColumnsDescription & table_columns,
        ContextPtr context);

    String getName() const override { return "ObjectPartSource"; }

protected:
    Chunk generate() override;

private:
    String part;
    ObjectPartIndexPtr index;
    std::vector<size_t> blocks;
    size_t next = 0;
    NamesAndTypesList columns;
    String partition_id;
    /// The physical columns that `columns` are read from.
    NamesAndTypesList physical_columns;
    std::shared_ptr<const ColumnsDescription> table_columns;
    ContextPtr context;
};

/// The columns of the min-max index of a table: of the sorting key and of the partition key, that can be compared.
NamesAndTypesList minmaxColumns(const NamesAndTypesList & required, const ColumnsDescription & columns);

/// Whether a block or a part with these ranges of `index.minmax_columns` may have rows that satisfy `condition`,
/// built on `condition_columns`.
bool mayContainRows(const KeyCondition & condition, const NamesAndTypesList & condition_columns, const ObjectPartIndex & index, const Hyperrectangle & ranges);

/// The conditions of a filter on the data skipping indexes that can rule out blocks.
using ObjectPartSkipConditions = std::vector<std::pair<const ObjectPartSkipIndex *, MergeTreeIndexConditionPtr>>;
ObjectPartSkipConditions skipConditions(const ObjectPartSkipIndices & indices, const ActionsDAG::Node * predicate, const ContextPtr & context);

/// Whether the granules of block `block` of a part may have rows that satisfy the conditions. A block without a
/// granule of an index, written before the index was added, may.
bool skipIndicesMayContainRows(const ObjectPartSkipConditions & conditions, const ObjectPartIndex & index, size_t block);

}
