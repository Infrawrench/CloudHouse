#include "ObjectPart.h"

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/addMissingDefaults.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/getColumnFromBlock.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>

#include <deque>
#include <unordered_map>


/// Imported from the host (`host.js`): copies the bytes `[offset, offset + size)` of a part to `dest`, and returns
/// how many there were. The engine waits for them: with JSPI the import suspends the engine and resumes it when the
/// host has read them.
extern "C" size_t ch_host_read_range(const char * part, size_t part_size, size_t offset, size_t size, char * dest);
/// Imported from the host: the engine is going to read the bytes `[offset, offset + size)` of a part, so the host can
/// start reading them, and read several ranges at once. It does not wait.
extern "C" void ch_host_prefetch(const char * part, size_t part_size, size_t offset, size_t size);

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

constexpr std::string_view magic = "CHPART02";
constexpr UInt64 revision = DBMS_TCP_PROTOCOL_VERSION;
/// The footer: the offset and the size of the index, the revision, and the magic.
constexpr size_t footer_size = 3 * sizeof(UInt64) + magic.size();
/// The end of a part that is read at once, which holds the footer and, mostly, the index.
constexpr size_t tail_size = 64 * 1024;
/// How many indexes the isolate keeps: they are small, and a part never changes.
constexpr size_t index_cache_size = 4096;

const String min_prefix = "_min.";
const String max_prefix = "_max.";
const String skip_prefix = "_skip.";

ObjectPartIndexPtr parseIndex(std::string_view data, UInt64 index_revision)
{
    ReadBufferFromMemory in(data.data(), data.size());
    NativeReader reader(in, index_revision);
    Block block = reader.read();

    auto index = std::make_shared<ObjectPartIndex>();
    index->revision = index_revision;
    for (const auto & column : block)
        if (column.name.starts_with(min_prefix))
            index->minmax_columns.emplace_back(column.name.substr(min_prefix.size()), column.type);

    const auto & offsets = block.getByName("_offset").column;
    const auto & sizes = block.getByName("_size").column;
    const auto & rows = block.getByName("_rows").column;
    std::vector<ColumnPtr> mins;
    std::vector<ColumnPtr> maxs;
    for (const auto & column : index->minmax_columns)
    {
        mins.push_back(block.getByName(min_prefix + column.name).column);
        maxs.push_back(block.getByName(max_prefix + column.name).column);
    }
    for (size_t i = 0; i < block.rows(); ++i)
    {
        ObjectPartIndex::Entry entry{.offset = offsets->getUInt(i), .size = sizes->getUInt(i), .rows = rows->getUInt(i), .ranges = {}};
        for (size_t c = 0; c < mins.size(); ++c)
            entry.ranges.emplace_back((*mins[c])[i], true, (*maxs[c])[i], true);
        index->blocks.push_back(std::move(entry));
    }
    for (const auto & column : block)
    {
        if (!column.name.starts_with(skip_prefix))
            continue;
        auto & granules = index->skip_granules[column.name.substr(skip_prefix.size())];
        for (size_t i = 0; i < block.rows(); ++i)
            granules.emplace_back(column.column->getDataAt(i));
    }
    return index;
}

/// The physical columns that `columns`, which may include subcolumns, are read from.
NamesAndTypesList physicalColumns(const NamesAndTypesList & columns)
{
    NamesAndTypesList result;
    NameSet seen;
    for (const auto & column : columns)
        if (seen.insert(column.getNameInStorage()).second)
            result.emplace_back(column.getNameInStorage(), column.getTypeInStorage());
    return result;
}

}


UInt64 ObjectPartIndex::rows() const
{
    UInt64 result = 0;
    for (const auto & block : blocks)
        result += block.rows;
    return result;
}


ObjectPartSkipIndices skipIndices(const StorageMetadataPtr & metadata, const MergeTreeSettings & settings)
{
    ObjectPartSkipIndices result;
    for (const auto & description : metadata->getSecondaryIndices())
    {
        auto index = MergeTreeIndexFactory::instance().get(metadata, description, settings);
        /// The granule of a block is one string, so the index has to keep it in one stream.
        const auto substreams = index->getSubstreams();
        if (substreams.size() != 1 || substreams.front().type != MergeTreeIndexSubstream::Type::Regular)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ObjectMergeTree has no data skipping indexes of type {}", description.type);
        SipHash hash;
        hash.update(description.definition_ast->formatWithSecretsOneLine());
        result.push_back({
            .name = description.name,
            .fingerprint = hash.get64(),
            /// As `MergeTreeIndexMinMax::getPhysicalFormat`: the stream `.idx2` is version 2 of the format, `.idx` 1.
            .version = static_cast<MergeTreeIndexVersion>(substreams.front().extension == ".idx2" ? 2 : 1),
            .index = std::move(index),
        });
    }
    return result;
}


struct ObjectPartWriter::State
{
    String data;
    WriteBufferFromString out{data};
    CompressedWriteBuffer compressed{out, CompressionCodecFactory::instance().get("ZSTD", 1)};
    Block header;
    NativeWriter writer;
    NamesAndTypesList minmax_columns;
    ObjectPartSkipIndices skip_indices;
    MutableColumns offsets_sizes_rows;
    MutableColumns mins;
    MutableColumns maxs;
    MutableColumns skip_granules;

    State(const Block & header_, NamesAndTypesList minmax_columns_, ObjectPartSkipIndices skip_indices_)
        : header(header_.cloneEmpty())
        , writer(compressed, revision, std::make_shared<const Block>(header))
        , minmax_columns(std::move(minmax_columns_))
        , skip_indices(std::move(skip_indices_))
    {
        out.write(magic.data(), magic.size());
        writeBinaryLittleEndian(revision, out);
        for (size_t i = 0; i < 3; ++i)
            offsets_sizes_rows.push_back(ColumnUInt64::create());
        for (const auto & column : minmax_columns)
        {
            mins.push_back(column.type->createColumn());
            maxs.push_back(column.type->createColumn());
        }
        for (size_t i = 0; i < skip_indices.size(); ++i)
            skip_granules.push_back(ColumnString::create());
    }
};

ObjectPartWriter::ObjectPartWriter(const Block & header, NamesAndTypesList minmax_columns, ObjectPartSkipIndices skip_indices, size_t rows_per_block_)
    : state(std::make_unique<State>(header, std::move(minmax_columns), std::move(skip_indices)))
    , rows_per_block(rows_per_block_)
{
}

ObjectPartWriter::~ObjectPartWriter() = default;

void ObjectPartWriter::write(const Block & block)
{
    const size_t rows = block.rows();
    for (size_t start = 0; start < rows; start += rows_per_block)
    {
        const size_t length = std::min(rows_per_block, rows - start);
        if (start == 0 && length == rows)
        {
            writeBlock(block);
        }
        else
        {
            Block piece = block.cloneEmpty();
            for (size_t i = 0; i < block.columns(); ++i)
                piece.getByPosition(i).column = block.getByPosition(i).column->cut(start, length);
            writeBlock(piece);
        }
    }
}

void ObjectPartWriter::writeBlock(const Block & block)
{
    if (block.rows() == 0)
        return;
    auto & s = *state;
    /// Each block is compressed on its own, so that it can be read without the ones before it.
    const UInt64 offset = s.out.count();
    s.writer.write(block);
    s.writer.flush();
    const UInt64 size = s.out.count() - offset;

    s.offsets_sizes_rows[0]->insert(offset);
    s.offsets_sizes_rows[1]->insert(size);
    s.offsets_sizes_rows[2]->insert(static_cast<UInt64>(block.rows()));
    size_t i = 0;
    for (const auto & column : s.minmax_columns)
    {
        Field min;
        Field max;
        block.getByName(column.name).column->getExtremes(min, max, 0, block.rows());
        s.mins[i]->insert(min);
        s.maxs[i]->insert(max);
        ++i;
    }
    /// As `MergeTreeDataWriter`: the expression of an index adds its columns, and its aggregator makes the granule.
    for (size_t j = 0; j < s.skip_indices.size(); ++j)
    {
        const auto & skip_index = s.skip_indices[j];
        Block with_index = block;
        skip_index.index->index.expression->execute(with_index);
        auto aggregator = skip_index.index->createIndexAggregator();
        size_t position = 0;
        aggregator->update(with_index, &position, with_index.rows());
        String granule;
        WriteBufferFromString out(granule);
        writeBinaryLittleEndian(skip_index.fingerprint, out);
        aggregator->getGranuleAndReset()->serializeBinary(out);
        out.finalize();
        s.skip_granules[j]->insertData(granule.data(), granule.size());
    }
    total_rows += block.rows();
}

String ObjectPartWriter::finish()
{
    auto & s = *state;
    s.compressed.finalize();

    Block index;
    auto uint64 = std::make_shared<DataTypeUInt64>();
    index.insert({std::move(s.offsets_sizes_rows[0]), uint64, "_offset"});
    index.insert({std::move(s.offsets_sizes_rows[1]), uint64, "_size"});
    index.insert({std::move(s.offsets_sizes_rows[2]), uint64, "_rows"});
    size_t i = 0;
    for (const auto & column : s.minmax_columns)
    {
        index.insert({std::move(s.mins[i]), column.type, min_prefix + column.name});
        index.insert({std::move(s.maxs[i]), column.type, max_prefix + column.name});
        ++i;
    }
    for (size_t j = 0; j < s.skip_indices.size(); ++j)
        index.insert({std::move(s.skip_granules[j]), std::make_shared<DataTypeString>(), skip_prefix + s.skip_indices[j].name});

    const UInt64 index_offset = s.out.count();
    NativeWriter index_writer(s.out, revision, std::make_shared<const Block>(index.cloneEmpty()));
    index_writer.write(index);
    index_writer.flush();
    const UInt64 index_size = s.out.count() - index_offset;
    writeBinaryLittleEndian(index_offset, s.out);
    writeBinaryLittleEndian(index_size, s.out);
    writeBinaryLittleEndian(revision, s.out);
    s.out.write(magic.data(), magic.size());
    s.out.finalize();
    return std::move(s.data);
}


String readPartRange(const String & part, UInt64 offset, UInt64 size)
{
    String data(size, '\0');
    const size_t read = ch_host_read_range(part.data(), part.size(), offset, size, data.data());
    if (read != size)
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Read {} bytes of part {} at {} instead of {}", read, part, offset, size);
    return data;
}

void prefetchPartRange(const String & part, UInt64 offset, UInt64 size)
{
    ch_host_prefetch(part.data(), part.size(), offset, size);
}

void prefetchPartIndex(const String & part, UInt64 bytes)
{
    if (!cachedPartIndex(part) && bytes > 0)
        prefetchPartRange(part, bytes - std::min<UInt64>(bytes, tail_size), std::min<UInt64>(bytes, tail_size));
}

void prefetchPartBlocks(const String & part, const ObjectPartIndex & index, const std::vector<size_t> & blocks)
{
    /// The runs of consecutive blocks, each in one read.
    for (size_t i = 0; i < blocks.size();)
    {
        size_t j = i + 1;
        while (j < blocks.size() && blocks[j] == blocks[j - 1] + 1)
            ++j;
        const auto & first = index.blocks[blocks[i]];
        const auto & last = index.blocks[blocks[j - 1]];
        prefetchPartRange(part, first.offset, last.offset + last.size - first.offset);
        i = j;
    }
}

namespace
{

std::unordered_map<String, ObjectPartIndexPtr> & indexCache()
{
    static std::unordered_map<String, ObjectPartIndexPtr> cache;
    return cache;
}

}

ObjectPartIndexPtr cachedPartIndex(const String & part)
{
    auto it = indexCache().find(part);
    return it == indexCache().end() ? nullptr : it->second;
}

ObjectPartIndexPtr readPartIndex(const String & part, UInt64 bytes)
{
    auto & cache = indexCache();
    static std::deque<String> order;
    if (auto it = cache.find(part); it != cache.end())
        return it->second;

    if (bytes < footer_size + 16)
        throw Exception(ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED, "Part {} is too small", part);
    const UInt64 tail_offset = bytes - std::min<UInt64>(bytes, tail_size);
    String tail = readPartRange(part, tail_offset, bytes - tail_offset);

    ReadBufferFromMemory footer(tail.data() + tail.size() - footer_size, footer_size);
    UInt64 index_offset;
    UInt64 index_size;
    UInt64 index_revision;
    readBinaryLittleEndian(index_offset, footer);
    readBinaryLittleEndian(index_size, footer);
    readBinaryLittleEndian(index_revision, footer);
    String part_magic(magic.size(), '\0');
    footer.readStrict(part_magic.data(), part_magic.size());
    if (part_magic != magic || index_offset + index_size + footer_size != bytes)
        throw Exception(ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED, "Part {} is not a part of ObjectMergeTree of the format {}", part, magic);

    ObjectPartIndexPtr index;
    if (index_offset >= tail_offset)
        index = parseIndex(std::string_view(tail).substr(index_offset - tail_offset, index_size), index_revision);
    else
        index = parseIndex(readPartRange(part, index_offset, index_size), index_revision);

    if (cache.size() >= index_cache_size)
    {
        cache.erase(order.front());
        order.pop_front();
    }
    cache.emplace(part, index);
    order.push_back(part);
    return index;
}

Block readPartBlock(
    const String & part,
    const ObjectPartIndex & index,
    size_t block_index,
    const NamesAndTypesList & columns,
    const ColumnsDescription & table_columns,
    const ContextPtr & context)
{
    const auto & entry = index.blocks.at(block_index);
    const String data = readPartRange(part, entry.offset, entry.size);
    ReadBufferFromMemory in(data.data(), data.size());
    CompressedReadBuffer compressed(in);
    NativeReader reader(compressed, index.revision);
    Block block = reader.read();

    /// The part was written with the columns of its time: cast the ones whose type has changed since, and leave out
    /// the ones that are not there any more.
    Block converted;
    bool missing = false;
    for (const auto & column : columns)
    {
        if (!block.has(column.name))
        {
            missing = true;
            continue;
        }
        auto source = block.getByName(column.name);
        if (!source.type->equals(*column.type))
            source.column = castColumn(source, column.type);
        converted.insert({source.column, column.type, column.name});
    }
    if (!missing)
        return converted;

    /// The columns added since get their defaults, which may refer to any column of the part, read or not.
    Block all;
    for (const auto & column : block)
    {
        if (!table_columns.hasPhysical(column.name))
            continue;
        const auto type = table_columns.getPhysical(column.name).type;
        all.insert({type->equals(*column.type) ? column.column : castColumn(column, type), type, column.name});
    }
    auto defaults = addMissingDefaults(all.cloneEmpty(), columns, table_columns, context);
    size_t rows = entry.rows;
    ExpressionActions(std::move(defaults)).execute(all, rows);
    Block result;
    for (const auto & column : columns)
        result.insert(all.getByName(column.name));
    return result;
}


ObjectPartSource::ObjectPartSource(
    SharedHeader header,
    String part_,
    ObjectPartIndexPtr index_,
    std::vector<size_t> blocks_,
    NamesAndTypesList columns_,
    const ColumnsDescription & table_columns_,
    ContextPtr context_)
    : ISource(std::move(header))
    , part(std::move(part_))
    , index(std::move(index_))
    , blocks(std::move(blocks_))
    , columns(std::move(columns_))
    , physical_columns(physicalColumns(columns))
    , table_columns(std::make_shared<const ColumnsDescription>(table_columns_))
    , context(std::move(context_))
{
}

Chunk ObjectPartSource::generate()
{
    if (next == blocks.size())
        return {};
    const size_t block_index = blocks[next++];
    if (columns.empty())
        return Chunk(Columns{}, index->blocks[block_index].rows);

    Block block = readPartBlock(part, *index, block_index, physical_columns, *table_columns, context);
    Columns result;
    result.reserve(columns.size());
    for (const auto & column : columns)
        result.push_back(getColumnFromBlock(block, column));
    const size_t rows = block.rows();
    return Chunk(std::move(result), rows);
}


NamesAndTypesList minmaxColumns(const NamesAndTypesList & required, const ColumnsDescription & columns)
{
    NamesAndTypesList result;
    NameSet seen;
    for (const auto & column : required)
    {
        if (!seen.insert(column.name).second || !columns.hasPhysical(column.name))
            continue;
        const auto type = columns.getPhysical(column.name).type;
        /// The types whose values compare as `KeyCondition` expects, and whose `getExtremes` are their range.
        if (type->isValueRepresentedByNumber() || isStringOrFixedString(type))
            result.emplace_back(column.name, type);
    }
    return result;
}

bool mayContainRows(const KeyCondition & condition, const NamesAndTypesList & condition_columns, const ObjectPartIndex & index, const Hyperrectangle & ranges)
{
    Hyperrectangle hyperrectangle;
    DataTypes types;
    for (const auto & column : condition_columns)
    {
        types.push_back(column.type);
        size_t position = 0;
        bool found = false;
        for (const auto & indexed : index.minmax_columns)
        {
            if (indexed.name == column.name && indexed.type->equals(*column.type))
            {
                found = true;
                break;
            }
            ++position;
        }
        hyperrectangle.push_back(found ? ranges[position] : Range::createWholeUniverse());
    }
    return condition.checkInHyperrectangle(hyperrectangle, types).can_be_true;
}


ObjectPartSkipConditions skipConditions(const ObjectPartSkipIndices & indices, const ActionsDAG::Node * predicate, const ContextPtr & context)
{
    ObjectPartSkipConditions result;
    if (!predicate)
        return result;
    for (const auto & skip_index : indices)
    {
        auto condition = skip_index.index->createIndexCondition(predicate, context);
        if (!condition->alwaysUnknownOrTrue())
            result.emplace_back(&skip_index, std::move(condition));
    }
    return result;
}

bool skipIndicesMayContainRows(const ObjectPartSkipConditions & conditions, const ObjectPartIndex & index, size_t block)
{
    for (const auto & [skip_index, condition] : conditions)
    {
        auto it = index.skip_granules.find(skip_index->name);
        if (it == index.skip_granules.end())
            continue;
        ReadBufferFromString in(it->second.at(block));
        UInt64 fingerprint;
        readBinaryLittleEndian(fingerprint, in);
        if (fingerprint != skip_index->fingerprint)
            continue;
        auto granule = skip_index->index->createIndexGranule();
        granule->deserializeBinary(in, skip_index->version);
        if (!condition->mayBeTrueOnGranule(granule, {}))
            return false;
    }
    return true;
}

}
