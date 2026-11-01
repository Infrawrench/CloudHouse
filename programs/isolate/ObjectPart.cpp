#include "ObjectPart.h"

#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypeLowCardinality.h>
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
/// Imported from the host: puts a part that the engine wrote in object storage, and returns once it is there.
extern "C" int ch_host_write_part(const char * part, size_t part_size, const char * data, size_t size);
/// Imported from the host: the message of the error of its last import that returned -1.
extern "C" size_t ch_host_error_size();
extern "C" void ch_host_take_error(char * dest, size_t size);
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
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
}

namespace
{

constexpr std::string_view magic = "CHPART03";
constexpr std::string_view magic_v2 = "CHPART02";
constexpr UInt64 revision = DBMS_TCP_PROTOCOL_VERSION;
/// The footer: the offset and the size of the index, the revision, and the magic.
constexpr size_t footer_size = 3 * sizeof(UInt64) + magic.size();
/// The end of a part that is read at once, which holds the footer and, mostly, the index: a few KiB, unless the part
/// has many blocks or a data skipping index with large granules, which take a second read.
constexpr size_t tail_size = 16 * 1024;
/// How many indexes the isolate keeps: they are small, and a part never changes.
constexpr size_t index_cache_size = 4096;

const String min_prefix = "_min.";
const String max_prefix = "_max.";
const String skip_prefix = "_skip.";
const String partition_column = "_partition";
const String offset_prefix = "_col_offset.";
const String size_prefix = "_col_size.";

ObjectPartIndexPtr parseIndex(std::string_view data, UInt64 index_revision, UInt8 format)
{
    ReadBufferFromMemory in(data.data(), data.size());
    NativeReader reader(in, index_revision);
    Block block = reader.read();

    auto index = std::make_shared<ObjectPartIndex>();
    index->format = format;
    index->revision = index_revision;
    std::vector<ColumnPtr> chunk_offsets;
    std::vector<ColumnPtr> chunk_sizes;
    for (const auto & column : block)
    {
        if (column.name.starts_with(min_prefix))
            index->minmax_columns.emplace_back(column.name.substr(min_prefix.size()), column.type);
        if (column.name.starts_with(offset_prefix))
        {
            const String name = column.name.substr(offset_prefix.size());
            index->columns.push_back(name);
            chunk_offsets.push_back(column.column);
            chunk_sizes.push_back(block.getByName(size_prefix + name).column);
        }
    }

    const ColumnPtr empty = ColumnUInt64::create(block.rows(), 0);
    const auto & offsets = format == 2 ? block.getByName("_offset").column : empty;
    const auto & sizes = format == 2 ? block.getByName("_size").column : empty;
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
        ObjectPartIndex::Entry entry{.offset = offsets->getUInt(i), .size = sizes->getUInt(i), .rows = rows->getUInt(i), .ranges = {}, .chunks = {}};
        for (size_t c = 0; c < mins.size(); ++c)
            entry.ranges.emplace_back((*mins[c])[i], true, (*maxs[c])[i], true);
        for (size_t c = 0; c < chunk_offsets.size(); ++c)
        {
            entry.chunks.emplace_back(chunk_offsets[c]->getUInt(i), chunk_sizes[c]->getUInt(i));
            entry.size += chunk_sizes[c]->getUInt(i);
        }
        index->blocks.push_back(std::move(entry));
    }
    if (block.has(partition_column) && block.rows() > 0)
        index->partition = block.getByName(partition_column).column->getDataAt(0);
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

UInt64 ObjectPartIndex::bytesToRead(size_t block, const Names & names) const
{
    const auto & entry = blocks.at(block);
    if (format == 2)
        return entry.size;
    UInt64 result = 0;
    for (size_t c = 0; c < columns.size(); ++c)
        if (std::find(names.begin(), names.end(), columns[c]) != names.end())
            result += entry.chunks[c].second;
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
    /// The chunks of one column, which are together in the part.
    struct ColumnStream
    {
        String data;
        WriteBufferFromString out{data};
        /// A table may have dozens of columns, so the buffer of each is small.
        CompressedWriteBuffer compressed{out, CompressionCodecFactory::instance().get("ZSTD", 1), 64 * 1024};
        NativeWriter writer;
        MutableColumnPtr offsets = ColumnUInt64::create();
        MutableColumnPtr sizes = ColumnUInt64::create();

        explicit ColumnStream(const ColumnWithTypeAndName & column)
            : writer(compressed, revision, std::make_shared<const Block>(Block{column.cloneEmpty()}))
        {
        }
    };

    Block header;
    std::vector<std::unique_ptr<ColumnStream>> streams;
    NamesAndTypesList minmax_columns;
    ObjectPartSkipIndices skip_indices;
    MutableColumnPtr rows = ColumnUInt64::create();
    MutableColumns mins;
    MutableColumns maxs;
    MutableColumns skip_granules;
    String partition;

    State(const Block & header_, NamesAndTypesList minmax_columns_, ObjectPartSkipIndices skip_indices_)
        : header(header_.cloneEmpty())
        , minmax_columns(std::move(minmax_columns_))
        , skip_indices(std::move(skip_indices_))
    {
        for (const auto & column : header)
            streams.push_back(std::make_unique<ColumnStream>(column));
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

void ObjectPartWriter::setPartition(String partition)
{
    state->partition = std::move(partition);
}

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
    /// The chunk of each column is compressed on its own, so that it can be read without the others.
    for (size_t c = 0; c < s.streams.size(); ++c)
    {
        auto & stream = *s.streams[c];
        const UInt64 offset = stream.out.count();
        stream.writer.write(Block{block.getByName(s.header.getByPosition(c).name)});
        stream.writer.flush();
        stream.offsets->insert(offset);
        stream.sizes->insert(stream.out.count() - offset);
    }
    s.rows->insert(static_cast<UInt64>(block.rows()));
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
    String data;
    WriteBufferFromString out(data);
    out.write(magic.data(), magic.size());
    writeBinaryLittleEndian(revision, out);

    Block index;
    auto uint64 = std::make_shared<DataTypeUInt64>();
    index.insert({std::move(s.rows), uint64, "_rows"});
    for (size_t c = 0; c < s.streams.size(); ++c)
    {
        auto & stream = *s.streams[c];
        stream.compressed.finalize();
        stream.out.finalize();
        /// The offsets were within the column: they become offsets in the part.
        const UInt64 base = out.count();
        out.write(stream.data.data(), stream.data.size());
        auto offsets = ColumnUInt64::create();
        for (size_t i = 0; i < stream.offsets->size(); ++i)
            offsets->insert(base + stream.offsets->getUInt(i));
        const String & name = s.header.getByPosition(c).name;
        index.insert({std::move(offsets), uint64, offset_prefix + name});
        index.insert({std::move(stream.sizes), uint64, size_prefix + name});
        stream.data = {};
    }
    size_t i = 0;
    for (const auto & column : s.minmax_columns)
    {
        index.insert({std::move(s.mins[i]), column.type, min_prefix + column.name});
        index.insert({std::move(s.maxs[i]), column.type, max_prefix + column.name});
        ++i;
    }
    for (size_t j = 0; j < s.skip_indices.size(); ++j)
        index.insert({std::move(s.skip_granules[j]), std::make_shared<DataTypeString>(), skip_prefix + s.skip_indices[j].name});
    const auto string = std::make_shared<DataTypeString>();
    index.insert({string->createColumnConst(index.rows(), s.partition)->convertToFullColumnIfConst(), string, partition_column});

    const UInt64 index_offset = out.count();
    NativeWriter index_writer(out, revision, std::make_shared<const Block>(index.cloneEmpty()));
    index_writer.write(index);
    index_writer.flush();
    const UInt64 index_size = out.count() - index_offset;
    writeBinaryLittleEndian(index_offset, out);
    writeBinaryLittleEndian(index_size, out);
    writeBinaryLittleEndian(revision, out);
    out.write(magic.data(), magic.size());
    out.finalize();
    return data;
}


String takeHostError()
{
    String message(ch_host_error_size(), '\0');
    ch_host_take_error(message.data(), message.size());
    return message;
}

String readPartRange(const String & part, UInt64 offset, UInt64 size)
{
    String data(size, '\0');
    const size_t read = ch_host_read_range(part.data(), part.size(), offset, size, data.data());
    /// A part that a merge has replaced since the query saw it may be gone: the host syncs and runs the query again.
    if (read == static_cast<size_t>(-1))
        throw Exception(ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER, "Cannot read part {}: {}", part, takeHostError());
    if (read != size)
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Read {} bytes of part {} at {} instead of {}", read, part, offset, size);
    return data;
}

void writePart(const String & part, const String & data)
{
    if (ch_host_write_part(part.data(), part.size(), data.data(), data.size()) != 0)
        throw Exception(ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER, "Cannot write part {}: {}", part, takeHostError());
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

void prefetchPartBlocks(const String & part, const ObjectPartIndex & index, const std::vector<size_t> & blocks, const Names & columns)
{
    /// The runs of consecutive blocks, each in one read of at most `max_prefetch_bytes`, which the host holds until the
    /// engine has read all of it: it shares the memory of the isolate with the engine.
    constexpr UInt64 max_prefetch_bytes = 2 * 1024 * 1024;
    if (index.format == 3)
    {
        /// The runs of the chunks of each column, in the order that the engine reads them: block by block.
        std::vector<size_t> wanted;
        for (size_t c = 0; c < index.columns.size(); ++c)
            if (columns.empty() || std::find(columns.begin(), columns.end(), index.columns[c]) != columns.end())
                wanted.push_back(c);
        for (size_t i = 0; i < blocks.size();)
        {
            /// The blocks of this run: consecutive, and of at most `max_prefetch_bytes` in all the columns.
            size_t j = i + 1;
            UInt64 bytes = index.bytesToRead(blocks[i], columns.empty() ? index.columns : columns);
            while (j < blocks.size() && blocks[j] == blocks[j - 1] + 1)
            {
                const UInt64 more = index.bytesToRead(blocks[j], columns.empty() ? index.columns : columns);
                if (bytes + more > max_prefetch_bytes)
                    break;
                bytes += more;
                ++j;
            }
            /// The range of each column, and ranges close to each other in one read, with what is between them: a read
            /// costs more than a few KiB.
            std::vector<std::pair<UInt64, UInt64>> ranges;
            for (const size_t c : wanted)
            {
                const auto & first = index.blocks[blocks[i]].chunks[c];
                const auto & last = index.blocks[blocks[j - 1]].chunks[c];
                ranges.emplace_back(first.first, last.first + last.second);
            }
            std::sort(ranges.begin(), ranges.end());
            constexpr UInt64 max_gap = 256 * 1024;
            for (size_t r = 0; r < ranges.size();)
            {
                UInt64 begin = ranges[r].first;
                UInt64 end = ranges[r].second;
                size_t next = r + 1;
                while (next < ranges.size() && ranges[next].first <= end + max_gap && ranges[next].second - begin <= max_prefetch_bytes)
                    end = std::max(end, ranges[next++].second);
                prefetchPartRange(part, begin, end - begin);
                r = next;
            }
            i = j;
        }
        return;
    }
    for (size_t i = 0; i < blocks.size();)
    {
        size_t j = i + 1;
        UInt64 bytes = index.blocks[blocks[i]].size;
        while (j < blocks.size() && blocks[j] == blocks[j - 1] + 1 && bytes + index.blocks[blocks[j]].size <= max_prefetch_bytes)
            bytes += index.blocks[blocks[j++]].size;
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
    if ((part_magic != magic && part_magic != magic_v2) || index_offset + index_size + footer_size != bytes)
        throw Exception(ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED, "Part {} is not a part of ObjectMergeTree of the format {}", part, magic);
    const UInt8 format = part_magic == magic ? 3 : 2;

    ObjectPartIndexPtr index;
    if (index_offset >= tail_offset)
        index = parseIndex(std::string_view(tail).substr(index_offset - tail_offset, index_size), index_revision, format);
    else
        index = parseIndex(readPartRange(part, index_offset, index_size), index_revision, format);

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
    /// The columns of the block that are asked for, or, if one was added since the part was written, all of them, as
    /// its default may refer to any.
    auto read = [&](bool all)
    {
        if (index.format == 2)
        {
            const String data = readPartRange(part, entry.offset, entry.size);
            ReadBufferFromMemory in(data.data(), data.size());
            CompressedReadBuffer compressed(in);
            NativeReader reader(compressed, index.revision);
            return reader.read();
        }
        Block result;
        for (size_t c = 0; c < index.columns.size(); ++c)
        {
            if (!all && !columns.contains(index.columns[c]))
                continue;
            const String data = readPartRange(part, entry.chunks[c].first, entry.chunks[c].second);
            ReadBufferFromMemory in(data.data(), data.size());
            CompressedReadBuffer compressed(in);
            NativeReader reader(compressed, index.revision);
            result.insert(reader.read().getByPosition(0));
        }
        return result;
    };
    bool has_all = true;
    if (index.format == 3)
        for (const auto & column : columns)
            has_all &= std::find(index.columns.begin(), index.columns.end(), column.name) != index.columns.end();
    Block block = read(!has_all);

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


VirtualColumnsDescription objectPartVirtuals()
{
    VirtualColumnsDescription virtuals;
    auto name = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    virtuals.addEphemeral("_part", name, "The name of the part.", VirtualsMaterializationPlace::Reader);
    virtuals.addEphemeral("_partition_id", name, "The id of the partition of the part.", VirtualsMaterializationPlace::Reader);
    return virtuals;
}

namespace
{

/// The name of a part is `<uuid>_<rows>_<bytes>_<partition id>` (`ObjectPartName`), and the id may contain `_`.
String partitionIdOfPart(const String & part)
{
    size_t position = 0;
    for (size_t i = 0; i < 3; ++i)
        position = part.find('_', position) + 1;
    return part.substr(position);
}

bool isPartVirtual(const String & name)
{
    return name == "_part" || name == "_partition_id";
}

NamesAndTypesList withoutVirtuals(const NamesAndTypesList & columns)
{
    NamesAndTypesList result;
    for (const auto & column : columns)
        if (!isPartVirtual(column.name))
            result.push_back(column);
    return result;
}

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
    , partition_id(partitionIdOfPart(part))
    , physical_columns(physicalColumns(withoutVirtuals(columns)))
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

    const size_t rows = index->blocks[block_index].rows;
    Block block = physical_columns.empty() ? Block{} : readPartBlock(part, *index, block_index, physical_columns, *table_columns, context);
    Columns result;
    result.reserve(columns.size());
    for (const auto & column : columns)
    {
        if (column.name == "_part")
            result.push_back(column.type->createColumnConst(rows, part)->convertToFullColumnIfConst());
        else if (column.name == "_partition_id")
            result.push_back(column.type->createColumnConst(rows, partition_id)->convertToFullColumnIfConst());
        else
            result.push_back(getColumnFromBlock(block, column));
    }
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
