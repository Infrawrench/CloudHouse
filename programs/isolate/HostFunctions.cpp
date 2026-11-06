#include <Columns/ColumnString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include "ObjectPart.h"

/// Functions that need Unicode tables, which the isolate build does not have (ICU would add megabytes to a module with
/// little room), and V8 has: the host runs them, a block at a time, as a call into JavaScript per row costs much more
/// than the work (`src/text.ts` of the Worker).
extern "C" size_t ch_host_transform_strings(int operation, const char * chars, size_t chars_size, const UInt64 * offsets, size_t rows);
extern "C" void ch_host_take_strings(char * chars, UInt64 * offsets);

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{

enum Operation
{
    LowerCase = 0,
    UpperCase = 1,
    NFC = 2,
    NFD = 3,
    NFKC = 4,
    NFKD = 5,
};

/// Most text is ASCII, which these functions do not need the host for. Normalizing it changes nothing.
template <Operation operation>
struct HostStringsImpl
{
    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        UInt8 any = 0;
        for (const UInt8 byte : data)
            any |= byte;
        if ((any & 0x80) == 0)
        {
            res_offsets.assign(offsets);
            res_data.resize(data.size());
            for (size_t i = 0; i < data.size(); ++i)
            {
                UInt8 byte = data[i];
                if constexpr (operation == LowerCase)
                    byte += (byte >= 'A' && byte <= 'Z') ? ('a' - 'A') : 0;
                else if constexpr (operation == UpperCase)
                    byte -= (byte >= 'a' && byte <= 'z') ? ('a' - 'A') : 0;
                res_data[i] = byte;
            }
            return;
        }
        const size_t size = ch_host_transform_strings(
            operation, reinterpret_cast<const char *>(data.data()), data.size(), offsets.data(), input_rows_count);
        if (size == static_cast<size_t>(-1))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot transform strings in the host: {}", takeHostError());
        res_data.resize(size);
        res_offsets.resize(input_rows_count);
        ch_host_take_strings(reinterpret_cast<char *>(res_data.data()), res_offsets.data());
    }

    [[noreturn]] static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &, size_t)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Functions on UTF-8 cannot work with FixedString argument");
    }
};

struct NameLowerUTF8 { static constexpr auto name = "lowerUTF8"; };
struct NameUpperUTF8 { static constexpr auto name = "upperUTF8"; };
struct NameNormalizeUTF8NFC { static constexpr auto name = "normalizeUTF8NFC"; };
struct NameNormalizeUTF8NFD { static constexpr auto name = "normalizeUTF8NFD"; };
struct NameNormalizeUTF8NFKC { static constexpr auto name = "normalizeUTF8NFKC"; };
struct NameNormalizeUTF8NFKD { static constexpr auto name = "normalizeUTF8NFKD"; };

}

REGISTER_FUNCTION(HostStrings)
{
    const auto document = [](const char * description, const char * syntax)
    {
        FunctionDocumentation documentation;
        documentation.description = description;
        documentation.syntax = syntax;
        documentation.category = FunctionDocumentation::Category::String;
        return documentation;
    };
    factory.registerFunction<FunctionStringToString<HostStringsImpl<LowerCase>, NameLowerUTF8>>(
        document("Converts a string in UTF-8 to lowercase.", "lowerUTF8(s)"));
    factory.registerFunction<FunctionStringToString<HostStringsImpl<UpperCase>, NameUpperUTF8>>(
        document("Converts a string in UTF-8 to uppercase.", "upperUTF8(s)"));
    factory.registerFunction<FunctionStringToString<HostStringsImpl<NFC>, NameNormalizeUTF8NFC>>(
        document("Normalizes a string in UTF-8 to NFC.", "normalizeUTF8NFC(s)"));
    factory.registerFunction<FunctionStringToString<HostStringsImpl<NFD>, NameNormalizeUTF8NFD>>(
        document("Normalizes a string in UTF-8 to NFD.", "normalizeUTF8NFD(s)"));
    factory.registerFunction<FunctionStringToString<HostStringsImpl<NFKC>, NameNormalizeUTF8NFKC>>(
        document("Normalizes a string in UTF-8 to NFKC.", "normalizeUTF8NFKC(s)"));
    factory.registerFunction<FunctionStringToString<HostStringsImpl<NFKD>, NameNormalizeUTF8NFKD>>(
        document("Normalizes a string in UTF-8 to NFKD.", "normalizeUTF8NFKD(s)"));
}

}
