/// What the isolate build of the engine can do: the interpreters, functions, formats, storages and so on that
/// it registers. A Worker has to fit in 64 MiB of code, so this is a short list instead of the `register*` of
/// the server, which pull in everything.
///
/// Nothing else is linked in: the objects of `clickhouse_functions` and `clickhouse_aggregate_functions` are
/// in static archives, see `CMakeLists.txt`, and the linker takes only the ones that the calls below reach.
/// To add something, add its registration function here.

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <Databases/DatabaseFactory.h>
#include <Disks/DiskFactory.h>
#include <Formats/FormatFactory.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/InterpreterFactory.h>
#include <Parsers/registerParsers.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Storages/StorageFactory.h>
#include <TableFunctions/TableFunctionFactory.h>


/// Functions, by the name in their `REGISTER_FUNCTION`.
#define APPLY_FOR_ISOLATE_FUNCTIONS(M) \
    /* Needed by the query analyzer and the planner. */ \
    M(Logical) M(If) M(MultiIf) M(CastOverloadResolvers) M(Conversion) M(Tuple) M(TupleElement) M(Array) M(ArrayElement) \
    M(In) M(IndexHint) M(Identity) M(Materialize) M(Ignore) M(ArrayJoin) M(GetScalar) M(ActionName) M(GroupingSpecializations) \
    M(GlobalVariable) M(ToTypeName) M(FinalizeAggregation) M(ScalarSubqueryResult) M(FilterContains) M(GetSubcolumn) \
    M(Nested) M(Replicate) M(FirstNonDefault) M(DefaultValueOfTypeName) M(ValidateNestedArraySizes) M(IsNotDistinctFrom) \
    M(IsDistinctFrom) M(IsZeroOrNull) M(NotHas) M(MapMiscellaneous) M(TupleConcat) \
    /* Arithmetic and comparison. */ \
    M(Plus) M(Minus) M(Multiply) M(Divide) M(DivideDecimals) M(MultiplyDecimals) M(IntDiv) M(IntDivOrZero) M(Modulo) M(ModuloLegacy) M(PositiveModulo) \
    M(Negate) M(Abs) M(Least) M(Greatest) \
    M(Equals) M(NotEquals) M(Less) M(Greater) M(LessOrEquals) M(GreaterOrEquals) \
    /* Conditions and NULLs. */ \
    M(CaseWithExpression) M(Coalesce) M(IfNull) M(NullIf) M(IsNull) M(IsNotNull) M(AssumeNotNull) M(ToNullable) M(Transform) \
    M(CastOrDefault) M(ToBool) M(ThrowIf) \
    /* Math. */ \
    M(Round) M(Sqrt) M(Exp) M(Log) M(Pow) M(Sign) M(Pi) M(E) \
    /* Strings. */ \
    M(Length) M(Empty) M(Lower) M(Upper) M(Concat) M(ConcatWithSeparator) M(Substring) M(Position) \
    M(Like) M(NotLike) M(ILike) M(NotILike) M(Match) M(ReplaceOne) M(ReplaceAll) M(ReplaceRegexpOne) M(ReplaceRegexpAll) \
    M(Trim) M(SplitByChar) M(SplitByString) M(ArrayStringConcat) M(StartsWith) M(EndsWith) M(Reverse) M(Repeat) \
    M(LengthUTF8) M(Format) M(Left) M(Right) M(PadString) M(Extract) M(FormatReadableSize) \
    /* Dates and times. */ \
    M(Now) M(Today) M(Yesterday) M(ToYear) M(ToQuarter) M(ToMonth) M(ToDayOfMonth) M(ToDayOfWeek) M(ToDayOfYear) M(ToHour) \
    M(ToMinute) M(ToSecond) M(ToStartOfDay) M(ToStartOfMonth) M(ToStartOfQuarter) M(ToStartOfYear) M(ToStartOfHour) \
    M(ToStartOfMinute) M(ToStartOfInterval) M(ToMonday) M(ToYYYYMM) M(ToYYYYMMDD) M(FormatDateTime) M(DateDiff) M(DateTrunc) \
    M(AddSeconds) M(AddMinutes) M(AddHours) M(AddDays) M(AddWeeks) M(AddMonths) M(AddYears) \
    M(SubtractSeconds) M(SubtractMinutes) M(SubtractHours) M(SubtractDays) M(SubtractWeeks) M(SubtractMonths) M(SubtractYears) \
    M(ToInterval) M(Timezone) M(ToTimeZone) M(ToUnixTimestamp64Milli) M(FromUnixTimestamp64Milli) \
    /* Arrays and maps. */ \
    M(Range) M(Has) M(IndexOf) M(ArrayMap) M(ArrayFilter) M(ArrayExists) M(ArrayAll) M(ArraySort) M(ArrayDistinct) \
    M(ArrayEnumerate) M(ArrayConcat) M(ArrayPushBack) M(ArraySlice) M(ArrayReverse) M(ArrayUniq) M(ArrayCount) \
    M(ArrayAggregation) M(Map) \
    /* Hashes, random numbers, and the rest. */ \
    M(Hashing) M(Rand) M(Rand64) M(GenerateUUIDv4) M(Version) M(CurrentDatabase) M(CurrentUser) M(Sleep) \
    /* More of the functions that queries use often. */ \
    M(ToCustomWeek) M(ToISOWeek) M(ToRelativeDayNum) M(ToStartOfFiveMinutes) M(ToStartOfFifteenMinutes) M(MonthName) M(DateName) \
    M(ArrayFlatten) M(ArrayZip) M(ArrayIntersect) M(ArrayFirst) M(ArrayResize) M(ArrayFill) M(ArrayCumSum) M(ArrayDifference) \
    M(ArrayReduce) M(HasAll) M(HasAny) \
    M(BitAnd) M(BitOr) M(BitXor) M(BitNot) M(BitShiftLeft) M(BitShiftRight) M(BinaryRepr) \
    M(ExtractAll) M(SplitByRegexp) M(PositionCaseInsensitive) M(CountSubstrings) M(ToValidUTF8) M(MultiSearchAny) \
    M(Log2) M(Log10) M(Exp2) M(Cbrt) M(IsNaN) M(IsFinite) M(RunningDifference) M(RowNumberInBlock) \
    M(Base64Encode) M(Base64Decode) M(TryBase64Decode) M(Base64URLEncode) M(Base64URLDecode) M(TryBase64URLDecode) \
    M(Base32Encode) M(Base32Decode) M(Base58Encode) M(Base58Decode) \
    /* For the conditions of data skipping indexes. */ \
    M(BitWrapperFunc)

/// Aggregate functions and combinators, by the name of their registration function.
#define APPLY_FOR_ISOLATE_AGGREGATE_FUNCTIONS(M) \
    M(AggregateFunctionCount) M(AggregateFunctionSum) M(AggregateFunctionAvg) M(AggregateFunctionsMinMax) \
    M(AggregateFunctionsArgMinArgMax) M(AggregateFunctionsAny) M(AggregateFunctionsUniq) M(AggregateFunctionGroupArray) \
    M(AggregateFunctionGroupUniqArray) M(AggregateFunctionsQuantile) M(AggregateFunctionsStatisticsStable) \
    M(AggregateFunctionNothing) M(AggregateFunctionGroupConcat) M(AggregateFunctionSumCount) \
    M(AggregateFunctionsQuantileExact) M(AggregateFunctionsQuantileTDigest) M(AggregateFunctionTopK) M(AggregateFunctionUniqCombined) \
    M(AggregateFunctionUniqUpTo) M(AggregateFunctionsBitwise) M(AggregateFunctionSumMap) M(AggregateFunctionGroupArraySorted) \
    M(AggregateFunctionAnyHeavy)

#define APPLY_FOR_ISOLATE_AGGREGATE_FUNCTION_COMBINATORS(M) \
    M(AggregateFunctionCombinatorIf) M(AggregateFunctionCombinatorArray) M(AggregateFunctionCombinatorNull) \
    M(AggregateFunctionCombinatorState) M(AggregateFunctionCombinatorMerge) M(AggregateFunctionCombinatorDistinct) \
    M(AggregateFunctionCombinatorOrFill)

#define APPLY_FOR_ISOLATE_INTERPRETERS(M) \
    M(SelectQueryAnalyzer) M(InsertQuery) M(CreateQuery) M(RenameQuery) M(ShowTablesQuery) M(ShowColumnsQuery) \
    M(UseQuery) M(SetQuery) M(ExistsQuery) M(ShowCreateQuery) M(DescribeQuery) M(ExplainQuery) M(OptimizeQuery) M(AlterQuery) M(DeleteQuery)

#define APPLY_FOR_ISOLATE_FORMATS(M) \
    M(InputFormatNative) M(OutputFormatNative) M(InputFormatRowBinary) M(OutputFormatRowBinary) \
    M(InputFormatTabSeparated) M(OutputFormatTabSeparated) M(InputFormatValues) M(OutputFormatValues) \
    M(InputFormatCSV) M(OutputFormatCSV) M(InputFormatJSON) M(OutputFormatJSON) M(OutputFormatJSONCompact) \
    M(InputFormatJSONEachRow) M(OutputFormatJSONEachRow) M(InputFormatJSONCompactEachRow) M(OutputFormatJSONCompactEachRow) \
    M(OutputFormatPretty) M(OutputFormatVertical) M(OutputFormatNull) \
    M(InputFormatJSONCompact) M(InputFormatTSKV) M(OutputFormatTSKV) M(InputFormatJSONObjectEachRow) M(OutputFormatJSONObjectEachRow) \
    M(InputFormatJSONColumns) M(OutputFormatJSONColumns) M(InputFormatJSONCompactColumns) M(OutputFormatJSONCompactColumns) \
    M(InputFormatJSONColumnsWithMetadata) M(OutputFormatJSONColumnsWithMetadata) M(InputFormatBSONEachRow) M(OutputFormatBSONEachRow) \
    M(InputFormatTemplate) M(OutputFormatTemplate) M(InputFormatCustomSeparated) M(OutputFormatCustomSeparated) \
    M(InputFormatMsgPack) M(OutputFormatMsgPack) M(InputFormatAvro) M(OutputFormatAvro) M(InputFormatRawBLOB) M(OutputFormatRawBLOB) \
    M(InputFormatNpy) M(OutputFormatNpy) M(InputFormatForm) M(InputFormatRegexp) M(InputFormatJSONAsString) M(InputFormatJSONAsObject) \
    M(InputFormatLineAsString) M(InputFormatMySQLDump) M(InputFormatOne) M(InputFormatBuffers) M(OutputFormatBuffers) \
    M(OutputFormatJSONEachRowWithProgress) M(OutputFormatJSONCompactEachRowWithProgress) M(OutputFormatXML) M(OutputFormatMarkdown) \
    M(OutputFormatPrometheus) M(OutputFormatSQLInsert) M(OutputFormatHash) \
    M(InputFormatParquet) M(OutputFormatParquet) M(InputFormatParquetMetadata) M(InputFormatArrow) M(OutputFormatArrow) \
    M(TSVSchemaReader) M(CSVSchemaReader) M(JSONSchemaReader) M(JSONEachRowSchemaReader) M(JSONCompactEachRowSchemaReader) \
    M(ValuesSchemaReader) M(NativeSchemaReader) M(JSONObjectEachRowSchemaReader) M(JSONAsStringSchemaReader) M(JSONAsObjectSchemaReader) \
    M(JSONColumnsSchemaReader) M(JSONCompactColumnsSchemaReader) M(JSONColumnsWithMetadataSchemaReader) \
    M(RowBinaryWithNamesAndTypesSchemaReader) M(AvroSchemaReader) M(LineAsStringSchemaReader) M(RawBLOBSchemaReader) \
    M(MsgPackSchemaReader) M(CustomSeparatedSchemaReader) M(RegexpSchemaReader) M(TSKVSchemaReader) M(TemplateSchemaReader) \
    M(MySQLSchemaReader) M(BSONEachRowSchemaReader) M(OneSchemaReader) M(NpySchemaReader) M(FormSchemaReader) \
    M(ParquetSchemaReader) M(ParquetMetadataSchemaReader) M(ArrowSchemaReader)

#define APPLY_FOR_ISOLATE_TABLE_FUNCTIONS(M) \
    M(Numbers) M(Zeros) M(GenerateSeries) M(Values) M(Null) M(Format)

#define APPLY_FOR_ISOLATE_STORAGES(M) \
    M(Memory) M(Null) M(View)

#define APPLY_FOR_ISOLATE_DATABASES(M) \
    M(Memory)


namespace DB
{

#define M(NAME) void registerFunction##NAME(FunctionFactory &);
APPLY_FOR_ISOLATE_FUNCTIONS(M)
#undef M

#define M(NAME) void register##NAME(AggregateFunctionFactory &);
APPLY_FOR_ISOLATE_AGGREGATE_FUNCTIONS(M)
#undef M

#define M(NAME) void register##NAME(AggregateFunctionCombinatorFactory &);
APPLY_FOR_ISOLATE_AGGREGATE_FUNCTION_COMBINATORS(M)
#undef M

#define M(NAME) void registerInterpreter##NAME(InterpreterFactory &);
APPLY_FOR_ISOLATE_INTERPRETERS(M)
#undef M

#define M(NAME) void register##NAME(FormatFactory &);
APPLY_FOR_ISOLATE_FORMATS(M)
#undef M

#define M(NAME) void registerTableFunction##NAME(TableFunctionFactory &);
APPLY_FOR_ISOLATE_TABLE_FUNCTIONS(M)
#undef M

#define M(NAME) void registerStorage##NAME(StorageFactory &);
APPLY_FOR_ISOLATE_STORAGES(M)
#undef M

#define M(NAME) void registerDatabase##NAME(DatabaseFactory &);
APPLY_FOR_ISOLATE_DATABASES(M)
#undef M

void registerDiskLocal(DiskFactory & factory, bool global_skip_access_check);
void registerStorageObjectMergeTree(StorageFactory & factory);
void registerDatabaseCatalog(DatabaseFactory & factory);
void registerInterpreterCatalogDropQuery(InterpreterFactory & factory);

void registerIsolate();
void registerIsolate()
{
    registerParsers();

#define M(NAME) registerInterpreter##NAME(InterpreterFactory::instance());
    APPLY_FOR_ISOLATE_INTERPRETERS(M)
#undef M

#define M(NAME) registerFunction##NAME(FunctionFactory::instance());
    APPLY_FOR_ISOLATE_FUNCTIONS(M)
#undef M

#define M(NAME) register##NAME(AggregateFunctionFactory::instance());
    APPLY_FOR_ISOLATE_AGGREGATE_FUNCTIONS(M)
#undef M

#define M(NAME) register##NAME(AggregateFunctionCombinatorFactory::instance());
    APPLY_FOR_ISOLATE_AGGREGATE_FUNCTION_COMBINATORS(M)
#undef M

#define M(NAME) register##NAME(FormatFactory::instance());
    APPLY_FOR_ISOLATE_FORMATS(M)
#undef M

#define M(NAME) registerTableFunction##NAME(TableFunctionFactory::instance());
    APPLY_FOR_ISOLATE_TABLE_FUNCTIONS(M)
#undef M

#define M(NAME) registerStorage##NAME(StorageFactory::instance());
    APPLY_FOR_ISOLATE_STORAGES(M)
#undef M

    registerStorageObjectMergeTree(StorageFactory::instance());
    registerDatabaseCatalog(DatabaseFactory::instance());
    registerInterpreterCatalogDropQuery(InterpreterFactory::instance());

#define M(NAME) registerDatabase##NAME(DatabaseFactory::instance());
    APPLY_FOR_ISOLATE_DATABASES(M)
#undef M

    registerDiskLocal(DiskFactory::instance(), /* global_skip_access_check= */ true);

    QueryPlanStepRegistry::registerPlanSteps();
}

}
