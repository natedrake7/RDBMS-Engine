#pragma once

#include <cstdint>
#include <cstddef>
#include "../AdditionalLibraries/DataTypes/DateTime/DateTime.h"
#include "../AdditionalLibraries/Dictionary/Dictionary.h"

#include <limits>

namespace Constants
{
    constexpr size_t PAGE_SIZE = 8 * 1024;
    constexpr size_t MAX_NUMBER_OF_PAGES = 15000;
    constexpr size_t MAX_NUMBER_SYSTEM_PAGES = 1000000;
    constexpr size_t EXTENT_SIZE = 8;
    constexpr size_t EXTENT_BYTE_SIZE = EXTENT_SIZE * PAGE_SIZE;
    constexpr size_t EXTENT_BIT_MAP_SIZE = 64000;
    constexpr size_t LARGE_DATA_OBJECT_SIZE = 8060;
    constexpr size_t LARGE_DATA_MAX_SIZE = 2147483648;
    constexpr size_t LOG_BATCH_SIZE = 1024 * 1024; // 1 MB

    constexpr int32_t INVALID_DATABASE_ID = -1;
    constexpr int32_t INVALID_TABLE_ID = -1;
    constexpr int32_t INVALID_COLUMN_ID = -1;
    constexpr int32_t INVALID_SCHEMA_ID = -1;
    constexpr int16_t INVALID_ORDINAL_POS = -1;

    // table types
    typedef uint16_t table_id_t;

    // data types
    typedef unsigned char object_t;

    // extent types
    typedef uint32_t extent_id_t;
    typedef uint32_t extent_num_t;

    // Page types
    typedef uint32_t page_id_t;
    typedef int16_t page_size_t;
    typedef uint16_t page_offset_t;
    typedef uint16_t large_page_index_t;

    typedef uint32_t log_sequence_number_t;
    typedef uint64_t transaction_id_t;

    constexpr transaction_id_t INVALID_LOG_SEQUENCE_NUMBER = std::numeric_limits<log_sequence_number_t>::max();
    constexpr table_id_t INVALID_TABLE_ORDINAL_POS = std::numeric_limits<table_id_t>::max();
    constexpr transaction_id_t INVALID_TRANSACTION_ID = std::numeric_limits<transaction_id_t>::max();
    constexpr page_id_t INVALID_PAGE_ID = std::numeric_limits<page_id_t>::max();
    constexpr int32_t INVALID_PAGE_INDEX_ID = -1;
    constexpr size_t ROW_ID_SIZE = sizeof(page_id_t) + sizeof(int32_t);

    constexpr std::string_view WILDCARD = "*";

    constexpr int8_t INVALID_DECIMAL_PRECISION = -1;
    constexpr int8_t INVALID_DECIMAL_SCALE = -1;

    constexpr int MAX_DECIMAL_PRECISION = 38;
    constexpr int MAX_DECIMAL_SCALE = 38;

    constexpr int64_t INVALID_TOP = -1;

    enum class AlterTableType: uint8_t {
        AddColumn = 0,
        AlterColumn = 1,
        DropColumn = 2,
        RenameColumn = 3,
    };

    enum OrderType : uint8_t {
        ASCENDING = 0,
        DESCENDING = 1,
    };

    enum AggregateFunction: uint8_t {
        NONE = 0,
        SUM = 1,
        AVERAGE = 2,
        COUNT = 3,
        MIN = 4,
        MAX = 5
    };

    enum class PageType : uint8_t
    {
        DATA = 0,
        IAM = 1,
        LOB = 2,
        INDEX = 3,
        METADATA = 4,
        GAM = 5,
        FREESPACE = 6,
        OVERFLOW = 7,
        Error = 8
    };

    enum class TableType : uint8_t
    {
        HEAP = 0,
        CLUSTERED = 1,
    };

    enum Operator: uint8_t{
        OperatorNone = 0,
        Equal = 1,
        NotEqual = 2,
        GreaterThan = 3,
        LessThan = 4,
        GreaterOrEqual = 5,
        LessOrEqual = 6
    };

    enum ConditionType: uint8_t{
        ConditionNone = 0,
        And = 1,
        Or = 2
    };

    enum TreeType : uint8_t 
    {
        Clustered = 0,
        NonClustered = 1
    };


    typedef uint8_t byte;

    // block types
    typedef uint16_t block_size_t;

    // column types
    typedef uint8_t column_index_t;

    // record size
    typedef uint32_t row_size_t;

    // header literal size
    typedef uint16_t header_literal_t;
    typedef uint16_t row_header_size_t;

    // number of column - table
    typedef uint16_t column_number_t;
    typedef uint16_t table_number_t;

    // bit map constants
    typedef uint16_t bit_map_size_t;
    typedef uint16_t bit_map_pos_t;
    typedef uint16_t byte_map_size_t;
    typedef uint16_t byte_map_pos_t;

    // decimal constants
    typedef uint8_t fraction_index_t;

    // B-tree constants
    typedef uint16_t key_size_t;

    constexpr uint16_t OBJECT_METADATA_SIZE_T = sizeof(page_size_t) + sizeof(page_id_t) + sizeof(large_page_index_t);
    constexpr uint16_t PAGE_HEADER_SIZE = sizeof(page_id_t) + 2 * sizeof(page_size_t) + sizeof(PageType);
    constexpr uint16_t OVERFLOW_POINTER_SIZE = sizeof(page_offset_t) + sizeof(page_id_t);

    constexpr uint16_t PAGE_FREE_SPACE_SIZE = PAGE_SIZE - PAGE_HEADER_SIZE - 7;
    constexpr uint16_t NEXT_PAGE_FREE_SPACE = PAGE_FREE_SPACE_SIZE + 1;
    constexpr uint16_t GAM_PAGE_SIZE = 64000;
    constexpr uint32_t GAM_NUMBER_OF_PAGES = 64000 * 8;
    constexpr page_id_t HEADER_PAGE_ID = 1;

    enum class JoinType : uint8_t {
        Inner = 0,
        Left = 1,
        Right = 2,
        Full = 3
    };

    enum class DataType : uint8_t
    {
        TinyInt = 0,
        SmallInt = 1,
        Int = 2,
        BigInt = 3,
        Decimal = 4,
        String = 5,
        UnicodeString = 6,
        Bool = 7,
        DateTime = 8,
        Guid = 9,
        RowIdentifier = 10,
        Invalid = 11
    };


    enum class FunctionType : uint8_t {
        // -----------------------
        // Date/Time Functions
        // -----------------------
        GetDate = 0,         // SQL Server style
        DateAdd = 1,
        DateDiff = 2,
        DatePart = 3,
        Year = 4,
        Month = 5,
        Day = 6,

        // -----------------------
        // GUID/Identifier Functions
        // -----------------------
        NewGuid = 20,

        // -----------------------
        // String Functions
        // -----------------------
        Concat = 40,
        Length = 41,
        AsciiValue = 42,
        Char = 43,
        CharIndex = 44,
        Instr = 45,          // alias for CharIndex
        Lower = 46,
        Upper = 47,
        Trim = 48,
        TrimLeft = 49,
        TrimRight = 50,
        Replace = 51,
        Substr = 52,
        Left = 53,
        Right = 54,
        Reverse = 55,
        Repeat = 56,
        Space = 57,
        Soundex = 58,

        // -----------------------
        // Mathematical Functions
        // -----------------------
        Abs = 80,
        Ceil = 81,
        Floor = 82,
        Round = 83,
        Power = 84,
        Sqrt = 85,
        Exp = 86,
        Log = 87,
        Log10 = 88,
        Rand = 89,

        // -----------------------
        // Conversion Functions
        // -----------------------
        Cast = 100,
        Convert = 101,

        // -----------------------
        // Aggregate Functions
        // -----------------------
        Count = 120,
        Sum = 121,
        Avg = 122,
        Min = 123,
        Max = 124
    };

    static Dictionary<string, block_size_t> ColumnTypeSizes = {
        {"tinyint", sizeof(int8_t)},
        {"smallint", sizeof(int16_t)},
        {"int", sizeof(int32_t)},
        {"bigint", sizeof(int64_t)},
        {"datetime", DataTypes::DateTime::DateTimeSize()},
        {"bool", sizeof(bool)},
        {"string", 0},
        {"decimal", 0},
        {"unicodestring", 0},
        {"guid", 16}
        //all other types must have their size defined since it is not constant (e.g. string, decimal dont have fixed sizes)
    };

    static Dictionary<string, DataType> ColumnTypesDictionary = {
        {"tinyint", DataType::TinyInt},
        {"smallint", DataType::SmallInt},
        {"int", DataType::Int},
        {"bigint", DataType::BigInt},
        {"datetime", DataType::DateTime},
        {"bool", DataType::Bool},
        {"string", DataType::String},
        {"decimal", DataType::Decimal},
        {"unicodestring", DataType::UnicodeString},
        {"guid", DataType::Guid}
        //all other types must have their size defined since it is not constant (e.g. string, decimal dont have fixed sizes)
    };

    static Dictionary<DataType, string> ColumnTypesToStringDictionary = {
        {DataType::TinyInt, "TinyInt"},
        {DataType::SmallInt, "SmallInt"},
        {DataType::Int, "Int"},
        {DataType::BigInt, "BigInt"},
        {DataType::DateTime, "DateTime"},
        {DataType::Bool, "Bool"},
        {DataType::String, "String"},
        {DataType::Decimal, "Decimal"},
        {DataType::UnicodeString, "Unicodestring"},
        {DataType::Guid, "Guid"}
        //all other types must have their size defined since it is not constant (e.g. string, decimal dont have fixed sizes)
    };
}
