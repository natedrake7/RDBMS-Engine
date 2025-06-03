#pragma once

#include <cstdint>
#include <cstddef>
#include "../AdditionalLibraries/AdditionalDataTypes/DateTime/DateTime.h"
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

    constexpr page_id_t INVALID_PAGE_ID = std::numeric_limits<page_id_t>::max();

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

    enum class KeyType: uint8_t
    {
        Int = 0,
        Decimal = 1,
        String = 2,
        Bool = 3,
        DateTime = 4,
        Composite = 5
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

    constexpr uint16_t PAGE_FREE_SPACE_SIZE = PAGE_SIZE - PAGE_HEADER_SIZE - 7;
    constexpr uint16_t NEXT_PAGE_FREE_SPACE = PAGE_FREE_SPACE_SIZE + 1;
    constexpr uint16_t GAM_PAGE_SIZE = 64000;
    constexpr uint32_t GAM_NUMBER_OF_PAGES = 64000 * 8;
    constexpr page_id_t HEADER_PAGE_ID = 1;

    enum class ColumnType : uint8_t
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
        ColumnTypeCount = 10
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

    static Dictionary<string, ColumnType> ColumnTypesDictionary = {
        {"tinyint", ColumnType::TinyInt},
        {"smallint", ColumnType::SmallInt},
        {"int", ColumnType::Int},
        {"bigint", ColumnType::BigInt},
        {"datetime", ColumnType::DateTime},
        {"bool", ColumnType::Bool},
        {"string", ColumnType::String},
        {"decimal", ColumnType::Decimal},
        {"unicodestring", ColumnType::UnicodeString},
        {"guid", ColumnType::Guid}
        //all other types must have their size defined since it is not constant (e.g. string, decimal dont have fixed sizes)
    };
}
