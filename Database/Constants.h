#pragma once

#include <cstdint>

namespace Constants
{
    constexpr size_t PAGE_SIZE = 8 * 1024;
    constexpr size_t MAX_NUMBER_OF_PAGES = 8000;
    constexpr size_t MAX_NUMBER_SYSTEM_PAGES = 40;
    constexpr size_t EXTENT_SIZE = 8;
    constexpr size_t EXTENT_BYTE_SIZE = EXTENT_SIZE * PAGE_SIZE;
    constexpr size_t EXTENT_BIT_MAP_SIZE = 64000;
    constexpr size_t LARGE_DATA_OBJECT_SIZE = 1024;

    // table types
    typedef uint16_t table_id_t;

    // data types
    typedef unsigned char object_t;

    // extent types
    typedef uint32_t extent_id_t;
    typedef uint32_t extent_num_t;

    // Page types
    typedef uint32_t page_id_t;
    typedef uint16_t page_size_t;
    typedef uint16_t page_offset_t;
    typedef uint16_t large_page_index_t;

    enum class PageType : uint8_t
    {
        DATA = 0,
        IAM = 1,
        LOB = 2,
        INDEX = 3,
        METADATA = 4,
        GAM = 5,
        FREESPACE = 6,
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
        DateTime = 4
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
    typedef uint16_t column_index_t;

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

    constexpr uint16_t PAGE_FREE_SPACE_SIZE = 8088;
    constexpr uint16_t GAM_PAGE_SIZE = 64000;
    constexpr uint32_t GAM_NUMBER_OF_PAGES = 64000 * 8;
}
