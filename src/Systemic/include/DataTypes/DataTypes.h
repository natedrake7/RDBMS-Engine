#pragma once
#include <cstdint>

typedef uint8_t UnsignedTinyInt;
typedef uint16_t UnsignedSmallInt;
typedef uint32_t UnsignedInt;
typedef uint64_t UnsignedBigInt;
typedef int8_t TinyInt;
typedef int16_t SmallInt;
typedef int32_t Int;
typedef int64_t BigInt;

typedef Int file_descriptor_t;

typedef uint8_t byte_t;

// block types
typedef uint16_t block_size_t;

// column types
typedef uint8_t column_index_t;

// record size
typedef uint16_t row_size_t;

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

// table types
typedef uint16_t table_id_t;
typedef int32_t column_id_t;

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

enum class DataType : uint8_t{
    TinyInt = 0,
    SmallInt = 1,
    Int = 2,
    BigInt = 3,
    Decimal = 4,
    String = 5,
    Bool = 6,
    DateTime = 7,
    Guid = 8,
    Json = 9,
    RowIdentifier = 10,
    Unknown = 11
};

enum class StringComparisonType: UnsignedTinyInt{
    Equals = 0,
    EqualsIgnoreOrdinalCase = 1,
    StartsWith = 2,
    StartsWithIgnoreOrdinalCase = 3,
    EndsWith = 4,
    EndsWithIgnoreOrdinalCase = 5,
    Contains = 6,
    ContainsIgnoreCase = 7
};

constexpr Int DATETIME_SIZE = sizeof(BigInt);