#pragma once
#include <cstdint>
#include <type_traits>

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

static constexpr Int DATATYPE_COUNT = 12;

enum class DataType: UnsignedTinyInt {
    String = 0,
    Bool = 1,
    TinyInt = 2,
    SmallInt = 3,
    Int = 4,
    BigInt = 5,
    Decimal = 6,
    DateTime = 7,
    Guid = 8,
    Json = 9,
    Null = 10,
    RowIdentifier = 11
};

static_assert(static_cast<UnsignedTinyInt>(DataType::RowIdentifier) == DATATYPE_COUNT - 1,
              "DataType must be 0-based and contiguous so it can index tables of size DATATYPE_COUNT");

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

namespace DataTypes{
    class String;
    class Decimal;
    class JsonBinary;

    template<typename>
    inline constexpr auto AlwaysFalse = false;

    template<typename T>
    concept PrimitiveColumn = std::is_trivially_copyable_v<T>
                       && !std::is_pointer_v<T>;

    template <typename T>
    concept NonPrimitiveType = std::is_same_v<T, String>
        || std::is_same_v<T, JsonBinary>
        || std::is_same_v<T, Decimal>;

    template <typename T>
    concept IsString = std::is_same_v<T, String>;

    template <typename T>
    concept IsJson = std::is_same_v<T, JsonBinary>;

    template <typename T>
    concept IsDecimal = std::is_same_v<T, Decimal>;
}