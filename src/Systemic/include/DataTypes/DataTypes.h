#pragma once
#include <cstdint>
#include <string>
#include <type_traits>

#include "DataTypes.Traits.h"
#include "../Reflection/Enum.h"

typedef uint32_t data_size_t;

inline constexpr auto INVALID_SIZE = std::numeric_limits<data_size_t>::max();

typedef uint16_t protocol_version_t;

typedef uint32_t request_id_t;
typedef uint32_t statement_ordinal_t;

typedef uint64_t session_id_t;

static inline constexpr session_id_t INVALID_SESSION_ID = 0;

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
typedef uint32_t value_size_t;

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


enum class DataType : UnsignedTinyInt{
    String   [[
        = DataTypes::Traits::Comparison{},
        = DataTypes::Traits::Concatenation{},
        = DataTypes::Traits::CaseInsensitiveComparison{}
    ]] = 0,
    Bool     [[= DataTypes::Traits::Comparison{}]] = 1,
    TinyInt  [[= DataTypes::Traits::Comparison{}, = DataTypes::Traits::Arithmetic{}]]  = 2,
    SmallInt [[= DataTypes::Traits::Comparison{}, = DataTypes::Traits::Arithmetic{}]]  = 3,
    Int      [[= DataTypes::Traits::Comparison{}, = DataTypes::Traits::Arithmetic{}]]  = 4,
    BigInt   [[= DataTypes::Traits::Comparison{}, = DataTypes::Traits::Arithmetic{}]]  = 5,
    Decimal  [[= DataTypes::Traits::Comparison{}, = DataTypes::Traits::Arithmetic{}]]  = 6,
    DateTime [[= DataTypes::Traits::Comparison{}]] = 7,
    Guid     [[= DataTypes::Traits::Comparison{}]] = 8,
    Json  = 9,
    Null = 10
};

static constexpr auto DATATYPE_COUNT = Reflection::EnumCount<DataType>;

inline constexpr DataType PromoteType(const DataType lhs, const DataType rhs){
    return lhs > rhs ? lhs : rhs;
}

static_assert(Reflection::IsContiguous<DataType>(),
    "Datatypes are not contiguous. Consider making them contiguous for better performance"
);

enum class StringComparisonType: UnsignedTinyInt{
    Equals = 0,
    EqualsIgnoreCase = 1,
    StartsWith = 2,
    StartsWithIgnoreCase = 3,
    EndsWith = 4,
    EndsWithIgnoreCase = 5,
    Contains = 6,
    ContainsIgnoreCase = 7
};

namespace DataTypes{
    class StringValue;
    class StringView;
    class Guid;
    class DateTime;
    class String;
    class Decimal;
    class JsonBinary;

    template<typename...>
    inline constexpr auto AlwaysFalse = false;

    template <typename T>
    concept NonPrimitiveType = std::is_same_v<T, String>
        || std::is_same_v<T, JsonBinary>
        || std::is_same_v<T, Decimal>
        || std::is_same_v<T, StringValue>;

    template<typename T>
    concept Primitive = std::is_trivially_copyable_v<T>
                       && !std::is_pointer_v<T>
                        && !NonPrimitiveType<T>;

    template<typename T>
    concept TriviallyCopiable = Primitive<T>
            || std::is_same_v<T, Decimal>;

    template<typename T>
    concept NonTriviallyCopiable = !TriviallyCopiable<T>;

    template <typename T>
    concept IsString = std::is_same_v<T, String>;

    template<typename T>
    concept IsStringValue = std::is_same_v<T, StringValue>;

    template <typename T>
    concept IsStringLike =
        std::is_same_v<T, String>
        || std::is_same_v<T, StringValue>
        || std::is_same_v<T, StringView>
        || std::is_same_v<T, std::string>
        || std::is_same_v<T, std::string_view>
        || std::is_same_v<T, char*>
        || std::is_same_v<T, const char*>
        || std::is_same_v<T, char>
        || std::is_same_v<T, const char>;

    template<typename T>
    concept IsArithmetic = std::is_arithmetic_v<T>
        || std::is_same_v<T, Decimal>;

    template<typename T>
    concept IsInteger = std::is_integral_v<T>;

    template <typename T>
    concept IsJson = std::is_same_v<T, JsonBinary>;

    template <typename T>
    concept IsDecimal = std::is_same_v<T, Decimal>;

    template<typename ...Ts>
    struct TypeList{
        static constexpr std::size_t SIZE = sizeof...(Ts);

        template<std::size_t I>
        using At = Ts...[I];

        template<typename T>
        static constexpr std::size_t IndexOf(){
            constexpr bool matches[]{
                std::is_same_v<T, Ts>...
            };

            for (std::size_t i = 0;i < SIZE; i++)
                if (matches[i])
                    return i;

            return SIZE;
        }
    };

    using DataTypeStorage = TypeList<
        StringValue,
        bool,
        TinyInt,
        SmallInt,
        Int,
        BigInt,
        Decimal,
        DateTime,
        Guid,
        JsonBinary,
        void
    >;

    static_assert(DataTypeStorage::SIZE == DATATYPE_COUNT, "DataTypeStorage needs exactly one entry per DataType");

    template<DataType TYPE>
    using StorageOf = DataTypeStorage::At<static_cast<std::size_t>(TYPE)>;

    template <typename T>
    constexpr static DataType DataTypeOf(){
        if constexpr (std::is_same_v<T, DataTypes::String>)
            return DataType::String;
        else{
            constexpr auto index = DataTypeStorage::IndexOf<T>();
            static_assert(
                index < DataTypeStorage::SIZE && !std::is_void_v<T>,
                "DataTypeStorage index out of range or invalid type"
            );

            return static_cast<DataType>(index);
        }

        return DataType::Null;
    }
}