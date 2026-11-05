#pragma once
#include <cassert>
#include <cstdint>
#include <string>
#include <type_traits>
#include <utility>

#include <Systemic/DataTypes/DataTypes.Traits.h>
#include <Systemic/Reflection/Enum.h>

typedef uint8_t buffer_pool_slot_t;
typedef uint16_t schema_version_t;
typedef uint32_t data_size_t;

inline constexpr auto INVALID_SIZE = std::numeric_limits<data_size_t>::max();

typedef uint16_t protocol_version_t;

typedef uint32_t request_id_t;
typedef uint32_t statement_ordinal_t;

typedef uint64_t session_id_t;

static inline constexpr session_id_t INVALID_SESSION_ID = 0;

typedef uint8_t     UnsignedTinyInt;
typedef uint16_t    UnsignedSmallInt;
typedef uint32_t    UnsignedInt;
typedef uint64_t    UnsignedBigInt;
typedef __uint128_t UnsignedHugeInt;

typedef int8_t      TinyInt;
typedef int16_t     SmallInt;
typedef int32_t     Int;
typedef int64_t     BigInt;
typedef __int128_t  HugeInt;

typedef float       Float;
typedef double      Double;

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

typedef uint32_t gam_index_t;
// Page types
typedef uint32_t page_id_t;
typedef uint16_t page_size_t;
typedef uint16_t page_offset_t;
typedef uint16_t large_page_index_t;

typedef uint64_t log_sequence_number_t;
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
    HugeInt  [[= DataTypes::Traits::Comparison{}, = DataTypes::Traits::Arithmetic{}]]  = 6,

    UnsignedTinyInt  [[= DataTypes::Traits::Comparison{}, = DataTypes::Traits::Arithmetic{}]]  = 7,
    UnsignedSmallInt [[= DataTypes::Traits::Comparison{}, = DataTypes::Traits::Arithmetic{}]]  = 8,
    UnsignedInt      [[= DataTypes::Traits::Comparison{}, = DataTypes::Traits::Arithmetic{}]]  = 9,
    UnsignedBigInt   [[= DataTypes::Traits::Comparison{}, = DataTypes::Traits::Arithmetic{}]]  = 10,
    UnsignedHugeInt  [[= DataTypes::Traits::Comparison{}, = DataTypes::Traits::Arithmetic{}]]  = 11,

    Float  [[= DataTypes::Traits::Comparison{}, = DataTypes::Traits::Arithmetic{}]]     = 12,
    Double  [[= DataTypes::Traits::Comparison{}, = DataTypes::Traits::Arithmetic{}]]    = 13,

    Decimal  [[= DataTypes::Traits::Comparison{}, = DataTypes::Traits::Arithmetic{}]]  = 14,
    DateTime [[= DataTypes::Traits::Comparison{}]] = 15,
    Guid     [[= DataTypes::Traits::Comparison{}]] = 16,
    Json  = 17,
    Null = 18
};

static_assert(Reflection::IsContiguous<DataType>(),
    "Datatypes are not contiguous. Consider making them contiguous for better performance"
);

inline constexpr auto DATATYPE_COUNT = Reflection::EnumCount<DataType>;

namespace DataTypes::Detail{
    inline constexpr bool IsSignedIntegerType(const DataType type){
        return type >= DataType::TinyInt && type <= DataType::HugeInt;
    }

    inline constexpr bool IsUnsignedIntegerType(const DataType type){
        return type >= DataType::UnsignedTinyInt && type <= DataType::UnsignedHugeInt;
    }

    inline constexpr bool IsIntegerType(const DataType type){
        return IsSignedIntegerType(type) || IsUnsignedIntegerType(type);
    }

    inline constexpr bool IsFloatingPointType(const DataType type){
        return type == DataType::Float || type == DataType::Double;
    }

    inline constexpr bool IsNumericType(const DataType type){
        return IsIntegerType(type) || IsFloatingPointType(type) || type == DataType::Decimal;
    }

    // Lowest to highest. DataType values never change, so a new type only has to be placed here.
    inline constexpr DataType PROMOTION_ORDER[] = {
        DataType::Null,
        DataType::String,
        DataType::Bool,
        DataType::TinyInt, DataType::UnsignedTinyInt,
        DataType::SmallInt, DataType::UnsignedSmallInt,
        DataType::Int, DataType::UnsignedInt,
        DataType::BigInt, DataType::UnsignedBigInt,
        DataType::HugeInt, DataType::UnsignedHugeInt,
        DataType::Decimal, DataType::Float, DataType::Double,
        DataType::DateTime,
        DataType::Guid,
        DataType::Json
    };

    static_assert(std::size(PROMOTION_ORDER) == DATATYPE_COUNT, "PROMOTION_ORDER needs exactly one entry per DataType");

    inline constexpr std::size_t PromotionRank(const DataType type){
        for (std::size_t i = 0; i < std::size(PROMOTION_ORDER); i++)
            if (PROMOTION_ORDER[i] == type)
                return i;
        return DATATYPE_COUNT;
    }

    inline constexpr DataType ComputePromotion(const DataType lhs, const DataType rhs){
        const auto leftRank = PromotionRank(lhs), rightRank = PromotionRank(rhs);
        const auto higher = leftRank > rightRank ? lhs : rhs;
        const auto lower = leftRank > rightRank ? rhs : lhs;

        // A signed type below an unsigned one can't hold it: the next type up is the wider signed one
        return IsUnsignedIntegerType(higher) && IsSignedIntegerType(lower)
            ? PROMOTION_ORDER[PromotionRank(higher) + 1]
            : higher;
    }

    inline constexpr auto PROMOTION_TABLE = []{
        std::array<std::array<DataType, DATATYPE_COUNT>, DATATYPE_COUNT> table{};
        for (std::size_t i = 0; i < DATATYPE_COUNT; i++)
            for (std::size_t j = 0; j < DATATYPE_COUNT; j++)
                table[i][j] = ComputePromotion(static_cast<DataType>(i), static_cast<DataType>(j));
        return table;
    }();
}

inline constexpr DataType PromoteType(const DataType lhs, const DataType rhs){
    return DataTypes::Detail::PROMOTION_TABLE[static_cast<std::size_t>(lhs)][static_cast<std::size_t>(rhs)];
}

static_assert(PromoteType(DataType::Int, DataType::UnsignedInt) == DataType::BigInt);
static_assert(PromoteType(DataType::UnsignedBigInt, DataType::BigInt) == DataType::HugeInt);
static_assert(PromoteType(DataType::Null, DataType::Int) == DataType::Int);

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
        || std::is_same_v<T, StringValue>;

    template<typename T>
    concept Primitive = std::is_trivially_copyable_v<T>
                       && !std::is_pointer_v<T>
                        && !NonPrimitiveType<T>;

    template<typename T>
    concept TriviallyCopiable = Primitive<T>;

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
        HugeInt,
        UnsignedTinyInt,
        UnsignedSmallInt,
        UnsignedInt,
        UnsignedBigInt,
        UnsignedHugeInt,
        Float,
        Double,
        Decimal,
        DateTime,
        Guid,
        JsonBinary,
        void
    >;

    static_assert(DataTypeStorage::SIZE == DATATYPE_COUNT, "DataTypeStorage needs exactly one entry per DataType");
    static_assert(std::is_same_v<DataTypeStorage::At<static_cast<std::size_t>(DataType::Float)>, Float>);
    static_assert(std::is_same_v<DataTypeStorage::At<static_cast<std::size_t>(DataType::Double)>, Double>);
    static_assert(std::is_same_v<DataTypeStorage::At<static_cast<std::size_t>(DataType::Decimal)>, Decimal>);

    template<DataType TYPE>
    using StorageOf = DataTypeStorage::At<static_cast<std::size_t>(TYPE)>;

    template <typename T>
    constexpr static DataType DataTypeOf(){
        if constexpr (std::is_same_v<T, String>)
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

    template<typename Visitor>
    decltype(auto) VisitDataType(const DataType type, Visitor&& visitor){
        template for (constexpr auto enumerator : Reflection::Enumerators<DataType>){
            using TDataType = StorageOf<([:enumerator:])>;
            if constexpr (!std::is_void_v<TDataType>){
                if (type == [:enumerator:])
                    return visitor.template operator()<TDataType>();
            }
        }
        assert(false && "VisitDataType: base Expression or unknown type");
        std::unreachable();
    }
}