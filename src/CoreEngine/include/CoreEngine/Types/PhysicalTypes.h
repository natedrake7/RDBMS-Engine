#pragma once
#include "Systemic/DataTypes/DataTypes.h"
#include "Systemic/DataTypes/Decimal.h"

namespace CoreEngine::Types{
    enum class PhysicalType : UnsignedTinyInt{
        Bool = 0,
        Int8 = 1, UInt8 = 2, Int16 = 3, UInt16 = 4, Int32 = 5, UInt32 = 6, Int64 = 7, UInt64 = 8, Int128 = 9, UInt128 = 10,
        Float = 11, Double = 12,
        Guid = 13, String = 14, Json = 15, Null
    };

    inline constexpr auto PHYSICAL_TYPE_COUNT = Reflection::EnumCount<PhysicalType>;

    static_assert(Reflection::IsContiguous<PhysicalType>(),
        "Physical Types are not contiguous. Consider making them contiguous for better performance"
    );

    using PhysicalStorage = DataTypes::TypeList<
        bool,
        TinyInt,
        UnsignedTinyInt,
        SmallInt,
        UnsignedSmallInt,
        Int,
        UnsignedInt,
        BigInt,
        UnsignedBigInt,
        HugeInt,
        UnsignedHugeInt,
        Float,
        Double,
        DataTypes::Guid,
        DataTypes::StringValue,
        DataTypes::JsonBinary,
        void
    >;

    static_assert(PhysicalStorage::SIZE == PHYSICAL_TYPE_COUNT, "DataTypeStorage needs exactly one entry per DataType");

    template<PhysicalType TYPE>
    using StorageOf = PhysicalStorage::At<static_cast<std::size_t>(TYPE)>;

    template <typename T>
    constexpr static PhysicalType PhysicalTypeOf(){
        constexpr auto index = PhysicalStorage::IndexOf<T>();
        static_assert(
            index < PhysicalStorage::SIZE && !std::is_void_v<T>,
            "PhysicalStorage index out of range or invalid type"
        );

        return static_cast<PhysicalType>(index);

        return PhysicalType::Null;
    }

    template<typename Visitor>
    decltype(auto) VisitPhysicalType(const PhysicalType type, Visitor&& visitor){
        template for (constexpr auto enumerator : Reflection::Enumerators<PhysicalType>){
            using TPhysicalType = StorageOf<([:enumerator:])>;
            if constexpr (!std::is_void_v<TPhysicalType>){
                if (type == [:enumerator:])
                    return visitor.template operator()<TPhysicalType>();
            }
        }
        assert(false && "VisitDataType: base Expression or unknown type");
        std::unreachable();
    }

    inline constexpr Int DECIMAL_64_MAX_PRECISION = 18;

    [[nodiscard]] inline constexpr PhysicalType PhysicalTypeOf(const DataType type, const DataTypes::DecimalType decimal = DataTypes::DecimalType()){
        switch (type){
        case DataType::Bool:             return PhysicalType::Bool;
        case DataType::TinyInt:          return PhysicalType::Int8;
        case DataType::UnsignedTinyInt:  return PhysicalType::UInt8;
        case DataType::SmallInt:         return PhysicalType::Int16;
        case DataType::UnsignedSmallInt: return PhysicalType::UInt16;
        case DataType::Int:              return PhysicalType::Int32;
        case DataType::UnsignedInt:      return PhysicalType::UInt32;
        case DataType::BigInt:
        case DataType::DateTime:         return PhysicalType::Int64;
        case DataType::UnsignedBigInt:   return PhysicalType::UInt64;
        case DataType::HugeInt:          return PhysicalType::Int128;
        case DataType::UnsignedHugeInt:  return PhysicalType::UInt128;
        case DataType::Float:            return PhysicalType::Float;
        case DataType::Double:           return PhysicalType::Double;
        case DataType::Guid:             return PhysicalType::Guid;
        case DataType::String:           return PhysicalType::String;
        case DataType::Json:             return PhysicalType::Json;
        case DataType::Decimal:
            return PhysicalType::Int128;
            // return decimal._precision <= DECIMAL_64_MAX_PRECISION
            //     ? PhysicalType::Int64
            //     : PhysicalType::Int128;
        case DataType::Null:
            return PhysicalType::Null;
        }

        std::unreachable();
    }
}
