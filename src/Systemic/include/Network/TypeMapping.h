#pragma once
#include "../../../Systemic/include/Network/WireTypes.h"
#include "../../../Systemic/include/DataTypes/DataTypes.StaticData.h"
#include "../../../Systemic/include/DataStructures/StaticArray.h"

namespace Network{
    [[nodiscard]] constexpr WireType MapToWire(const DataType type){
        switch (type){
        case DataType::Null:
            return WireType::Null;
        case DataType::Bool:
            return WireType::Bool;
        case DataType::TinyInt:
            return WireType::TinyInt;
        case DataType::SmallInt:
            return WireType::SmallInt;
        case DataType::Int:
            return WireType::Int;
        case DataType::BigInt:
            return WireType::BigInt;
        case DataType::DateTime:
            return WireType::DateTime;
        case DataType::Guid:
            return WireType::Guid;
        case DataType::String:
            return WireType::String;
        case DataType::Decimal:
            return WireType::Decimal;
        case DataType::Json:
            return WireType::Json;
        default:
            break;
            // no default: -Wswitch flags any new DataType that isn't mapped
        }

        return WireType::Invalid;
    }

    inline constexpr auto WIRE_TYPES = []{
        DataStructures::StaticArray<WireType, DATATYPE_COUNT> table{};
        for (auto i = 0; i < DATATYPE_COUNT; ++i)
            table.Push(MapToWire(static_cast<DataType>(i)));
        return table;
    }();

    // Every client-visible type maps to a real wire type, and every fixed-width type
    // has the same size in engine vectors as on the wire, so the encoder can memcpy it.
    inline constexpr bool IsMappingValid = []{
        for (Int i = 0; i < DATATYPE_COUNT; ++i){
            const auto type = static_cast<DataType>(i);
            const auto wire = WIRE_TYPES[i];

            if (type == DataType::RowIdentifier)
                continue;
            if (wire == WireType::Invalid)
                return false;
            if (!IsVariableLength(wire) && VECTOR_COLUMN_SIZES_BY_DATATYPE[i] != FixedWidth(wire))
                return false;
        }
        return true;
    }();

    static_assert(IsMappingValid, "DataType -> WireType mapping is incomplete or a fixed width disagrees with the engine");

    [[nodiscard]] inline WireType ToWire(const DataType type){
        return WIRE_TYPES[static_cast<size_t>(type)];
    }
}
