#pragma once
#include "../../../Systemic/include/Network/WireTypes.h"
#include "../../../Systemic/include/DataTypes/DataTypes.StaticData.h"
#include "../../../Systemic/include/DataStructures/StaticArray.h"

namespace Network{
    inline static constexpr auto CreateWireTypesArray(){
        DataStructures::StaticArray<WireType, DATATYPE_COUNT> table{};
        template for (constexpr auto type : Reflection::Enumerators<DataType>){
            auto wire = WireType::Invalid;
            template for (constexpr auto candidate : Reflection::Enumerators<WireType>){
                if constexpr (std::meta::identifier_of(type) == std::meta::identifier_of(candidate))
                    wire = [:candidate:];
            }
            table.Push(wire);
        }
        return table;
    }

    inline constexpr auto WIRE_TYPES = CreateWireTypesArray();

    // Every client-visible type maps to a real wire type, and every fixed-width type
    // has the same size in engine vectors as on the wire, so the encoder can memcpy it.
    inline constexpr bool IsMappingValid = []{
        for (Int i = 0; i < DATATYPE_COUNT; ++i){
            const auto wire = WIRE_TYPES[i];
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
