#pragma once
#include "../../../Systemic/include/DataTypes/DataTypes.h"

namespace CoreEngine{
    class ExecutionContext;
    struct SelectionVector;
    struct DataVector;
}

namespace CoreEngine::StorageTypes{
    class Table;

    using TableMaterializationFunction = void(*)(
        const Table*,
        const ExecutionContext&,
        const SelectionVector*,
        DataVector*,
        UnsignedSmallInt slotIndex,
        column_index_t ordinalPosition
    );

    struct ColumnMaterializationInfo{
        TableMaterializationFunction _function;
        column_index_t _ordinalPosition;
        DataType _type;

        ColumnMaterializationInfo() = default;
        ColumnMaterializationInfo(
            const TableMaterializationFunction function,
            const column_index_t ordinalPosition,
            const DataType type
        ): _function(function), _ordinalPosition(ordinalPosition), _type(type) {}
    };

    template <typename... Ts>
    constexpr auto MakeMaterializerTable() {
        DataStructures::StaticArray<TableMaterializationFunction, DATATYPE_COUNT> table{};  // all nullptr
        ((table[static_cast<size_t>(DataTypes::DataTypeOf<Ts>())] =
              &Table::MaterializeColumn<Ts>
        ), ...);
        return table;
    }

    // Types here are the *in-vector* representations, not the storage ones:
    // a String column materializes into StringValue entries.
    inline constexpr auto COLUMN_MATERIALIZERS = MakeMaterializerTable<
        bool, TinyInt, SmallInt, Int, BigInt,
        DataTypes::Decimal, DataTypes::StringValue, DataTypes::DateTime,
        DataTypes::Guid, DataTypes::JsonBinary>();
}
