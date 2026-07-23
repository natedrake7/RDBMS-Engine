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
}
