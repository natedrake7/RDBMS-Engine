#pragma once
#include "../../../Systemic/include/DataTypes/DataTypes.h"
#include "../Pages/AllocationPageView.h"

namespace Memory{
    class IAllocator;
}

namespace Storage{
    struct FileKey;
}

namespace CoreEngine{
    class ExecutionContext;
    struct SelectionVector;
    struct DataVector;
}

namespace CoreEngine::StorageTypes{
    struct RID;
    class Table;

    using PageMaterializationFunction  = void(*)(
        const Storage::FileKey*,
        const ::Memory::IAllocator*,
        const DataStructures::PolymorphicArray<RID>&,
        DataVector*,
        column_index_t
    );

    struct FilterColumnInfo{
        PageMaterializationFunction  _function;
        column_index_t _ordinalPosition;
        DataType _type;

        FilterColumnInfo() = default;
        FilterColumnInfo(
            const PageMaterializationFunction function,
            const column_index_t ordinalPosition,
            const DataType type
        ): _function(function), _ordinalPosition(ordinalPosition), _type(type) {}
    };
}