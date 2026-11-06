#pragma once
#include <Systemic/DataTypes/DataTypes.h>

namespace CoreEngine{
    class ExecutionContext;
    struct SelectionVector;
    struct DataVector;
}

namespace CoreEngine::StorageTypes{
    using ColumnDecodeFunction = void(*)(
        const ::Memory::IAllocator*,
        const object_t* const*,
        Int32_t, DataVector*, UInt32_t,
        column_index_t
    );

    struct ColumnDecodeInfo{
        ColumnDecodeFunction _function;
        column_index_t _ordinalPosition;
        DataType _type;

        ColumnDecodeInfo() = default;
        ColumnDecodeInfo(
            const ColumnDecodeFunction function,
            const column_index_t ordinalPosition,
            const DataType type
        ): _function(function), _ordinalPosition(ordinalPosition), _type(type) {}
    };

    struct ScanPlan{
        ColumnDecodeInfo* _info;
        UInt32_t _size;

        explicit ScanPlan() = default;
        explicit ScanPlan(ColumnDecodeInfo* info, const UInt32_t size)
            : _info(info), _size(size) {}
        ScanPlan(const ScanPlan&) = delete;
        ScanPlan& operator=(const ScanPlan&) = delete;
        ScanPlan(ScanPlan&&) = default;
        ScanPlan& operator=(ScanPlan&&) = default;

        static ScanPlan Create(const ::Memory::IAllocator* allocator, const UInt32_t size){
            return ScanPlan(
                allocator->AllocateRaw<ColumnDecodeInfo>(size * sizeof(ColumnDecodeInfo)),
                size
            );
        }
    };
}
