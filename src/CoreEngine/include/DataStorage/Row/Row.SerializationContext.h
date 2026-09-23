#pragma once
#include "Row.h"
#include "../../../../Systemic/include/DataTypes/DataTypes.h"
#include "../../../../Systemic/include/Memory/IAllocator.h"
#include "../../../../Systemic/include/DataTypes/Value.h"

namespace CoreEngine::StorageTypes{
    struct ColumnPlacement{
        enum class Kind: UnsignedTinyInt{
            Null = 0,
            Inline = 1,
            OffRow = 2,
            ExistingLob = 3
        };

        Kind _kind;
        UnsignedInt _size;

        ColumnPlacement() :
            _kind(Kind::Null), _size(0) {}
        ColumnPlacement(const Kind kind, const UnsignedInt size)
            : _kind(kind), _size(size) {}
    };

    struct RowSerializationContext{
        RowHeader _header;
        const ::Memory::IAllocator* _allocator;

        Value* _values;
        ColumnPlacement* _placements;

        RowSerializationContext(
            const ::Memory::IAllocator* allocator,
            const Int columnCount
        ):  _allocator(allocator),
            _values(static_cast<Value*>(allocator->AllocateRaw(sizeof(Value) * columnCount))),
            _placements(static_cast<ColumnPlacement*>(allocator->AllocateRaw(sizeof(ColumnPlacement) * columnCount)))
        {}
    };

}
