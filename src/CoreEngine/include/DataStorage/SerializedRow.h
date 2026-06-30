#pragma once
#include "Row.h"
#include "../../../Systemic/include/DataTypes/DataTypes.h"
#include "../../../Systemic/include/DataTypes/DateTime.h"
#include "../../../Systemic/include/DataTypes/Decimal.h"
#include "../../../Systemic/include/DataTypes/Guid.h"
#include "../../../Systemic/include/DataTypes/JsonBinary.h"

namespace Errors{
    struct RuntimeStatus;
}

namespace Expressions
{
    class Expression;
}

namespace CoreEngine{
    class ExecutionContext;
}

namespace Pages{
    struct RawRowReference;
}

namespace CoreEngine::StorageTypes {
    class Column;

    struct InsertSlot{
        enum class SlotKind: UnsignedTinyInt{
            Value = 0,
            Default = 1,
            Null = 2
        };

        column_index_t _slot;
        SlotKind _kind;

        InsertSlot()
            : _slot(0), _kind(SlotKind::Null) {}
        explicit InsertSlot(const SlotKind kind)
            : _slot(0), _kind(kind) {}
        InsertSlot(const column_index_t slot, const SlotKind kind)
            : _slot(slot), _kind(kind) {}

        static InsertSlot ValueSlot(const column_index_t slot){
            return InsertSlot(slot, SlotKind::Value);
        }
        static InsertSlot DefaultSlot(const column_index_t slot){
            return InsertSlot(slot, SlotKind::Default);
        }
        static InsertSlot NullSlot(){
            return InsertSlot(SlotKind::Null);
        }
    };

    struct InsertPlan{
        DataStructures::PolymorphicArray<InsertSlot> _slotMap;
        DataStructures::PolymorphicArray<Expressions::Expression*> _sharedDefaults;

        InsertPlan() = default;
        InsertPlan(
            DataStructures::PolymorphicArray<InsertSlot>&& slotMap,
            DataStructures::PolymorphicArray<Expressions::Expression*>&& sharedDefaults
        ) : _slotMap(std::move(slotMap)), _sharedDefaults(std::move(sharedDefaults)) {}

        InsertPlan(InsertPlan&& other) noexcept = default;
        InsertPlan& operator=(InsertPlan&& other) noexcept = default;
    };

    struct RowSerializationContext{
        RowHeader header;
        const ::Memory::IAllocator* allocator;
        row_size_t capacity;

        RowSerializationContext(
            const ::Memory::IAllocator* allocator,
            const row_size_t capacity
        ):allocator(allocator), capacity(capacity) {}
    };

    class SerializedRow final{
        object_t* _data;
        UnsignedInt capacity;
        UnsignedInt offset;

    public:
        SerializedRow();
        SerializedRow(
            object_t* buffer,
            UnsignedSmallInt capacity,
            UnsignedSmallInt startingOffset
        );
        SerializedRow& operator=(SerializedRow&& other)noexcept;
        SerializedRow(SerializedRow&& other) noexcept;

        static SerializedRow FromRowPtr(const Pages::RawRowReference& rowPtr);

        UnsignedSmallInt SetData(const void* otherData, UnsignedSmallInt dataSize);
        void SetData(const void* otherData, UnsignedSmallInt dataSize, Int offSet) const;

        void AlignSizeWithOffset();

        Value MaterializeColumn(
            const ExecutionContext& context,
            const Column* column
        ) const;

        [[nodiscard]] object_t* ColumnAt(column_index_t index, Int& outSize)const;
        [[nodiscard]] Int ColumnSize(column_index_t index)const;

        [[nodiscard]] object_t* Data()const;
        [[nodiscard]] UnsignedSmallInt Size()const;
        [[nodiscard]] UnsignedSmallInt Offset()const;
    };
}
