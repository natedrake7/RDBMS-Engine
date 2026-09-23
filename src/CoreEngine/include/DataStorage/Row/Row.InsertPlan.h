#pragma once
#include "../../../../Systemic/include/DataTypes/DataTypes.h"
#include "../../../../Systemic/include/DataStructures/PolymorphicArray.h"

namespace Expressions{
    class Expression;
}

namespace CoreEngine::StorageTypes{

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
        ) : _slotMap(std::move(slotMap)),
            _sharedDefaults(std::move(sharedDefaults)) {}

        InsertPlan(const InsertPlan&) = delete;
        InsertPlan& operator=(const InsertPlan&) = delete;

        InsertPlan(InsertPlan&& other) noexcept = default;
        InsertPlan& operator=(InsertPlan&& other) noexcept = default;
    };
}