#pragma once
#include "../../../../Systemic/include/DataTypes/DataTypes.h"
#include "../../../../Systemic/include/DataStructures/PolymorphicArray.h"

namespace Expressions{
    class Expression;
}

namespace CoreEngine::StorageTypes{
    struct ColumnPlacement;

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

    enum class InsertColumnLayout: UnsignedTinyInt{
        Fixed = 0,
        Variable = 1
    };

    enum class InsertColumnSource: UnsignedTinyInt{
        Vector = 0,
        Default = 1,
        Null = 2,
        Identity = 3,
        Computed = 4
    };

    struct InsertColumPlan{
        UnsignedSmallInt _width;
        InsertColumnLayout _layout;
        InsertColumnSource _source;
        DataType _type;
        column_index_t _slot;
        bool _nullable;

        InsertColumPlan() = default;
    };

    static inline constexpr bool IsVariableLayout(const DataType type){
        return type == DataType::String || type == DataType::Json || type == DataType::Decimal;
    }

    struct InsertPlan{
        DataStructures::PolymorphicArray<InsertSlot> _slotMap;
        DataStructures::PolymorphicArray<Expressions::Expression*> _sharedDefaults;
        DataStructures::PolymorphicArray<InsertColumPlan> _columnsPlans;

        InsertPlan() = default;
        InsertPlan(
            DataStructures::PolymorphicArray<InsertSlot>&& slotMap,
            DataStructures::PolymorphicArray<Expressions::Expression*>&& sharedDefaults,
            DataStructures::PolymorphicArray<InsertColumPlan>&& columnsPlans
        ) : _slotMap(std::move(slotMap)),
            _sharedDefaults(std::move(sharedDefaults)),
            _columnsPlans(std::move(columnsPlans)){}

        InsertPlan(const InsertPlan&) = delete;
        InsertPlan& operator=(const InsertPlan&) = delete;

        InsertPlan(InsertPlan&&) noexcept = default;
        InsertPlan& operator=(InsertPlan&&) noexcept = default;
    };

    struct ChunkInsertState{
        const DataChunk* _chunk;

        UnsignedInt _rowCount;
        UnsignedInt _columnCount;

        const DataVector** _vectors;

        UnsignedInt* _rowSizes;
        UnsignedInt* _rowStart;
        UnsignedSmallInt* _cursor;
        UnsignedBigInt* _slowRows;
        Int _slowCount;

        object_t* _buffer;
        SerializedRow* _rows;

        ColumnPlacement* _slowPlacements;
        UnsignedInt* _slowIndex;

        [[nodiscard]] bool IsSlowRow(const UnsignedInt index)const{
            return EngineBitmap::GetBitmapBit(this->_slowRows, index);
        }

        void MarkSlowRow(const UnsignedInt index){
            if (this->IsSlowRow(index))
                return;

            EngineBitmap::SetBitmapBit(this->_slowRows, index, true);
            this->_slowCount++;
        }
    };
}