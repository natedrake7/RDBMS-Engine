#pragma once
#include <Systemic/Constants.h>
#include <Systemic/DataStructures/PolymorphicArray.h>
#include <Systemic/DataTypes/DataTypes.h>

namespace CoreEngine{
    enum class ColumnOrigin: UInt8_t{
        Base = 0,
        Computed = 1
    };

    struct ColumnIdentity{
        UInt16_t _slotIndex;
        column_index_t _ordinalPosition;
        UInt16_t _virtualId;
        ColumnOrigin _origin;

        ColumnIdentity()
            :   _slotIndex(DEFAULT_SLOT_INDEX),
                _ordinalPosition(0),
                _virtualId(0),
                _origin(ColumnOrigin::Base){}

        ColumnIdentity(
            const UInt16_t slotIndex,
            const column_index_t ordinalPosition
        ):  _slotIndex(slotIndex), _ordinalPosition(ordinalPosition),
            _virtualId(0), _origin(ColumnOrigin::Base){}

        explicit ColumnIdentity(const UInt16_t virtualId)
            :   _slotIndex(0), _ordinalPosition(0),
                _virtualId(virtualId), _origin(ColumnOrigin::Computed){}

        static ColumnIdentity Base(
            const UInt16_t slotIndex,
            const column_index_t ordinalPosition
        ){
            return ColumnIdentity(slotIndex, ordinalPosition);
        }

        static ColumnIdentity Computed(const UInt16_t virtualId){
            return ColumnIdentity(virtualId);
        }

        [[nodiscard]] bool friend operator==(const ColumnIdentity& lhs, const ColumnIdentity& rhs){
            if (lhs._origin != rhs._origin)
                return false;

            return (
                lhs._origin == ColumnOrigin::Base
                && lhs._slotIndex == rhs._slotIndex
                && lhs._ordinalPosition == rhs._ordinalPosition
            )
            || (
                lhs._origin == ColumnOrigin::Computed
                && lhs._virtualId == rhs._virtualId
            );
        }

    };

    struct SchemaColumn{
        ColumnIdentity _identity;
        DataType _type;

        SchemaColumn(const ColumnIdentity& identity, const DataType type)
            : _identity(identity), _type(type){}

        static SchemaColumn Base(
            const UInt16_t slotIndex,
            const column_index_t ordinalPosition,
            const DataType& type
        ){
            return SchemaColumn(ColumnIdentity::Base(slotIndex, ordinalPosition), type);
        }

        static SchemaColumn Computed(
            const UInt16_t virtualId,
            const DataType type
        ){
            return SchemaColumn(ColumnIdentity::Computed(virtualId), type);
        }
    };

    struct OutputSchema{
        DataStructures::PolymorphicArray<SchemaColumn> _columns;

        OutputSchema(const ::Memory::IAllocator* allocator, const Int32_t capacity)
            : _columns(allocator, capacity){}

        Int32_t IndexOf(const ColumnIdentity& identity) const{
            for (Int32_t i = 0; i < _columns.Size(); ++i){
                if (_columns[i]._identity == identity)
                    return i;
            }
            return INVALID_COLUMN_INDEX;
        }
    };
}
