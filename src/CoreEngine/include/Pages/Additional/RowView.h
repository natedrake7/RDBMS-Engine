#pragma once
#include "../../../../Systemic/include/QueryResult.h"
#include "../../../../Systemic/include/DataTypes/DataTypes.h"
#include "../../DataStorage/Row.h"
#include "../PageView.h"
#include "../../../../Systemic/include/DataStructures/PolymorphicArray.h"

namespace CoreEngine
{
    class ExecutionContext;
}

namespace Memory
{
    class IAllocator;
}

class Value;

namespace Pages{
    struct Frame;

    struct RowPhysicalLazyState{
        CoreEngine::StorageTypes::RowHeader header;
        DataStructures::PolymorphicArray<block_size_t> sizes;

        PageView pageView;

        page_offset_t dataOffset;
        bool isHeaderInitialized;

        explicit RowPhysicalLazyState(
            const ::Memory::IAllocator* allocator,
            Frame* framePtr
        );
    };

    struct RowLazyState{
        DataStructures::PolymorphicArray<const RowView*> joinedRows;

        explicit RowLazyState(const ::Memory::IAllocator* allocator);
        RowLazyState(const RowLazyState& other);
    };

    struct RowView{
        RowPhysicalLazyState* physicalState;
        RowLazyState* logicalState;

        UnsignedSmallInt numberOfColumns;
        UnsignedSmallInt indexPosition;
        Int keySize;

        RowView();
        explicit RowView(Int numberOfColumns);
        RowView(
            Frame* framePtr,
            const ::Memory::IAllocator* allocator,
            Int indexPosition,
            Int offset
        );

        static RowView* NullReference(
            const ::Memory::IAllocator* allocator,
            Int numberOfColumns
        );
        static RowView* Copy(
            const RowView* other,
            const ::Memory::IAllocator* allocator
        );

        RowView(const RowView& other);
        RowView& operator=(const RowView& other);

        RowView(RowView&& other) noexcept;
        RowView& operator=(RowView&& other) noexcept;

        ~RowView();

        [[nodiscard]] QueryResult Materialize(const Memory::IAllocator* allocator)const;
        [[nodiscard]] Value PartialMaterialize(const Memory::IAllocator* allocator, column_index_t columnIndex)const;
        [[nodiscard]] Int Size()const;

        void Join(const RowView* other) const;
    };
}
