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

    struct RowLazyState{
        CoreEngine::StorageTypes::RowHeader header;
        DataStructures::PolymorphicArray<RowReference> joinedRows;
        DataStructures::PolymorphicArray<block_size_t> sizes;

        page_offset_t dataOffset;
        UnsignedSmallInt numberOfColumns;

        bool isHeaderInitialized;
        explicit RowLazyState(const ::Memory::IAllocator* allocator);
    };

    struct RowReference{
        PageView* pageView;
        RowLazyState* lazyState;

        Int indexPosition;
        Int keySize;

        RowReference();
        RowReference(
            Frame* framePtr,
            const ::Memory::IAllocator* allocator,
            Int indexPosition,
            Int offset
        );

        RowReference(const RowReference& other) = delete;
        RowReference& operator=(const RowReference& other);

        RowReference(RowReference&& other) noexcept;
        RowReference& operator=(RowReference&& other) noexcept;

        ~RowReference();

        [[nodiscard]] QueryResult Materialize(const Memory::IAllocator* allocator)const;
        [[nodiscard]] Value PartialMaterialize(const Memory::IAllocator* allocator, column_index_t columnIndex)const;
        [[nodiscard]] Int Size()const;

        void Join(RowReference& other) const;
        void Join(const RowReference& other) const;
    };
}
