#pragma once
#include "../../../../Systemic/include/QueryResult.h"
#include "../../../../Systemic/include/DataTypes/DataTypes.h"
#include "../../../../Systemic/include/DataStructures/Array.h"
#include "../../DataStorage/Row.h"
#include "../PageView.h"

namespace DatabaseEngine
{
    class ExecutionContext;
}

namespace Memory
{
    class Allocator;
}

class Value;

namespace Pages{
    struct Frame;

    struct RowLazyState{
        DatabaseEngine::StorageTypes::RowHeader header;

        std::vector<Int> sizes;
        page_offset_t dataOffset;

        bool isHeaderInitialized;

        DataStructures::Array<RowReference> joinedRows;
    };

    struct RowReference{
        PageView* pageView;
        RowLazyState* lazyState;

        Int indexPosition;
        Int keySize;

        RowReference();
        RowReference(
            Frame* framePtr,
            const Memory::Allocator& allocator,
            Int indexPosition,
            Int offset
        );

        RowReference(const RowReference& other) = delete;
        RowReference& operator=(const RowReference& other) = delete;

        RowReference(RowReference&& other) noexcept;
        RowReference& operator=(RowReference&& other) noexcept;

        ~RowReference();

        [[nodiscard]] QueryResult Materialize(const Memory::Allocator* allocator)const;
        [[nodiscard]] Value PartialMaterialize(const Memory::Allocator* allocator, column_index_t columnIndex)const;
        [[nodiscard]] Int Size()const;

        void Join(const RowReference& other) const;
    };
}
