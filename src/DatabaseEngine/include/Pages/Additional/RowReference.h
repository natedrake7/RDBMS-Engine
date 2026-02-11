#pragma once
#include "../../../../Systemic/include/QueryResult.h"
#include "../../../../Systemic/include/DataTypes/DataTypes.h"
#include "../../DataStorage/Row.h"
#include "../PageView.h"
#include "DataStructures/Array.h"

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
        PageView pageView;
        Int indexPosition;
        Int keySize;

        RowLazyState* lazyState;

        RowReference();
        RowReference(Frame* framePtr, Int indexPosition, Int offset);

        RowReference(const RowReference& other);
        RowReference& operator=(const RowReference& other);

        RowReference(RowReference&& other) noexcept;
        RowReference& operator=(RowReference&& other) noexcept;

        ~RowReference();

        [[nodiscard]] QueryResult Materialize()const;
        [[nodiscard]] Value PartialMaterialize(const Memory::Allocator* allocator, column_index_t columnIndex)const;
        [[nodiscard]] Int Size()const;

        void Join(const RowReference& other) const;
    };
}
