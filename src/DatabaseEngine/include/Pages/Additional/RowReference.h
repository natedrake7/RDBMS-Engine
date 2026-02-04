#pragma once
#include "../../Database.h"
#include "../../../../Systemic/include/DataTypes/DataTypes.h"

class Value;

namespace Pages{
    class PageView;

    struct RowReference{
        PageView* pagePtr;
        Int indexPosition;
        Int keySize;

        mutable DatabaseEngine::StorageTypes::RowHeader header;

        mutable Dictionary<column_index_t, Value> cache;
        mutable std::vector<Int> sizes;
        mutable page_offset_t dataOffset;
        mutable bool isHeaderInitialized;

        RowReference();
        RowReference(PageView* pagePtr, Int indexPosition, Int offset);

        RowReference(const RowReference& other);
        RowReference& operator=(const RowReference& other);

        RowReference(RowReference&& other) noexcept;
        RowReference& operator=(RowReference&& other) noexcept;

        ~RowReference();

        [[nodiscard]] QueryResult Materialize()const;
        [[nodiscard]] Value PartialMaterialize(column_index_t columnIndex)const;
    };
}
