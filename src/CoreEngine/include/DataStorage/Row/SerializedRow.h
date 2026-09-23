#pragma once
#include "Row.h"
#include "../../../../Systemic/include/DataTypes/DataTypes.h"

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

namespace CoreEngine::StorageTypes{
    class Column;

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
