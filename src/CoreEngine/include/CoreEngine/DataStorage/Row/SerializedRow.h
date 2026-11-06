#pragma once
#include <CoreEngine/DataStorage/Row/Row.h>
#include <Systemic/DataTypes/DataTypes.h>

#include "CoreEngine/DataStorage/Schema/Schemas.h"

namespace Errors{
    struct RuntimeStatus;
}

namespace Expressions
{
    class Expression;
}

namespace CoreEngine{
    namespace Pages{
        struct RawRowReference;
    }

    class ExecutionContext;
}

namespace CoreEngine::StorageTypes{
    class Column;

    class SerializedRow final{
        object_t* _data;
        UInt32_t capacity;
        UInt32_t offset;

    public:
        explicit SerializedRow();
        explicit SerializedRow(
            object_t* buffer,
            UInt16_t capacity,
            UInt16_t startingOffset
        );
        SerializedRow& operator=(SerializedRow&& other)noexcept;
        SerializedRow(SerializedRow&& other) noexcept;

        static SerializedRow FromRowPtr(const Pages::RawRowReference& rowPtr);

        UInt16_t SetData(const void* otherData, UInt16_t dataSize);
        void SetData(const void* otherData, UInt16_t dataSize, Int32_t offSet) const;

        void AlignSizeWithOffset();

        Value MaterializeColumn(
            const ExecutionContext& context,
            const Schemas::ColumnSchema* column
        ) const;

        [[nodiscard]] object_t* ColumnAt(column_index_t index, Int32_t& outSize)const;
        [[nodiscard]] Int32_t ColumnSize(column_index_t index)const;

        [[nodiscard]] object_t* Data()const;
        [[nodiscard]] UInt16_t Size()const;
        [[nodiscard]] UInt16_t Offset()const;
    };
}
