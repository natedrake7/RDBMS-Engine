#pragma once
#include "../../../Vectorization/Vectorization.h"
#include "../../../Contexts/ExecutionContext.h"
#include "../../Systemic/include/DataTypes/StringValue.h"
#include "../../Expression.h"

namespace Expressions{
    class Expression;
}

namespace CoreEngine{
    struct SelectionVector;
    class ExecutionContext;
    struct DataVector;
}

namespace CoreEngine::VectorizedKernels{
    DataVector* ColumnScanKernel(
        const Expressions::Expression* self,
        const ExecutionContext*,
        const DataChunk* chunk
    );

    template<typename T>
    DataVector* ConstantScanKernel(
        const Expressions::Expression* self,
        const ExecutionContext* context,
        const DataChunk*
    ){
        const auto* allocator = context->GetAllocator();
        const auto& value = self->AsConstant()->value;

        constexpr auto type = DataTypes::DataTypeOf<T>();

        const auto isNull = value.IsNull();
        auto* outVector = DataVector::ConstantVector(allocator, isNull, type);

        auto* slot = outVector->_data;
        if constexpr (DataTypes::Primitive<T>){
            const auto scalar = value.Get<T>();
            std::memcpy(slot, &scalar, sizeof(T));
        }
        else if constexpr (DataTypes::IsStringValue<T>){
            new (slot) DataTypes::StringValue(
                DataTypes::StringValue::Create(allocator, reinterpret_cast<const char*>(value.Data()), value.Size())
            );
        }
        else if constexpr (DataTypes::IsJson<T>)
            new (slot) DataTypes::JsonBinary(allocator, value.Data(), value.Size());
        else if constexpr (DataTypes::IsDecimal<T>)
            new (slot) DataTypes::Decimal(value.Data(), value.Size());
        else
            static_assert(DataTypes::AlwaysFalse<T>, "ConstantScanKernel: unsupported type");

        return outVector;
    }

    DataVector* LogicalAndKernel(
        const Expressions::Expression* self,
        const ExecutionContext* context,
        const DataChunk* chunk
    );

    DataVector* LogicalOrKernel(
        const Expressions::Expression* self,
        const ExecutionContext* context,
        const DataChunk* chunk
    );

    DataVector* LogicalNotKernel(
        const Expressions::Expression* self,
        const ExecutionContext* context,
        const DataChunk* chunk
    );
}
