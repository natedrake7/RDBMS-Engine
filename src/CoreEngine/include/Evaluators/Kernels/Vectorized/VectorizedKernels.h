#pragma once
#include "../../../Vectorization/Vectorization.h"
#include "../../../Contexts/ExecutionContext.h"
#include "../../Systemic/include/DataTypes/StringValue.h"
#include "../../Expression.h"
#include "../../Systemic/include/Coercions/Coercions.h"
#include "../../Systemic/include/DataTypes/BoundVariable.h"

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

        const auto isNull = value.IsNull();
        auto* outVector = DataVector::ConstantVector(allocator, isNull, DataTypes::DataTypeOf<T>());

        auto* outData = outVector->template DataAs<T>();
        if constexpr (DataTypes::Primitive<T>)
            *outData = value.Get<T>(allocator);
        else if constexpr (DataTypes::IsStringValue<T>)
            *outData = DataTypes::StringValue::Create(allocator, reinterpret_cast<const char*>(value.Data()), value.Size());
        else if constexpr (DataTypes::IsJson<T>)
            *outData = DataTypes::JsonBinary(allocator, value.Data(), value.Size());
        else if constexpr (DataTypes::IsDecimal<T>)
            *outData = DataTypes::Decimal(value.Data(), value.Size());
        else
            static_assert(DataTypes::AlwaysFalse<T>, "ConstantScanKernel: unsupported type");

        return outVector;
    }

    template<typename T>
    DataVector* VariableScanKernel(
        const Expressions::Expression* self,
        const ExecutionContext* context,
        const DataChunk*
    ){
        const auto* allocator = context->GetAllocator();

        const auto* variableExpr = self->AsVariable();
        const auto& value = context->GetVariable(DataTypes::StringView::ViewOf(variableExpr->normalizedName))->GetValue();

        const auto isNull = value.IsNull();
        auto* outVector = DataVector::ConstantVector(allocator, isNull, DataTypes::DataTypeOf<T>());

        auto* outData = outVector->template DataAs<T>();
        if constexpr (DataTypes::Primitive<T>)
            *outData = value.Get<T>(allocator);
        else if constexpr (DataTypes::IsStringValue<T>)
            *outData = DataTypes::StringValue::Create(allocator, reinterpret_cast<const char*>(value.Data()), value.Size());
        else if constexpr (DataTypes::IsJson<T>)
            *outData = DataTypes::JsonBinary(allocator, value.Data(), value.Size());
        else if constexpr (DataTypes::IsDecimal<T>)
            *outData = DataTypes::Decimal(value.Data(), value.Size());
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

    template<typename TFrom, typename TTo>
    DataVector* CastKernel(
        const Expressions::Expression* self,
        const ExecutionContext* context,
        const DataChunk* chunk
    ){
        const auto* castExpr = self->AsCast();
        const auto* allocator = context->GetAllocator();

        const auto* childVector = castExpr->childExpr->vectorizedKernel(castExpr->childExpr, context, chunk);

        const auto* __restrict childData = childVector->template DataAs<TFrom>();

        if (childVector->IsConstant()){
            const auto isNull = childVector->GetNullValue(0);
            auto* outVector = DataVector::ConstantVector(allocator, isNull, DataTypes::DataTypeOf<TTo>());
            auto* outData = outVector->template DataAs<TTo>();
            if (!isNull)
                *outData = DataTypes::Coercions::To<TFrom, TTo>(*childData, allocator);
            return outVector;
        }

        auto* outVector = DataVector::FlatVector(allocator, DataTypes::DataTypeOf<TTo>(), chunk->_numberOfRows);
        auto* __restrict outData = outVector->template DataAs<TTo>();

        if constexpr (std::is_arithmetic_v<TFrom> && std::is_arithmetic_v<TTo>){
            if (childVector->IsFlat()){
                outVector->CopyValidity(childVector);

                for (Int i = 0;i < chunk->_numberOfRows; i++)
                    outData[i] = static_cast<TTo>(childData[i]);

                return outVector;
            }

            // dictionary path, arithmetic
            for (Int i = 0; i < chunk->_numberOfRows; i++){
                const auto childIndex = childVector->PhysicalIndex(i);
                outVector->SetNullValue(i, childVector->GetNullValue(childIndex));
            }

            for (Int i = 0; i < chunk->_numberOfRows; i++){
                const auto childIndex = childVector->PhysicalIndex(i);
                outData[i] = static_cast<TTo>(childData[childIndex]);
            }

            return outVector;
        }
        else{
            //fallback scalar evaluation
            if (childVector->IsFlat()){
                outVector->CopyValidity(childVector);

                for (Int i = 0;i < chunk->_numberOfRows; i++){
                    if (outVector->GetNullValue(i))
                        continue;

                    outData[i] = DataTypes::Coercions::To<TFrom, TTo>(childData[i], allocator);
                }

                return outVector;
            }

            //dictionary scalar fallback
            for (Int i = 0;i < chunk->_numberOfRows; i++){
                const auto childIndex = childVector->PhysicalIndex(i);
                const auto isNull = childVector->GetNullValue(childIndex);
                outVector->SetNullValue(i, isNull);
                if (isNull)
                    continue;

                outData[i] = DataTypes::Coercions::To<TFrom, TTo>(childData[childIndex], allocator);
            }

            return outVector;
        }
    }
}
