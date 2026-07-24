#pragma once
#include "../../../Evaluators/Expression.h"
#include "../../../Contexts/ExecutionContext.h"
#include "../../../Vectorization/Vectorization.h"
#include "../../../../../Systemic/include/DataTypes/StringValue.h"
#include <functional>


namespace CoreEngine::VectorizedKernels{
    template<typename T, typename Op>
    DataVector* BinaryArithmeticKernel(
        const Expressions::Expression* self,
        const ExecutionContext* context,
        const DataChunk* chunk
    ){
        const auto* binaryExpr = self->AsBinary();

        const auto* left = binaryExpr->left->vectorizedKernel(binaryExpr->left, context, chunk);
        const auto* right = binaryExpr->right->vectorizedKernel(binaryExpr->right, context, chunk);

        const auto chunkSize = chunk->_numberOfRows;
        const auto* allocator = context->GetAllocator();

        auto* out = DataVector::FlatVector(allocator, DataTypes::DataTypeOf<T>(), chunkSize);

        for (Int i = 0;i < chunkSize; i++){
            const auto leftIndex = left->PhysicalIndex(i);
            const auto rightIndex = right->PhysicalIndex(i);

            const auto isNull = left->GetNullValue(leftIndex) || right->GetNullValue(rightIndex);
            out->SetNullValue(i, isNull);
            if (isNull)
                continue;

            if constexpr (DataTypes::Primitive<T>)
                *out->template SlotAt<T>(i) = Op{}(*left->template SlotAt<T>(leftIndex), *right->template SlotAt<T>(rightIndex));
            else if constexpr (DataTypes::IsStringValue<T>){
                auto concatStr = DataTypes::String::Concat(allocator, *left->template SlotAt<T>(leftIndex), *right->template SlotAt<T>(rightIndex));
                *out->template SlotAt<T>(i) = std::move(
                    DataTypes::StringValue::Create(
                        allocator,
                        concatStr.Data(),
                        concatStr.Size()
                ));
            }
            else if constexpr (DataTypes::IsDecimal<T>)
                *out->template SlotAt<T>(i) = std::move(Op{}(*left->template SlotAt<T>(leftIndex), *right->template SlotAt<T>(rightIndex)));
            else
                static_assert(DataTypes::AlwaysFalse<T>, "BinaryArithmeticKernel: unsupported type");
        }

        return out;
    }

    template<typename T>
    DataVector* BinaryDivideKernel(
        const Expressions::Expression* self,
        const ExecutionContext* context,
        const DataChunk* chunk
    ){
        const auto* binaryExpr = self->AsBinary();

        auto* left = binaryExpr->left->vectorizedKernel(binaryExpr->left, context, chunk);
        auto* right = binaryExpr->right->vectorizedKernel(binaryExpr->right, context, chunk);

        const auto chunkSize = chunk->_numberOfRows;
        auto* out = DataVector::FlatVector(context->GetAllocator(), DataTypes::DataTypeOf<T>(), chunkSize);

        for (Int i = 0;i < chunkSize; i++){
            const auto leftIndex = left->PhysicalIndex(i);
            const auto rightIndex = right->PhysicalIndex(i);

            const auto isNull = left->GetNullValue(leftIndex) || right->GetNullValue(rightIndex);
            out->SetNullValue(i, isNull);
            if (isNull)
                continue;

            if constexpr (DataTypes::Primitive<T>)
                *out->template SlotAt<T>(i) = *left->template SlotAt<T>(leftIndex) / *right->template SlotAt<T>(rightIndex);
            else if constexpr (DataTypes::IsDecimal<T>)
                *out->template SlotAt<T>(i) = std::move(*left->template SlotAt<T>(leftIndex) / *right->template SlotAt<T>(rightIndex));
            else
                static_assert(DataTypes::AlwaysFalse<T>, "BinaryDivideKernel: unsupported type");
        }

        return out;
    }

    template<typename T>
    DataVector* BinaryModuloKernel(
        const Expressions::Expression* self,
        const ExecutionContext* context,
        const DataChunk* chunk
    ){
        const auto* binaryExpr = self->AsBinary();

        auto* left = binaryExpr->left->vectorizedKernel(binaryExpr->left, context, chunk);
        auto* right = binaryExpr->right->vectorizedKernel(binaryExpr->right, context, chunk);

        const auto chunkSize = chunk->_numberOfRows;
        auto* out = DataVector::FlatVector(context->GetAllocator(), DataTypes::DataTypeOf<T>(), chunkSize);

        for (Int i = 0;i < chunkSize; i++){
            const auto leftIndex = left->PhysicalIndex(i);
            const auto rightIndex = right->PhysicalIndex(i);

            const auto isNull = left->GetNullValue(leftIndex) || right->GetNullValue(rightIndex);
            out->SetNullValue(i, isNull);
            if (isNull)
                continue;

            if constexpr (DataTypes::Primitive<T>)
                *out->template SlotAt<T>(i) = *left->template SlotAt<T>(leftIndex) % *right->template SlotAt<T>(rightIndex);
            else if constexpr (DataTypes::IsDecimal<T>)
                *out->template SlotAt<T>(i) = std::move(*left->template SlotAt<T>(leftIndex) % *right->template SlotAt<T>(rightIndex));
            else
                static_assert(DataTypes::AlwaysFalse<T>, "BinaryDivideKernel: unsupported type");
        }

        return out;
    }

    // Comparison: result is Bool. A NULL operand makes the result UNKNOWN (NULL),
    // NOT false -- the consuming filter is what collapses UNKNOWN to "reject".
    template<typename T, typename Comparison>
    DataVector* BinaryComparisonKernel(
        const Expressions::Expression* self,
        const ExecutionContext* context,
        const DataChunk* chunk
    ){
        const auto* binaryExpr = self->AsBinary();

        auto* left = binaryExpr->left->vectorizedKernel(binaryExpr->left, context, chunk);
        auto* right = binaryExpr->right->vectorizedKernel(binaryExpr->right, context, chunk);

        const auto chunkSize = chunk->_numberOfRows;
        // A comparison always yields Bool; T is the operand type, not the result type.
        auto* out = DataVector::FlatVector(context->GetAllocator(), DataType::Bool, chunkSize);

        for (Int i = 0;i < chunkSize; i++){
            const auto leftIndex = left->PhysicalIndex(i);
            const auto rightIndex = right->PhysicalIndex(i);

            const auto isNull = left->GetNullValue(leftIndex) || right->GetNullValue(rightIndex);
            out->SetNullValue(i, isNull);
            if (isNull)
                continue;

            *out->template SlotAt<bool>(i) = Comparison{}(*left->template SlotAt<T>(leftIndex), *right->template SlotAt<T>(rightIndex));
        }

        return out;
    }
}
