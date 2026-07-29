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

        const auto* leftData = left->DataAs<T>();
        const auto* rightData = right->DataAs<T>();

        const auto lConst = left->IsConstant();
        const auto lFlat = left->IsFlat();

        const auto rConst = right->IsConstant();
        const auto rFlat = right->IsFlat();

        if (lConst && rConst){
            const auto isNull = left->GetNullValue(0) || right->GetNullValue(0);
            auto* out = DataVector::ConstantVector(allocator, isNull, DataTypes::DataTypeOf<T>());
            auto* outData = out->template DataAs<T>();

            if (!isNull){
                if constexpr (
                    DataTypes::Primitive<T>
                    || DataTypes::IsDecimal<T>
                ) DataVector::ConstantAccess(outData) = Op{}(DataVector::ConstantAccess(leftData), DataVector::ConstantAccess(rightData));
                else if constexpr (DataTypes::IsStringValue<T>){
                    auto concatStr = DataTypes::String::Concat(allocator, DataVector::ConstantAccess(leftData), DataVector::ConstantAccess(rightData));
                    DataVector::ConstantAccess(outData) = DataTypes::StringValue::Create(
                        allocator,
                        concatStr.Data(),
                        concatStr.Size()
                    );
                }
            }

            return out;
        }

        auto* out = DataVector::FlatVector(allocator, DataTypes::DataTypeOf<T>(), chunkSize);
        auto* outData = out->template DataAs<T>();

        const auto ArithmeticOp = [out, outData, allocator](const T& leftValue, const T& rightValue, const Int index) -> void{
            if constexpr (DataTypes::Primitive<T>)
                DataVector::FlatAccess(outData, index) = Op{}(leftValue, rightValue);
            else if constexpr (DataTypes::IsStringValue<T>){
                if (out->GetNullValue(index))
                    return;

                auto concatStr = DataTypes::String::Concat(allocator, leftValue, rightValue);
                DataVector::FlatAccess(outData, index) = DataTypes::StringValue::Create(
                    allocator,
                    concatStr.Data(),
                    concatStr.Size()
                );
            }
            else if constexpr (DataTypes::IsDecimal<T>){
                if (out->GetNullValue(index))
                    return;

                DataVector::FlatAccess(outData, index) = Op{}(leftValue, rightValue);
            }
            else
                static_assert(DataTypes::AlwaysFalse<T>, "BinaryArithmeticKernel: unsupported type");
        };

        if (lFlat && rConst){
            if (right->GetNullValue(0)){
                out->SetAllNull();
                return out;
            }
            out->CopyValidity(left);
            for (Int i = 0;i < chunkSize; i++)
                ArithmeticOp(DataVector::FlatAccess(leftData, i), DataVector::ConstantAccess(rightData), i);

            return out;
        }

        if (lConst && rFlat){
            if (left->GetNullValue(0)){
                out->SetAllNull();
                return out;
            }
            out->CopyValidity(right);

            for (Int i = 0;i < chunkSize; i++)
                ArithmeticOp(DataVector::ConstantAccess(leftData), DataVector::FlatAccess(rightData, i), i);

            return out;
        }

        if (lFlat && rFlat){
            out->OrValidity(left, right);

            for (Int i = 0;i < chunkSize; i++)
                ArithmeticOp(DataVector::FlatAccess(leftData, i), DataVector::FlatAccess(rightData, i), i);
            return out;
        }

        for (Int i = 0;i < chunkSize; i++){
            const auto leftIndex = left->PhysicalIndex(i);
            const auto rightIndex = right->PhysicalIndex(i);

            const auto isNull = left->GetNullValue(leftIndex) || right->GetNullValue(rightIndex);
            out->SetNullValue(i, isNull);
            if (isNull)
                continue;

            ArithmeticOp(DataVector::FlatAccess(leftData, leftIndex), DataVector::FlatAccess(rightData, rightIndex), i);
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
        const auto* allocator = context->GetAllocator();

        const auto* leftData = left->DataAs<T>();
        const auto* rightData = right->DataAs<T>();

        auto* out = DataVector::FlatVector(context->GetAllocator(), DataTypes::DataTypeOf<T>(), chunkSize);
        auto* outData = out->template DataAs<T>();

        for (Int i = 0;i < chunkSize; i++){
            const auto leftIndex = left->PhysicalIndex(i);
            const auto rightIndex = right->PhysicalIndex(i);

            const auto isNull = left->GetNullValue(leftIndex) || right->GetNullValue(rightIndex);
            out->SetNullValue(i, isNull);
            if (isNull)
                continue;

            if constexpr (DataTypes::Primitive<T>)
                DataVector::FlatAccess(outData, i) = leftData[leftIndex] / rightData[rightIndex];
            else if constexpr (DataTypes::IsDecimal<T>)
                DataVector::FlatAccess(outData, i) = leftData[leftIndex] / rightData[rightIndex];
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

        const auto* left = binaryExpr->left->vectorizedKernel(binaryExpr->left, context, chunk);
        const auto* right = binaryExpr->right->vectorizedKernel(binaryExpr->right, context, chunk);

        const auto chunkSize = chunk->_numberOfRows;
        const auto* allocator = context->GetAllocator();

        const auto* leftData = left->DataAs<T>();
        const auto* rightData = right->DataAs<T>();

        const auto lConst = left->IsConstant();
        const auto lFlat = left->IsFlat();

        const auto rConst = right->IsConstant();
        const auto rFlat = right->IsFlat();

        if (lConst && rConst){
            const auto isNull = left->GetNullValue(0) || right->GetNullValue(0);
            auto* out = DataVector::ConstantVector(allocator, isNull, DataType::Bool);

            auto* outData = out->template DataAs<bool>();

            if (!isNull)
                DataVector::ConstantAccess(outData) = Comparison{}(DataVector::ConstantAccess(leftData), DataVector::ConstantAccess(rightData));

            return out;
        }

        auto* out = DataVector::FlatVector(allocator, DataType::Bool, chunkSize);
        auto* outData = out->template DataAs<bool>();

        if (lFlat && rConst){
            if (right->GetNullValue(0)){
                out->SetAllNull();
                return out;
            }
            out->CopyValidity(left);

            for (Int i = 0;i < chunkSize; i++)
                DataVector::FlatAccess(outData, i) = Comparison{}(DataVector::FlatAccess(leftData, i), DataVector::ConstantAccess(rightData));

            return out;
        }

        if (lConst && rFlat){
            if (left->GetNullValue(0)){
                out->SetAllNull();
                return out;
            }
            out->CopyValidity(right);
            for (Int i = 0;i < chunkSize; i++)
                DataVector::FlatAccess(outData, i) = Comparison{}(DataVector::ConstantAccess(leftData), DataVector::FlatAccess(rightData, i));

            return out;
        }

        if (lFlat && rFlat){
            out->OrValidity(left, right);
            for (Int i = 0;i < chunkSize; i++)
                DataVector::FlatAccess(outData, i) = Comparison{}(DataVector::FlatAccess(leftData, i), DataVector::FlatAccess(rightData, i));
            return out;
        }

        for (Int i = 0;i < chunkSize; i++){
            const auto leftIndex = left->PhysicalIndex(i);
            const auto rightIndex = right->PhysicalIndex(i);

            const auto isNull = left->GetNullValue(leftIndex) || right->GetNullValue(rightIndex);
            out->SetNullValue(i, isNull);
            if (isNull)
                continue;

            DataVector::FlatAccess(outData, i) = Comparison{}(DataVector::FlatAccess(leftData, leftIndex), DataVector::FlatAccess(rightData, rightIndex));
        }

        return out;
    }
}
