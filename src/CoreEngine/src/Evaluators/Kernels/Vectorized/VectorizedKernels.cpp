#include "../../../../include/Evaluators/Kernels/Vectorized/VectorizedKernels.h"
#include "../../../../include/Evaluators/Expression.h"
#include "Contexts/ExecutionContext.h"

namespace CoreEngine::VectorizedKernels{
    DataVector* ColumnScanKernel(
        const Expressions::Expression* self,
        const ExecutionContext*,
        const DataChunk* chunk
    ){
        const auto* columnExpression = self->AsColumn();
        return chunk->_columns[columnExpression->_boundReference._position];
    }

    DataVector* LogicalAndKernel(
        const Expressions::Expression* self,
        const ExecutionContext* context,
        const DataChunk* chunk
    ){
        const auto* logicalExpr = self->AsLogical();
        const auto* left = logicalExpr->left->vectorizedKernel(logicalExpr->left, context, chunk);
        const auto* right = logicalExpr->right->vectorizedKernel(logicalExpr->right, context, chunk);

        const auto* leftData = left->template DataAs<bool>();
        const auto* rightData = right->template DataAs<bool>();

        auto* out = DataVector::FlatVector(context->GetAllocator(), DataType::Bool, chunk->_numberOfRows);
        auto* outData = out->template DataAs<bool>();

        for (Int i = 0; i < chunk->_numberOfRows; i++){
            const auto leftIndex = left->PhysicalIndex(i);
            const auto rightIndex = right->PhysicalIndex(i);

            const auto leftNull = left->GetNullValue(leftIndex);
            const auto rightNull = right->GetNullValue(rightIndex);

            const auto leftVal = !leftNull && leftData[leftIndex];
            const auto rightVal = !rightNull && rightData[rightIndex];

            const auto isNull = static_cast<bool>(
                (leftNull && rightNull)
                | (leftNull && rightVal)
                | (rightNull && leftVal)
            );

            out->SetNullValue(i, isNull);
            outData[i] = leftVal && rightVal;
        }

        return out;
    }

    DataVector* LogicalOrKernel(
        const Expressions::Expression* self,
        const ExecutionContext* context,
        const DataChunk* chunk
    ){
        const auto* logicalExpr = self->AsLogical();
        const auto* left = logicalExpr->left->vectorizedKernel(logicalExpr->left, context, chunk);
        const auto* right = logicalExpr->right->vectorizedKernel(logicalExpr->right, context, chunk);

        const auto* leftData = left->DataAs<bool>();
        const auto* rightData = right->DataAs<bool>();

        auto* out = DataVector::FlatVector(context->GetAllocator(), DataType::Bool, chunk->_numberOfRows);
        auto* outData = out->template DataAs<bool>();

        for (Int i = 0; i < chunk->_numberOfRows; i++){
            const auto leftIndex = left->PhysicalIndex(i);
            const auto rightIndex = right->PhysicalIndex(i);

            const auto leftNull = left->GetNullValue(leftIndex);
            const auto rightNull = right->GetNullValue(rightIndex);

            const auto leftVal = !leftNull && leftData[leftIndex];
            const auto rightVal = !rightNull && rightData[rightIndex];

            const auto isNull = static_cast<bool>(
                (leftNull && rightNull)
                | (leftNull && !rightVal)
                | (rightNull && !leftVal)
            );

            out->SetNullValue(i, isNull);
            outData[i] = leftVal || rightVal;
        }

        return out;
    }

    DataVector* LogicalNotKernel(
        const Expressions::Expression* self,
        const ExecutionContext* context,
        const DataChunk* chunk
    ){
        const auto* logicalExpr = self->AsLogical();

        const auto* left = logicalExpr->left->vectorizedKernel(logicalExpr->left, context, chunk);
        const auto* leftData = left->DataAs<bool>();

        auto* out = DataVector::FlatVector(context->GetAllocator(), DataType::Bool, chunk->_numberOfRows);
        auto* outData = out->template DataAs<bool>();

        for (Int i = 0; i < chunk->_numberOfRows; i++){
            const auto index = left->PhysicalIndex(i);
            const auto isNull = left->GetNullValue(index);
            out->SetNullValue(i, isNull);
            outData[i] = !isNull && !leftData[index];
        }

        return out;
    }
}
