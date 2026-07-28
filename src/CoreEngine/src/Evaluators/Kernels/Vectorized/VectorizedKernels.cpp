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

        auto* out = DataVector::FlatVector(context->GetAllocator(), DataType::Bool, chunk->_numberOfRows);

        for (Int i = 0; i < chunk->_numberOfRows; i++){
            const auto leftIndex = left->PhysicalIndex(i);
            const auto rightIndex = right->PhysicalIndex(i);

            const auto leftNull = left->GetNullValue(leftIndex);
            const auto rightNull = right->GetNullValue(rightIndex);

            const auto leftVal = !leftNull && *left->template SlotAt<bool>(leftIndex);
            const auto rightVal = !rightNull && *right->template SlotAt<bool>(rightIndex);

            const auto value  = leftVal && rightVal;
            const auto isNull = static_cast<bool>(
                (leftNull && rightNull)
                | (leftNull && rightVal)
                | (rightNull && leftVal)
            );

            out->SetNullValue(i, isNull);
            *out->template SlotAt<bool>(i) = value;
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

        auto* out = DataVector::FlatVector(context->GetAllocator(), DataType::Bool, chunk->_numberOfRows);

        for (Int i = 0; i < chunk->_numberOfRows; i++){
            const auto leftIndex = left->PhysicalIndex(i);
            const auto rightIndex = right->PhysicalIndex(i);

            const auto leftNull = left->GetNullValue(leftIndex);
            const auto rightNull = right->GetNullValue(rightIndex);

            const auto leftVal = !leftNull && *left->template SlotAt<bool>(leftIndex);
            const auto rightVal = !rightNull && *right->template SlotAt<bool>(rightIndex);

            const auto value  = leftVal || rightVal;
            const auto isNull = static_cast<bool>(
                (leftNull && rightNull)
                | (leftNull && !rightVal)
                | (rightNull && !leftVal)
            );

            out->SetNullValue(i, isNull);
            *out->template SlotAt<bool>(i) = value;
        }

        return out;
    }
}
