#include "../../../../include/Evaluators/Kernels/Row/RowKernels.h"
#include "Evaluators/Kernels/Row/RowKernels.Binary.h"

namespace CoreEngine::RowKernels{
    void ConstantScanNullKernel(
        const Expressions::Expression*,
        const Expressions::EvaluationContext&,
        void*,
        bool* outNull
    ){
        *outNull = true;
    }

    void LogicalAndKernel(
        const Expressions::Expression* self,
        const Expressions::EvaluationContext& context,
        void* outVal,
        bool* outNull
    ){
        const auto* logicalExpr = self->AsLogical();

        auto left = false, leftNull = false;
        logicalExpr->left->rowKernel(logicalExpr->left, context, &left, &leftNull);

        if (!leftNull && !left) {
            *static_cast<bool*>(outVal) = false;
            *outNull = false;
            return;
        }

        auto right = false, rightNull = false;
        logicalExpr->right->rowKernel(logicalExpr->right, context, &right, &rightNull);

        if (!rightNull && !right) {
            *static_cast<bool*>(outVal) = false;
            *outNull = false;
            return;
        }

        if (leftNull || rightNull){
            *outNull = true;
            return;
        }

        *static_cast<bool*>(outVal) = true; *outNull = false;
    }

    void LogicalOrKernel(
        const Expressions::Expression* self,
        const Expressions::EvaluationContext& context,
        void* outVal,
        bool* outNull
    ){
        const auto* logicalExpr = self->AsLogical();

        auto left = false, leftNull = false;
        logicalExpr->left->rowKernel(logicalExpr->left, context, &left, &leftNull);

        if (!leftNull && left) {
            *static_cast<bool*>(outVal) = true;
            *outNull = false;
            return;
        }

        auto right = false, rightNull = false;
        logicalExpr->right->rowKernel(logicalExpr->right, context, &right, &rightNull);

        if (!rightNull && right) {
            *static_cast<bool*>(outVal) = true;
            *outNull = false;
            return;
        }

        if (leftNull || rightNull){
            *outNull = true;
            return;
        }

        *static_cast<bool*>(outVal) = false; *outNull = false;
    }
}
