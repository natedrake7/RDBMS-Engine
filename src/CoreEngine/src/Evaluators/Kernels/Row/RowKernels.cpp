#include "../../../../include/Evaluators/Kernels/Row/RowKernels.h"
#include "DataTypes/JsonBinary.h"
#include "Evaluators/Kernels/Row/RowKernels.Binary.h"

namespace CoreEngine::RowKernels{
    void RegisterKernels(){
        RegisterBinaryKernels();
    }

    void StringColumnScanKernel(
        const Expressions::Expression* self,
        const Expressions::EvaluationContext& context,
        void* outVal,
        bool* outNull
    ){
        auto* columnExpr = self->AsColumn();
        UnsignedSmallInt size = 0;
        auto* value = context.page.GetColumnAt(
            context.row->_index,
            columnExpr->columnIndex,
            size,
            outNull
        );
        if (*outNull)
            return;

        new (outVal) DataTypes::String(value, size, context.allocator);  // construct in place
    }

    void JsonColumnScanKernel(
        const Expressions::Expression* self,
        const Expressions::EvaluationContext& context,
        void* outVal,
        bool* outNull
    ){
        auto* columnExpr = self->AsColumn();
        UnsignedSmallInt size = 0;
        auto* value = context.page.GetColumnAt(
            context.row->_index,
            columnExpr->columnIndex,
            size,
            outNull
        );
        if (*outNull)
            return;

        new (outVal) DataTypes::JsonBinary(context.allocator, value, size);  // construct in place
    }

    void ConstantStringScanKernel(
        const Expressions::Expression* self,
        const Expressions::EvaluationContext& context,
        void* outVal,
        bool* outNull
    ){
        const auto& value = self->AsConstant()->value;
        *outNull = value.IsNull();
        if (*outNull) return;
        new (outVal) DataTypes::String(value.Data(), value.Size(), context.allocator);
    }

    void ConstantJsonScanKernel(
        const Expressions::Expression* self,
        const Expressions::EvaluationContext& context,
        void* outVal,
        bool* outNull
    ){
        const auto& value = self->AsConstant()->value;
        *outNull = value.IsNull();
        if (*outNull) return;
        new (outVal) DataTypes::JsonBinary(context.allocator, value.Data(), value.Size());  // construct in place
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
