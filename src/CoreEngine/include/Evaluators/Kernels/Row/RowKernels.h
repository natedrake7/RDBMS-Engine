#pragma once
#include <type_traits>
#include "../../Expression.h"
#include "../../../Pages/PageView.h"

namespace CoreEngine::RowKernels{
    void RegisterKernels();

    template<typename T>
    concept PrimitiveColumn = std::is_trivially_copyable_v<T>
                           && !std::is_pointer_v<T>;

    template<PrimitiveColumn T>
    void PrimitiveColumnScanKernel(
        const Expressions::Expression* self,
        const Expressions::EvaluationContext& context,
        void* outVal,
        bool* outNull
    ){
        auto* columnExpr = self->AsColumn();
        *static_cast<T*>(outVal) = context.page.GetColumnAt<T>(
            context.row->_index,
            columnExpr->columnIndex,
            outNull
        );
    }

    void StringColumnScanKernel(
        const Expressions::Expression* self,
        const Expressions::EvaluationContext& context,
        void* outVal,
        bool* outNull
    );
    void JsonColumnScanKernel(
        const Expressions::Expression* self,
        const Expressions::EvaluationContext& context,
        void* outVal,
        bool* outNull
    );


    template<PrimitiveColumn T>
    void ConstantScanKernel(
        const Expressions::Expression* self,
        const Expressions::EvaluationContext& context,
        void* outVal,
        bool* outNull
    ){
        const auto& value = self->AsConstant()->value;
        *outNull = value.IsNull();
        if (*outNull)
            return;

        *static_cast<T*>(outVal) = value.Get<T>();
    }

    void ConstantStringScanKernel(
        const Expressions::Expression* self,
        const Expressions::EvaluationContext& context,
        void* outVal,
        bool* outNull
    );

    void ConstantJsonScanKernel(
        const Expressions::Expression* self,
        const Expressions::EvaluationContext& context,
        void* outVal,
        bool* outNull
    );
}
