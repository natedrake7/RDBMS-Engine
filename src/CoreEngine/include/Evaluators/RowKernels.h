#pragma once
#include <type_traits>

#include "Expression.h"
#include "../Pages/PageView.h"

namespace CoreEngine::RowKernels{
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
}
