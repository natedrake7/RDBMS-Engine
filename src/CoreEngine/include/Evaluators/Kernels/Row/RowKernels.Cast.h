#pragma once
#include "Coercions/Coercions.h"
#include "RowKernels.h"

namespace CoreEngine::RowKernels{
    template<typename TFrom, typename TTo>
    void CastRowKernel(
        const Expressions::Expression* self,
        const Expressions::EvaluationContext& context,
        void* outVal,
        bool* outNull
    ){
        const auto* castExpr = self->AsCast();
        TFrom from;
        bool childNull = false;
        castExpr->childExpr->rowKernel(castExpr->childExpr, context, &from, &childNull);
        if (childNull){
            *outNull = true;
            return;
        }

        auto result = DataTypes::Coercions::To<TFrom, TTo>(from, context._allocator);

        if constexpr (DataTypes::Primitive<TTo>)
            *static_cast<TTo*>(outVal) = result;
        else
            *static_cast<TTo*>(outVal) = std::move(result);
        *outNull = false;
    }

    struct CastKernelTable{
        Expressions::RowKernelFunction cells[DATATYPE_COUNT][DATATYPE_COUNT];
    };

    [[nodiscard]] constexpr CastKernelTable RegisterCastKernels();
    [[nodiscard]] Expressions::RowKernelFunction LookupCastKernel(DataType fromType, DataType toType);
}
