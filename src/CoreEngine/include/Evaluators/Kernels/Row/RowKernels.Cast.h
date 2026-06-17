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
        castExpr->childExpr->rowKernel(castExpr->childExpr, context, &from, outNull);
        if (*outNull) return;

        auto result = DataTypes::Coercions::To<TFrom, TTo>(from, context.allocator);

        if constexpr (DataTypes::Primitive<TTo>)
            *static_cast<TTo*>(outVal) = result;
        else
            new (outVal) TTo(std::move(result));
    }

    struct CastKernelTable{
        Expressions::RowKernelFunction cells[DATATYPE_COUNT][DATATYPE_COUNT];
    };

    [[nodiscard]] constexpr CastKernelTable RegisterCastKernels();
    [[nodiscard]] Expressions::RowKernelFunction LookupCastKernel(DataType fromType, DataType toType);
}
