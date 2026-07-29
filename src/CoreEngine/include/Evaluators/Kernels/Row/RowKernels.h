#pragma once
#include "../../Expression.h"
#include "../../../Pages/PageView.h"
#include "../../Systemic/include/DataTypes/Variable.h"
#include "../../../Contexts/ExecutionContext.h"

namespace CoreEngine::RowKernels{
    template<typename T>
    void ColumnScanKernel(
        const Expressions::Expression* self,
        const Expressions::EvaluationContext& context,
        void* outVal,
        bool* outNull
    ){
        const auto* columnExpr = self->AsColumn();
        if constexpr (DataTypes::NonPrimitiveType<T>){
            UnsignedSmallInt size = 0;
            auto* data = context._pages[columnExpr->_slotIndex].GetColumnAt(
                context._rids[columnExpr->_slotIndex]._index,
                columnExpr->ordinalPosition, size, outNull
            );
            if (*outNull) return;

            if constexpr (DataTypes::IsString<T>)
                new (outVal) DataTypes::String(data, size, context._allocator);
            else if constexpr (DataTypes::IsJson<T>)
                new (outVal) DataTypes::JsonBinary(context._allocator, data, size);
            else if constexpr (DataTypes::IsDecimal<T>)
                new (outVal) DataTypes::Decimal(data, size);
        }
        else if constexpr (DataTypes::Primitive<T>) {
            *static_cast<T*>(outVal) =
                context._pages[columnExpr->_slotIndex].GetColumnAt<T>(
                        context._allocator,
                    context._rids[columnExpr->_slotIndex]._index,
                    columnExpr->ordinalPosition, outNull
                );
        }
        else
            static_assert(DataTypes::AlwaysFalse<T>, "ColumnScanKernel: unsupported type");
    }

    template<typename T>
    void ConstantScanKernel(
        const Expressions::Expression* self,
        const Expressions::EvaluationContext& context,
        void* outVal,
        bool* outNull
    ){
        const auto& value = self->AsConstant()->value;
        *outNull = value.IsNull();
        if (*outNull) return;

        if constexpr (DataTypes::Primitive<T>)
            *static_cast<T*>(outVal) = value.Get<T>();
        else if constexpr (DataTypes::IsString<T>)
            new (outVal) DataTypes::String(value.Data(), value.Size(), context._allocator);
        else if constexpr (DataTypes::IsJson<T>)
            new (outVal) DataTypes::JsonBinary(context._allocator, value.Data(), value.Size());
        else if constexpr (DataTypes::IsDecimal<T>)
            new (outVal) DataTypes::Decimal(value.Data(), value.Size());
        else
            static_assert(DataTypes::AlwaysFalse<T>, "ConstantScanKernel: unsupported type");
    }

    void ConstantScanNullKernel(
        const Expressions::Expression*,
        const Expressions::EvaluationContext&,
        void*,
        bool* outNull
    );

    template<typename T>
    void VariableScanKernel(
        const Expressions::Expression* self,
        const Expressions::EvaluationContext& context,
        void* outVal,
        bool* outNull
    ){
        const auto* variableExpr = self->AsVariable();
        const auto& value = context._executionContext->GetVariables()->Get(variableExpr->name).GetValue();
        *outNull = value.IsNull();
        if (*outNull) return;

        if constexpr (DataTypes::Primitive<T>)
            *static_cast<T*>(outVal) = value.Get<T>();
        else if constexpr (DataTypes::IsString<T>)
            new (outVal) DataTypes::String(value.Data(), value.Size(), context._allocator);
        else if constexpr (DataTypes::IsJson<T>)
            new (outVal) DataTypes::JsonBinary(context._allocator, value.Data(), value.Size());
        else if constexpr (DataTypes::IsDecimal<T>)
            new (outVal) DataTypes::Decimal(value.Data(), value.Size());
        else
            static_assert(DataTypes::AlwaysFalse<T>, "ConstantScanKernel: unsupported type");
    }

    void LogicalAndKernel(
        const Expressions::Expression* self,
        const Expressions::EvaluationContext& context,
        void* outVal,
        bool* outNull
    );

    void LogicalOrKernel(
        const Expressions::Expression* self,
        const Expressions::EvaluationContext& context,
        void* outVal,
        bool* outNull
    );

    void LogicalNotKernel(
        const Expressions::Expression* self,
        const Expressions::EvaluationContext& context,
        void* outVal,
        bool* outNull
    );

    template<typename T>
    Value KernelToValue(
        const Expressions::Expression* self,
        const Expressions::EvaluationContext& context
    ){
        bool outNull = false;
        T out;
        self->rowKernel(self, context, &out, &outNull);
        if (outNull)
            return Value::Null(context._allocator);

        return Value(out, context._allocator);
    }
}
