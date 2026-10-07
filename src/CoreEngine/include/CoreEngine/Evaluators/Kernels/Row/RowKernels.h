#pragma once
#include <CoreEngine/Evaluators/Expression.h>
#include <CoreEngine/Pages/PageView.h>
#include <CoreEngine/Contexts/ExecutionContext.h>
#include <Systemic/DataTypes/StringValue.h>
#include <Systemic/DataTypes/BoundVariable.h>

namespace CoreEngine::RowKernels{
    template<typename T>
    void ColumnScanKernel(
        const Expressions::Expression* self,
        const Expressions::EvaluationContext& context,
        void* outVal,
        bool* outNull
    ){
        const auto* columnExpr = self->As<Expressions::ColumnExpression>();
        if constexpr (DataTypes::NonPrimitiveType<T>){
            UnsignedSmallInt size = 0;
            auto* data = context._pages[columnExpr->_slotIndex].GetColumnAt(
                context._rids[columnExpr->_slotIndex]._index,
                columnExpr->ordinalPosition, size, outNull
            );
            if (*outNull) return;

            if constexpr (DataTypes::IsStringValue<T>)
                *static_cast<T*>(outVal) = std::move(DataTypes::StringValue::Create(context._allocator, reinterpret_cast<const char*>(data), size));
            else if constexpr (DataTypes::IsJson<T>)
                *static_cast<T*>(outVal) = std::move(DataTypes::JsonBinary(context._allocator, data, size));
            else if constexpr (DataTypes::IsDecimal<T>)
                *static_cast<T*>(outVal) = std::move(DataTypes::Decimal(data, size));
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
        const auto& value = self->As<Expressions::ConstantExpression>()->value;
        *outNull = value.IsNull();
        if (*outNull) return;

        if constexpr (DataTypes::Primitive<T>)
            *static_cast<T*>(outVal) = value.Get<T>(context._allocator);
        else if constexpr (DataTypes::IsStringValue<T>)
            *static_cast<T*>(outVal) = std::move(DataTypes::StringValue::Create(context._allocator, reinterpret_cast<const char*>(value.Data()), value.Size()));
        else if constexpr (DataTypes::IsJson<T>)
            *static_cast<T*>(outVal) = std::move(DataTypes::JsonBinary(context._allocator, value.Data(), value.Size()));
        else if constexpr (DataTypes::IsDecimal<T>)
            *static_cast<T*>(outVal) = std::move(value.AsDecimal());
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
        const auto* variableExpr = self->As<Expressions::VariableExpression>();
        const auto& value = context._executionContext->GetVariable(DataTypes::StringView::ViewOf(variableExpr->normalizedName))->GetValue();
        *outNull = value.IsNull();
        if (*outNull) return;

        if constexpr (DataTypes::Primitive<T>)
            *static_cast<T*>(outVal) = value.Get<T>(context._allocator);
        else if constexpr (DataTypes::IsStringValue<T>)
            *static_cast<T*>(outVal) = std::move(DataTypes::StringValue::Create(context._allocator, reinterpret_cast<const char*>(value.Data()), value.Size()));
        else if constexpr (DataTypes::IsJson<T>)
            *static_cast<T*>(outVal) = std::move(DataTypes::JsonBinary(context._allocator, value.Data(), value.Size()));
        else if constexpr (DataTypes::IsDecimal<T>)
            *static_cast<T*>(outVal) = std::move(DataTypes::Decimal(value.Data(), value.Size()));
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
            return Value::Null();

        if constexpr(DataTypes::TriviallyCopiable<T>)
            return Value(out);
        else
            return Value(out, context._allocator);
    }
}
