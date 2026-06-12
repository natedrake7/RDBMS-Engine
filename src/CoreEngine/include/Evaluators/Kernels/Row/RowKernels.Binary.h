#pragma once
#include "../../Expressions.Additional.h"
#include "../../../../../Systemic/include/DataTypes/DataTypes.h"
#include "../../Expression.h"
#include "RowKernels.h"

namespace CoreEngine::RowKernels{
    // ---------------------------------------------------------------------------
    // Binary kernels
    // ---------------------------------------------------------------------------
    // Precondition (established by the binder): both operands have already been
    // homogenized to the SAME type T -- CAST nodes were inserted during semantic
    // analysis -- so each kernel is monomorphic in T with zero per-row dispatch.

    template<PrimitiveColumn T, typename Op>
    void BinaryArithmeticKernel(
        const Expressions::Expression* self,
        const Expressions::EvaluationContext& context,
        void* outVal,
        bool* outNull
    ){
        const auto* binaryExpr = self->AsBinary();
        bool leftNull = false, rightNull = false;
        T left, right;

        Expressions::EvaluateExpression(binaryExpr->left, context, &left, &leftNull);
        Expressions::EvaluateExpression(binaryExpr->right, context, &right, &rightNull);

        if (leftNull || rightNull){
            *outNull = true;
            return;
        }   // strict propagation

        *static_cast<T*>(outVal) = Op{}(left, right);
        *outNull = false;
    }

    // Divide / Modulo: divide-by-zero yields NULL (swap for a raised error if your
    // semantics demand it). Modulo is only registered for integral T.
    template<PrimitiveColumn T>
    void BinaryDivideKernel(
        const Expressions::Expression* self,
        const Expressions::EvaluationContext& context,
        void* outVal,
        bool* outNull
    ){
        const auto* binaryExpr = self->AsBinary();
        bool leftNull = false, rightNull = false;
        T left, right;

        Expressions::EvaluateExpression(binaryExpr->left, context, &left, &leftNull);
        Expressions::EvaluateExpression(binaryExpr->right, context, &right, &rightNull);

        if (leftNull || rightNull || right == T{}){
            *outNull = true;
            return;
        }

        *static_cast<T*>(outVal) = left / right;
        *outNull = false;
    }

    template<PrimitiveColumn T>
    void BinaryModuloKernel(
        const Expressions::Expression* self,
        const Expressions::EvaluationContext& context,
        void* outVal,
        bool* outNull
    ){
        const auto* binaryExpr = self->AsBinary();
        bool leftNull = false, rightNull = false;
        T left, right;

        Expressions::EvaluateExpression(binaryExpr->left, context, &left, &leftNull);
        Expressions::EvaluateExpression(binaryExpr->right, context, &right, &rightNull);

        if (leftNull || rightNull || right == T{}){
            *outNull = true;
            return;
        }

        *static_cast<T*>(outVal) = left % right;
        *outNull = false;
    }

    // Comparison: result is Bool. A NULL operand makes the result UNKNOWN (NULL),
    // NOT false -- the consuming filter is what collapses UNKNOWN to "reject".
    template<typename T, typename Comparison>
    void BinaryComparisonKernel(
        const Expressions::Expression* self,
        const Expressions::EvaluationContext& context,
        void* outVal,
        bool* outNull
    ){
        const auto* binaryExpr = self->AsBinary();

        T left, right;
        bool leftNull = false, rightNull = false;

        Expressions::EvaluateExpression(binaryExpr->left, context, &left, &leftNull);
        Expressions::EvaluateExpression(binaryExpr->right, context, &right, &rightNull);

        if (leftNull || rightNull){
            *outNull = true;
            return;
        }

        *static_cast<bool*>(outVal) = Comparison{}(left, right);
        *outNull = false;
    }

    void BinaryStringAdditionKernel(
        const Expressions::Expression* self,
        const Expressions::EvaluationContext& context,
        void* outVal,
        bool* outNull
    );

    // ---------------------------------------------------------------------------
    // Dispatch table: [operator][promoted operand type] -> monomorphic kernel.
    // Populated once at startup by RegisterRowBinaryKernels(); looked up once per
    // node at bind time. A nullptr slot means "op invalid for this type" -- the
    // semantic pass must have rejected it before binding.
    // ---------------------------------------------------------------------------
    static constexpr Int BINARY_OPERATIONS_COUNT = 12;   // Expressions::BinaryOperator enumerator count

    // extern + single definition in RowKernels.cpp: one shared table across all TUs.
    // (A `static` here would give every TU its own zero-filled copy, and only
    //  RowKernels.cpp's copy gets populated -> nullptr lookups everywhere else.)
    extern Expressions::RowKernelFunction RowBinaryKernelTable[BINARY_OPERATIONS_COUNT][DATATYPE_COUNT];

    inline Expressions::RowKernelFunction LookupBinaryKernel(
        const Expressions::BinaryOperator op,
        const DataType operandType
    ){
        return RowBinaryKernelTable[static_cast<Int>(op)][static_cast<Int>(operandType)];
    }

    void RegisterBinaryKernels();

    void RegisterBool();
    void RegisterString();
    void RegisterDateTime();
    void RegisterDecimal();
    void RegisterJson();
    void RegisterGuid();

    template<typename T>
    void RegisterIntegral(const DataType dataType){
        const auto castType = static_cast<Int>(dataType);

        RowBinaryKernelTable[static_cast<Int>(Expressions::BinaryOperator::Add)][castType] = &BinaryArithmeticKernel<T, std::plus<T>>;
        RowBinaryKernelTable[static_cast<Int>(Expressions::BinaryOperator::Subtract)][castType] = &BinaryArithmeticKernel<T, std::minus<T>>;
        RowBinaryKernelTable[static_cast<Int>(Expressions::BinaryOperator::Multiply)][castType] = &BinaryArithmeticKernel<T, std::multiplies<T>>;
        RowBinaryKernelTable[static_cast<Int>(Expressions::BinaryOperator::Divide)][castType] = &BinaryDivideKernel<T>;
        RowBinaryKernelTable[static_cast<Int>(Expressions::BinaryOperator::Modulo)][castType] = &BinaryModuloKernel<T>;

        RowBinaryKernelTable[static_cast<Int>(Expressions::BinaryOperator::Equal)][castType] = &BinaryComparisonKernel<T, std::equal_to<T>>;
        RowBinaryKernelTable[static_cast<Int>(Expressions::BinaryOperator::NotEqual)][castType] = &BinaryComparisonKernel<T, std::not_equal_to<T>>;
        RowBinaryKernelTable[static_cast<Int>(Expressions::BinaryOperator::Greater)][castType] = &BinaryComparisonKernel<T, std::greater<T>>;
        RowBinaryKernelTable[static_cast<Int>(Expressions::BinaryOperator::GreaterEqual)][castType] = &BinaryComparisonKernel<T, std::greater_equal<T>>;
        RowBinaryKernelTable[static_cast<Int>(Expressions::BinaryOperator::Less)][castType] = &BinaryComparisonKernel<T, std::less<T>>;
        RowBinaryKernelTable[static_cast<Int>(Expressions::BinaryOperator::LessEqual)][castType] = &BinaryComparisonKernel<T, std::less_equal<T>>;
    }
}
