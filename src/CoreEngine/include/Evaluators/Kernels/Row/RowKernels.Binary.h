#pragma once
#include "../../Expressions.Additional.h"
#include "../../../../../Systemic/include/DataTypes/DataTypes.h"
#include "../../Expression.h"
namespace CoreEngine::RowKernels{
    template<DataTypes::PrimitiveColumn T, typename Op>
    void BinaryArithmeticKernel(
        const Expressions::Expression* self,
        const Expressions::EvaluationContext& context,
        void* outVal,
        bool* outNull
    ){
        const auto* binaryExpr = self->AsBinary();
        bool leftNull = false, rightNull = false;
        T left, right;

        binaryExpr->left->rowKernel(binaryExpr->left, context, &left, &leftNull);
        binaryExpr->right->rowKernel(binaryExpr->right, context, &right, &rightNull);

        if (leftNull || rightNull){
            *outNull = true;
            return;
        }   // strict propagation

        *static_cast<T*>(outVal) = Op{}(left, right);
        *outNull = false;
    }

    // Divide / Modulo: divide-by-zero yields NULL (swap for a raised error if your
    // semantics demand it). Modulo is only registered for integral T.
    template<DataTypes::PrimitiveColumn T>
    void BinaryDivideKernel(
        const Expressions::Expression* self,
        const Expressions::EvaluationContext& context,
        void* outVal,
        bool* outNull
    ){
        const auto* binaryExpr = self->AsBinary();
        bool leftNull = false, rightNull = false;
        T left, right;

        binaryExpr->left->rowKernel(binaryExpr->left, context, &left, &leftNull);
        binaryExpr->right->rowKernel(binaryExpr->right, context, &right, &rightNull);

        if (leftNull || rightNull || right == T{}){
            *outNull = true;
            return;
        }

        *static_cast<T*>(outVal) = left / right;
        *outNull = false;
    }

    template<DataTypes::PrimitiveColumn T>
    void BinaryModuloKernel(
        const Expressions::Expression* self,
        const Expressions::EvaluationContext& context,
        void* outVal,
        bool* outNull
    ){
        const auto* binaryExpr = self->AsBinary();
        bool leftNull = false, rightNull = false;
        T left, right;

        binaryExpr->left->rowKernel(binaryExpr->left, context, &left, &leftNull);
        binaryExpr->right->rowKernel(binaryExpr->right, context, &right, &rightNull);

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

        binaryExpr->left->rowKernel(binaryExpr->left, context, &left, &leftNull);
        binaryExpr->right->rowKernel(binaryExpr->right, context, &right, &rightNull);

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

    static constexpr Int BINARY_OPERATIONS_COUNT = 12;   // Expressions::BinaryOperator enumerator count

    struct BinaryKernelTable{
        Expressions::RowKernelFunction cells[BINARY_OPERATIONS_COUNT][DATATYPE_COUNT];
    };

    constexpr void RegisterBool(BinaryKernelTable& table);
    constexpr void RegisterString(BinaryKernelTable& table);
    constexpr void RegisterDateTime(BinaryKernelTable& table);
    constexpr void RegisterDecimal(BinaryKernelTable& table);
    constexpr void RegisterJson(BinaryKernelTable& table);
    constexpr void RegisterGuid(BinaryKernelTable& table);

    [[nodiscard]] constexpr BinaryKernelTable RegisterBinaryKernels();

    Expressions::RowKernelFunction LookupBinaryKernel(
        Expressions::BinaryOperator op,
        DataType operandType
    );
}
