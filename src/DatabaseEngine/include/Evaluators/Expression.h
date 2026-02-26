#pragma once
#include "Expressions.Additional.h"
#include "../../../Systemic/include/QueryResult.h"
#include "../../../Systemic/include/DataTypes/Value.h"
#include "../../../Systemic/include/DataStructures/PolymorphicArray.h"
#include <string>


class Variable;

namespace DatabaseEngine{
    class ExecutionContext;
    class ScanState;
}

namespace Memory{
    class IAllocator;
}

namespace Pages{
    struct RowReference;
}

namespace Expressions{
    class BinaryExpression;
    class LogicalExpression;
    class FunctionExpression;
    class VariableExpression;
    class ColumnExpression;
    class ConstantExpression;
    class BranchExpression;

    struct EvaluationContext {
        enum class EvaluationContextType : UnsignedTinyInt {
            Constant = 0,
            SingleRow = 1,
            MaterializedRow = 2,
            Join = 3,
            Aggregate = 4,
            Window = 5,
        };

        QueryResult materializedRow;

        const Pages::RowReference* row;
        const Pages::RowReference* outerRow;
        const Pages::RowReference* innerRow;

        const Memory::IAllocator* allocator;
        const Dictionary<std::string, Variable>* variables;

        EvaluationContextType type;

        EvaluationContext();
        EvaluationContext(
            EvaluationContextType type,
            const ::Memory::IAllocator* allocator
        );
        explicit EvaluationContext(
            EvaluationContextType type,
            const DatabaseEngine::ExecutionContext& executionContext
        );
        explicit EvaluationContext(
            const Pages::RowReference* row,
            const DatabaseEngine::ExecutionContext& executionContext
        );
        explicit EvaluationContext(
            const QueryResult& row,
            const DatabaseEngine::ExecutionContext& executionContext
        );
        EvaluationContext(
            const Pages::RowReference* outerRow,
            const Pages::RowReference* innerRow,
            const DatabaseEngine::ExecutionContext& executionContext
        );
    };

    class Expression {
    public:
        std::string name;
        column_index_t columnIndex;
        ExpressionType expressionType;

        virtual ~Expression() = default;
        Expression();

        void SetIndex(column_index_t index);

        [[nodiscard]] virtual Value Evaluate(const EvaluationContext& context) const = 0;
        [[nodiscard]] virtual DataType GetReturnType() const = 0;

        [[nodiscard]] bool IsBinary()const;
        [[nodiscard]] bool IsLogical()const;
        [[nodiscard]] bool IsConstant()const;
        [[nodiscard]] bool IsVariable()const;
        [[nodiscard]] bool IsColumn()const;
        [[nodiscard]] bool IsFunction()const;
        [[nodiscard]] bool IsBranch()const;

        [[nodiscard]] BinaryExpression* AsBinary();
        [[nodiscard]] LogicalExpression* AsLogical();
        [[nodiscard]] ColumnExpression* AsColumn();
        [[nodiscard]] VariableExpression* AsVariable();
        [[nodiscard]] ConstantExpression* AsConstant();
        [[nodiscard]] BranchExpression* AsBranch();
        [[nodiscard]] FunctionExpression* AsFunction();

        [[nodiscard]] const BinaryExpression* AsBinary()const;
        [[nodiscard]] const LogicalExpression* AsLogical()const;
        [[nodiscard]] const ColumnExpression* AsColumn()const;
        [[nodiscard]] const VariableExpression* AsVariable()const;
        [[nodiscard]] const ConstantExpression* AsConstant()const;
        [[nodiscard]] const BranchExpression* AsBranch()const;
        [[nodiscard]] const FunctionExpression* AsFunction()const;
    };

    class ColumnExpression final : public Expression {
    public:
        std::string alias;
        std::string tableAlias;

        int32_t tableId;
        int32_t columnId;

        DataType returnType;
        block_size_t size;

        column_index_t index;

        ColumnExpression(const std::string& name, const std::string& tableAlias);
        explicit ColumnExpression(column_index_t index);

        [[nodiscard]] Value Evaluate(const EvaluationContext& context)const override;
        [[nodiscard]] DataType GetReturnType() const override;
        [[nodiscard]] bool HasTableAlias() const;
    };

    class ConstantExpression final : public Expression {
    public:
      Value value;

        explicit ConstantExpression(const Value& value);
        explicit ConstantExpression(Value& value);
        explicit ConstantExpression(Value&& value);

      [[nodiscard]] Value Evaluate(const EvaluationContext& context)const override;
      [[nodiscard]] DataType GetReturnType() const override;
    };

    class BinaryExpression final : public Expression {
        [[nodiscard]] bool ValidateAddition()const;
        [[nodiscard]] bool ValidateSubtraction()const;
        [[nodiscard]] bool ValidateMultiplication()const;
        [[nodiscard]] bool ValidateDivision()const;
        [[nodiscard]] bool ValidateModulo()const;

    public:
        Expression* left;
        Expression* right;

        BinaryOperator operation;

        BinaryExpression(Expression* left, Expression* right, BinaryOperator operation);
        ~BinaryExpression()override;

        [[nodiscard]] Value Evaluate(const EvaluationContext& context)const override;
        [[nodiscard]] DataType GetReturnType() const override;

        [[nodiscard]] bool ValidateOperation()const;
    };

    class FunctionExpression final : public Expression {
        [[nodiscard]] bool ValidateUnlimitedArgumentTypes(const FunctionInfo& info, std::string& errorMessage)const;
        [[nodiscard]] bool ValidateArgumentTypes(const FunctionInfo& info, std::string& errorMessage)const;
        [[nodiscard]] static bool ValidateReturnType(
        const FunctionInfo& info,
            std::string& errorMessage,
            DataType expectedType,
            DataType returnType,
            Int index
        );
        static void ConstructInvalidCastMessage(std::string& errorMessage, DataType fromType, DataType toType);
        bool PerformAdditionalValidations(std::string& errorMessage)const;

    public:
        std::vector<Expression*> arguments;
        Constants::FunctionType functionType;

        FunctionExpression(Constants::FunctionType functionType, std::vector<Expression*>& arguments);
        [[nodiscard]] Value Evaluate(const EvaluationContext& context)const override;

        //String Function
        [[nodiscard]] static Value Concat(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments);
        [[nodiscard]] static Value Length(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments);
        [[nodiscard]] static Value TrimLeft(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments);
        [[nodiscard]] static Value TrimRight(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments);
        [[nodiscard]] static Value Trim(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments);
        [[nodiscard]] static Value AsciiValue(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments);
        [[nodiscard]] static Value Char(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments);
        [[nodiscard]] static Value CharIndex(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments);
        [[nodiscard]] static Value Lower(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments);
        [[nodiscard]] static Value Upper(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments);
        [[nodiscard]] static Value Replace(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments);
        [[nodiscard]] static Value Substr(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments);
        [[nodiscard]] static Value Left(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments);
        [[nodiscard]] static Value Right(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments);
        [[nodiscard]] static Value Reverse(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments);
        [[nodiscard]] static Value Space(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments);

        //DateTime Functions
        [[nodiscard]] static Value GetDate(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments);

        //Guid Functions
        [[nodiscard]] static Value NewGuid(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments);

        //Null Checking Functions
        [[nodiscard]] static Value NullIf(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments);
        [[nodiscard]] static bool ValidateNullIf(const std::vector<Expression*>& arguments, std::string& errorMessage);

        [[nodiscard]] static Value Coalesce(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments);
        [[nodiscard]] static bool ValidateCoalesce(const std::vector<Expression*>& arguments, std::string& errorMessage);

        [[nodiscard]] bool ValidateNumberOfArguments(std::string& errorMessage)const;
        [[nodiscard]] DataType GetReturnType() const override;
    };

    class LogicalExpression final : public Expression{
    public:
        LogicalType logicalType;

        Expression* left;
        Expression* right;

        LogicalExpression(
          Expression *leftExpression,
          Expression *RightExpression,
          LogicalType logicalType
        );
        LogicalExpression();

        [[nodiscard]] bool IsOr()const;
        [[nodiscard]] bool IsAnd()const;
        [[nodiscard]] bool HasAtLeastOneConstant()const;

        [[nodiscard]] Value Evaluate(const EvaluationContext& context)const override;
        [[nodiscard]] DataType GetReturnType() const override;
    };

    class BranchExpression final : public Expression {
        [[nodiscard]] Value EvaluateSwitch(const EvaluationContext &context)const;
        [[nodiscard]] Value EvaluateTernary(const EvaluationContext &context)const;

    public:
        BranchType branchType;
        std::vector<Expression*> branches;
        std::vector<Expression*> results;
        std::vector<Expression*> arguments;

        Expression* baseCase;

        explicit BranchExpression(BranchType type);
        [[nodiscard]]Value Evaluate(const EvaluationContext &context) const override;
        [[nodiscard]]DataType GetReturnType() const override;

        [[nodiscard]] bool HasBaseCase()const;
        [[nodiscard]] bool ValidateNumberOfArguments()const;
    };

    class VariableExpression final : public Expression {
    public:
      std::string name;
      std::string normalizedName;
      DataType dataType;

      explicit VariableExpression(const std::string& name);

      [[nodiscard]]Value Evaluate(const EvaluationContext &context) const override;
      [[nodiscard]]DataType GetReturnType() const override;
    };
}