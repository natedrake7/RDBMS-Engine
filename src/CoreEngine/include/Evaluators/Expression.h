#pragma once
#include "Expressions.Additional.h"
#include "../../../Systemic/include/QueryResult.h"
#include "../../../Systemic/include/DataTypes/Value.h"
#include "../../../Systemic/include/DataStructures/PolymorphicArray.h"
#include "../../../Systemic/include/DataStructures/Dictionary.h"

namespace DataTypes
{
    struct JsonPathStep;
}

class Variable;

namespace CoreEngine{
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
    class JsonExpression;
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
        const Dictionary<DataTypes::String, Variable>* variables;

        EvaluationContextType type;

        EvaluationContext(const ::Memory::IAllocator* allocator);
        EvaluationContext(
            EvaluationContextType type,
            const ::Memory::IAllocator* allocator
        );
        explicit EvaluationContext(
            EvaluationContextType type,
            const CoreEngine::ExecutionContext& executionContext
        );
        explicit EvaluationContext(
            const Pages::RowReference* row,
            const CoreEngine::ExecutionContext& executionContext
        );
        explicit EvaluationContext(
            const QueryResult& row,
            const CoreEngine::ExecutionContext& executionContext
        );
        EvaluationContext(
            const Pages::RowReference* outerRow,
            const Pages::RowReference* innerRow,
            const CoreEngine::ExecutionContext& executionContext
        );
    };

    class Expression {
    public:
        DataTypes::String name;
        column_index_t columnIndex;
        ExpressionType expressionType;

        virtual ~Expression() = default;
        Expression();

        [[nodiscard]] virtual Value Evaluate(const EvaluationContext& context) const = 0;
        [[nodiscard]] virtual DataType GetReturnType() const = 0;

        [[nodiscard]] bool IsBinary()const;
        [[nodiscard]] bool IsLogical()const;
        [[nodiscard]] bool IsConstant()const;
        [[nodiscard]] bool IsVariable()const;
        [[nodiscard]] bool IsColumn()const;
        [[nodiscard]] bool IsFunction()const;
        [[nodiscard]] bool IsBranch()const;
        [[nodiscard]] bool IsJson()const;

        [[nodiscard]] BinaryExpression* AsBinary();
        [[nodiscard]] LogicalExpression* AsLogical();
        [[nodiscard]] ColumnExpression* AsColumn();
        [[nodiscard]] VariableExpression* AsVariable();
        [[nodiscard]] ConstantExpression* AsConstant();
        [[nodiscard]] BranchExpression* AsBranch();
        [[nodiscard]] FunctionExpression* AsFunction();
        [[nodiscard]] JsonExpression* AsJson();

        [[nodiscard]] const BinaryExpression* AsBinary()const;
        [[nodiscard]] const LogicalExpression* AsLogical()const;
        [[nodiscard]] const ColumnExpression* AsColumn()const;
        [[nodiscard]] const VariableExpression* AsVariable()const;
        [[nodiscard]] const ConstantExpression* AsConstant()const;
        [[nodiscard]] const BranchExpression* AsBranch()const;
        [[nodiscard]] const FunctionExpression* AsFunction()const;
        [[nodiscard]] const JsonExpression* AsJson()const;

        void SetIndex(column_index_t index);
    };

    class ColumnExpression final : public Expression {
    public:
        DataTypes::String alias;
        DataTypes::String tableAlias;

        Int tableId;
        Int columnId;

        DataType returnType;
        block_size_t size;

        ColumnExpression(const DataTypes::String& name, const DataTypes::String& tableAlias);
        ColumnExpression(DataTypes::String&& name, DataTypes::String&& tableAlias);
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
        [[nodiscard]] bool ValidateUnlimitedArgumentTypes(const FunctionInfo& info, DataTypes::String& errorMessage)const;
        [[nodiscard]] bool ValidateArgumentTypes(const FunctionInfo& info, DataTypes::String& errorMessage)const;
        [[nodiscard]] static bool ValidateReturnType(
        const FunctionInfo& info,
            DataTypes::String& errorMessage,
            DataType expectedType,
            DataType returnType,
            Int index
        );
        static void ConstructInvalidCastMessage(DataTypes::String& errorMessage, DataType fromType, DataType toType);
        bool PerformAdditionalValidations(DataTypes::String& errorMessage)const;

    public:
        DataStructures::PolymorphicArray<Expression*> arguments;
        Constants::FunctionType functionType;

        FunctionExpression(Constants::FunctionType functionType, DataStructures::PolymorphicArray<Expression*>& arguments);
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
        [[nodiscard]] static bool ValidateNullIf(const DataStructures::PolymorphicArray<Expression*>& arguments, DataTypes::String& errorMessage);

        [[nodiscard]] static Value Coalesce(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments);
        [[nodiscard]] static bool ValidateCoalesce(const DataStructures::PolymorphicArray<Expression*>& arguments, DataTypes::String& errorMessage);

        [[nodiscard]] bool ValidateNumberOfArguments(DataTypes::String& errorMessage)const;
        [[nodiscard]] DataType GetReturnType() const override;

        [[nodiscard]] bool IsPlugin()const;
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
      DataTypes::String name;
      DataTypes::String normalizedName;
      DataType dataType;

      explicit VariableExpression(const DataTypes::String& name, const ::Memory::IAllocator* allocator);

      [[nodiscard]]Value Evaluate(const EvaluationContext &context) const override;
      [[nodiscard]]DataType GetReturnType() const override;
    };

    class JsonExpression final : public Expression {

    [[nodiscard]] Value EvaluateJsonPath(const EvaluationContext &context, const Value& columnValue) const;

    public:
        ColumnExpression* columnPtr;
        DataStructures::PolymorphicArray<DataTypes::JsonPathStep> pathSegments;
        DataType type;

        explicit JsonExpression(ColumnExpression* columnPtr, const ::Memory::IAllocator* allocator);

        [[nodiscard]]Value Evaluate(const EvaluationContext &context) const override;
        [[nodiscard]]DataType GetReturnType() const override;
    };
}