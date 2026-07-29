#pragma once
#include "BoundReference.h"
#include "Expression.h"
#include "Expressions.Additional.h"
#include "../../../Systemic/include/DataTypes/Value.h"
#include "../../../Systemic/include/DataStructures/PolymorphicArray.h"
#include "../Pages/PageView.h"

namespace DataTypes{
    struct JsonPathStep;
}

class Variable;

namespace CoreEngine{
    struct DataChunk;
    struct OutputSchema;
    struct DataVector;
    struct SelectionVector;

    namespace StorageTypes{
        class Table;
        struct RID;
    }

    class ExecutionContext;
    struct ScanState;
}

namespace Memory{
    class IAllocator;
}

namespace Pages{
    class PageView;
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
    class CastExpression;

    struct EvaluationContext {
        enum class EvaluationContextType : UnsignedTinyInt {
            Constant = 0,
            SingleRow = 1,
            MaterializedRow = 2,
            Join = 3,
            Aggregate = 4,
            Window = 5,
        };

        const CoreEngine::StorageTypes::RID* _rids;
        const Pages::PageView* _pages;

        const CoreEngine::ExecutionContext* _executionContext;
        const ::Memory::IAllocator* _allocator;

        EvaluationContextType _type;

        explicit EvaluationContext(
            EvaluationContextType type,
            const ::Memory::IAllocator* allocator
        );
        EvaluationContext(
            EvaluationContextType type,
            const CoreEngine::ExecutionContext* executionContext
        );
        explicit EvaluationContext(
            const CoreEngine::StorageTypes::RID* row,
            const CoreEngine::ExecutionContext* executionContext
        );
    };

    using VectorizedKernelFunction = CoreEngine::DataVector* (*)(
        const Expression* self,
        const CoreEngine::ExecutionContext* context,
        const CoreEngine::DataChunk* chunk
    );

    using RowKernelFunction = void(*)(
        const Expression* self,
        const EvaluationContext& evaluationContext,
        void* outVal,
        bool* outNull
    );

    class Expression {
    protected:
        [[nodiscard]] Value EvaluateJoin(const EvaluationContext& context) const;

    public:
        DataTypes::String name;

        VectorizedKernelFunction vectorizedKernel = nullptr;
        RowKernelFunction rowKernel = nullptr;

        column_index_t ordinalPosition;
        ExpressionType expressionType;

        Expression();

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
        [[nodiscard]] CastExpression* AsCast();

        [[nodiscard]] const BinaryExpression* AsBinary()const;
        [[nodiscard]] const LogicalExpression* AsLogical()const;
        [[nodiscard]] const ColumnExpression* AsColumn()const;
        [[nodiscard]] const VariableExpression* AsVariable()const;
        [[nodiscard]] const ConstantExpression* AsConstant()const;
        [[nodiscard]] const BranchExpression* AsBranch()const;
        [[nodiscard]] const FunctionExpression* AsFunction()const;
        [[nodiscard]] const JsonExpression* AsJson()const;
        [[nodiscard]] const CastExpression* AsCast()const;

        [[nodiscard]] bool IsColumnType()const;

        void SetIndex(column_index_t index);
    };

    class ColumnExpression final : public Expression {
        static void BindVectorizedKernel(ColumnExpression* self);
        static void ResolveReference(ColumnExpression* self, const CoreEngine::OutputSchema* schema);

    public:
        DataTypes::String alias;
        DataTypes::String tableAlias;

        BoundReference _boundReference;

        Int tableId;
        Int columnId;

        UnsignedSmallInt _slotIndex;

        DataType returnType;
        block_size_t size;

        ColumnExpression(const DataTypes::String& name, const DataTypes::String& tableAlias);
        ColumnExpression(DataTypes::String&& name, DataTypes::String&& tableAlias);
        explicit ColumnExpression(column_index_t index, DataType dataType);

        static void BindAndResolveExpressionKernel(
            Expression* expression,
            const CoreEngine::OutputSchema* schema
        );

        static void BindRowKernel(Expression* self);

        [[nodiscard]] DataType GetReturnType() const;
        [[nodiscard]] bool HasTableAlias() const;
    };

    class ConstantExpression final : public Expression {
    public:
        Value value;

        explicit ConstantExpression(const Value& value);
        explicit ConstantExpression(Value& value);
        explicit ConstantExpression(Value&& value);

        static void BindVectorizedKernel(Expression* self);
        static void BindRowKernel(Expression* self);

        [[nodiscard]] DataType GetReturnType() const;
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

        static void BindRowKernel(Expression* self);
        static void BindVectorizedKernel(Expression* self, const CoreEngine::OutputSchema* schema);

        [[nodiscard]] static constexpr DataType GetReturnType();
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

        [[nodiscard]] bool ValidateOperation()const;

        static void BindRowKernel(Expression* self);
        static void BindVectorizedKernel(Expression* self, const CoreEngine::OutputSchema* schema);

        [[nodiscard]] DataType GetReturnType() const;
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
        [[nodiscard]] Value Evaluate(const EvaluationContext& context)const;

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
        [[nodiscard]] DataType GetReturnType() const;

        [[nodiscard]] bool IsPlugin()const;
    };


    class BranchExpression final : public Expression {
        [[nodiscard]] Value EvaluateSwitch(const EvaluationContext &context)const;
        [[nodiscard]] Value EvaluateTernary(const EvaluationContext &context)const;

    public:
        BranchType branchType;
        DataStructures::PolymorphicArray<Expression*> branches;
        DataStructures::PolymorphicArray<Expression*> results;
        DataStructures::PolymorphicArray<Expression*> arguments;

        Expression* baseCase;

        explicit BranchExpression(BranchType type, const ::Memory::IAllocator* allocator);
        [[nodiscard]]DataType GetReturnType() const;

        [[nodiscard]] bool HasBaseCase()const;
        [[nodiscard]] bool ValidateNumberOfArguments()const;
    };

    class VariableExpression final : public Expression {
        void BindVectorizedKernel();
    public:
        DataTypes::String name;
        DataTypes::String normalizedName;
        DataType dataType;

        explicit VariableExpression(const DataTypes::String& name, const ::Memory::IAllocator* allocator);

        static void BindRowKernel(Expression* self);

        [[nodiscard]]Value Evaluate(const EvaluationContext &context) const;
        [[nodiscard]]DataType GetReturnType() const;
    };

    class JsonExpression final : public Expression {

    [[nodiscard]] Value EvaluateJsonPath(const EvaluationContext &context, const Value& columnValue) const;

    public:
        ColumnExpression* columnPtr;
        DataStructures::PolymorphicArray<DataTypes::JsonPathStep> pathSegments;
        DataType type;

        explicit JsonExpression(ColumnExpression* columnPtr, const ::Memory::IAllocator* allocator);

        [[nodiscard]]DataType GetReturnType() const;
    };

    class CastExpression final: public Expression{
        void BindVectorizedKernel();
    public:
        Expression* childExpr;
        DataType targetType;
        bool isTryCast;

        CastExpression(Expression* expression, DataType targetType, bool isTryCast);

        static void BindRowKernel(Expression* self);

        [[nodiscard]] DataType GetReturnType() const;
    };

    // Value EvaluateExpression(const Expression* expression, const EvaluationContext& context);

    CoreEngine::DataVector* EvaluateExpression(
        const Expression* expression,
        const CoreEngine::ExecutionContext* context,
        const CoreEngine::DataChunk* chunk
    );
    void EvaluateExpression(
        const Expression* expression,
        const EvaluationContext& context,
        void* outVal,
        bool* outNull
    );

    Value EvaluateExpression(
        const Expression* expression,
        const EvaluationContext& context
    );

    [[nodiscard]] bool RowModeFilter(
        const Expression* expression,
        const EvaluationContext& context
    );

    DataType GetExpressionReturnType(const Expression* expression);

    void BindExpressionRowKernel(Expression* expression);

    void BindAndResolveExpressionKernel(Expression* expression, const CoreEngine::OutputSchema* schema);
}