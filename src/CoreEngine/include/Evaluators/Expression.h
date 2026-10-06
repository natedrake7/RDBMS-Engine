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

    using ExpressionNodes = DataTypes::TypeList<
        void,
        ColumnExpression,
        ConstantExpression,
        BinaryExpression,
        LogicalExpression,
        VariableExpression,
        BranchExpression,
        FunctionExpression,
        JsonExpression,
        CastExpression
    >;

    static_assert(ExpressionNodes::SIZE == EXPRESSION_TYPE_COUNT, "one node class per ExpressionType");

    // Membership only: usable while TNode is still incomplete (e.g. inside its own class body).
    template <typename TNode>
    concept ListedExpressionNode = !std::is_void_v<TNode> && (ExpressionNodes::IndexOf<TNode>() < ExpressionNodes::SIZE);

    // Needs the complete class: only for casts (As), where the class is always complete.
    template <typename TNode>
    concept ExpressionNodeClass = ListedExpressionNode<TNode>
        && std::derived_from<TNode, Expression>
        && requires(const TNode& node){
            { TNode::TYPE }       -> std::convertible_to<ExpressionType>;
            { node.GetReturnType() } -> std::same_as<DataType>;
            { node.ForEachChild([](const Expression*){}) };
        };

    template <ListedExpressionNode TNode>
    inline constexpr auto ExpressionTypeOf = static_cast<ExpressionType>(ExpressionNodes::IndexOf<TNode>());

    template <ExpressionType TYPE>
    using NodeOf = ExpressionNodes::At<static_cast<std::size_t>(TYPE)>;

    template <typename TNode>
    concept VectorizedNode = ExpressionNodeClass<TNode> && (
        requires(Expression* self, const CoreEngine::OutputSchema* schema){
            { TNode::BindVectorizedKernel(self, schema) } -> std::same_as<void>;
        }
        ||
        requires(Expression* self, const CoreEngine::OutputSchema* schema){
            { TNode::BindVectorizedKernel(self) } -> std::same_as<void>;
        }
    );

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
    public:
        DataTypes::String name;

        VectorizedKernelFunction vectorizedKernel = nullptr;
        RowKernelFunction rowKernel = nullptr;

        column_index_t ordinalPosition = 0;
        ExpressionType expressionType = ExpressionType::Expression;

        template<ListedExpressionNode TNode>
        [[nodiscard]] inline constexpr bool Is()const{
            return this->expressionType == ExpressionTypeOf<TNode>;
        }

        template<ExpressionNodeClass TNode>
        [[nodiscard]] TNode* As(){
            assert(this->Is<TNode>());
            return static_cast<TNode*>(this);
        }

        template<ExpressionNodeClass TNode>
        [[nodiscard]] const TNode* As()const{
            assert(this->Is<TNode>());
            return static_cast<const TNode*>(this);
        }

        void SetIndex(column_index_t index);
    };

    class ColumnExpression final : public Expression {
        static void ResolveReference(ColumnExpression* self, const CoreEngine::OutputSchema* schema);

    public:
        static inline constexpr auto TYPE = ExpressionTypeOf<ColumnExpression>;

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

        static void BindVectorizedKernel(Expression* self, const CoreEngine::OutputSchema* schema);

        static void BindRowKernel(Expression* self);

        [[nodiscard]] DataType GetReturnType() const;
        [[nodiscard]] bool HasTableAlias() const;

        template <typename Self, typename F>
        void ForEachChild(this Self&&, F&&){}
    };

    class ConstantExpression final : public Expression {
    public:
        static inline constexpr auto TYPE = ExpressionTypeOf<ConstantExpression>;

        Value value;

        explicit ConstantExpression(const Value& value);
        explicit ConstantExpression(Value& value);
        explicit ConstantExpression(Value&& value);

        static void BindVectorizedKernel(Expression* self, const CoreEngine::OutputSchema* schema);
        static void BindRowKernel(Expression* self);

        [[nodiscard]] DataType GetReturnType() const;

        template <typename Self, typename F>
        void ForEachChild(this Self&&, F&&){}
    };

    class LogicalExpression final : public Expression{
    public:
        static inline constexpr auto TYPE = ExpressionTypeOf<LogicalExpression>;

        LogicalType logicalType;

        Expression* left;
        Expression* right;

        LogicalExpression(
            Expression *leftExpression,
            Expression *RightExpression,
            LogicalType logicalType
        );

        [[nodiscard]] bool IsOr()const;
        [[nodiscard]] bool IsAnd()const;
        [[nodiscard]] bool IsNot()const;
        [[nodiscard]] bool HasAtLeastOneConstant()const;

        static void BindRowKernel(Expression* self);
        static void BindVectorizedKernel(Expression* self, const CoreEngine::OutputSchema* schema);

        [[nodiscard]] static constexpr DataType GetReturnType();

        template <typename Self, typename F>
        void ForEachChild(this Self&& self, F&& f){
            f(self.left);
            f(self.right);
        }

    };

    class BinaryExpression final : public Expression {
        [[nodiscard]] bool ValidateAddition()const;
        [[nodiscard]] bool ValidateSubtraction()const;
        [[nodiscard]] bool ValidateMultiplication()const;
        [[nodiscard]] bool ValidateDivision()const;
        [[nodiscard]] bool ValidateModulo()const;

    public:
        static inline constexpr auto TYPE = ExpressionTypeOf<BinaryExpression>;

        Expression* left;
        Expression* right;

        BinaryOperator operation;

        BinaryExpression(Expression* left, Expression* right, BinaryOperator operation);

        [[nodiscard]] bool ValidateOperation()const;

        static void BindRowKernel(Expression* self);
        static void BindVectorizedKernel(Expression* self, const CoreEngine::OutputSchema* schema);

        [[nodiscard]] DataType GetReturnType() const;

        template <typename Self, typename F>
        void ForEachChild(this Self&& self, F&& f){ f(self.left); f(self.right); }
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
        static inline constexpr auto TYPE = ExpressionTypeOf<FunctionExpression>;

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

        template <typename Self, typename F>
        void ForEachChild(this Self&& self, F&& f){
            for (auto* argument : self.arguments)
                f(argument);
        }
    };


    class BranchExpression final : public Expression {
        [[nodiscard]] Value EvaluateSwitch(const EvaluationContext &context)const;
        [[nodiscard]] Value EvaluateTernary(const EvaluationContext &context)const;

    public:
        static inline constexpr auto TYPE = ExpressionTypeOf<BranchExpression>;

        BranchType branchType;
        DataStructures::PolymorphicArray<Expression*> branches;
        DataStructures::PolymorphicArray<Expression*> results;
        DataStructures::PolymorphicArray<Expression*> arguments;

        Expression* baseCase;

        explicit BranchExpression(BranchType type, const ::Memory::IAllocator* allocator);
        [[nodiscard]]DataType GetReturnType() const;

        [[nodiscard]] bool HasBaseCase()const;
        [[nodiscard]] bool ValidateNumberOfArguments()const;

        template <typename Self, typename F>
        void ForEachChild(this Self&& self, F&& f){
            for (auto* argument : self.arguments)
                f(argument);
            for (auto* branch : self.branches)
                f(branch);
            for (auto* result : self.results)
                f(result);
            if (self.HasBaseCase())
                f(self.baseCase);
        }
    };

    class VariableExpression final : public Expression {
    public:
        static inline constexpr auto TYPE = ExpressionTypeOf<VariableExpression>;

        DataTypes::String name;
        DataTypes::String normalizedName;
        DataType dataType;

        explicit VariableExpression(const DataTypes::String& name, const ::Memory::IAllocator* allocator);

        static void BindRowKernel(Expression* self);
        static void BindVectorizedKernel(Expression* self, const CoreEngine::OutputSchema* schema);

        [[nodiscard]]Value Evaluate(const EvaluationContext &context) const;
        [[nodiscard]]DataType GetReturnType() const;

        template <typename Self, typename F>
        void ForEachChild(this Self&&, F&&){}
    };

    class JsonExpression final : public Expression {
    [[nodiscard]] Value EvaluateJsonPath(const EvaluationContext &context, const Value& columnValue) const;

    public:
        static inline constexpr auto TYPE = ExpressionTypeOf<JsonExpression>;

        ColumnExpression* columnPtr;
        DataStructures::PolymorphicArray<DataTypes::JsonPathStep> pathSegments;
        DataType type;

        explicit JsonExpression(ColumnExpression* columnPtr, const ::Memory::IAllocator* allocator);

        [[nodiscard]]DataType GetReturnType() const;

        template <typename Self, typename F>
        void ForEachChild(this Self&& self, F&& f){ f(self.columnPtr); }
    };

    class CastExpression final: public Expression{
    public:
        static inline constexpr auto TYPE = ExpressionTypeOf<CastExpression>;

        Expression* childExpr;
        DataType targetType;
        bool isTryCast;

        CastExpression(Expression* expression, DataType targetType, bool isTryCast);

        static void BindRowKernel(Expression* self);
        static void BindVectorizedKernel(Expression* self, const CoreEngine::OutputSchema* schema);

        [[nodiscard]] DataType GetReturnType() const;

        template <typename Self, typename F>
        void ForEachChild(this Self&& self, F&& f){ f(self.childExpr); }
    };

    template <typename Visitor>
    decltype(auto) VisitNode(const ExpressionType type, Visitor&& visitor){
        template for (constexpr auto enumerator : Reflection::Enumerators<ExpressionType>){
            using TNode = NodeOf<([:enumerator:])>;
            if constexpr (!std::is_void_v<TNode>){
                if (type == [:enumerator:])
                    return visitor.template operator()<TNode>();
            }
        }
        assert(false && "VisitNode: base Expression or unknown type");
        std::unreachable();
    }

    inline constexpr bool ExpressionNodesAreConsistent = []{
        template for (constexpr auto enumerator : Reflection::Enumerators<ExpressionType>){
            using TNode = NodeOf<([:enumerator:])>;
            if constexpr (!std::is_void_v<TNode>){
                static_assert(ExpressionNodeClass<TNode>, "node class must derive from Expression and declare TYPE, GetReturnType() and ForEachChild()");
                // static_assert(VectorizedNode<TNode>, "VECTORIZED node without BindVectorizedKernel(Expression*, const OutputSchema*)");
                if (TNode::TYPE != [:enumerator:])
                    return false;      // list order matches the enum
            }
        }
        return true;
    }();
    static_assert(ExpressionNodesAreConsistent, "ExpressionNodes order does not match ExpressionType");

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

    template <typename TExpression, typename F>
    requires std::same_as<std::remove_const_t<TExpression>, Expression>
    void ForEachChild(TExpression* expression, F&& f){
        VisitNode(expression->expressionType, [&]<typename TNode>(){
            expression->template As<TNode>()->ForEachChild(f);
        });
    }

    template <typename TExpression, typename TCallback>
    requires std::same_as<std::remove_const_t<TExpression>, Expression>
    void ForEachColumnReference(TExpression* expression, TCallback&& visitor){
        if (expression == nullptr)
            return;

        if (expression->template Is<ColumnExpression>())
            visitor(expression->template As<ColumnExpression>());
        else
            ForEachChild(expression, [&](TExpression* child){ ForEachColumnReference(child, visitor); });
    }}