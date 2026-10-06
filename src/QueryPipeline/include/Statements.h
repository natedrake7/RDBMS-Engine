#pragma once
#include "Parser.h"
#include "ValidationMessages.h"
#include "../../CoreEngine/include/DatabaseConstants.h"
#include "../../CoreEngine/include/Evaluators/Expression.h"
#include "../../Systemic/include/DataTypes/Variable.h"
#include "../../Server/include/Security/Security.h"
#include "../../CoreEngine/include/Errors.h"
#include "../../Systemic/include/Headers.h"
#include "../../CoreEngine/include/DataStorage/Row/SerializedRow.h"
#include "../../CoreEngine/include/DataStorage/Row/Row.InsertPlan.h"

namespace CoreEngine{
    class SystemCatalog;
}

namespace QueryPipeline {
    struct QueryContext;
    struct JoinOrderAnalyzeResult;
    struct PredicatePushDownResult;
}

namespace Network {
    class Server;
}

namespace Headers {
    struct DefaultValuesHeader;
}

namespace QueryPipeline {
    struct CompileValidationScope;
    class LogicalPlan;
}

namespace QueryPipeline::Statements {
    struct Statement;
    struct SelectStatement;

    struct ColumnName {
        DataTypes::String name;
        DataTypes::String alias;

        Int tableId;
        Int columnId;
        UnsignedSmallInt _slotIndex;
        column_index_t ordinalPosition;
        DataType returnType;
    };

    struct CompilationScope {
        Dictionary<DataTypes::String, UnsignedSmallInt> _tableAliasesDict;
        DataStructures::PolymorphicArray<Dictionary<DataTypes::String, Headers::ColumnHeader>> _tableColumnsArray;
        int* _indexPos;
        Statement* _statement;

        explicit CompilationScope()
            : _indexPos(nullptr), _statement(nullptr){}
        explicit CompilationScope(const ::Memory::IAllocator* allocator, UnsignedSmallInt numberOfTables);
        explicit CompilationScope(
            Dictionary<DataTypes::String, UnsignedSmallInt>& tableAliasesDictionary,
            DataStructures::PolymorphicArray<Dictionary<DataTypes::String, Headers::ColumnHeader>>& tableColumnsArray,
            Statement* statement,
            int* indexPos = nullptr
        );
        explicit CompilationScope(
            Dictionary<DataTypes::String, UnsignedSmallInt>& tableAliasesDictionary,
            Statement* statement,
            int* indexPos = nullptr
        );
    };

    struct DecimalType {
        TinyInt precision;
        TinyInt scale;

        DecimalType();
        DecimalType(TinyInt precision, TinyInt scale);
        [[nodiscard]] bool Validate() const;
    };

    struct ColumnType {
        DataTypes::String name;
        int size;

        DecimalType decimal;

        ColumnType();
        explicit ColumnType(DataTypes::String&& name);
        explicit ColumnType(const DataTypes::String& name);
        ColumnType(const DataTypes::String& name, Int size);
        ColumnType(DataTypes::String& name, Int size);
        ColumnType(const DataTypes::String& name, DecimalType decimal);
        ColumnType(DataTypes::String& name, DecimalType decimal);
    };

    struct Identity {
        SmallInt seed;
        SmallInt incrementFactor;
        BigInt cacheBlock;

        [[nodiscard]] Errors::CompilationStatus Validate(const QueryContext& context) const;
    };

    struct NewColumn {
        ColumnName name;
        ColumnType type;
        Identity* identity;
        Value defaultValue;

        bool isPrimaryKey;
        bool isNullable;

        column_index_t index;

        [[nodiscard]] bool HasIdentity() const;
    };

    struct OrderColumn {
        Expressions::Expression* expression;
        Int outputIndex;
        Constants::OrderType type;

        OrderColumn();
    };

    struct AlterColumn {
        ColumnName name;
        ColumnType type;

        Int columnId;
        column_index_t index;

        AlterColumn();
    };

    struct DropColumn {
        ColumnName name;

        Int columnId;
        column_index_t index;
    };

    struct RenameColumn {
        ColumnName oldName;
        ColumnName newName;

        column_index_t ordinalPosition;
        Int columnId;
    };

    struct PrimaryKeyConstraint {
        DataTypes::String name;
        DataStructures::PolymorphicArray<ColumnName> columns;
    };

    struct WhereClause {
        Expressions::Expression* expression;

        WhereClause();
        [[nodiscard]] bool IsValid() const;
    };

    struct OrderByStatement {
        DataStructures::PolymorphicArray<OrderColumn*> columns;

        bool Validate(
            const DataStructures::PolymorphicArray<OrderColumn*>& selectColumns,
            const Dictionary<DataTypes::String, Headers::ColumnHeader>& columnsDict
        );
    };

    // can be a table, a view, a subquery, or a function returning a table
    struct DataSource {
        DataTypes::String database;
        DataTypes::String schema;
        DataTypes::String name;
        DataTypes::String alias;

        Int _databaseId;
        Int _tableId;
        Int _schemaId;
        UnsignedSmallInt _ordinalPosition;
        UnsignedSmallInt _slotIndex;

        explicit DataSource(const ::Memory::IAllocator* allocator);
        [[nodiscard]] DataTypes::String GetAlias(const QueryContext& context) const;
        [[nodiscard]] DataTypes::String GetFullName(const QueryContext& context) const;
        [[nodiscard]] Errors::CompilationStatus Compile(
            const QueryContext& context,
            Int databaseId,
            UnsignedSmallInt& outSlotCount
        );
        [[nodiscard]] Errors::CompilationStatus ValidateTableCreate(const QueryContext& context, Int selectedDatabaseId);
    };

    struct Inserts {
        DataStructures::PolymorphicArray<Expressions::Expression*> values;
    };

    struct Statement {
        DataSource* table;
        Int databaseId;
        UnsignedSmallInt _slotCount;

        Statement();
        virtual ~Statement() = default;

        virtual Errors::CompilationStatus CompileDerived(QueryContext& context) = 0;
        [[nodiscard]] virtual constexpr Security::Permission RequiredPermissions() const = 0;
        [[nodiscard]] Errors::CompilationStatus CompileBase(const QueryContext& context) const;
        [[nodiscard]] Errors::CompilationStatus Compile(QueryContext& context);
        virtual LogicalPlan* ToLogical(QueryContext& context) = 0;
    };

    struct DeclareVariableStatement final : Statement {
        Variable variable;
        Expressions::Expression* expression;
        DataType type;

        DeclareVariableStatement();

        Errors::CompilationStatus CompileDerived(QueryContext& context) override;
        [[nodiscard]] constexpr Security::Permission RequiredPermissions() const override;
        LogicalPlan* ToLogical(QueryContext& context) override;
    };

    struct SetVariableStatement final : Statement {
        Variable variable;
        Expressions::Expression* expression;
        DataType type;

        SetVariableStatement();

        Errors::CompilationStatus CompileDerived(QueryContext& context) override;
        [[nodiscard]] constexpr Security::Permission RequiredPermissions() const override;
        LogicalPlan* ToLogical(QueryContext& context) override;
    };

    struct CreateUserStatement final : Statement {
        DataTypes::String username;
        DataTypes::String password;
        DataTypes::String role;

        Errors::CompilationStatus CompileDerived(QueryContext& context) override;
        constexpr Security::Permission RequiredPermissions() const override;
        LogicalPlan* ToLogical(QueryContext& context) override;
    };

    struct GrantRoleStatement final : Statement {
        DataTypes::String username;
        DataTypes::String role;

        Errors::CompilationStatus CompileDerived(QueryContext& context) override;
        [[nodiscard]] constexpr Security::Permission RequiredPermissions() const override;
        LogicalPlan* ToLogical(QueryContext& context) override;
    };

    struct DeleteStatement final : Statement {
        WhereClause where;

        Errors::CompilationStatus CompileDerived(QueryContext& context) override;
        [[nodiscard]] constexpr Security::Permission RequiredPermissions() const override;
        LogicalPlan* ToLogical(QueryContext& context) override;
    };

    struct JoinStatement final : Statement {
        Expressions::Expression* expression;
        JoinType type;

        JoinStatement();
        [[nodiscard]] Errors::CompilationStatus CompileDerived(QueryContext& context) override;
        [[nodiscard]] Errors::CompilationStatus Compile(const QueryContext& context, Int databaseId, UnsignedSmallInt& slotCount);

        [[nodiscard]] bool IsRightJoin() const;
        [[nodiscard]] bool IsInnerJoin() const;
        [[nodiscard]] bool IsFullOuterJoin() const;

        LogicalPlan* ToLogical(QueryContext& context) override;
        [[nodiscard]] constexpr Security::Permission RequiredPermissions() const override;
    };

    struct CreateTableStatement final : Statement {
        DataStructures::PolymorphicArray<NewColumn*> columns;
        PrimaryKeyConstraint* constraint;
        DataStructures::PolymorphicArray<column_index_t> primaryKey;

        CreateTableStatement();

        [[nodiscard]] Errors::CompilationStatus CompileSchema(const QueryContext& context) const;
        [[nodiscard]] Errors::CompilationStatus CompileColumnExpression(
            const QueryContext& context,
            NewColumn*& column,
            Dictionary<DataTypes::String, column_index_t>& columnNamesToIndexes,
            bool& primaryKeyFound,
            column_index_t& index
        );
        [[nodiscard]] Errors::CompilationStatus CompileDerived(QueryContext& context) override;
        [[nodiscard]] LogicalPlan* ToLogical(QueryContext& context) override;
        [[nodiscard]] constexpr Security::Permission RequiredPermissions() const override;
    };

    struct SelectStatement final : Statement {
        DataStructures::PolymorphicArray<Headers::ColumnHeader> columnHeaders;
        DataStructures::PolymorphicArray<Expressions::Expression*> _projections;
        DataStructures::PolymorphicArray<JoinStatement*> _joins;
        OrderByStatement* orderBy;
        WhereClause where;
        BigInt top;
        UnsignedSmallInt _visibleProjectionCount;
        bool distinct;

        explicit SelectStatement(const ::Memory::IAllocator* allocator);

        [[nodiscard]] Dictionary<DataTypes::String, column_index_t> CreatePostProjectionIndicesDictionary() const;
        [[nodiscard]] bool HasTopStatement() const;
        [[nodiscard]] bool HasJoins() const;
        [[nodiscard]] bool HasWhere() const;
        [[nodiscard]] bool IsConstant() const;
        [[nodiscard]] Errors::CompilationStatus CompileNoTableStatement(QueryContext& context);
        [[nodiscard]] Errors::CompilationStatus Compile(QueryContext& context, Dictionary<DataTypes::String, table_id_t>& aliasesDict);
        [[nodiscard]] Errors::CompilationStatus CompileWhereClause(QueryContext& context, CompilationScope& compilationScope);
        [[nodiscard]] static LogicalPlan* BuildTableScanPlan(
            const QueryContext& context,
            DataSource* table,
            const PredicatePushDownResult& predicatesResult
        );
        [[nodiscard]] LogicalPlan* BuildJoinsPlan(
            const QueryContext& context,
            const JoinOrderAnalyzeResult& joinReorderResult,
            const PredicatePushDownResult& predicatesResult
        ) const;
        [[nodiscard]] LogicalPlan* BuildOrderByStatement(
            const QueryContext& context,
            LogicalPlan* current,
            const Dictionary<DataTypes::String, column_index_t>& postProjectionIndicesDictionary
        ) const;
        [[nodiscard]] Errors::CompilationStatus CompileDerived(QueryContext& context) override;
        [[nodiscard]] constexpr Security::Permission RequiredPermissions() const override;
        [[nodiscard]] LogicalPlan* ToLogical(QueryContext& context) override;
    };

    struct CreateDbStatement final : Statement {
        DataTypes::String name;

        Errors::CompilationStatus CompileDerived(QueryContext& context) override;
        [[nodiscard]] constexpr Security::Permission RequiredPermissions() const override;
        LogicalPlan* ToLogical(QueryContext& context) override;
    };

    struct DropDbStatement final : Statement {
        DataTypes::String name;

        Errors::CompilationStatus CompileDerived(QueryContext& context) override;
        [[nodiscard]] constexpr Security::Permission RequiredPermissions() const override;
        LogicalPlan* ToLogical(QueryContext& context) override;
    };

    struct UseDatabaseStatement final : Statement {
        DataTypes::String name;

        Errors::CompilationStatus CompileDerived(QueryContext& context) override;
        [[nodiscard]] constexpr Security::Permission RequiredPermissions() const override;
        LogicalPlan* ToLogical(QueryContext& context) override;
    };

    struct InsertStatement final: Statement {
        DataStructures::PolymorphicArray<ColumnName> columns;
        DataStructures::PolymorphicArray<Inserts> values;
        DataStructures::PolymorphicArray<DataType> valueTypes;

        CoreEngine::StorageTypes::InsertPlan insertPlan;

        SelectStatement* selectStatement;

        [[nodiscard]] static Int InsertDefaultValue(
            const ::Memory::IAllocator* allocator,
            DataStructures::PolymorphicArray<Expressions::Expression*>& defaultExpressions,
            const Headers::ColumnHeader& header,
            Headers::DefaultValuesHeader& defaultValue
        );
        void InsertNullValues(const QueryContext& context, const Headers::ColumnHeader& header);
        [[nodiscard]] Errors::CompilationStatus ValidateReturnType(
            const QueryContext& context,
            CompilationScope& compilationScope,
            Expressions::Expression*& expression,
            const DataTypes::String& columnName
        ) const;
        [[nodiscard]] Errors::CompilationStatus ValidateSelectStatement(QueryContext& context, CompilationScope& compilationScope) const;
        [[nodiscard]] bool HasSelectStatement() const;

        [[nodiscard]] Errors::CompilationStatus ResolveAliases(QueryContext& context, CompilationScope& compilationScope);
        [[nodiscard]] Errors::CompilationStatus CompileDerived(QueryContext& context) override;

        [[nodiscard]] constexpr Security::Permission RequiredPermissions() const override;
        [[nodiscard]] LogicalPlan* ToLogical(QueryContext& context) override;
    };

    struct CreateSchemaStatement final : Statement {
        DataTypes::String name;

        Errors::CompilationStatus CompileDerived(QueryContext& context) override;
        [[nodiscard]] constexpr Security::Permission RequiredPermissions() const override;
        LogicalPlan* ToLogical(QueryContext& context) override;
    };

    struct UpdateColumn {
        Expressions::Expression* value;
        ColumnName name;

        UpdateColumn();
    };

    struct UpdateStatement final : Statement {
        DataStructures::PolymorphicArray<UpdateColumn*> updates;
        WhereClause where;

        [[nodiscard]] Errors::CompilationStatus ValidateReturnType(
            const QueryContext& context,
            CompilationScope& compilationScope,
            const UpdateColumn* update
        ) const;
        Errors::CompilationStatus ResolveAliases(
            QueryContext& context,
            CompilationScope& compilationScope
        );
        Errors::CompilationStatus CompileDerived(QueryContext& context) override;
        [[nodiscard]] constexpr Security::Permission RequiredPermissions() const override;
        LogicalPlan* ToLogical(QueryContext& context) override;
    };

    struct CreateIndexStatement final : Statement {
        DataTypes::String name;
        DataStructures::PolymorphicArray<DataTypes::String> columns;
        DataStructures::PolymorphicArray<column_index_t> columnIndices;
        bool isUnique;

        Errors::CompilationStatus CompileDerived(QueryContext& context) override;
        [[nodiscard]] constexpr Security::Permission RequiredPermissions() const override;
        LogicalPlan* ToLogical(QueryContext& context) override;
    };

    struct AlterTableStatement final : Statement {
        Constants::AlterTableType type;

        union {
            NewColumn* newColumn;
            AlterColumn* alterColumn;
            DropColumn* dropColumn;
            RenameColumn* renameColumn;
        } column;

        [[nodiscard]] Errors::CompilationStatus CompileAddColumn(
            const QueryContext& context,
            const Dictionary<DataTypes::String, Headers::ColumnHeader>& headers
        ) const;
        [[nodiscard]] Errors::CompilationStatus CompileAlterColumn(
            const QueryContext& context,
            const Dictionary<DataTypes::String, Headers::ColumnHeader>& headers
        ) const;
        [[nodiscard]] Errors::CompilationStatus CompileDropColumn(
            const QueryContext& context,
            const Dictionary<DataTypes::String, Headers::ColumnHeader>& headers
        ) const;
        [[nodiscard]] Errors::CompilationStatus CompileRenameColumn(
            const QueryContext& context,
            const Dictionary<DataTypes::String, Headers::ColumnHeader>& headers
        ) const;
        Errors::CompilationStatus CompileDerived(QueryContext& context) override;
        [[nodiscard]] constexpr Security::Permission RequiredPermissions() const override;
        LogicalPlan* ToLogical(QueryContext& context) override;
    };

    /**
     * @name Expression Compilation Functions
     * Functions to compile expressions, resolve their corresponding table,
     * assign ordinal positions in table, optimize etc.
     * @{
     */

    Errors::CompilationStatus CompileNode(
       QueryContext& context,
       Expressions::Expression*& slot,
       CompilationScope& compilationScope
    );

    // Fallback for nodes with nothing to bind. Taking the base pointer (a derived-to-base conversion)
    // and the most-const parameters guarantees every node-specific overload outranks it.
     Errors::CompilationStatus BindNode(
        const QueryContext& context,
        const Expressions::Expression* node,
        const CompilationScope& compilationScope
    );

     Errors::CompilationStatus BindNode(
        QueryContext& context,
        Expressions::BranchExpression* node,
        CompilationScope& compilationScope
    );

     Errors::CompilationStatus BindNode(
        QueryContext& context,
        Expressions::ColumnExpression* node,
        const CompilationScope& statementValidationScope
    );

     Errors::CompilationStatus BindNode(
        const QueryContext& context,
        Expressions::VariableExpression* node,
        const CompilationScope&
    );

     Errors::CompilationStatus BindNode(
        const QueryContext& context,
        ColumnName& column,
        CompilationScope& statementValidationScope
    );

     Errors::CompilationStatus BindNode(
        const QueryContext&,
        Expressions::ConstantExpression* node,
        CompilationScope&
    );

     Errors::CompilationStatus BindNode(
        QueryContext& context,
        Expressions::JsonExpression* node,
        const CompilationScope& compilationScope
    );

     Errors::CompilationStatus CompileColumnWhenTableAliasExists(
        QueryContext& context,
        Expressions::ColumnExpression* column,
        const CompilationScope& statementValidationScope
    );

     Errors::CompilationStatus CompileColumnWhenNoTableAliasExists(
        QueryContext& context,
        Expressions::ColumnExpression* column,
        const CompilationScope& statementValidationScope
    );

     bool ValidateExpressionCoercionTypes(
        const Expressions::Expression* left,
        const Expressions::Expression* right
    );

     bool ValidateExpressionCoercionTypes(
        DataType type,
        const Expressions::Expression* expression
    );

     Errors::CompilationStatus CompileWildcard(
        QueryContext& context,
        const Expressions::ColumnExpression* column,
        const CompilationScope& compilationScope,
        SelectStatement* statement
    );

     void AssignColumnsFromWildCardExpression(
        QueryContext& context,
        const Dictionary<DataTypes::String, Headers::ColumnHeader>& columnsDict,
        const DataTypes::String& tableAlias,
        const CompilationScope& statementValidationScope,
        DataStructures::PolymorphicArray<Expressions::Expression*>& results,
        UnsignedSmallInt slotIndex
    );

    /** @} End of Expression Compilation Functions */

    /**
     * @name Type Checking Functions
     * Functions to validate the types of each expression tree
     * @{
     */

    // Fallback, see BindNode
    Errors::CompilationStatus TypeCheckNode(
        const QueryContext& context,
        const Expressions::Expression* node,
        Expressions::Expression*& slot
    );

    Errors::CompilationStatus TypeCheckNode(
        const QueryContext& context,
        Expressions::BinaryExpression* node,
        Expressions::Expression*& slot
    );

    Errors::CompilationStatus TypeCheckNode(
        const QueryContext& context,
        const Expressions::LogicalExpression* node,
        Expressions::Expression*&
    );

    Errors::CompilationStatus TypeCheckNode(
        const QueryContext& context,
        const Expressions::CastExpression* node,
        Expressions::Expression*& slot
    );

    Errors::CompilationStatus TypeCheckNode(
        const QueryContext& context,
        Expressions::BranchExpression* node,
        Expressions::Expression*& slot
    );

    Errors::CompilationStatus TypeCheckNode(
        const QueryContext& context,
        const Expressions::FunctionExpression* node,
        Expressions::Expression*& slot
    );

    /** @} End of Type Checking Functions */

    /**
     * @name Folding-Optimization Functions
     * Functions to optimize and pre-evaluate if can, expressions to reduce runtime overhead
     * @{
     */

    // Fallback, see BindNode
    Errors::CompilationStatus FoldNode(
        const QueryContext& context,
        const Expressions::Expression* node,
        Expressions::Expression*& slot
    );

    Errors::CompilationStatus FoldNode(
        const QueryContext& context,
        const Expressions::BinaryExpression* node,
        Expressions::Expression*& slot
    );

    Errors::CompilationStatus FoldNode(
        const QueryContext& context,
        Expressions::LogicalExpression* node,
        Expressions::Expression*& slot
    );

    Errors::CompilationStatus FoldNode(
        const QueryContext& context,
        const Expressions::BranchExpression* node,
        Expressions::Expression*& slot
    );

    Errors::CompilationStatus FoldNode(
        const QueryContext& context,
        const Expressions::CastExpression* node,
        Expressions::Expression*& slot
    );

    void FoldToConstant(
        const QueryContext& context,
        Expressions::Expression*& slot
    );

    /** @} End of Folding-Optimization Functions */

    /**
     * @name Propagation-Optimization Functions
     * Functions to propagate child expressions as parents if can be
     * @{
     */

    static void PropagateExpression(Expressions::Expression*& expression, Expressions::Expression*& childExpr);

    static bool TryPropagateChildExpression(
        Expressions::Expression*& expression,
        Expressions::Expression*& leftExpr,
        Expressions::Expression*& rightExpr,
        bool dominantValue
    );

    /** @} End of Propagation-Optimization Functions */

    /**
     * @name Evaluation-Optimization Functions
     * Functions to evaluate expressions to constants
     * @{
     */

    static void EvaluateExpression(const QueryContext& context, Expressions::Expression*& expression);

    static void AssignConstantToExpression(const QueryContext& context, Expressions::Expression*& expression);

    /** @} End of Evaluation-Optimization Functions */

    /**
     * @name Index Assignment Functions
     * Functions to assign column's ordinal position in the table
     * @{
     */

    static void AssignColumnIndicesToExpression(
        const Dictionary<Int, column_index_t>& columnIndicesDictionary,
        Expressions::Expression* expression
    );

    /** @} End of Index Assignment Functions */

    /**
     * @name Post Projection Alias Resolvement Functions
     * Functions to resolve aliases used in post projection statements (order by)
     * @{
     */

    static Errors::CompilationStatus ResolveOrderByExpression(
        const QueryContext& context,
        OrderColumn* column,
        const Dictionary<DataTypes::String, Int>& aliasesDictionary,
        Int visibleProjectionCount
    );

    /** @} End of Post Projection Alias Resolvement Functions */

    /**
     * @name Post Projection Index Assignment Functions
     * Functions to assign expression's index from the results to the post projection statements (order by)
     * @{
     */

    static void AssignPostProjectionIndicesToExpression(
        const Dictionary<DataTypes::String, column_index_t>& columnIndicesDictionary,
        Expressions::Expression* expression
    );

    /** @} End of Post Projection Index Assignment Functions */

    /**
     * @name Helper Functions
     * Functions to construct helper error messages etc.
     * @{
     */

    Errors::CompilationStatus ClauseCannotBeEvaluatedToBool(const QueryContext& context, DataType type);

    void InsertCastExpression(
        const QueryContext& context,
        Expressions::Expression*& expression,
        DataType type
    );

    bool IsNullConstant(const Expressions::Expression* expression);

    Expressions::Expression* TypedNull(const QueryContext& context, DataType type);

    template<typename Pass>
    Errors::CompilationStatus WalkExpressionPostOrder(QueryContext& context, Expressions::Expression*& slot, Pass&& pass){
        if (slot == nullptr)
            return Errors::CompilationStatus::Ok();
        if (slot->expressionType == Expressions::ExpressionType::Expression)
            return Errors::CompilationStatus::Error(Messages::UNKNOWN_OPERATION, context.GetAllocator());

        return Expressions::VisitNode(slot->expressionType, [&]<typename TNode>(){
            auto* node = slot->As<TNode>();
            auto status = Errors::CompilationStatus::Ok();

            node->ForEachChild([&]<typename TChildType>(TChildType& child){
                if (!status.IsOk())
                    return;

                if constexpr (std::same_as<std::remove_cvref_t<TChildType>, Expressions::Expression*>)
                    status = WalkExpressionPostOrder(context, child, pass);
                else{
                    // Json's columnPtr is a ColumnExpression*: it gets visited but can never be replaced
                    Expressions::Expression* view = child;
                    status = WalkExpressionPostOrder(context, view, pass);
                    assert(view == child && "a pass replaced Json's anchored column");
                }
            });

            if (!status.IsOk())
                return status;

            return pass(node, slot);
        });
    }

    /** @} End of Helper Functions */
}