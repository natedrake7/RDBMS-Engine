#pragma once
#include "../../CoreEngine/include/DatabaseConstants.h"
#include "../../CoreEngine/include/Evaluators/Expression.h"
#include "../../Systemic/include/DataTypes/Variable.h"
#include "../../Systemic/include/DataTypes/Guid.h"
#include "../../Server/include/Security/Security.h"
#include "../../CoreEngine/include/Errors.h"
#include "../../Systemic/include/Headers.h"
#include "../../CoreEngine/include/DataStorage/SerializedRow.h"

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

    struct StatementValidationScope {
        Dictionary<DataTypes::String, UnsignedSmallInt> _tableAliasesDict;
        DataStructures::PolymorphicArray<Dictionary<DataTypes::String, Headers::ColumnHeader>> _tableColumnsArray;
        int* _indexPos;
        Statement* _statement;

        StatementValidationScope(const ::Memory::IAllocator* allocator, UnsignedSmallInt numberOfTables);
        StatementValidationScope(
            Dictionary<DataTypes::String, UnsignedSmallInt>& tableAliasesDictionary,
            DataStructures::PolymorphicArray<Dictionary<DataTypes::String, Headers::ColumnHeader>>& tableColumnsArray,
            Statement* statement,
            int* indexPos = nullptr
        );
        StatementValidationScope(
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

        [[nodiscard]] Errors::ValidationStatus Validate(const QueryContext& context) const;
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
        [[nodiscard]] Errors::ValidationStatus Compile(
            const QueryContext& context,
            Int databaseId,
            UnsignedSmallInt& outSlotCount
        );
        [[nodiscard]] Errors::ValidationStatus ValidateTableCreate(const QueryContext& context, Int selectedDatabaseId);
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

        virtual Errors::ValidationStatus CompileDerived(QueryContext& context) = 0;
        [[nodiscard]] virtual constexpr Security::Permission RequiredPermissions() const = 0;
        [[nodiscard]] Errors::ValidationStatus CompileBase(const QueryContext& context) const;
        [[nodiscard]] Errors::ValidationStatus Compile(QueryContext& context);
        virtual LogicalPlan* ToLogical(QueryContext& context) = 0;
    };

    struct DeclareVariableStatement final : Statement {
        Variable variable;
        Expressions::Expression* expression;
        DataType type;

        DeclareVariableStatement();

        Errors::ValidationStatus CompileDerived(QueryContext& context) override;
        [[nodiscard]] constexpr Security::Permission RequiredPermissions() const override;
        LogicalPlan* ToLogical(QueryContext& context) override;
    };

    struct SetVariableStatement final : Statement {
        Variable variable;
        Expressions::Expression* expression;
        DataType type;

        SetVariableStatement();

        Errors::ValidationStatus CompileDerived(QueryContext& context) override;
        [[nodiscard]] constexpr Security::Permission RequiredPermissions() const override;
        LogicalPlan* ToLogical(QueryContext& context) override;
    };

    struct CreateUserStatement final : Statement {
        DataTypes::String username;
        DataTypes::String password;
        DataTypes::String role;

        Errors::ValidationStatus CompileDerived(QueryContext& context) override;
        constexpr Security::Permission RequiredPermissions() const override;
        LogicalPlan* ToLogical(QueryContext& context) override;
    };

    struct GrantRoleStatement final : Statement {
        DataTypes::String username;
        DataTypes::String role;

        Errors::ValidationStatus CompileDerived(QueryContext& context) override;
        constexpr Security::Permission RequiredPermissions() const override;
        LogicalPlan* ToLogical(QueryContext& context) override;
    };

    struct DeleteStatement final : Statement {
        WhereClause where;

        Errors::ValidationStatus CompileDerived(QueryContext& context) override;
        constexpr Security::Permission RequiredPermissions() const override;
        LogicalPlan* ToLogical(QueryContext& context) override;
    };

    struct JoinStatement final : Statement {
        Expressions::Expression* expression;
        JoinType type;

        JoinStatement();
        [[nodiscard]] Errors::ValidationStatus CompileDerived(QueryContext& context) override;
        [[nodiscard]] Errors::ValidationStatus Compile(QueryContext& context, Int databaseId);

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

        [[nodiscard]] Errors::ValidationStatus CompileSchema(const QueryContext& context) const;
        Errors::ValidationStatus CompileColumnExpression(
            const QueryContext& context,
            NewColumn*& column,
            Dictionary<DataTypes::String, column_index_t>& columnNamesToIndexes,
            bool& primaryKeyFound,
            column_index_t& index
        );
        Errors::ValidationStatus CompileDerived(QueryContext& context) override;
        LogicalPlan* ToLogical(QueryContext& context) override;
        [[nodiscard]] constexpr Security::Permission RequiredPermissions() const override;
    };

    struct SelectStatement final : Statement {
        DataStructures::PolymorphicArray<Headers::ColumnHeader> columnHeaders;
        DataStructures::PolymorphicArray<Expressions::Expression*> _projections;
        DataStructures::PolymorphicArray<JoinStatement*> _joins;
        OrderByStatement* orderBy;
        WhereClause where;
        BigInt top;
        bool distinct;

        explicit SelectStatement(const ::Memory::IAllocator* allocator);

        [[nodiscard]] Dictionary<DataTypes::String, column_index_t> CreatePostProjectionIndicesDictionary() const;
        [[nodiscard]] bool HasTopStatement() const;
        [[nodiscard]] bool HasJoins() const;
        [[nodiscard]] bool HasWhere() const;
        [[nodiscard]] bool IsConstant() const;
        [[nodiscard]] Errors::ValidationStatus CompileNoTableStatement(QueryContext& context);
        [[nodiscard]] Errors::ValidationStatus Compile(QueryContext& context, Dictionary<DataTypes::String, table_id_t>& aliasesDict);
        [[nodiscard]] Errors::ValidationStatus CompileWhereClause(QueryContext& context, StatementValidationScope& statementValidationScope);
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
        void BuildOrderByStatement(LogicalPlan*& current, const Dictionary<DataTypes::String, column_index_t>& postProjectionIndicesDictionary) const;
        [[nodiscard]] Errors::ValidationStatus CompileDerived(QueryContext& context) override;
        [[nodiscard]] constexpr Security::Permission RequiredPermissions() const override;
        [[nodiscard]] LogicalPlan* ToLogical(QueryContext& context) override;
    };

    struct CreateDbStatement final : Statement {
        DataTypes::String name;

        Errors::ValidationStatus CompileDerived(QueryContext& context) override;
        [[nodiscard]] constexpr Security::Permission RequiredPermissions() const override;
        LogicalPlan* ToLogical(QueryContext& context) override;
    };

    struct DropDbStatement final : Statement {
        DataTypes::String name;

        Errors::ValidationStatus CompileDerived(QueryContext& context) override;
        [[nodiscard]] constexpr Security::Permission RequiredPermissions() const override;
        LogicalPlan* ToLogical(QueryContext& context) override;
    };

    struct UseDatabaseStatement final : Statement {
        DataTypes::String name;

        Errors::ValidationStatus CompileDerived(QueryContext& context) override;
        [[nodiscard]] constexpr Security::Permission RequiredPermissions() const override;
        LogicalPlan* ToLogical(QueryContext& context) override;
    };

    struct InsertStatement final: Statement {
        DataStructures::PolymorphicArray<ColumnName> columns;
        DataStructures::PolymorphicArray<Inserts> values;

        CoreEngine::StorageTypes::InsertPlan insertPlan;

        SelectStatement* selectStatement;

        [[nodiscard]] static Int InsertDefaultValue(
            const ::Memory::IAllocator* allocator,
            DataStructures::PolymorphicArray<Expressions::Expression*>& defaultExpressions,
            const Headers::ColumnHeader& header,
            Headers::DefaultValuesHeader& defaultValue
        );
        void InsertNullValues(const QueryContext& context, const Headers::ColumnHeader& header);
        [[nodiscard]] Errors::ValidationStatus ValidateReturnType(
            const QueryContext& context,
            StatementValidationScope& validationScope,
            Expressions::Expression*& expression,
            const DataTypes::String& columnName
        ) const;
        [[nodiscard]] Errors::ValidationStatus ValidateSelectStatement(QueryContext& context, StatementValidationScope& validationScope) const;
        [[nodiscard]] bool HasSelectStatement() const;

        [[nodiscard]] Errors::ValidationStatus ResolveAliases(QueryContext& context, StatementValidationScope& validationScope);
        [[nodiscard]] Errors::ValidationStatus CompileDerived(QueryContext& context) override;

        [[nodiscard]] constexpr Security::Permission RequiredPermissions() const override;
        [[nodiscard]] LogicalPlan* ToLogical(QueryContext& context) override;
    };

    struct CreateSchemaStatement final : Statement {
        DataTypes::String name;

        Errors::ValidationStatus CompileDerived(QueryContext& context) override;
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

        [[nodiscard]] Errors::ValidationStatus ValidateReturnType(
            const QueryContext& context,
            StatementValidationScope& validationScope,
            const UpdateColumn* update
        ) const;
        Errors::ValidationStatus ResolveAliases(
            QueryContext& context,
            StatementValidationScope& validationScope
        );
        Errors::ValidationStatus CompileDerived(QueryContext& context) override;
        [[nodiscard]] constexpr Security::Permission RequiredPermissions() const override;
        LogicalPlan* ToLogical(QueryContext& context) override;
    };

    struct CreateIndexStatement final : Statement {
        DataTypes::String name;
        DataStructures::PolymorphicArray<DataTypes::String> columns;
        DataStructures::PolymorphicArray<column_index_t> columnIndices;
        bool isUnique;

        Errors::ValidationStatus CompileDerived(QueryContext& context) override;
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

        [[nodiscard]] Errors::ValidationStatus CompileAddColumn(
            const QueryContext& context,
            const Dictionary<DataTypes::String, Headers::ColumnHeader>& headers
        ) const;
        [[nodiscard]] Errors::ValidationStatus CompileAlterColumn(
            const QueryContext& context,
            const Dictionary<DataTypes::String, Headers::ColumnHeader>& headers
        ) const;
        [[nodiscard]] Errors::ValidationStatus CompileDropColumn(
            const QueryContext& context,
            const Dictionary<DataTypes::String, Headers::ColumnHeader>& headers
        ) const;
        [[nodiscard]] Errors::ValidationStatus CompileRenameColumn(
            const QueryContext& context,
            const Dictionary<DataTypes::String, Headers::ColumnHeader>& headers
        ) const;
        Errors::ValidationStatus CompileDerived(QueryContext& context) override;
        [[nodiscard]] constexpr Security::Permission RequiredPermissions() const override;
        LogicalPlan* ToLogical(QueryContext& context) override;
    };

    /**
     * @name Expression Compilation Functions
     * Functions to compile expressions, resolve their corresponding table,
     * assign ordinal positions in table, optimize etc.
     * @{
     */

    static Errors::ValidationStatus CompileExpression(
        QueryContext& context,
        Expressions::Expression*& expression
    );

    static Errors::ValidationStatus CompileExpression(
        QueryContext& context,
        StatementValidationScope& statementValidationScope,
        Expressions::Expression*& expression
    );

    static Errors::ValidationStatus CompileBinaryExpression(
        QueryContext& context,
        Expressions::BinaryExpression* binaryExpr,
        Expressions::Expression*& expression
    );

    static Errors::ValidationStatus CompileBinaryExpression(
        QueryContext& context,
        Expressions::BinaryExpression* binaryExpr,
        Expressions::Expression*& expression,
        StatementValidationScope& statementValidationScope
    );

    static Errors::ValidationStatus CompileLogicalExpression(
        QueryContext& context,
        Expressions::LogicalExpression* logicalExpr,
        Expressions::Expression*& expression
    );

    static Errors::ValidationStatus CompileLogicalExpression(
        QueryContext& context,
        Expressions::LogicalExpression* logicalExpr,
        Expressions::Expression*& expression,
        StatementValidationScope& statementValidationScope
    );

    static Errors::ValidationStatus CompileFunctionExpression(
        QueryContext& context,
        const Expressions::FunctionExpression* funcExpr,
        Expressions::Expression*& expression
    );

    static Errors::ValidationStatus CompileFunctionExpression(
        QueryContext& context,
        const Expressions::FunctionExpression* funcExpr,
        Expressions::Expression*& expression,
        StatementValidationScope& statementValidationScope
    );

    static Errors::ValidationStatus CompileBranchExpression(
        QueryContext& context,
        Expressions::BranchExpression* branchExpr,
        Expressions::Expression*& expression,
        StatementValidationScope& statementValidationScope
    );

    static Errors::ValidationStatus CompileBranchExpression(
        QueryContext& context,
        Expressions::BranchExpression* branchExpr,
        Expressions::Expression*& expression
    );

    static Errors::ValidationStatus CompileColumnExpression(
        const QueryContext& context,
        const Expressions::ColumnExpression* columnExpr
    );

    static Errors::ValidationStatus CompileColumnExpression(
        const QueryContext& context,
        Expressions::ColumnExpression* column,
        const StatementValidationScope& statementValidationScope
    );

    static Errors::ValidationStatus CompileVariableExpression(
        const QueryContext& context,
        Expressions::VariableExpression* variableExpr
    );

    static Errors::ValidationStatus CompileColumnExpression(
        const QueryContext& context,
        ColumnName& column,
        StatementValidationScope& statementValidationScope
    );

    static Errors::ValidationStatus CompileConstantExpression(
        Expressions::ConstantExpression* constantExpr
    );

    static Errors::ValidationStatus CompileJsonExpression(
        const QueryContext& context,
        const Expressions::JsonExpression* jsonExpr,
        const StatementValidationScope& statementValidationScope
    );

    static Errors::ValidationStatus CompileJsonExpression(
        const QueryContext& context,
        const Expressions::JsonExpression* jsonExpr
    );

    static Errors::ValidationStatus CompileCastExpression(
        QueryContext& context,
        Expressions::CastExpression* castExpr,
        Expressions::Expression*& expression
    );

    static Errors::ValidationStatus CompileCastExpression(
        QueryContext& context,
        Expressions::CastExpression* castExpr,
        Expressions::Expression*& expression,
        StatementValidationScope& statementValidationScope
    );

    static Errors::ValidationStatus CompileColumnWhenTableAliasExists(
        const QueryContext& context,
        Expressions::ColumnExpression* column,
        const StatementValidationScope& statementValidationScope
    );

    static Errors::ValidationStatus CompileColumnWhenNoTableAliasExists(
        const QueryContext& context,
        Expressions::ColumnExpression* column,
        const StatementValidationScope& statementValidationScope
    );

    static bool ValidateExpressionCoercionTypes(
        const Expressions::Expression* left,
        const Expressions::Expression* right
    );

    static bool ValidateExpressionCoercionTypes(
        DataType type,
        const Expressions::Expression* expression
    );

    static Errors::ValidationStatus CompileWildcard(
        const QueryContext& context,
        const Expressions::ColumnExpression* column,
        const StatementValidationScope& validationScope,
        SelectStatement* statement
    );

    static void AssignColumnsFromWildCardExpression(
        const QueryContext& context,
        const Dictionary<DataTypes::String, Headers::ColumnHeader>& columnsDict,
        const DataTypes::String& tableAlias,
        const StatementValidationScope& statementValidationScope,
        DataStructures::PolymorphicArray<Expressions::Expression*>& results,
        UnsignedSmallInt slotIndex
    );

    /** @} End of Expression Compilation Functions */

    /**
     * @name Folding-Optimization Functions
     * Functions to optimize and pre-evaluate if can, expressions to reduce runtime overhead
     * @{
     */

    static void FoldExpression(const QueryContext& context, Expressions::Expression*& expression);

    static void FoldBinaryExpression(const QueryContext& context, Expressions::Expression*& expression);

    static void FoldLogicalExpression(const QueryContext& context, Expressions::Expression*& expression);

    static void FoldFunctionExpression(const QueryContext& context, Expressions::Expression*& expression);

    static void FoldBranchExpression(const QueryContext& context, Expressions::Expression*& expression);

    static void FoldCastExpression(const QueryContext& context, Expressions::Expression*& expression);

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

    static void AssignColumnIndicesToBinaryExpression(
        const Dictionary<Int, column_index_t>& columnIndicesDictionary,
        const Expressions::BinaryExpression* expression
    );

    static void AssignColumnIndicesToLogicalExpression(
        const Dictionary<Int, column_index_t>& columnIndicesDictionary,
        const Expressions::LogicalExpression* expression
    );

    static void AssignColumnIndicesToBranchExpression(
        const Dictionary<Int, column_index_t>& columnIndicesDictionary,
        const Expressions::BranchExpression* expression
    );

    static void AssignColumnIndicesToFunctionExpression(
        const Dictionary<Int, column_index_t>& columnIndicesDictionary,
        const Expressions::FunctionExpression* expression
    );

    static void AssignColumnIndicesToColumnExpression(
        const Dictionary<Int, column_index_t>& columnIndicesDictionary,
        Expressions::ColumnExpression* expression
    );

    static void AssignColumnIndicesToCastExpression(
        const Dictionary<Int, column_index_t>& columnIndicesDictionary,
        const Expressions::CastExpression* castExpr
    );

    /** @} End of Index Assignment Functions */

    /**
     * @name Post Projection Alias Resolvement Functions
     * Functions to resolve aliases used in post projection statements (order by)
     * @{
     */

    static Errors::ValidationStatus CompilePostProjectionExpression(
        const QueryContext& context,
        Expressions::Expression* expression,
        const Dictionary<DataTypes::String, const Expressions::Expression*>& postProjectionAliases
    );

    static Errors::ValidationStatus CompilePostProjectionColumnExpression(
        const QueryContext& context,
        Expressions::ColumnExpression* column,
        const Dictionary<DataTypes::String, const Expressions::Expression*>& postProjectionAliases
    );

    static Errors::ValidationStatus CompilePostProjectionBinaryExpression(
        const QueryContext& context,
        const Expressions::BinaryExpression* expression,
        const Dictionary<DataTypes::String, const Expressions::Expression*>& postProjectionAliases
    );

    static Errors::ValidationStatus CompilePostProjectionLogicalExpression(
        const QueryContext& context,
        const Expressions::LogicalExpression* expression,
        const Dictionary<DataTypes::String, const Expressions::Expression*>& postProjectionAliases
    );

    static Errors::ValidationStatus CompilePostProjectionFunctionExpression(
        const QueryContext& context,
        const Expressions::FunctionExpression* expression,
        const Dictionary<DataTypes::String, const Expressions::Expression*>& postProjectionAliases
    );

    static Errors::ValidationStatus CompilePostProjectionBranchExpression(
        const QueryContext& context,
        const Expressions::BranchExpression* expression,
        const Dictionary<DataTypes::String, const Expressions::Expression*>& postProjectionAliases
    );

    static Errors::ValidationStatus CompilePostProjectionJsonExpression(
        const QueryContext& context,
        const Expressions::JsonExpression* expression,
        const Dictionary<DataTypes::String, const Expressions::Expression*>& postProjectionAliases
    );

    static Errors::ValidationStatus CompilePostProjectionCastExpression(
        const QueryContext& context,
        const Expressions::CastExpression* castExpr,
        const Dictionary<DataTypes::String, const Expressions::Expression*>& postProjectionAliases
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

    static void AssignPostProjectionIndicesToBinaryExpression(
        const Dictionary<DataTypes::String, column_index_t>& columnIndicesDictionary,
        const Expressions::BinaryExpression* expression
    );

    static void AssignPostProjectionIndicesToLogicalExpression(
        const Dictionary<DataTypes::String, column_index_t>& columnIndicesDictionary,
        const Expressions::LogicalExpression* expression
    );

    static void AssignPostProjectionIndicesToFunctionExpression(
        const Dictionary<DataTypes::String, column_index_t>& columnIndicesDictionary,
        const Expressions::FunctionExpression* expression
    );

    static void AssignPostProjectionIndicesToBranchExpression(
        const Dictionary<DataTypes::String, column_index_t>& columnIndicesDictionary,
        const Expressions::BranchExpression* expression
    );

    static void AssignPostProjectionIndicesToColumnExpression(
        const Dictionary<DataTypes::String, column_index_t>& columnIndicesDictionary,
        Expressions::ColumnExpression* expression
    );

    /** @} End of Post Projection Index Assignment Functions */

    /**
     * @name Helper Functions
     * Functions to construct helper error messages etc.
     * @{
     */

    static Errors::ValidationStatus ClauseCannotBeEvaluatedToBool(const QueryContext& context, DataType type);

    static void InsertCastExpression(
        const QueryContext& context,
        Expressions::Expression*& expression,
        DataType type
    );

    /** @} End of Helper Functions */
}