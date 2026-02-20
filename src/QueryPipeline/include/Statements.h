#pragma once
#include <string>
#include <vector>
#include "../../DatabaseEngine/include/DatabaseConstants.h"
#include "../../DatabaseEngine/include/Evaluators/Expression.h"
#include "../../Systemic/include/DataTypes/Variable.h"
#include "../../Systemic/include/DataTypes/Guid.h"
#include "../../Systemic/include/Security/Security.h"
#include "../../Systemic/include/Errors.h"
#include "../../Systemic/include/Headers.h"

namespace DatabaseEngine{
    class SystemCatalog;
}

namespace QueryPipeline {
    struct CompileResult;
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
    std::string name;
    std::string alias;

    Int tableId;
    Int columnId;
    column_index_t index;
    DataType returnType;
  };

  struct StatementValidationScope {
    const Dictionary<std::string, table_id_t>* tableAliasesDictionary;
    Dictionary<int, Dictionary<std::string, Headers::ColumnHeader>>* tablesColumnsDictionary;
    int* indexPos;
    Statement* statement;

    StatementValidationScope(
      const Dictionary<std::string, table_id_t>& tableAliasesDictionary,
      Dictionary<int, Dictionary<std::string, Headers::ColumnHeader>>& tablesColumnsDictionary,
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
        std::string name;
        int size;

        DecimalType decimal;


        ColumnType();
        explicit ColumnType(const std::string& name);
        ColumnType(const std::string& name, Int size);
        ColumnType(std::string& name, Int size);
        ColumnType(const std::string& name, DecimalType decimal);
        ColumnType(std::string& name, DecimalType decimal);
    };

  struct Identity{
    uint16_t seed;
    uint16_t incrementFactor;
    int64_t cacheBlock;

    Identity() = default;
    ~Identity() = default;

    [[nodiscard]] Errors::ValidationStatus Validate() const;
  };

  struct NewColumn {
    ColumnName name;
    ColumnType type;
    Identity* identity;
    Value defaultValue;

    bool isPrimaryKey;
    bool isNullable;

    column_index_t index;

    [[nodiscard]] bool HasIdentity()const;
  };

  struct OrderColumn {
    Expressions::Expression* expression;
    OrderType type;

    OrderColumn();
    ~OrderColumn();
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
    std::string name;
    std::vector<ColumnName> columns;
  };

  struct WhereClause{
    Expressions::Expression* expression;

    WhereClause();
    [[nodiscard]] bool IsValid() const;
  };

  struct OrderByStatement{
    std::vector<OrderColumn*> columns;

    ~OrderByStatement();
    bool Validate(const std::vector<OrderColumn*>& selectColumns, const Dictionary<std::string, Headers::ColumnHeader>& columnsDict);
  };

//can be a table a view or a subquery or a function returning a table literally many things
//add inheritance
  struct DataSource {
    std::string database;
    std::string schema;
    std::string name;
    std::string alias;

    Int databaseId;
    Int tableId;
    Int schemaId;
    int16_t ordinalPosition;

    Network::Server* server;
    DatabaseEngine::SystemCatalog* catalog;

    DataSource();
    [[nodiscard]] std::string GetAlias() const;
    [[nodiscard]] std::string GetFullName()const;
    [[nodiscard]] Errors::ValidationStatus Validate(Int selectedDatabaseId);
    [[nodiscard]] Errors::ValidationStatus ValidateTableCreate(Int selectedDatabaseId);
  };

  struct SubQuery : DataSource {
    SelectStatement* statement;
  };

  struct Inserts {
    std::vector<Expressions::Expression*> values;
  };

  struct Statement {
    Dictionary<Int, Dictionary<std::string, Headers::ColumnHeader>> tableColumnsDictionary;
    DataTypes::Guid sessionId;
    Network::Server* server;
    DatabaseEngine::SystemCatalog* catalog;
    DataSource* table;
    Int databaseId;

    Statement();
    virtual ~Statement() = default;

    virtual Errors::ValidationStatus CompileDerived(CompileResult& context) = 0;
    virtual constexpr Security::Permission RequiredPermissions()const = 0;
    Errors::ValidationStatus CompileBase()const;
    Errors::ValidationStatus Compile(CompileResult& context);
    virtual LogicalPlan* ToLogical(CompileResult& context) = 0;
  };

  struct DeclareVariableStatement final: Statement {
    Variable variable;

    Expressions::Expression* expression;

    DeclareVariableStatement();

    Errors::ValidationStatus CompileDerived(CompileResult& context) override;
    constexpr Security::Permission RequiredPermissions()const override;
    LogicalPlan* ToLogical(CompileResult& context) override;
  };

  struct SetVariableStatement final: Statement {
    Variable variable;
    Expressions::Expression* expression;

    SetVariableStatement();

    Errors::ValidationStatus CompileDerived(CompileResult& context) override;
    constexpr Security::Permission RequiredPermissions()const override;
    LogicalPlan* ToLogical(CompileResult& context) override;
  };

  struct CreateUserStatement final: Statement {
      std::string username;
      std::string password;
      std::string role;

      CreateUserStatement() = default;
      ~CreateUserStatement() override = default;

      Errors::ValidationStatus CompileDerived(CompileResult& context) override;
      constexpr Security::Permission RequiredPermissions()const override;
      LogicalPlan* ToLogical(CompileResult& context) override;
  };

  struct GrantRoleStatement final : Statement {
    std::string username;
    std::string role;

    GrantRoleStatement() = default;
    ~GrantRoleStatement() override = default;

    Errors::ValidationStatus CompileDerived(CompileResult& context) override;
    constexpr Security::Permission RequiredPermissions()const override;
    LogicalPlan* ToLogical(CompileResult& context) override;
  };

  struct DeleteStatement final : Statement {
    WhereClause where;

    ~DeleteStatement() override = default;

    Errors::ValidationStatus CompileDerived(CompileResult& context) override;
    constexpr Security::Permission RequiredPermissions()const override;
    LogicalPlan* ToLogical(CompileResult& context) override;
  };

  struct JoinStatement final : Statement{
    Expressions::Expression* expression;
    JoinType type;

    JoinStatement();
    [[nodiscard]]Errors::ValidationStatus CompileDerived(CompileResult& context) override;
    [[nodiscard]]Errors::ValidationStatus Validate(Int databaseId);

    [[nodiscard]]bool IsRightJoin()const;
    [[nodiscard]]bool IsInnerJoin()const;
    [[nodiscard]]bool IsFullOuterJoin()const;

    LogicalPlan* ToLogical(CompileResult& context)override;
    constexpr Security::Permission RequiredPermissions()const override;
  };

  struct CreateTableStatement final: Statement {
    std::vector<NewColumn*> columns;
    PrimaryKeyConstraint* constraint;
    std::vector<column_index_t> primaryKey;

    CreateTableStatement();
    ~CreateTableStatement() override;

    Errors::ValidationStatus CompileSchema(CompileResult& context) const;
    Errors::ValidationStatus CompileColumnExpression(
      NewColumn*& column,
      Dictionary<std::string, column_index_t>& columnNamesToIndexes,
      bool& primaryKeyFound,
      column_index_t& index
    );
    Errors::ValidationStatus CompileDerived(CompileResult& context) override;
    LogicalPlan* ToLogical(CompileResult& context) override;
    constexpr Security::Permission RequiredPermissions()const override;
  };

  struct SelectStatement final: Statement{
    std::vector<Headers::ColumnHeader> columnHeaders;
    std::vector<Expressions::Expression*> results;
    std::vector<JoinStatement*> joins;
    OrderByStatement* orderBy;
    WhereClause where;
    BigInt top;
    bool distinct;

    SelectStatement();
    ~SelectStatement() override;

    [[nodiscard]] Dictionary<std::string, column_index_t> CreatePostProjectionIndicesDictionary()const;
    [[nodiscard]] bool HasTopStatement()const;
    [[nodiscard]] bool HasJoins()const;
    [[nodiscard]] bool HasWhere()const;
    [[nodiscard]] bool IsConstant()const;
    [[nodiscard]] Errors::ValidationStatus CompileNoTableStatement(CompileResult& context);
    [[nodiscard]] Errors::ValidationStatus Compile(CompileResult& context, Dictionary<std::string, table_id_t>& tableAliasesDictionary);
    [[nodiscard]] Errors::ValidationStatus CompileWhereClause(CompileResult& context, StatementValidationScope& statementValidationScope);
    [[nodiscard]] static LogicalPlan* BuildTableScanPlan(
        const CompileResult& context,
        DataSource* table,
        const PredicatePushDownResult& predicatesResult
    );
    LogicalPlan* BuildJoinsPlan(
        const CompileResult& context,
        const JoinOrderAnalyzeResult& joinReorderResult,
        const PredicatePushDownResult& predicatesResult
    ) const;
    [[nodiscard]] Dictionary<Int, column_index_t> BuildColumnsIndicesDictionary(const std::vector<table_id_t>& joinOrder)const;
    void AssignColumnsToIndices(const std::vector<table_id_t>& order)const;
    void BuildOrderByStatement(LogicalPlan*& current, const Dictionary<std::string, column_index_t>& postProjectionIndicesDictionary) const;
    [[nodiscard]] Errors::ValidationStatus CompileDerived(CompileResult& context) override;
    constexpr Security::Permission RequiredPermissions()const override;
    [[nodiscard]] LogicalPlan* ToLogical(CompileResult& context) override;
  };

  struct CreateDbStatement final : Statement{
    std::string name;

    Errors::ValidationStatus CompileDerived(CompileResult& context) override;
    constexpr Security::Permission RequiredPermissions()const override;
    LogicalPlan* ToLogical(CompileResult& context) override;
  };

  struct DropDbStatement final : Statement{
    std::string name;

    Errors::ValidationStatus CompileDerived(CompileResult& context) override;
    constexpr Security::Permission RequiredPermissions()const override;
    LogicalPlan* ToLogical(CompileResult& context) override;
  };

  struct UseDatabaseStatement final : Statement {
    std::string name;

    Errors::ValidationStatus CompileDerived(CompileResult& context) override;
    constexpr Security::Permission RequiredPermissions()const override;
    LogicalPlan* ToLogical(CompileResult& context) override;
  };

  struct InsertStatement final : Statement{
    std::vector<ColumnName> columns;
    std::vector<Inserts> values;

    std::vector<column_index_t> columnIndices;
    SelectStatement* selectStatement;

    ~InsertStatement() override;

    void InsertDefaultValuesForMissingColumns(
        const CompileResult& context,
        const Headers::ColumnHeader& header,
        const Headers::DefaultValuesHeader& defaultValue
    );
    void InsertNullValuesForMissingColumns(const Headers::ColumnHeader& header);
    [[nodiscard]] Errors::ValidationStatus ValidateReturnType(const Expressions::Expression* expression, const std::string& columnName)const;
    [[nodiscard]] Errors::ValidationStatus ValidateSelectStatement(CompileResult& context)const;
    [[nodiscard]] bool HasSelectStatement() const;
    [[nodiscard]] Errors::ValidationStatus ResolveAliases(CompileResult& context);
    [[nodiscard]] Errors::ValidationStatus CompileDerived(CompileResult& context) override;
    constexpr Security::Permission RequiredPermissions()const override;
    [[nodiscard]] LogicalPlan* ToLogical(CompileResult& context) override;
  };

  struct CreateSchemaStatement final : Statement {
    std::string name;

    Errors::ValidationStatus CompileDerived(CompileResult& context) override;
    constexpr Security::Permission RequiredPermissions()const override;
    LogicalPlan * ToLogical(CompileResult& context) override;
  };

  struct UpdateColumn{
    ColumnName name;
    Expressions::Expression* value;

    UpdateColumn();
    ~UpdateColumn();
  };

  struct UpdateStatement final : Statement {
    std::vector<UpdateColumn*> updates;
    WhereClause where;

    [[nodiscard]] Errors::ValidationStatus ValidateReturnType(const UpdateColumn* update)const;
    Errors::ValidationStatus ResolveAliases(CompileResult& context, Dictionary<std::string, table_id_t>& tableAliasesDictionary);
    Errors::ValidationStatus CompileDerived(CompileResult& context) override;
    constexpr Security::Permission RequiredPermissions()const override;
    LogicalPlan * ToLogical(CompileResult& context) override;
  };

  struct CreateIndexStatement final : Statement {
    std::string name;
    std::vector<std::string> columns;
    std::vector<column_index_t> columnIndices;
    bool isUnique;

    Errors::ValidationStatus CompileDerived(CompileResult& context) override;
    constexpr Security::Permission RequiredPermissions()const override;
    LogicalPlan * ToLogical(CompileResult& context) override;
  };

  struct AlterTableStatement final : Statement {
    AlterTableType type;

    union {
      NewColumn* newColumn;
      AlterColumn* alterColumn;
      DropColumn* dropColumn;
      RenameColumn* renameColumn;
    } column;

    [[nodiscard]] Errors::ValidationStatus CompileAddColumn(const Dictionary<std::string, Headers::ColumnHeader>& headers)const;
    [[nodiscard]] Errors::ValidationStatus CompileAlterColumn(const Dictionary<std::string, Headers::ColumnHeader>& headers)const;
    [[nodiscard]] Errors::ValidationStatus CompileDropColumn(const Dictionary<std::string, Headers::ColumnHeader>& headers)const;
    [[nodiscard]] Errors::ValidationStatus CompileRenameColumn(const Dictionary<std::string, Headers::ColumnHeader>& headers)const;
    Errors::ValidationStatus CompileDerived(CompileResult& context) override;
    constexpr Security::Permission RequiredPermissions()const override;
    LogicalPlan * ToLogical(CompileResult& context) override;
  };

  /**
   * @name Expression Compilation Functions
   * Functions to compile expressions, resolve their corresponding table
   * assign ordinal positions in table, optimize etc
   * @{
   */

  static Errors::ValidationStatus CompileExpression(
    CompileResult& context,
    Expressions::Expression*& expression
  );

  static Errors::ValidationStatus CompileExpression(
    CompileResult& context,
    StatementValidationScope& statementValidationScope,
    Expressions::Expression*& expression
  );

  static Errors::ValidationStatus CompileBinaryExpression(
    CompileResult& context,
    Expressions::BinaryExpression* binaryExpr,
    Expressions::Expression*& expression
  );

  static Errors::ValidationStatus CompileBinaryExpression(
    CompileResult& context,
    Expressions::BinaryExpression* binaryExpr,
    Expressions::Expression*& expression,
    StatementValidationScope& statementValidationScope
  );

  static Errors::ValidationStatus CompileLogicalExpression(
    CompileResult& context,
    Expressions::LogicalExpression* logicalExpr,
    Expressions::Expression*& expression
  );

  static Errors::ValidationStatus CompileLogicalExpression(
    CompileResult& context,
    Expressions::LogicalExpression* logicalExpr,
    Expressions::Expression*& expression,
    StatementValidationScope& statementValidationScope
  );

  static Errors::ValidationStatus CompileFunctionExpression(
    CompileResult& context,
    const Expressions::FunctionExpression* funcExpr,
    Expressions::Expression*& expression
  );

  static Errors::ValidationStatus CompileFunctionExpression(
    CompileResult& context,
    const Expressions::FunctionExpression* funcExpr,
    Expressions::Expression*& expression,
    StatementValidationScope& statementValidationScope
  );

  static Errors::ValidationStatus CompileBranchExpression(
    CompileResult& context,
    Expressions::BranchExpression* branchExpr,
    Expressions::Expression*& expression,
    StatementValidationScope& statementValidationScope
  );

  static Errors::ValidationStatus CompileBranchExpression(
    CompileResult& context,
    Expressions::BranchExpression* branchExpr,
    Expressions::Expression*& expression
  );

  static Errors::ValidationStatus CompileColumnExpression(const Expressions::ColumnExpression *columnExpr);

  static Errors::ValidationStatus CompileColumnExpression(
    Expressions::ColumnExpression* column,
    const StatementValidationScope& statementValidationScope
  );

  static Errors::ValidationStatus CompileVariableExpression(
    const CompileResult& context,
    Expressions::VariableExpression* variableExpr
  );

  static Errors::ValidationStatus CompileColumnExpression(
    ColumnName& column,
    StatementValidationScope& statementValidationScope
  );

  static Errors::ValidationStatus CompileConstantExpression(Expressions::ConstantExpression* literalExpr);

  static Errors::ValidationStatus CompileColumnWhenTableAliasExists(
    Expressions::ColumnExpression* column,
    const StatementValidationScope& statementValidationScope
  );

  static Errors::ValidationStatus CompileColumnWhenNoTableAliasExists(
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
    const Expressions::ColumnExpression* column,
    const StatementValidationScope& statementValidationScope,
    SelectStatement *statement
  );

  static void AssignColumnsFromWildCardExpression(
    const Dictionary<std::string, Headers::ColumnHeader> &columnsDict,
    const std::string& tableAlias,
    const StatementValidationScope& statementValidationScope,
    std::vector<Expressions::Expression*>& results
  );

  /** @} End of Expression Compilation Functions */

  /**
   * @name Folding-Optimization Functions
   * Functions to optimize and pre-evaluate if can, expressions to reduce runtime overhead
   * @{
   */

  static void FoldExpression(const CompileResult& context, Expressions::Expression*& expression);

  static void FoldExpression(const CompileResult& context, const Expressions::BinaryExpression* castExpr, Expressions::Expression*& expression);

  static void FoldExpression(const CompileResult& context, Expressions::LogicalExpression* castExpr, Expressions::Expression*& expression);

  static void FoldExpression(const CompileResult& context, const Expressions::FunctionExpression* castExpr, Expressions::Expression*& expression);

  static void FoldExpression(const CompileResult& context, Expressions::BranchExpression* castExpr, Expressions::Expression*& expression);

  /** @} End of Folding-Optimization Functions */

  /**
   * @name Propagation-Optimization Functions
   * Functions to propagate child expressions as parents if can be
   * @{
   */

  static void PropagateExpression(Expressions::Expression*& expression, Expressions::Expression*& childExpr);

  static void TryPropagateChildExpression(
    Expressions::Expression*& expression,
    Expressions::Expression*& leftExpr,
    Expressions::Expression*& rightExpr
  );

  /** @} End of Propagation-Optimization Functions */

/**
 * @name Evaluation-Optimization Functions
 * Functions to evaluate  expressions to constants
 * @{
 */

static void EvaluateExpression(const CompileResult& context, Expressions::Expression*& expression);

static void AssignConstantToExpression(const CompileResult& context, Expressions::Expression*& expression);

/** @} End of Propagation-Optimization Functions */

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

  /** @} End of Index Assignment Functions */


  /**
   * @name Post Projection Alias Resolvement Functions
   * Functions to resolve aliases used in post projection statements(order by)
   * @{
   */
  static Errors::ValidationStatus CompilePostProjectionExpression(
    Expressions::Expression *expression,
    const Dictionary<std::string, const Expressions::Expression*>& postProjectionAliases
  );

  static Errors::ValidationStatus CompilePostProjectionColumnExpression(
    Expressions::ColumnExpression* column,
    const Dictionary<std::string, const Expressions::Expression*>& postProjectionAliases
  );

  static Errors::ValidationStatus CompilePostProjectionBinaryExpression(
    const Expressions::BinaryExpression* expression,
    const Dictionary<std::string, const Expressions::Expression*>& postProjectionAliases
  );

  static Errors::ValidationStatus CompilePostProjectionLogicalExpression(
    const Expressions::LogicalExpression* expression,
    const Dictionary<std::string, const Expressions::Expression*>& postProjectionAliases
  );

  static Errors::ValidationStatus CompilePostProjectionFunctionExpression(
    const Expressions::FunctionExpression* expression,
    const Dictionary<std::string, const Expressions::Expression*>& postProjectionAliases
  );

  static Errors::ValidationStatus CompilePostProjectionBranchExpression(
    const Expressions::BranchExpression* expression,
    const Dictionary<std::string, const Expressions::Expression*>& postProjectionAliases
  );

  /** @} End of Post Projection Alias Resolvement Functions */


  /**
   * @name Post Projection Index Assignment Functions
   * Functions to assign expression's index from the results to the post projection statements(order by)
   * @{
   */

  static void AssignPostProjectionIndicesToExpression(
    const Dictionary<std::string, column_index_t>& columnIndicesDictionary,
    Expressions::Expression* expression
  );

  static void AssignPostProjectionIndicesToBinaryExpression(
    const Dictionary<std::string, column_index_t>& columnIndicesDictionary,
    const Expressions::BinaryExpression* expression
  );

  static void AssignPostProjectionIndicesToLogicalExpression(
    const Dictionary<std::string, column_index_t>& columnIndicesDictionary,
    const Expressions::LogicalExpression* expression
  );

  static void AssignPostProjectionIndicesToFunctionExpression(
    const Dictionary<std::string, column_index_t>& columnIndicesDictionary,
    const Expressions::FunctionExpression* expression
  );

  static void AssignPostProjectionIndicesToBranchExpression(
    const Dictionary<std::string, column_index_t>& columnIndicesDictionary,
    const Expressions::BranchExpression* expression
  );

  static void AssignPostProjectionIndicesToColumnExpression(
    const Dictionary<std::string, column_index_t>& columnIndicesDictionary,
    Expressions::ColumnExpression* expression
  );

  /** @} End of Post Projection Index Assignment Functions */

  /**
   * @name Helper Functions
   * Functions construct helper error messages etc...
   * @{
   */

  static Errors::ValidationStatus ClauseCannotBeEvaluatedToBool(DataType type);

  /** @} End of Helper Functions */
}