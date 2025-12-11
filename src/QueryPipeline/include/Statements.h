#pragma once
#include <string>
#include <vector>
#include "../../Database/include/Constants.h"
#include "../../Expressions/include/Expression.h"
#include "../../Systemic/include/DataTypes/Variable.h"
#include "../../Systemic/include/DataTypes/Guid.h"
#include "../../Systemic/include/Security/Security.h"
#include "../../Systemic/include/Errors.h"
#include "../../Systemic/include/Headers.h"
#include "Parser.h"

namespace Headers {
  struct DefaultValuesHeader;
}

namespace QueryPipeline {
  struct ParserValidationScope;
  class LogicalPlan;
}

namespace QueryPipeline::Statements {

  struct SelectStatement;

  struct StatementValidationScope {
    const Dictionary<std::string, table_id_t>* tableAliasesDictionary;
    Dictionary<int, Dictionary<std::string, Headers::ColumnHeader>>* tablesColumnsDictionary;
    int* indexPos;
    Statements::Statement* statement;

    StatementValidationScope(
      const Dictionary<std::string, table_id_t>& tableAliasesDictionary,
      Dictionary<int, Dictionary<std::string, Headers::ColumnHeader>>& tablesColumnsDictionary,
      Statement* statement,
      int* indexPos = nullptr
    );
  };

  struct DecimalType {
    int8_t precision;
    int8_t scale;

    DecimalType();
    DecimalType(const int8_t& precision, const int8_t& scale);
    [[nodiscard]] bool Validate() const;
  };

  struct ColumnType {
    std::string name;
    int size;

    DecimalType decimal;

    explicit ColumnType(const std::string& name);
    ColumnType(const std::string& name, const int& size);
    ColumnType(const std::string& name, const DecimalType& decimal);
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
    Statements::ColumnName name;
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
    Statements::ColumnName name;
    ColumnType type;

    int32_t columnId;
    column_index_t index;
  };

  struct DropColumn {
    Statements::ColumnName name;

    int32_t columnId;
    Constants::column_index_t index;
  };

  struct RenameColumn {
    Statements::ColumnName oldName;
    Statements::ColumnName newName;

    column_index_t ordinalPosition;
    int32_t columnId;
  };

  struct PrimaryKeyConstraint {
    std::string name;
    vector<ColumnName> columns;
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

    int32_t databaseId;
    int32_t tableId;
    int32_t schemaId;
    int16_t ordinalPosition;

    DataSource();
    [[nodiscard]] std::string GetAlias() const;
    [[nodiscard]] std::string GetFullName()const;
    [[nodiscard]] Errors::ValidationStatus Validate(const int32_t& selectedDatabaseId);
    [[nodiscard]] Errors::ValidationStatus ValidateTableCreate(const int32_t& selectedDatabaseId);
  };

  struct SubQuery : DataSource {
    Statements::SelectStatement* statement;
  };

  struct Inserts {
    std::vector<Expressions::Expression*> values;
  };

  struct Statement {
    DataTypes::Guid sessionId;

    int32_t databaseId;

    DataSource* table;
    Dictionary<int32_t, Dictionary<std::string, Headers::ColumnHeader>> tableColumnsDictionary;

    Statement();
    virtual ~Statement() = default;
    virtual Errors::ValidationStatus Validate(ParserValidationScope& validationScope) = 0;
    virtual Security::Permission RequiredPermissions() const = 0;
    Errors::ValidationStatus ValidateBase()const;

    Errors::ValidationStatus ValidateStatement(ParserValidationScope& validationScope);
    virtual QueryPipeline::LogicalPlan* ToLogical() = 0;
  };

  struct DeclareVariableStatement final: public Statement {
    Variable variable;

    Expressions::Expression* expression;

    DeclareVariableStatement();
    Errors::ValidationStatus Validate(ParserValidationScope& validationScope) override;
    Security::Permission RequiredPermissions() const override;

    QueryPipeline::LogicalPlan* ToLogical() override;
  };

  struct SetVariableStatement final: public Statement {
    Variable variable;
    Expressions::Expression* expression;

    SetVariableStatement();
    Errors::ValidationStatus Validate(ParserValidationScope& validationScope) override;
    Security::Permission RequiredPermissions() const override;

    QueryPipeline::LogicalPlan* ToLogical() override;
  };

  struct CreateUserStatement final : public Statement {
      std::string username;
      std::string password;

      std::string role;

      CreateUserStatement() = default;
      ~CreateUserStatement() override = default;

      Errors::ValidationStatus Validate(ParserValidationScope& validationScope) override;
      Security::Permission RequiredPermissions() const override;
      QueryPipeline::LogicalPlan* ToLogical() override;
  };

  struct GrantRoleStatement final : public Statement {
    std::string username;
    std::string role;

    GrantRoleStatement() = default;
    ~GrantRoleStatement() override = default;
    Errors::ValidationStatus Validate(ParserValidationScope& validationScope) override;
    Security::Permission RequiredPermissions() const override;
    QueryPipeline::LogicalPlan* ToLogical() override;
  };

  struct DeleteStatement final : Statement {
    WhereClause where;
    ~DeleteStatement() override = default;
    Errors::ValidationStatus Validate(ParserValidationScope& validationScope) override;
    QueryPipeline::LogicalPlan * ToLogical() override;
    Security::Permission RequiredPermissions() const override;
  };

  struct JoinStatement final : public Statement{
    Expressions::Expression* expression;
    Constants::JoinType type;

    JoinStatement();
    [[nodiscard]]Errors::ValidationStatus Validate(ParserValidationScope& validationScope) override;
    [[nodiscard]]Errors::ValidationStatus Validate(const int32_t& databaseId);

    [[nodiscard]]bool IsRightJoin()const;

    QueryPipeline::LogicalPlan* ToLogical()override;
    Security::Permission RequiredPermissions() const override;
  };

  struct CreateTableStatement final: Statement {
    std::vector<NewColumn*> columns;
    PrimaryKeyConstraint* constraint;
    vector<column_index_t> primaryKey;

    CreateTableStatement();
    ~CreateTableStatement() override;

    Errors::ValidationStatus Validate(ParserValidationScope& validationScope) override;
    QueryPipeline::LogicalPlan* ToLogical() override;
    Security::Permission RequiredPermissions() const override;
  };

  struct SelectStatement final : Statement{
    int64_t top;
    bool distinct;
    std::vector<Expressions::Expression*> results;
    std::vector<Headers::ColumnHeader> columnHeaders;

    std::vector<JoinStatement*> joins;

    WhereClause where;
    OrderByStatement* orderBy;

    SelectStatement();
    ~SelectStatement() override;

    [[nodiscard]] Dictionary<std::string, Constants::column_index_t> CreatePostProjectionIndicesDictionary()const;

    [[nodiscard]] bool HasTopStatement()const;
    [[nodiscard]] bool HasJoins()const;
    [[nodiscard]] bool HasWhere()const;
    [[nodiscard]] bool IsConstant()const;
    [[nodiscard]] Errors::ValidationStatus CompileNoTableStatement(ParserValidationScope& validationScope);
    [[nodiscard]] Errors::ValidationStatus Compile(ParserValidationScope& validationScope, Dictionary<std::string, table_id_t>& tableAliasesDictionary);
    [[nodiscard]] Errors::ValidationStatus CompileWhereClause(ParserValidationScope& validationScope, StatementValidationScope& statementValidationScope);
    void AssignColumnsToIndices(const Dictionary<int32_t, Constants::column_index_t> &columnIndicesDictionary)const;

    [[nodiscard]] Errors::ValidationStatus Validate(ParserValidationScope& validationScope) override;
    [[nodiscard]] LogicalPlan* ToLogical() override;
    Security::Permission RequiredPermissions() const override;
  };

  struct CreateDbStatement final : Statement{
    std::string name;

    Errors::ValidationStatus Validate(ParserValidationScope& validationScope) override;
    LogicalPlan* ToLogical() override;
    Security::Permission RequiredPermissions() const override;
  };

  struct DropDbStatement final : Statement{
    std::string name;

    Errors::ValidationStatus Validate(ParserValidationScope& validationScope) override;
    LogicalPlan* ToLogical() override;
    Security::Permission RequiredPermissions() const override;
  };

  struct UseDatabaseStatement final : Statement {
    std::string name;

    Errors::ValidationStatus Validate(ParserValidationScope& validationScope) override;
    LogicalPlan* ToLogical() override;
    Security::Permission RequiredPermissions() const override;
  };

  struct InsertStatement final : Statement{
    std::vector<ColumnName> columns;
    std::vector<Inserts> values;

    std::vector<column_index_t> columnIndices;
    SelectStatement* selectStatement;

    ~InsertStatement() override;

    void InsertDefaultValuesForMissingColumns(const Headers::ColumnHeader& header, const Headers::DefaultValuesHeader& defaultValue);
    void InsertNullValuesForMissingColumns(const Headers::ColumnHeader& header);

    [[nodiscard]] Errors::ValidationStatus ValidateReturnType(const Expressions::Expression* expression, const std::string& columnName)const;
    [[nodiscard]] bool HasSelectStatement() const;
    [[nodiscard]] Errors::ValidationStatus ValidateSelectStatement(ParserValidationScope& validationScope)const;
    [[nodiscard]] Errors::ValidationStatus ResolveAliases(ParserValidationScope& validationScope);
    [[nodiscard]] Errors::ValidationStatus Validate(ParserValidationScope& validationScope) override;
    [[nodiscard]] LogicalPlan* ToLogical() override;
    Security::Permission RequiredPermissions() const override;
  };

  struct CreateSchemaStatement final : Statement {
    std::string name;

    Errors::ValidationStatus Validate(ParserValidationScope& validationScope) override;
    QueryPipeline::LogicalPlan * ToLogical() override;
    Security::Permission RequiredPermissions() const override;
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
    Errors::ValidationStatus ResolveAliases(ParserValidationScope& validationScope, Dictionary<std::string, table_id_t>& tableAliasesDictionary);
    Errors::ValidationStatus Validate(ParserValidationScope& validationScope) override;
    QueryPipeline::LogicalPlan * ToLogical() override;
    Security::Permission RequiredPermissions() const override;
  };

  struct CreateIndexStatement final : Statement {
    DataSource* table;
    std::string name;
    vector<std::string> columns;
    vector<column_index_t> columnIndices;
    bool isUnique;

    Errors::ValidationStatus Validate(ParserValidationScope& validationScope) override;
    QueryPipeline::LogicalPlan * ToLogical() override;
    Security::Permission RequiredPermissions() const override;
  };

  struct AlterTableStatement final : Statement {
    DataSource* table;
    Constants::AlterTableType type;

    NewColumn* newColumn;
    AlterColumn* alterColumn;
    DropColumn* dropColumn;
    RenameColumn* renameColumn;

    [[nodiscard]] Errors::ValidationStatus ValidateAddColumn(const Dictionary<std::string, Headers::ColumnHeader>& headers)const;
    [[nodiscard]] Errors::ValidationStatus ValidateAlterColumn(const Dictionary<std::string, Headers::ColumnHeader>& headers)const;
    [[nodiscard]] Errors::ValidationStatus ValidateDropColumn(const Dictionary<std::string, Headers::ColumnHeader>& headers)const;
    [[nodiscard]] Errors::ValidationStatus ValidateRenameColumn(const Dictionary<std::string, Headers::ColumnHeader>& headers)const;
    Errors::ValidationStatus Validate(ParserValidationScope& validationScope) override;
    QueryPipeline::LogicalPlan * ToLogical() override;
    Security::Permission RequiredPermissions() const override;
  };

  /**
   * @name Expression Compilation Functions
   * Functions to compile expressions, resolve their corresponding table
   * assign ordinal positions in table, optimize etc
   * @{
   */

  static Errors::ValidationStatus CompileExpression(
    ParserValidationScope& validationScope,
    Expressions::Expression*& expression
  );

  static Errors::ValidationStatus CompileExpression(
    ParserValidationScope& validationScope,
    StatementValidationScope& statementValidationScope,
    Expressions::Expression*& expression
  );

  static Errors::ValidationStatus CompileBinaryExpression(
    ParserValidationScope& validationScope,
    Expressions::BinaryExpression* binaryExpr,
    Expressions::Expression*& expression
  );

  static Errors::ValidationStatus CompileBinaryExpression(
    ParserValidationScope& validationScope,
    Expressions::BinaryExpression* binaryExpr,
    Expressions::Expression*& expression,
    StatementValidationScope& statementValidationScope
  );

  static Errors::ValidationStatus CompileLogicalExpression(
    ParserValidationScope& validationScope,
    Expressions::LogicalExpression* logicalExpr,
    Expressions::Expression*& expression
  );

  static Errors::ValidationStatus CompileLogicalExpression(
    ParserValidationScope& validationScope,
    Expressions::LogicalExpression* logicalExpr,
    Expressions::Expression*& expression,
    StatementValidationScope& statementValidationScope
  );

  static Errors::ValidationStatus CompileFunctionExpression(
    ParserValidationScope& validationScope,
    const Expressions::FunctionExpression* funcExpr,
    Expressions::Expression*& expression
  );

  static Errors::ValidationStatus CompileFunctionExpression(
    ParserValidationScope& validationScope,
    const Expressions::FunctionExpression* funcExpr,
    Expressions::Expression*& expression,
    StatementValidationScope& statementValidationScope
  );

  static Errors::ValidationStatus CompileBranchExpression(
    ParserValidationScope& validationScope,
    Expressions::BranchExpression* branchExpr,
    Expressions::Expression*& expression,
    StatementValidationScope& statementValidationScope
  );

  static Errors::ValidationStatus CompileBranchExpression(
    ParserValidationScope& validationScope,
    Expressions::BranchExpression* branchExpr,
    Expressions::Expression*& expression
  );

  static Errors::ValidationStatus CompileColumnExpression(const Expressions::ColumnExpression *columnExpr);

  static Errors::ValidationStatus CompileColumnExpression(Expressions::ColumnExpression* column, const StatementValidationScope& statementValidationScope);

  static Errors::ValidationStatus CompileVariableExpression(
    const ParserValidationScope& validationScope,
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
    const DataType& type,
    const Expressions::Expression* expression
  );

  static Errors::ValidationStatus CompileWildcard(
    const Expressions::ColumnExpression* column,
    const StatementValidationScope& statementValidationScope,
    SelectStatement *statement
  );

  /** @} End of Expression Compilation Functions */

  /**
   * @name Folding-Optimization Functions
   * Functions to optimize and pre-evaluate if can, expressions to reduce runtime overhead
   * @{
   */

  static void FoldExpression(Expressions::Expression*& expression);

  static void FoldExpression(const Expressions::BinaryExpression* castExpr, Expressions::Expression*& expression);

  static void FoldExpression(Expressions::LogicalExpression* castExpr, Expressions::Expression*& expression);

  static void FoldExpression(const Expressions::FunctionExpression* castExpr, Expressions::Expression*& expression);

  static void FoldExpression(Expressions::BranchExpression* castExpr, Expressions::Expression*& expression);

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

static void EvaluateExpression(Expressions::Expression*& expression);

static void AssignConstantToExpression(Expressions::Expression*& expression);

/** @} End of Propagation-Optimization Functions */

  /**
   * @name Index Assignment Functions
   * Functions to assign column's ordinal position in the table
   * @{
   */
  static void AssignColumnIndicesToExpression(
    const Dictionary<int32_t, Constants::column_index_t>& columnIndicesDictionary,
    Expressions::Expression* expression
  );

  static void AssignColumnIndicesToBinaryExpression(
    const Dictionary<int32_t, Constants::column_index_t>& columnIndicesDictionary,
    const Expressions::BinaryExpression* expression
  );

  static void AssignColumnIndicesToLogicalExpression(
    const Dictionary<int32_t, Constants::column_index_t>& columnIndicesDictionary,
    const Expressions::LogicalExpression* expression
  );

  static void AssignColumnIndicesToBranchExpression(
    const Dictionary<int32_t, Constants::column_index_t>& columnIndicesDictionary,
    const Expressions::BranchExpression* expression
  );

  static void AssignColumnIndicesToFunctionExpression(
    const Dictionary<int32_t, Constants::column_index_t>& columnIndicesDictionary,
    const Expressions::FunctionExpression* expression
  );

  static void AssignColumnIndicesToColumnExpression(
    const Dictionary<int32_t, Constants::column_index_t>& columnIndicesDictionary,
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
    const Dictionary<std::string, Constants::column_index_t>& columnIndicesDictionary,
    Expressions::Expression* expression
  );

  static void AssignPostProjectionIndicesToBinaryExpression(
    const Dictionary<std::string, Constants::column_index_t>& columnIndicesDictionary,
    const Expressions::BinaryExpression* expression
  );

  static void AssignPostProjectionIndicesToLogicalExpression(
    const Dictionary<std::string, Constants::column_index_t>& columnIndicesDictionary,
    const Expressions::LogicalExpression* expression
  );

  static void AssignPostProjectionIndicesToFunctionExpression(
    const Dictionary<std::string, Constants::column_index_t>& columnIndicesDictionary,
    const Expressions::FunctionExpression* expression
  );

  static void AssignPostProjectionIndicesToBranchExpression(
    const Dictionary<std::string, Constants::column_index_t>& columnIndicesDictionary,
    const Expressions::BranchExpression* expression
  );

  static void AssignPostProjectionIndicesToColumnExpression(
    const Dictionary<std::string, Constants::column_index_t>& columnIndicesDictionary,
    Expressions::ColumnExpression* expression
  );

  /** @} End of Post Projection Index Assignment Functions */

  /**
   * @name Helper Functions
   * Functions construct helper error messages etc...
   * @{
   */

  static Errors::ValidationStatus ClauseCannotBeEvaluatedToBool(const DataType& type);

  /** @} End of Helper Functions */
}