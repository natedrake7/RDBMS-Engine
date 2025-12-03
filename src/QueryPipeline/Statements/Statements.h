#pragma once
#include <string>
#include <vector>
#include "../../Database/Constants.h"
#include "../../Systemic/DataTypes/Value/Value.h"
#include "../../Systemic/DataTypes/Headers/Headers.h"
#include "../../Expressions/Expression.h"
#include "../../Systemic/Security/Security.h"
#include "../../Systemic/Errors/Errors.h"

namespace QueryPipeline {
  class LogicalPlan;
}

namespace QueryPipeline::Statements {

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
    virtual Errors::ValidationStatus Validate() = 0;
    virtual Security::Permission RequiredPermissions() const = 0;
    Errors::ValidationStatus ValidateBase()const;

    Errors::ValidationStatus ValidateStatement();
    virtual QueryPipeline::LogicalPlan* ToLogical() = 0;
  };

  struct CreateUserStatement final : public Statement {
      std::string username;
      std::string password;

      std::string role;

      CreateUserStatement() = default;
      ~CreateUserStatement() override= default;

      Errors::ValidationStatus Validate() override;
      Security::Permission RequiredPermissions() const override;
      QueryPipeline::LogicalPlan* ToLogical() override;
  };

  struct GrantRoleStatement final : public Statement {
    std::string username;
    std::string role;

    GrantRoleStatement() = default;
    ~GrantRoleStatement() override = default;
    Errors::ValidationStatus Validate() override;
    Security::Permission RequiredPermissions() const override;
    QueryPipeline::LogicalPlan* ToLogical() override;
  };

  struct DeleteStatement final : Statement {
    WhereClause where;
    ~DeleteStatement() override = default;
    Errors::ValidationStatus Validate() override;
    QueryPipeline::LogicalPlan * ToLogical() override;
    Security::Permission RequiredPermissions() const override;
  };

  struct JoinStatement final : public Statement{
    Expressions::Expression* expression;
    Constants::JoinType type;

    JoinStatement();
    [[nodiscard]]Errors::ValidationStatus Validate() override;
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

    Errors::ValidationStatus Validate() override;
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
    [[nodiscard]] Errors::ValidationStatus ValidateNoTableStatement();
    [[nodiscard]] Errors::ValidationStatus ResolveAliases(Dictionary<std::string, table_id_t>& tableAliasesDictionary);

    [[nodiscard]] Errors::ValidationStatus Validate() override;
    [[nodiscard]] LogicalPlan* ToLogical() override;
    Security::Permission RequiredPermissions() const override;
  };

  struct CreateDbStatement final : Statement{
    std::string name;

    Errors::ValidationStatus Validate() override;
    LogicalPlan* ToLogical() override;
    Security::Permission RequiredPermissions() const override;
  };

  struct DropDbStatement final : Statement{
    std::string name;

    Errors::ValidationStatus Validate() override;
    LogicalPlan* ToLogical() override;
    Security::Permission RequiredPermissions() const override;
  };

  struct UseDatabaseStatement final : Statement {
    std::string name;

    Errors::ValidationStatus Validate() override;
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
    [[nodiscard]] Errors::ValidationStatus ValidateSelectStatement()const;
    [[nodiscard]] Errors::ValidationStatus ResolveAliases();
    [[nodiscard]] Errors::ValidationStatus Validate() override;
    [[nodiscard]] LogicalPlan* ToLogical() override;
    Security::Permission RequiredPermissions() const override;
  };

  struct CreateSchemaStatement final : Statement {
    std::string name;

    Errors::ValidationStatus Validate() override;
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
    Errors::ValidationStatus ResolveAliases(Dictionary<std::string, table_id_t>& tableAliasesDictionary);
    Errors::ValidationStatus Validate() override;
    QueryPipeline::LogicalPlan * ToLogical() override;
    Security::Permission RequiredPermissions() const override;
  };

  struct CreateIndexStatement final : Statement {
    DataSource* table;
    std::string name;
    vector<std::string> columns;
    vector<column_index_t> columnIndices;
    bool isUnique;

    Errors::ValidationStatus Validate() override;
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
    Errors::ValidationStatus Validate() override;
    QueryPipeline::LogicalPlan * ToLogical() override;
    Security::Permission RequiredPermissions() const override;
  };

  static bool ResolveAliases(Dictionary<std::string, table_id_t>& tableAliasesDictionary, SelectStatement *statement);

  static Errors::ValidationStatus ResolveColumnAlias(
    ColumnName& column,
    const Dictionary<std::string, table_id_t>& tableAliasesDictionary,
    Dictionary<int, Dictionary<std::string, Headers::ColumnHeader>>& tablesColumnsDictionary);

  static Errors::ValidationStatus ResolveColumnAlias(
    Expressions::ColumnExpression* column,
    const Dictionary<std::string, table_id_t>& tableAliasesDictionary,
    Dictionary<int, Dictionary<std::string, Headers::ColumnHeader>>& tablesColumnsDictionary,
    Statement *statement,
    int* indexPos = nullptr
  );

  static Errors::ValidationStatus ResolveColumnAliasWhenTableAliasExists(
    Expressions::ColumnExpression* column,
    const Dictionary<std::string, table_id_t>& tableAliasesDictionary,
    Dictionary<int, Dictionary<std::string, Headers::ColumnHeader>>& tablesColumnsDictionary
  );

  static Errors::ValidationStatus ResolveColumnAliasWhenTableAliasDoesNotExist(
    Expressions::ColumnExpression* column,
    const Dictionary<std::string, table_id_t>& tableAliasesDictionary,
    Dictionary<int, Dictionary<std::string, Headers::ColumnHeader>>& tablesColumnsDictionary
  );

  static Errors::ValidationStatus ResolvePostProjectionColumnAlias(
    Expressions::ColumnExpression* column,
    const Dictionary<std::string, const Expressions::Expression*>& postProjectionAliases
    );

  static Errors::ValidationStatus ResolveExpressionAliases(
    const Dictionary<std::string, table_id_t>& tableAliasesDictionary,
    Dictionary<int, Dictionary<std::string, Headers::ColumnHeader>>& tablesColumnsDictionary,
    Statement *statement,
    Expressions::Expression *expr,
    int* indexPos = nullptr
  );

  static bool ValidateExpressionCoercionTypes(
    const Expressions::Expression* left,
    const Expressions::Expression* right
  );

  static Errors::ValidationStatus ResolvePostProjectionAliases(
    const Dictionary<std::string, const Expressions::Expression*>& postProjectionAliases,
    Expressions::Expression *expr
  );

  static Errors::ValidationStatus ResolveExpressionAliases(
    Statement *statement,
    Expressions::Expression *expr
    );

  static Errors::ValidationStatus ResolveWildCardAlias(
    const Expressions::ColumnExpression* column,
    const Dictionary<std::string, table_id_t>& tableAliasesDictionary,
    SelectStatement *statement,
    int* indexPos = nullptr
    );

  static void AssignColumnsToIndices(SelectStatement* statement, const Dictionary<int32_t, Constants::column_index_t> &columnIndicesDictionary);

  static void AssignColumnIndicesToResultExpression(
    SelectStatement* statement,
    const Dictionary<int32_t, Constants::column_index_t>& columnIndicesDictionary,
    Expressions::Expression* expr);

  static void AssignPostProjectionIndicesToExpression(
    const Dictionary<std::string, Constants::column_index_t>& columnIndicesDictionary,
    Expressions::Expression* expr);

}