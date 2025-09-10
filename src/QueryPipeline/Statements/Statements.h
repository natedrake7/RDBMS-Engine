#pragma once
#include <string>
#include <vector>
#include "../../Database/Constants.h"
#include "../../AdditionalLibraries/DataTypes/Value/Value.h"
#include "../../AdditionalLibraries/DataTypes/Headers/Headers.h"
#include "../../AdditionalLibraries/Expressions/Expression.h"

namespace QueryPipeline {
  class LogicalPlan;
}

namespace QueryPipeline::Statements {

  struct ColumnType {
    std::string name;
    int64_t size;
    int64_t beforeFraction;
    int64_t afterFraction;
  };

  struct Identity{
    uint16_t seed;
    uint16_t incrementFactor;
    int64_t cacheBlock;

    Identity() = default;
    ~Identity() = default;
  };

  struct NewColumn {
    Statements::ColumnName name;
    ColumnType type;
    Identity* autoIncrementKey;
    Value defaultValue;

    bool isPrimaryKey;
    bool isNullable;

    column_index_t index;
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
    std::vector<column_index_t> columnIndices;

    std::vector<OrderColumn*> columns;
    OrderType order;

    ~OrderByStatement();
    bool Validate(const std::vector<OrderColumn*>& selectColumns, const Dictionary<std::string, Headers::ColumnHeader>& columnsDict);
  };

  struct TableName {
    std::string database;
    std::string schema;
    std::string name;
    std::string alias;

    int32_t databaseId;
    int32_t tableId;
    int32_t schemaId;
    int16_t ordinalPosition;

    TableName();
    [[nodiscard]] std::string GetFullName()const;
    [[nodiscard]] bool Validate(int32_t& selectedDatabaseId);
  };

  struct Statement {
    int32_t databaseId;

    Statement(){
      this->databaseId = Constants::INVALID_DATABASE_ID;
    }

    virtual ~Statement() = default;
    virtual bool Validate() = 0;
    virtual QueryPipeline::LogicalPlan* ToLogical() = 0;
  };

  struct DeleteStatement final : Statement {
    TableName* table;
    WhereClause where;
    ~DeleteStatement() override = default;
    bool Validate() override;
    QueryPipeline::LogicalPlan * ToLogical() override;
  };

  struct JoinStatement final : public Statement{
    TableName* table;
    Expressions::LogicalExpression* expression;
    Constants::JoinType type;

    JoinStatement();
    ~JoinStatement()override;
    bool Validate() override;

    QueryPipeline::LogicalPlan* ToLogical()override;
  };

  struct CreateTableStatement final: Statement {
    TableName* table;
    std::vector<NewColumn*> columns;
    PrimaryKeyConstraint* constraint;
    vector<column_index_t> primaryKey;

    CreateTableStatement();
    ~CreateTableStatement() override;

    bool Validate() override;
    QueryPipeline::LogicalPlan* ToLogical() override;
  };

  struct SelectStatement final : Statement{
    TableName* table;

    std::vector<Expressions::Expression*> results;

    Dictionary<int32_t, Dictionary<std::string, Headers::ColumnHeader>> tableColumnsDictionary;

    std::vector<Headers::ColumnHeader> columnHeaders;

    std::vector<JoinStatement*> joins;

    WhereClause where;
    OrderByStatement* orderBy;

    ~SelectStatement() override;
    bool Validate() override;
    LogicalPlan* ToLogical() override;
  };

  struct CreateDbStatement final : Statement{
    std::string name;
    bool Validate() override;
    LogicalPlan* ToLogical() override;
  };

  struct DropDbStatement final : Statement{
    std::string name;
    bool Validate() override;
    LogicalPlan* ToLogical() override;
  };

  struct InsertStatement final : Statement{
    TableName* table;
    std::vector<ColumnName> columns;
    std::vector<Value> values;

    ~InsertStatement() override { delete this->table; };
    
    bool Validate() override;
    LogicalPlan* ToLogical() override;
  };

  struct CreateSchemaStatement final : Statement {
    std::string name;

    bool Validate() override;
    QueryPipeline::LogicalPlan * ToLogical() override;
  };

  struct UpdateColumnStatement{
    ColumnName name;
    Value value;
  };

  struct UpdateStatement final : Statement {
    TableName* table;
    std::vector<UpdateColumnStatement> columns;
    WhereClause where;

    bool Validate() override;
    QueryPipeline::LogicalPlan * ToLogical() override;
  };

  struct CreateIndexStatement final : Statement {
    TableName* table;
    std::string name;
    vector<std::string> columns;
    vector<column_index_t> columnIndices;
    bool isUnique;

    bool Validate() override;
    QueryPipeline::LogicalPlan * ToLogical() override;
  };

  struct AlterTableStatement final : Statement {
    TableName* table;
    Constants::AlterTableType type;

    NewColumn* addColumn;
    AlterColumn* alterColumn;
    DropColumn* dropColumn;
    RenameColumn* renameColumn;

    [[nodiscard]] bool ValidateAddColumn(const Dictionary<std::string, Headers::ColumnHeader>& headers)const;
    [[nodiscard]] bool ValidateAlterColumn(const Dictionary<std::string, Headers::ColumnHeader>& headers)const;
    [[nodiscard]] bool ValidateDropColumn(const Dictionary<std::string, Headers::ColumnHeader>& headers)const;
    [[nodiscard]] bool ValidateRenameColumn(const Dictionary<std::string, Headers::ColumnHeader>& headers)const;
    bool Validate() override;
    QueryPipeline::LogicalPlan * ToLogical() override;
  };

  static bool ResolveAliases(Dictionary<std::string, table_id_t>& tableAliasesDictionary, SelectStatement *statement);

  static bool ResolveColumnAlias(
    Expressions::ColumnExpression* column,
    const Dictionary<std::string, table_id_t>& tableAliasesDictionary,
    Dictionary<int, Dictionary<std::string, Headers::ColumnHeader>>& tablesColumnsDictionary,
    SelectStatement *statement,
    const int& indexPos
    );

  static bool ResolveExpressionAliases(
    const Dictionary<std::string, table_id_t>& tableAliasesDictionary,
    Dictionary<int, Dictionary<std::string, Headers::ColumnHeader>>& tablesColumnsDictionary,
    SelectStatement *statement,
    Expressions::Expression *expr,
    const int& indexPos
    );

  static bool ResolveExpressionAliases(
    SelectStatement *statement,
    Expressions::Expression *expr
    );

  static bool ResolveWildCardAlias(
    const Expressions::ColumnExpression* column,
    const Dictionary<std::string, table_id_t>& tableAliasesDictionary,
    Dictionary<int, Dictionary<std::string, Headers::ColumnHeader>>& tablesColumnsDictionary,
    SelectStatement *statement,
    const int& indexPos
    );

  static void AssignColumnsToIndices(SelectStatement* statement, const Dictionary<int32_t, Constants::column_index_t> &columnIndicesDictionary);

  static void AssignColumnIndicesToResultExpression(
    SelectStatement* statement,
    const Dictionary<int32_t, Constants::column_index_t>& columnIndicesDictionary,
    Expressions::Expression* expr);

}