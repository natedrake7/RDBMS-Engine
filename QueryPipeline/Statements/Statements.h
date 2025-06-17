#pragma once
#include <string>
#include <vector>
#include "../../Database/Constants.h"
#include "../../AdditionalLibraries/AdditionalDataTypes/Field/Field.h"
#include "../../AdditionalLibraries/AdditionalDataTypes/Headers/Headers.h"
#include "../../AdditionalLibraries/AdditionalDataTypes/Expression/Expression.h"

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

  struct AddColumn {
    Statements::ColumnName name;
    ColumnType type;
    Identity* autoIncrementKey;
    Field defaultValue;

    bool isPrimaryKey;
    bool isNullable;

    column_index_t index;
  };

  struct AlterColumn {
    Statements::ColumnName name;
    ColumnType type;

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

    WhereClause() { this->expression = nullptr; }
  };

  struct OrderByStatement{
    std::vector<column_index_t> columnIndices;

    std::vector<ColumnName> columns;
    std::string order;

    bool Validate(const std::vector<ColumnName>& selectColumns, const Dictionary<std::string, Headers::ColumnHeader>& columnsDict);
  };

  struct TableName {
    std::string name;
    std::string schema;
    std::string alias;

    int32_t tableId;
    int32_t schemaId;
    int16_t ordinalPosition;

    TableName() { this->schema = "dbo"; }

    [[nodiscard]] std::string GetFullName()const {
      return this->schema + "." + this->name;
    }
  };

  struct JoinStatement {
    TableName* table;
    Expressions::Expression* expression;
    Constants::JoinType type;

    JoinStatement() {
      this->type = Constants::JoinType::Inner;
      this->table = nullptr;
      this->expression = nullptr;
    }

    ~JoinStatement() {
      delete this->table;
    }
  };

  struct Statement {
    int32_t databaseId;

    Statement(){
      this->databaseId = -1;
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

  struct CreateTableStatement final: Statement {
    TableName* table;
    std::vector<AddColumn*> columns;
    PrimaryKeyConstraint* constraint;
    vector<column_index_t> primaryKey;

    CreateTableStatement();
    ~CreateTableStatement() override;

    bool Validate() override;
    QueryPipeline::LogicalPlan* ToLogical() override;
  };

  struct SelectStatement final : Statement{
    TableName* table;
    std::vector<ColumnName> columns;
    Dictionary<int32_t, Dictionary<std::string, Headers::ColumnHeader>> tableColumnsDictionary;

    std::vector<Headers::ColumnHeader> columnHeaders;

    std::vector<Constants::column_index_t> columnIndices;

    std::vector<JoinStatement*> joins;

    WhereClause where;
    OrderByStatement* orderBy;

    ~SelectStatement() override {
      delete this->table;
      delete this->orderBy;
    };
    
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
    std::vector<Field> values;

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
    Field value;
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

    AddColumn* addColumn;
    AlterColumn* alterColumn;
    DropColumn* dropColumn;
    RenameColumn* renameColumn;

    [[nodiscard]] bool ValidateAddColumn(const Dictionary<std::string, Headers::ColumnHeader>& headers)const;
    [[nodiscard]] bool ValidateAlterColumn(const Dictionary<std::string, Headers::ColumnHeader>& headers);
    [[nodiscard]] bool ValidateDropColumn(const Dictionary<std::string, Headers::ColumnHeader>& headers)const;
    [[nodiscard]] bool ValidateRenameColumn(const Dictionary<std::string, Headers::ColumnHeader>& headers)const;
    bool Validate() override;
    QueryPipeline::LogicalPlan * ToLogical() override;
  };

  static bool ResolveAliases(Dictionary<std::string, table_id_t>& tableAliasesDictionary, SelectStatement *statement);

  static bool ResolveColumnAlias(
    ColumnName& column,
    const Dictionary<std::string, table_id_t>& tableAliasesDictionary,
    Dictionary<int, Dictionary<std::string, Headers::ColumnHeader>>& tablesColumnsDictionary,
    SelectStatement *statement);

  static bool ResolveWildCardAlias(
    const ColumnName& column,
    const Dictionary<std::string, table_id_t>& tableAliasesDictionary,
    Dictionary<int, Dictionary<std::string, Headers::ColumnHeader>>& tablesColumnsDictionary,
    SelectStatement *statement);

  static bool ResolveExpressionAliases(
    Expressions::Expression* expression,
    const Dictionary<std::string, table_id_t>& tableAliasesDictionary,
    Dictionary<int, Dictionary<std::string, Headers::ColumnHeader>>& tablesColumnsDictionary,
    SelectStatement *statement);

  static void MapExpressionColumnsToIndices(Expressions::Expression* expression, const Dictionary<int32_t, Constants::column_index_t> &columnIndicesDictionary);

  static void AssignColumnsToIndices(SelectStatement* statement, Dictionary<int32_t, Constants::column_index_t> columnIndicesDictionary);

}