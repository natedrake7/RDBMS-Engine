#pragma once
#include <string>
#include <vector>
#include "../../Database/Constants.h"
#include "../../AdditionalLibraries/AdditionalDataTypes/Field/Field.h"
#include "../../AdditionalLibraries/AdditionalDataTypes/Headers/Headers.h"
#include "../../AdditionalLibraries/HashSet/HashSet.h"
#include "../../AdditionalLibraries/AdditionalDataTypes/Expression/Expression.h"

namespace QueryPipeline {
  class LogicalPlan;
}

namespace QueryPipeline::Statements {
  struct PrimaryKeyConstraint {
    std::string name;
    vector<std::string> columns;
  };

  struct ColumnType {
    std::string name;
    int64_t size;
    int64_t beforeFraction;
    int64_t afterFraction;
  };

  struct AutoIncrementKey{
    uint16_t seed;
    uint16_t incrementFactor;

    AutoIncrementKey() = default;
    ~AutoIncrementKey() = default;
  };

  struct AddColumn {
    std::string name;
    ColumnType type;
    AutoIncrementKey* autoIncrementKey;
    bool isPrimaryKey;
    bool isNullable;

    column_index_t index;
  };

  struct WhereClause{
    Expressions::Expression* expression;

    WhereClause() { this->expression = nullptr; }
  };

  struct TableName {
    std::string name;
    std::string schema;
    int32_t tableId;

    TableName() { this->schema = "dbo"; }
  };

  struct Statement {
    std::string dbName;
    Statement() = default;
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
    std::vector<AddColumn> columns;
    PrimaryKeyConstraint* constraint;
    vector<column_index_t> primaryKey;

    AutoIncrementKey* autoIncrementKey;

    CreateTableStatement();
    ~CreateTableStatement() override;

    bool Validate() override;
    QueryPipeline::LogicalPlan* ToLogical() override;
  };

  struct SelectStatement final : Statement{
    TableName* table;
    std::vector<std::string> columns;
    std::vector<Constants::column_index_t> columnIndices;
    WhereClause where;

    ~SelectStatement() override { delete this->table; };
    
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
    std::vector<std::string> columns;
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
    std::string name;
    Field value;
  };

  struct UpdateStatement final : Statement {
    TableName* table;
    std::vector<UpdateColumnStatement> columns;
    WhereClause where;

    bool Validate() override;
    QueryPipeline::LogicalPlan * ToLogical() override;
  };

}