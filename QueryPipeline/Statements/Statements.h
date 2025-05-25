#pragma once
#include <string>
#include <vector>
#include "../../Database/Constants.h"
#include "../../AdditionalLibraries/AdditionalDataTypes/Field/Field.h"
#include "../../AdditionalLibraries/AdditionalDataTypes/Headers/Headers.h"
#include "../../AdditionalLibraries/HashSet/HashSet.h"

namespace QueryPipeline {
  class LogicalPlan;
}

namespace QueryPipeline::Statements {
  enum class ExpressionType {
    And = 0,
    Or = 1,
    Predicate = 2
  };

  struct Expression {
    ExpressionType type;

    Expression* left;
    Expression* right;
        
    std::string column;
    std::string operation;
    Field value;

    Constants::column_index_t columnIndex;
      
    static Expression Predicate(
      const std::string& column,
      const std::string& operation,
      const Field& value);

    static Expression Logical(
      const ExpressionType& type,
      Expression* leftExpression,
      Expression* RightExpression);

    ~Expression();
    bool Validate(const Dictionary<string, Headers::ColumnHeader>& columnsDictionary);
    [[nodiscard]] bool IsComplex() const;
    void GetColumns(HashSet<column_index_t>& columnsSet)const;
  };

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

  struct AddColumn {
    std::string name;
    ColumnType type;
    bool isPrimaryKey;
    bool isNullable;

    column_index_t index;
  };

  struct WhereClause{
    Expression* expression;

    WhereClause() { this->expression = nullptr; }
  };

  struct TableName {
    std::string name;
    std::string schema;

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

}