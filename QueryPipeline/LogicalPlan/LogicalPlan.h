#pragma once
#include "../PhysicalPlan/PhysicalPlan.h"

namespace QueryPipeline {
  class LogicalPlan {
  public:
    int32_t databaseId;
    explicit LogicalPlan(const int32_t & databaseId);
    LogicalPlan(){
      this->databaseId = -1;
    }
    virtual ~LogicalPlan();
    virtual PhysicalPlan::PhysicalOperator* ToPhysical() = 0;
  };

  class LogicalCreateDatabase final : public LogicalPlan {
    public:
      std::string dbName;
      explicit LogicalCreateDatabase(std::string dbName);
      PhysicalPlan::PhysicalCreateDatabase* ToPhysical()override;
  };


  class LogicalProject final: public LogicalPlan {
    public:
      LogicalPlan* child;
      std::vector<Expressions::Expression*> resultExpressions;
      std::vector<Headers::ColumnHeader> columnsHeaders;

      LogicalProject(
        const int32_t & databaseId,
        LogicalPlan* child,
        std::vector<Expressions::Expression*>& resultExpressions,
        std::vector<Headers::ColumnHeader>& columnsHeaders);
      ~LogicalProject() override;
      PhysicalPlan::PhysicalProject* ToPhysical()override;
  };

  class LogicalTableScan final : public LogicalPlan {
    public:
      Statements::TableName* table;
      Expressions::LogicalExpression* expression;
      explicit LogicalTableScan(const int32_t & databaseId, Statements::TableName* table, Expressions::LogicalExpression* expression);
      PhysicalPlan::PhysicalOperator* ToPhysical() override;
  };

  class LogicalJoin final : public LogicalPlan {
    public:
    LogicalTableScan* left;
    LogicalTableScan* right;
    Expressions::LogicalExpression* condition;
    JoinType type;
    LogicalJoin(
      const int32_t& databaseId,
      LogicalTableScan* left,
      LogicalTableScan* right,
      Expressions::LogicalExpression* condition,
      const JoinType& type);
    PhysicalPlan::PhysicalOperator* ToPhysical()override;
  };

  class LogicalFilter final : public LogicalPlan {
    public:
      LogicalPlan* child;
      Expressions::LogicalExpression* filter;
      explicit LogicalFilter(const int32_t & databaseId, LogicalPlan* child, Expressions::LogicalExpression* filter);
      PhysicalPlan::PhysicalFilter* ToPhysical()override;
  };

  class LogicalOrder final : public LogicalPlan {
    public:
      LogicalPlan* child;
      std::vector<column_index_t> columns;
      Constants::OrderType orderType;

      explicit LogicalOrder(const int32_t & databaseId, LogicalPlan* child, std::vector<column_index_t>& columns, const Constants::OrderType& orderType);
      PhysicalPlan::PhysicalOperator* ToPhysical()override;
  };

  class LogicalInsert final : public LogicalPlan {
    public:
      Statements::TableName* table;
      std::vector<Field> fields;
      explicit LogicalInsert(const int32_t & databaseId, Statements::TableName* table, const std::vector<Field>& fields);
      PhysicalPlan::PhysicalInsert* ToPhysical()override;
  };

  class LogicalSchemaCreate final : public LogicalPlan {
    public:
      std::string schemaName;
      explicit LogicalSchemaCreate(const int32_t & databaseId, std::string& schemaName);
      PhysicalPlan::PhysicalSchemaCreate* ToPhysical()override;
  };

  class LogicalDelete final : public LogicalPlan {
  public:
    Statements::TableName* table;
    Expressions::LogicalExpression* expression;
    explicit LogicalDelete(const int32_t & databaseId, Statements::TableName* table, Expressions::LogicalExpression* expression);
    PhysicalPlan::PhysicalOperator* ToPhysical()override;
  };

  class LogicalUpdate final : public LogicalPlan {
    public:
      Statements::TableName* table;
      std::vector<Field> fields;
      Expressions::LogicalExpression* expression;

      explicit LogicalUpdate(const int32_t & databaseId, Statements::TableName* table, std::vector<Field>& fields, Expressions::LogicalExpression* expression);
      PhysicalPlan::PhysicalOperator* ToPhysical()override;
  };

  class LogicalTableCreate final : public LogicalPlan {
    public:
      Statements::TableName* table;
      std::string constraintName;
      std::vector<Statements::AddColumn*> columns;
      vector<column_index_t> primaryKey;

      explicit LogicalTableCreate(
        const int32_t & databaseId,
        Statements::TableName* table,
        std::vector<Statements::AddColumn*>& columns,
        std::vector<column_index_t> primaryKey,
        std::string  constraintName);
      PhysicalPlan::PhysicalTableCreate* ToPhysical()override;
  };

  class LogicalIndexCreate final : public LogicalPlan {
    public:
    Statements::TableName* table;
    std::string constraintName;
    std::vector<column_index_t> columns;
    explicit LogicalIndexCreate(const int32_t & databaseId, Statements::TableName* table, std::string& constraintName, std::vector<column_index_t>& columns);
    PhysicalPlan::PhysicalOperator * ToPhysical() override;
  };

  class LogicalAlterTable final : public LogicalPlan {
    public:
      Statements::TableName* table;
      Constants::AlterTableType type;

      Statements::AlterColumn* alterColumn;
      Statements::DropColumn* dropColumn;
      Statements::RenameColumn* renameColumn;
      Statements::AddColumn* addColumn;

      explicit LogicalAlterTable(
        const int32_t & databaseId,
        Statements::TableName* table,
        const AlterTableType& type,
        Statements::AlterColumn* alterColumn,
        Statements::AddColumn* addColumn,
        Statements::DropColumn* dropColumn,
        Statements::RenameColumn* renameColumn);

      PhysicalPlan::PhysicalOperator * ToPhysical() override;
  };
}

