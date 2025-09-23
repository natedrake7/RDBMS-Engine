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
        LogicalPlan* child,
        std::vector<Expressions::Expression*>& resultExpressions,
        std::vector<Headers::ColumnHeader>& columnsHeaders);
      ~LogicalProject() override;
      PhysicalPlan::PhysicalProject* ToPhysical()override;
  };

  class LogicalTableScan final : public LogicalPlan {
    public:
      Statements::TableName* table;
      Expressions::Expression* expression;
      explicit LogicalTableScan(  Statements::TableName* table, Expressions::Expression* expression);
      PhysicalPlan::PhysicalOperator* ToPhysical() override;
  };

  class LogicalJoin final : public LogicalPlan {
    public:
    LogicalTableScan* left;
    LogicalTableScan* right;
    Expressions::Expression* condition;
    JoinType type;
    LogicalJoin(
      LogicalTableScan* left,
      LogicalTableScan* right,
      Expressions::Expression* condition,
      const JoinType& type);
    PhysicalPlan::PhysicalOperator* ToPhysical()override;
  };

  class LogicalFilter final : public LogicalPlan {
    public:
      LogicalPlan* child;
      Expressions::Expression* filter;
      explicit LogicalFilter( LogicalPlan* child, Expressions::Expression* filter);
      PhysicalPlan::PhysicalFilter* ToPhysical()override;
  };

  class LogicalOrder final : public LogicalPlan {
    public:
      LogicalPlan* child;
      std::vector<Statements::OrderColumn*> expressions;

      explicit LogicalOrder(
        LogicalPlan* child,
        std::vector<Statements::OrderColumn*>& expressions
        );
      PhysicalPlan::PhysicalOperator* ToPhysical()override;
  };

  class LogicalInsert final : public LogicalPlan {
    public:
      Statements::TableName* table;
      std::vector<Statements::InsertColumns> fields;

      LogicalPlan* child;
      std::vector<column_index_t> columnsIndices;

      explicit LogicalInsert(
        Statements::TableName* table,
        std::vector<Statements::InsertColumns>& fields,
        LogicalPlan* child,
        std::vector<column_index_t>& columnIndices
      );
      ~LogicalInsert()override;
      PhysicalPlan::PhysicalInsert* ToPhysical()override;
  };

  class LogicalSchemaCreate final : public LogicalPlan {
    public:
      std::string schemaName;
      int32_t databaseId;
      explicit LogicalSchemaCreate(const int32_t& databaseId, std::string& schemaName);
      PhysicalPlan::PhysicalSchemaCreate* ToPhysical()override;
  };

  class LogicalDelete final : public LogicalPlan {
  public:
    Statements::TableName* table;
    Expressions::Expression* expression;
    explicit LogicalDelete(Statements::TableName* table, Expressions::Expression* expression);
    PhysicalPlan::PhysicalOperator* ToPhysical()override;
  };

  class LogicalUpdate final : public LogicalPlan {
    public:
      Statements::TableName* table;
      std::vector<Statements::UpdateColumn*> updates;
      Expressions::Expression* expression;

      explicit LogicalUpdate(Statements::TableName* table, std::vector<Statements::UpdateColumn*>& updates, Expressions::Expression* expression);
      PhysicalPlan::PhysicalOperator* ToPhysical()override;
  };

  class LogicalTableCreate final : public LogicalPlan {
    public:
      Statements::TableName* table;
      std::string constraintName;
      std::vector<Statements::NewColumn*> columns;
      vector<column_index_t> primaryKey;

      explicit LogicalTableCreate(
        Statements::TableName* table,
        std::vector<Statements::NewColumn*>& columns,
        std::vector<column_index_t> primaryKey,
        std::string  constraintName);
      PhysicalPlan::PhysicalTableCreate* ToPhysical()override;
  };

  class LogicalIndexCreate final : public LogicalPlan {
    public:
    Statements::TableName* table;
    std::string constraintName;
    std::vector<column_index_t> columns;
    explicit LogicalIndexCreate(Statements::TableName* table, std::string& constraintName, std::vector<column_index_t>& columns);
    PhysicalPlan::PhysicalOperator * ToPhysical() override;
  };

  class LogicalAlterTable final : public LogicalPlan {
    public:
      Statements::TableName* table;
      Constants::AlterTableType type;

      Statements::AlterColumn* alterColumn;
      Statements::DropColumn* dropColumn;
      Statements::RenameColumn* renameColumn;
      Statements::NewColumn* addColumn;

      explicit LogicalAlterTable(
        Statements::TableName* table,
        const AlterTableType& type,
        Statements::AlterColumn* alterColumn,
        Statements::NewColumn* addColumn,
        Statements::DropColumn* dropColumn,
        Statements::RenameColumn* renameColumn);

      PhysicalPlan::PhysicalOperator * ToPhysical() override;
  };
}

