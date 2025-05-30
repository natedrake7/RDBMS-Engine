#pragma once
#include "../PhysicalPlan/PhysicalPlan.h"

namespace QueryPipeline {
  class LogicalPlan {
  public:
    std::string dbName;
    explicit LogicalPlan(const std::string& dbName);
    LogicalPlan() = default;
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
      std::vector<column_index_t> columns;
      LogicalProject(const std::string& dbName, LogicalPlan* child, const std::vector<column_index_t>& columns);
      ~LogicalProject() override;
      PhysicalPlan::PhysicalProject* ToPhysical()override;
  };

  class LogicalTableScan final : public LogicalPlan {
    public:
      Statements::TableName* table;
      Expressions::Expression* expression;
      explicit LogicalTableScan(const std::string& dbName, Statements::TableName* table, Expressions::Expression* expression);
      PhysicalPlan::PhysicalOperator* ToPhysical() override;
  };

  class LogicalFilter final : public LogicalPlan {
    public:
      LogicalPlan* child;
      Expressions::Expression* filter;
      explicit LogicalFilter(const std::string& dbName, LogicalPlan* child, Expressions::Expression* filter);
      PhysicalPlan::PhysicalFilter* ToPhysical()override;
  };

  class LogicalInsert final : public LogicalPlan {
    public:
      Statements::TableName* table;
      std::vector<Field> fields;
      explicit LogicalInsert(const std::string& dbName, Statements::TableName* table, const std::vector<Field>& fields);
      PhysicalPlan::PhysicalInsert* ToPhysical()override;
  };

  class LogicalSchemaCreate final : public LogicalPlan {
    public:
      std::string schemaName;
      explicit LogicalSchemaCreate(const std::string& dbName, std::string& schemaName);
      PhysicalPlan::PhysicalSchemaCreate* ToPhysical()override;
  };

  class LogicalDelete final : public LogicalPlan {
  public:
    Statements::TableName* table;
    Expressions::Expression* expression;
    explicit LogicalDelete(const std::string& dbName, Statements::TableName* table, Expressions::Expression* expression);
    PhysicalPlan::PhysicalOperator* ToPhysical()override;
  };

  class LogicalUpdate final : public LogicalPlan {
    public:
      Statements::TableName* table;
      std::vector<Field> fields;
      Expressions::Expression* expression;

      explicit LogicalUpdate(const std::string& dbName, Statements::TableName* table, std::vector<Field>& fields, Expressions::Expression* expression);
      PhysicalPlan::PhysicalOperator* ToPhysical()override;
  };

  class LogicalTableCreate final : public LogicalPlan {
    public:
      Statements::TableName* table;
      std::string constraintName;
      std::vector<Statements::AddColumn> columns;
      Headers::Index primaryKey;

      explicit LogicalTableCreate(
        const std::string& dbName,
        Statements::TableName* table,
        std::vector<Statements::AddColumn>& columns,
        Headers::Index& primaryKey,
        std::string  constraintName);
      PhysicalPlan::PhysicalTableCreate* ToPhysical()override;
  };
}

