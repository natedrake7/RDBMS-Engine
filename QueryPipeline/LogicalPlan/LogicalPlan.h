#pragma once
#include "../PhysicalPlan/PhysicalPlan.h"

namespace QueryPipeline {
  class LogicalPlan {
  public:
    int32_t databaseId;
    explicit LogicalPlan(const int32_t & databaseId);
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
      LogicalProject(const int32_t & databaseId, LogicalPlan* child, const std::vector<column_index_t>& columns);
      ~LogicalProject() override;
      PhysicalPlan::PhysicalProject* ToPhysical()override;
  };

  class LogicalTableScan final : public LogicalPlan {
    public:
      Statements::TableName* table;
      Expressions::Expression* expression;
      explicit LogicalTableScan(const int32_t & databaseId, Statements::TableName* table, Expressions::Expression* expression);
      PhysicalPlan::PhysicalOperator* ToPhysical() override;
  };

  class LogicalFilter final : public LogicalPlan {
    public:
      LogicalPlan* child;
      Expressions::Expression* filter;
      explicit LogicalFilter(const int32_t & databaseId, LogicalPlan* child, Expressions::Expression* filter);
      PhysicalPlan::PhysicalFilter* ToPhysical()override;
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
    Expressions::Expression* expression;
    explicit LogicalDelete(const int32_t & databaseId, Statements::TableName* table, Expressions::Expression* expression);
    PhysicalPlan::PhysicalOperator* ToPhysical()override;
  };

  class LogicalUpdate final : public LogicalPlan {
    public:
      Statements::TableName* table;
      std::vector<Field> fields;
      Expressions::Expression* expression;

      explicit LogicalUpdate(const int32_t & databaseId, Statements::TableName* table, std::vector<Field>& fields, Expressions::Expression* expression);
      PhysicalPlan::PhysicalOperator* ToPhysical()override;
  };

  class LogicalTableCreate final : public LogicalPlan {
    public:
      Statements::TableName* table;
      std::string constraintName;
      std::vector<Statements::AddColumn> columns;
      Statements::AutoIncrementKey* autoIncrementKey;
      vector<column_index_t> primaryKey;

      explicit LogicalTableCreate(
        const int32_t & databaseId,
        Statements::TableName* table,
        std::vector<Statements::AddColumn>& columns,
        std::vector<column_index_t> primaryKey,
        Statements::AutoIncrementKey* autoIncrementKey,
        std::string  constraintName);
      PhysicalPlan::PhysicalTableCreate* ToPhysical()override;
  };
}

