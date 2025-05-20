#pragma once
#include "../PhysicalPlan/PhysicalPlan.h"

namespace QueryPipeline {
  class LogicalPlan {
  public:
    virtual ~LogicalPlan();
    virtual PhysicalPlan::PhysicalOperator* ToPhysical() = 0;
  };

  LogicalPlan* BuildLogicalPlan(const Statements::SelectStatement& statement);
  LogicalPlan* BuildLogicalPlan(const Statements::CreateDbStatement& statement);
  LogicalPlan* BuildLogicalPlan(const Statements::InsertStatement& statement);

  class LogicalCreateDatabase final : public LogicalPlan {
    public:
      std::string dbName;
      explicit LogicalCreateDatabase(const std::string &dbName);
      PhysicalPlan::PhysicalCreateDatabase* ToPhysical()override;
  };

  class LogicalProject final: public LogicalPlan {
    public:
      LogicalPlan* child;
      std::vector<column_index_t> columns;
      LogicalProject(LogicalPlan* child, const std::vector<column_index_t>& columns);
      ~LogicalProject() override;
      PhysicalPlan::PhysicalProject* ToPhysical()override;
  };

  class LogicalTableScan final : public LogicalPlan {
    public:
      std::string tableName;
      explicit LogicalTableScan(const std::string& name);
      PhysicalPlan::PhysicalTableScan* ToPhysical()override;
  };

  class LogicalFilter final : public LogicalPlan {
    public:
      LogicalPlan* child;
      Statements::Expression* filter;
      explicit LogicalFilter(LogicalPlan* child, Statements::Expression* filter);
      PhysicalPlan::PhysicalFilter* ToPhysical()override;
  };

  class LogicalInsert final : public LogicalPlan {
    public:
      std::string tableName;
      std::vector<Field> fields;
      explicit LogicalInsert(const std::string& tableName, const std::vector<Field>& fields);
      PhysicalPlan::PhysicalInsert* ToPhysical()override;
  };
}

