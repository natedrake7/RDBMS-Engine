#pragma once
#include "../Visitor.h"
#include "../PhysicalPlan/PhysicalPlan.h"

namespace QueryPipeline {
  class LogicalPlan {
  public:
    virtual ~LogicalPlan();
    virtual PhysicalPlan::PhysicalOperator* ToPhysical() = 0;
  };

  LogicalPlan* BuildLogicalPlan(const SelectStatement& statement);
  LogicalPlan* BuildLogicalPlan(const CreateDbStatement& statement);

  class LogicalCreateDatabase final : public LogicalPlan {
    public:
      std::string dbName;
      explicit LogicalCreateDatabase(const std::string &dbName);
      PhysicalPlan::PhysicalCreateDatabase* ToPhysical()override;
  };

  class LogicalProject final: public LogicalPlan {
    public:
      LogicalPlan* child;
      std::vector<std::string> columns;
      LogicalProject(LogicalPlan* child, const std::vector<std::string>& columns);
      ~LogicalProject() override;
      PhysicalPlan::PhysicalProject* ToPhysical()override;
  };

  class LogicalTableScan final : public LogicalPlan {
    public:
      std::string tableName;
      explicit LogicalTableScan(const std::string& name);
      PhysicalPlan::PhysicalTableScan* ToPhysical()override;
  };
}

