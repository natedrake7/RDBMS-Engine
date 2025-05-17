#pragma once
#include "../Visitor.h"

namespace QueryPipeline {
  class LogicalPlan {
  public:
    virtual ~LogicalPlan() = default;
  };

  LogicalPlan* BuildLogicalPlan(const SelectStatement& statement);
  LogicalPlan* BuildLogicalPlan(const CreateDbStatement& statement);

  class LogicalCreateDatabase final : public LogicalPlan {
    public:
      std::string dbName;
      explicit LogicalCreateDatabase(const std::string &dbName) : dbName(dbName) {}
  };

  class LogicalProject final: public LogicalPlan {
    public:
      LogicalPlan* child;
      std::vector<std::string> columns;
      LogicalProject(LogicalPlan* child, const std::vector<std::string>& columns) : child(child), columns(std::move(columns)) {}
      ~LogicalProject() override {
        delete child;
      }
  };

  class LogicalTableScan final : public LogicalPlan {
    public:
      std::string tableName;
      explicit LogicalTableScan(const std::string& name) : tableName(name) {}
  };
}

