#include "LogicalPlan.h"

namespace QueryPipeline {
  LogicalPlan::~LogicalPlan() = default;

  LogicalProject::LogicalProject(LogicalPlan *child, const std::vector<std::string> &columns)
        : child(child), columns(columns) {}

  LogicalProject::~LogicalProject(){
      delete child;
  }

  PhysicalPlan::PhysicalProject * LogicalProject::ToPhysical() {
     return new PhysicalPlan::PhysicalProject(this->child->ToPhysical(), this->columns);
  }

  LogicalTableScan::LogicalTableScan(const std::string &name) : tableName(name) {}

  PhysicalPlan::PhysicalTableScan * LogicalTableScan::ToPhysical(){
      return new PhysicalPlan::PhysicalTableScan(this->tableName);
  }

  LogicalCreateDatabase::LogicalCreateDatabase(const std::string &dbName) : dbName(dbName) {}

  PhysicalPlan::PhysicalCreateDatabase * LogicalCreateDatabase::ToPhysical(){
     return new PhysicalPlan::PhysicalCreateDatabase(this->dbName);
  }

  LogicalFilter::LogicalFilter(LogicalPlan* child, const vector<Expression>& filters): child(child), filters(filters) {}

  PhysicalPlan::PhysicalFilter * LogicalFilter::ToPhysical(){
    return new PhysicalPlan::PhysicalFilter(this->child->ToPhysical(), this->filters);
  }

  LogicalPlan * BuildLogicalPlan(const SelectStatement &statement){
      const auto scanTable = new LogicalTableScan(statement.table);

      LogicalPlan* current = scanTable;

      if (!statement.where.expressions.empty())
        current = new LogicalFilter(current, statement.where.expressions);
    

      if (!statement.columns.empty())
        current = new LogicalProject(current, statement.columns);

    return current;
  }

  LogicalPlan * BuildLogicalPlan(const CreateDbStatement &statement){
      return new LogicalCreateDatabase(statement.name);
    }
}

