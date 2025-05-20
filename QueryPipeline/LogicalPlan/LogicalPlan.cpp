#include "LogicalPlan.h"

#include "../Statements/Statements.h"

namespace QueryPipeline {
  LogicalPlan::~LogicalPlan() = default;

  LogicalProject::LogicalProject(LogicalPlan *child, const std::vector<column_index_t> &columns)
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

  LogicalFilter::LogicalFilter(LogicalPlan* child, Statements::Expression* filter): child(child), filter(filter) {}

  PhysicalPlan::PhysicalFilter * LogicalFilter::ToPhysical(){
    return new PhysicalPlan::PhysicalFilter(this->child->ToPhysical(), this->filter);
  }

  LogicalInsert::LogicalInsert(const std::string &tableName, const std::vector<Field> &fields): tableName(tableName), fields(fields) {}

  PhysicalPlan::PhysicalInsert * LogicalInsert::ToPhysical(){
    return new PhysicalPlan::PhysicalInsert(this->tableName, this->fields);
  }
}

