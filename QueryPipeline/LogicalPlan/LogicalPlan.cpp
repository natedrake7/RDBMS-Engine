#include "LogicalPlan.h"

#include <utility>

#include "../Statements/Statements.h"

namespace QueryPipeline {
   LogicalPlan::LogicalPlan(const std::string& dbName): dbName(dbName) {}

  LogicalPlan::~LogicalPlan() = default;

  LogicalProject::LogicalProject(const std::string& dbName, LogicalPlan *child, const std::vector<column_index_t> &columns)
: LogicalPlan(), child(child), columns(columns) {}

  LogicalProject::~LogicalProject(){
      delete child;
  }

  PhysicalPlan::PhysicalProject * LogicalProject::ToPhysical(){
     return new PhysicalPlan::PhysicalProject(dbName, this->child->ToPhysical(), this->columns);
  }

  LogicalTableScan::LogicalTableScan(const std::string& dbName, std::string name) : LogicalPlan(dbName), tableName(std::move(name)) {}

  PhysicalPlan::PhysicalTableScan * LogicalTableScan::ToPhysical(){
      return new PhysicalPlan::PhysicalTableScan(dbName, this->tableName);
  }

  LogicalCreateDatabase::LogicalCreateDatabase(std::string dbName) : dbName(std::move(dbName)) {}

  PhysicalPlan::PhysicalCreateDatabase * LogicalCreateDatabase::ToPhysical(){
     return new PhysicalPlan::PhysicalCreateDatabase(this->dbName);
  }

  LogicalFilter::LogicalFilter(const std::string& dbName, LogicalPlan* child, Statements::Expression* filter): LogicalPlan(dbName), child(child), filter(filter) {}

  PhysicalPlan::PhysicalFilter * LogicalFilter::ToPhysical(){
    return new PhysicalPlan::PhysicalFilter(dbName, this->child->ToPhysical(), this->filter);
  }

  LogicalInsert::LogicalInsert(const std::string& dbName, std::string tableName, const std::vector<Field> &fields): LogicalPlan(dbName), tableName(std::move(tableName)), fields(fields) {}

  PhysicalPlan::PhysicalInsert * LogicalInsert::ToPhysical(){
    return new PhysicalPlan::PhysicalInsert(dbName, this->tableName, this->fields);
  }

  LogicalTableCreate::LogicalTableCreate(const std::string& dbName, std::string  name, std::vector<Statements::AddColumn>& columns, std::vector<column_index_t>& primaryKey)
    : LogicalPlan(dbName), name(std::move(name)), columns(std::move(columns)), primaryKey(std::move(primaryKey)) {}

  PhysicalPlan::PhysicalTableCreate * LogicalTableCreate::ToPhysical(){
    return new PhysicalPlan::PhysicalTableCreate(dbName, this->name, this->columns, this->primaryKey);
  }
}

