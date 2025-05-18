#include "LogicalPlan.h"

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

  LogicalFilter::LogicalFilter(LogicalPlan* child, Expression* filter): child(child), filter(filter) {}

  PhysicalPlan::PhysicalFilter * LogicalFilter::ToPhysical(){
    return new PhysicalPlan::PhysicalFilter(this->child->ToPhysical(), this->filter);
  }

  LogicalInsert::LogicalInsert(const std::string &tableName, const std::vector<Field> &fields): tableName(tableName), fields(fields) {}

  PhysicalPlan::PhysicalInsert * LogicalInsert::ToPhysical(){
    return new PhysicalPlan::PhysicalInsert(this->tableName, this->fields);
  }

  LogicalPlan * BuildLogicalPlan(const SelectStatement &statement){
      const auto scanTable = new LogicalTableScan(statement.table);

      LogicalPlan* current = scanTable;

      if (statement.where.expression != nullptr)
        current = new LogicalFilter(current, statement.where.expression);
    

      if (!statement.columns.empty())
        current = new LogicalProject(current, statement.columnIndices);

    return current;
  }

  LogicalPlan * BuildLogicalPlan(const CreateDbStatement &statement){
      return new LogicalCreateDatabase(statement.name);
    }

  LogicalPlan* BuildLogicalPlan(const InsertStatement& statement) {
    return new LogicalInsert(statement.tableName, statement.values);
  }
}

