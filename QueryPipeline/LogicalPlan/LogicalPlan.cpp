#include "LogicalPlan.h"

#include "../../Server/Server.h"

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

  LogicalTableScan::LogicalTableScan(const std::string& dbName, std::string name, Statements::Expression* expression)
  : LogicalPlan(dbName), tableName(std::move(name)), expression(expression) {}

  PhysicalPlan::PhysicalOperator * LogicalTableScan::ToPhysical(){
      const auto indexes = Server::ServerInstance::Get().SelectIndexes(dbName, tableName);

      //if no indexes are available heap scan
      if (indexes.empty())
        return new PhysicalPlan::PhysicalTableScan(dbName, this->tableName);

      //if expression is complex defer from index seek
      const bool canIndexSeek = expression != nullptr && !expression->IsComplex();

      HashSet<column_index_t> expressionColumns;

      if (expression != nullptr)
        expression->GetColumns(expressionColumns);


      for (const auto& index: indexes) {
          if (canIndexSeek) {
            for (const auto& column: index.columns) {
                //if columns is first prefer it, else break because index scan will occur
                //index seek
              if (!expressionColumns.Contains(column))
                  break;


            }
          }

        //find the first non clustered and use it
        return new PhysicalPlan::PhysicalIndexScan(dbName, this->tableName, index.isClustered);
      }

    return new PhysicalPlan::PhysicalTableScan(dbName, this->tableName);
  }

  LogicalTableIndexSeek::LogicalTableIndexSeek(
    const std::string &dbName, std::string tableName,
    const Field& minValue, const Field& maxValue)
  : LogicalPlan(dbName), tableName(std::move(tableName)), minValue(minValue), maxValue(maxValue) {}

  PhysicalPlan::PhysicalIndexSeek * LogicalTableIndexSeek::ToPhysical(){
    return new PhysicalPlan::PhysicalIndexSeek(this->dbName, this->tableName, this->minValue, this->maxValue);
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

