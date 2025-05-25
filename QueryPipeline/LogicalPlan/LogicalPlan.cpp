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

  LogicalTableScan::LogicalTableScan(const std::string& dbName, Statements::TableName* table, Expressions::Expression* expression)
  : LogicalPlan(dbName), table(table), expression(expression) {}

  PhysicalPlan::PhysicalOperator * LogicalTableScan::ToPhysical(){
      const auto indexes = Server::ServerInstance::Get().SelectIndexes(dbName, this->table->name);

      //if no indexes are available heap scan
      if (indexes.empty())
        return new PhysicalPlan::PhysicalTableScan(dbName, this->table);

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
        return new PhysicalPlan::PhysicalIndexScan(dbName, this->table, this->expression, index.isClustered);
      }

    return new PhysicalPlan::PhysicalTableScan(dbName, this->table);
  }

  LogicalCreateDatabase::LogicalCreateDatabase(std::string dbName) : dbName(std::move(dbName)) {}

  PhysicalPlan::PhysicalCreateDatabase * LogicalCreateDatabase::ToPhysical(){
     return new PhysicalPlan::PhysicalCreateDatabase(this->dbName);
  }

  LogicalFilter::LogicalFilter(const std::string& dbName, LogicalPlan* child, Expressions::Expression* filter): LogicalPlan(dbName), child(child), filter(filter) {}

  PhysicalPlan::PhysicalFilter * LogicalFilter::ToPhysical(){
    return new PhysicalPlan::PhysicalFilter(dbName, this->child->ToPhysical(), this->filter);
  }

  LogicalInsert::LogicalInsert(const std::string& dbName, Statements::TableName* table, const std::vector<Field> &fields): LogicalPlan(dbName), table(table), fields(fields) {}

  PhysicalPlan::PhysicalInsert * LogicalInsert::ToPhysical(){
    return new PhysicalPlan::PhysicalInsert(dbName, this->table, this->fields);
  }

  LogicalSchemaCreate::LogicalSchemaCreate(const std::string &dbName, std::string &schemaName)
    : LogicalPlan(dbName), schemaName(std::move(schemaName)) {}

  PhysicalPlan::PhysicalSchemaCreate * LogicalSchemaCreate::ToPhysical(){
    return new PhysicalPlan::PhysicalSchemaCreate(this->dbName, this->schemaName);
  }

  LogicalDelete::LogicalDelete(const std::string &dbName, Statements::TableName *table, Expressions::Expression *expression)
    : LogicalPlan(dbName), table(table), expression(expression) {}

  PhysicalPlan::PhysicalOperator * LogicalDelete::ToPhysical(){
    const auto indexes = Server::ServerInstance::Get().SelectIndexes(dbName, this->table->name);

    //if no indexes are available heap scan
    if (indexes.empty())
      return new PhysicalPlan::PhysicalHeapDelete(dbName, this->table, this->expression);

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
      return new PhysicalPlan::PhysicalIndexScanDelete(dbName, this->table, this->expression);
    }

    return new PhysicalPlan::PhysicalHeapDelete(this->dbName, this->table, this->expression);
  }

  LogicalTableCreate::LogicalTableCreate(const std::string& dbName, Statements::TableName*  table, std::vector<Statements::AddColumn>& columns, std::vector<column_index_t>& primaryKey, std::string  constraintName)
    : LogicalPlan(dbName), table(table), constraintName(std::move(constraintName)), columns(std::move(columns)), primaryKey(std::move(primaryKey)) {}

  PhysicalPlan::PhysicalTableCreate * LogicalTableCreate::ToPhysical(){
    return new PhysicalPlan::PhysicalTableCreate(dbName, this->table, this->columns, this->primaryKey, this->constraintName);
  }
}

