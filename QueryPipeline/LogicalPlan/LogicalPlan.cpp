#include "LogicalPlan.h"

#include "../../Server/Server.h"

#include <utility>

#include "../Statements/Statements.h"

namespace QueryPipeline {
   LogicalPlan::LogicalPlan(const int32_t & databaseId): databaseId(databaseId) {}

  LogicalPlan::~LogicalPlan() = default;

  LogicalProject::LogicalProject(const int32_t & databaseId, LogicalPlan *child, const std::vector<column_index_t> &columns, std::vector<std::string>& columnLiterals)
: LogicalPlan(), child(child), columns(columns), columnLiterals(std::move(columnLiterals)) {}

  LogicalProject::~LogicalProject(){
      delete child;
  }

  PhysicalPlan::PhysicalProject * LogicalProject::ToPhysical(){
     return new PhysicalPlan::PhysicalProject(this->databaseId, this->child->ToPhysical(), this->columns, this->columnLiterals);
  }

  LogicalTableScan::LogicalTableScan(const int32_t & databaseId, Statements::TableName* table, Expressions::Expression* expression)
  : LogicalPlan(databaseId), table(table), expression(expression) {}

  PhysicalPlan::PhysicalOperator * LogicalTableScan::ToPhysical(){
      const auto indexes = Server::ServerInstance::Get().SelectIndexes(this->table->tableId);

      //if no indexes are available heap scan
      if (indexes.empty())
        return new PhysicalPlan::PhysicalTableScan(this->databaseId, this->table);

      //if expression is complex defer from index seek
      const bool canIndexSeek = expression != nullptr && !expression->IsComplex();

      HashSet<column_index_t> expressionColumns;

      if (expression != nullptr)
        expression->GetColumns(expressionColumns);

    for (const auto& index: indexes) {
        const auto indexHeader = Server::ServerInstance::Get().SelectIndexById(index.id);

          if (canIndexSeek) {
            for (const auto& column: index.columns) {
                //if columns is first prefer it, else break because index scan will occur
                //index seek
//              if (!expressionColumns.Contains(column))
                  break;

            }
          }

        //find the first non clustered and use it
        return new PhysicalPlan::PhysicalIndexScan(this->databaseId, this->table, this->expression, index.isClustered);
      }

    return new PhysicalPlan::PhysicalTableScan(this->databaseId, this->table);
  }

  LogicalCreateDatabase::LogicalCreateDatabase(std::string dbName) : dbName(std::move(dbName)) {}

  PhysicalPlan::PhysicalCreateDatabase * LogicalCreateDatabase::ToPhysical(){
     return new PhysicalPlan::PhysicalCreateDatabase(this->dbName);
  }

  LogicalFilter::LogicalFilter(const int32_t & databaseId, LogicalPlan* child, Expressions::Expression* filter): LogicalPlan(databaseId), child(child), filter(filter) {}

  PhysicalPlan::PhysicalFilter * LogicalFilter::ToPhysical(){
    return new PhysicalPlan::PhysicalFilter(this->databaseId, this->child->ToPhysical(), this->filter);
  }

  LogicalInsert::LogicalInsert(const int32_t & databaseId, Statements::TableName* table, const std::vector<Field> &fields): LogicalPlan(databaseId), table(table), fields(fields) {}

  PhysicalPlan::PhysicalInsert * LogicalInsert::ToPhysical(){
    return new PhysicalPlan::PhysicalInsert(this->databaseId, this->table, this->fields);
  }

  LogicalSchemaCreate::LogicalSchemaCreate(const int32_t &databaseId, std::string &schemaName)
    : LogicalPlan(databaseId), schemaName(std::move(schemaName)) {}

  PhysicalPlan::PhysicalSchemaCreate * LogicalSchemaCreate::ToPhysical(){
    return new PhysicalPlan::PhysicalSchemaCreate(this->databaseId, this->schemaName);
  }

  LogicalDelete::LogicalDelete(const int32_t &databaseId, Statements::TableName *table, Expressions::Expression *expression)
    : LogicalPlan(databaseId), table(table), expression(expression) {}

  PhysicalPlan::PhysicalOperator * LogicalDelete::ToPhysical(){
    const auto indexes = Server::ServerInstance::Get().SelectIndexes(this->table->tableId);

    //if no indexes are available heap scan
    if (indexes.empty())
      return new PhysicalPlan::PhysicalHeapDelete(this->databaseId, this->table, this->expression);

    //if expression is complex defer from index seek
    const bool canIndexSeek = expression != nullptr && !expression->IsComplex();

    HashSet<column_index_t> expressionColumns;

    if (expression != nullptr)
      expression->GetColumns(expressionColumns);

    for (const auto& index: indexes) {
        const auto indexHeader = Server::ServerInstance::Get().SelectIndexById(index.id);

          if (canIndexSeek) {
            for (const auto& column: index.columns) {
                //if columns is first prefer it, else break because index scan will occur
                //index seek
    //              if (!expressionColumns.Contains(column))
                  break;

            }
          }

        //find the first non clustered and use it
        return new PhysicalPlan::PhysicalIndexScanDelete(this->databaseId, this->table, this->expression);
    }

    return new PhysicalPlan::PhysicalHeapDelete(this->databaseId, this->table, this->expression);
  }

  LogicalTableCreate::LogicalTableCreate(
        const int32_t & databaseId,
        Statements::TableName*  table,
        std::vector<Statements::AddColumn>& columns,
        std::vector<column_index_t> primaryKey,
        Statements::AutoIncrementKey* autoIncrementKey,
        std::string  constraintName)
    : LogicalPlan(databaseId), table(table), columns(std::move(columns)),
      primaryKey(std::move(primaryKey)), constraintName(std::move(constraintName)),
      autoIncrementKey(autoIncrementKey) {}

  PhysicalPlan::PhysicalTableCreate * LogicalTableCreate::ToPhysical(){
    Headers::Index index;

    index.columns = std::move(primaryKey);
    if(this->autoIncrementKey){
      index.seed = this->autoIncrementKey->seed;
      index.incrementFactor = this->autoIncrementKey->incrementFactor;
      index.lastValue = this->autoIncrementKey->seed;
    }

    return new PhysicalPlan::PhysicalTableCreate(this->databaseId, this->table, this->columns, index, this->constraintName);
  }

  LogicalUpdate::LogicalUpdate(const int32_t& databaseId, Statements::TableName *table, vector<Field> & fields, Expressions::Expression *expression)
  : LogicalPlan(databaseId), table(table), fields(std::move(fields)), expression(expression) {}

  PhysicalPlan::PhysicalOperator* LogicalUpdate::ToPhysical(){
      const auto indexes = Server::ServerInstance::Get().SelectIndexes(this->table->tableId);

      //if no indexes are available heap scan
      if (indexes.empty())
        return new PhysicalPlan::PhysicalHeapUpdate(this->databaseId, this->table, this->expression, this->fields);

      //if expression is complex defer from index seek
      const bool canIndexSeek = expression != nullptr && !expression->IsComplex();
      
      HashSet<column_index_t> expressionColumns;

      if (expression != nullptr)
        expression->GetColumns(expressionColumns);

      for (const auto& index: indexes) {
          const auto indexHeader = Server::ServerInstance::Get().SelectIndexById(index.id);

            if (canIndexSeek) {
              for (const auto& column: indexHeader.columns) {
                  //if columns is first prefer it, else break because index scan will occur
                  //index seek
                    if (expressionColumns.Contains(column.ordinalPosition))
                      return new PhysicalPlan::PhysicalIndexSeekUpdate(this->databaseId, this->table, this->expression, this->fields);

                    break;
              }
            }

          //find the first non clustered and use it
          return new PhysicalPlan::PhysicalIndexScanUpdate(this->databaseId, this->table, this->expression, this->fields);
      }

      return new PhysicalPlan::PhysicalHeapUpdate(this->databaseId, this->table, this->expression, this->fields);
  }

  LogicalOrder::LogicalOrder(const int32_t & databaseId, LogicalPlan *child, vector<column_index_t> & columns, const OrderType & orderType)
    : LogicalPlan(databaseId), child(child), columns(std::move(columns)), orderType(orderType) {}

  PhysicalPlan::PhysicalOperator* LogicalOrder::ToPhysical(){
    return new PhysicalPlan::PhysicalOrderBy(this->databaseId, this->child->ToPhysical(), this->columns, this->orderType);
  }
}

