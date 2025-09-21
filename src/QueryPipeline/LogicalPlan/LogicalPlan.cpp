#include "LogicalPlan.h"

#include "../../Server/Server.h"

#include <utility>

#include "../Statements/Statements.h"

namespace QueryPipeline {
   LogicalPlan::LogicalPlan(const int32_t & databaseId): databaseId(databaseId) {}

  LogicalPlan::~LogicalPlan() = default;

  LogicalProject::LogicalProject(
    const int32_t & databaseId,
    LogicalPlan *child,
    std::vector<Expressions::Expression*> &resultExpressions,
    std::vector<Headers::ColumnHeader>& columnsHeaders)
  : child(child), resultExpressions(std::move(resultExpressions)), columnsHeaders(std::move(columnsHeaders)) {}

  LogicalProject::~LogicalProject(){
      delete child;
  }

  PhysicalPlan::PhysicalProject * LogicalProject::ToPhysical(){
     return new PhysicalPlan::PhysicalProject(
       this->databaseId,
       (this->child != nullptr) ? this->child->ToPhysical() : nullptr,
       this->resultExpressions,
       this->columnsHeaders
       );
  }

  LogicalTableScan::LogicalTableScan(const int32_t & databaseId, Statements::TableName* table, Expressions::Expression* expression)
  : LogicalPlan(databaseId), table(table), expression(expression) {}

  PhysicalPlan::PhysicalOperator * LogicalTableScan::ToPhysical(){
      const auto indexes = Server::ServerInstance::Get().SelectIndexes(this->table->tableId);

      //if no indexes are available heap scan
      if (indexes.empty())
        return new PhysicalPlan::PhysicalTableScan(this->databaseId, this->table);

      //if expression is complex defer from index seek
      const bool canIndexSeek = expression != nullptr;

      HashSet<column_index_t> expressionColumns;

      // if (expression != nullptr)
      //   expression->GetColumns(expressionColumns);

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

 LogicalJoin::LogicalJoin(
   const int32_t& databaseId,
   LogicalTableScan *left,
   LogicalTableScan *right,
   Expressions::Expression *condition,
   const JoinType &type)
   : LogicalPlan(databaseId), left(left), right(right), condition(condition), type(type) {}

  PhysicalPlan::PhysicalOperator * LogicalJoin::ToPhysical(){

    return new PhysicalPlan::PhysicalNestedLoopJoin(this->databaseId, 0, 0, this->condition);
  }

LogicalFilter::LogicalFilter(const int32_t & databaseId, LogicalPlan* child, Expressions::Expression* filter)
  : LogicalPlan(databaseId), child(child), filter(filter) {}

  PhysicalPlan::PhysicalFilter * LogicalFilter::ToPhysical(){
    return new PhysicalPlan::PhysicalFilter(this->databaseId, this->child->ToPhysical(), this->filter);
  }

  LogicalInsert::LogicalInsert(
    const int32_t & databaseId,
    Statements::TableName* table,
    std::vector<Statements::InsertColumns> &fields,
    LogicalPlan* child,
    std::vector<column_index_t>& selectColumnsIndices
  )
    : LogicalPlan(databaseId), table(table), fields(std::move(fields)), child(child), selectColumnsIndices(std::move(selectColumnsIndices)) {}

  LogicalInsert::~LogicalInsert(){
    delete this->child;
  }

  PhysicalPlan::PhysicalInsert * LogicalInsert::ToPhysical(){
    auto* physicalSelect = this->child != nullptr
        ? this->child->ToPhysical()
        : nullptr;

    return new PhysicalPlan::PhysicalInsert(this->databaseId, this->table, this->fields, physicalSelect, this->selectColumnsIndices);
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
    const bool canIndexSeek = expression != nullptr;// && !expression->IsComplex();

    // HashSet<column_index_t> expressionColumns;
    //
    // if (expression != nullptr)
    //   expression->GetColumns(expressionColumns);

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
        std::vector<Statements::NewColumn*>& columns,
        std::vector<column_index_t> primaryKey,
        std::string  constraintName)
    : LogicalPlan(databaseId), table(table), columns(std::move(columns)),
      primaryKey(std::move(primaryKey)), constraintName(std::move(constraintName)) {}

  PhysicalPlan::PhysicalTableCreate * LogicalTableCreate::ToPhysical(){
    Headers::Index index;

    index.columns = std::move(primaryKey);

    return new PhysicalPlan::PhysicalTableCreate(this->databaseId, this->table, this->columns, index, this->constraintName);
  }

  LogicalUpdate::LogicalUpdate(const int32_t& databaseId, Statements::TableName *table, std::vector<Statements::UpdateColumn*>& updates, Expressions::Expression *expression)
  : LogicalPlan(databaseId), table(table), updates(std::move(updates)), expression(expression) {}

  PhysicalPlan::PhysicalOperator* LogicalUpdate::ToPhysical(){
      const auto indexes = Server::ServerInstance::Get().SelectIndexes(this->table->tableId);

      //if no indexes are available heap scan
      if (indexes.empty())
        return new PhysicalPlan::PhysicalHeapUpdate(this->databaseId, this->table, this->expression, this->updates);

      //if expression is complex defer from index seek
      const bool canIndexSeek = expression != nullptr; //&& !expression->IsComplex();
      
      HashSet<column_index_t> expressionColumns;
      //
      // if (expression != nullptr)
      //   expression->GetColumns(expressionColumns);

      for (const auto& index: indexes) {
          const auto indexHeader = Server::ServerInstance::Get().SelectIndexById(index.id);

            if (canIndexSeek) {
              for (const auto& column: indexHeader.columns) {
                  //if columns is first prefer it, else break because index scan will occur
                  //index seek
                    if (expressionColumns.Contains(column.ordinalPosition))
                      return new PhysicalPlan::PhysicalIndexSeekUpdate(this->databaseId, this->table, this->expression, this->updates);

                    break;
              }
            }

          //find the first non clustered and use it
          return new PhysicalPlan::PhysicalIndexScanUpdate(this->databaseId, this->table, this->expression, this->updates);
      }

      return new PhysicalPlan::PhysicalHeapUpdate(this->databaseId, this->table, this->expression, this->updates);
  }

  LogicalOrder::LogicalOrder(const int32_t & databaseId, LogicalPlan *child, std::vector<Statements::OrderColumn*>& expressions)
    : LogicalPlan(databaseId), child(child), expressions(std::move(expressions)) {}

  PhysicalPlan::PhysicalOperator* LogicalOrder::ToPhysical(){
    return new PhysicalPlan::PhysicalOrderBy(this->databaseId, this->child->ToPhysical(), this->expressions);
  }

  LogicalIndexCreate::LogicalIndexCreate(
    const int32_t &databaseId,
    Statements::TableName *table,
    std::string &constraintName,
    std::vector<column_index_t> &columns)
      : LogicalPlan(databaseId), table(table), constraintName(std::move(constraintName)), columns(std::move(columns)) {}

  PhysicalPlan::PhysicalOperator * LogicalIndexCreate::ToPhysical(){
    return new PhysicalPlan::PhysicalIndexCreate(this->databaseId, this->table, this->constraintName, this->columns);
  }

  LogicalAlterTable::LogicalAlterTable(
    const int32_t &databaseId,
    Statements::TableName *table,
    const AlterTableType& type,
    Statements::AlterColumn *alterColumn,
    Statements::NewColumn *addColumn,
    Statements::DropColumn *dropColumn,
    Statements::RenameColumn *renameColumn)
    : LogicalPlan(databaseId), table(table), type(type) , alterColumn(alterColumn), dropColumn(dropColumn), renameColumn(renameColumn), addColumn(addColumn) {}

    PhysicalPlan::PhysicalOperator * LogicalAlterTable::ToPhysical(){
      switch (this->type) {
        case AlterTableType::AlterColumn:
          return new PhysicalPlan::PhysicalAlterColumn(this->databaseId, this->table, this->alterColumn);
        case AlterTableType::AddColumn:
          return new PhysicalPlan::PhysicalAddColumn(this->databaseId, this->table, this->addColumn);
        case AlterTableType::DropColumn:
          return new PhysicalPlan::PhysicalDropColumn(this->databaseId, this->table, this->dropColumn);
        case AlterTableType::RenameColumn:
          return new PhysicalPlan::PhysicalRenameColumn(this->databaseId, this->table, this->renameColumn);
        default:
          return nullptr;
      }
    }
}

