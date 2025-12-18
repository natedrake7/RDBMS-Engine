#include "../include/LogicalPlan.h"

#include <iostream>

#include "../include/Optimizer.h"
#include "../include/Statements.h"
#include "../../DatabaseEngine/include/SystemDatabases/SystemCatalog.h"

#include <utility>

#include "PipelineConstants.h"
#include "Managers/StatisticsManager.h"

namespace QueryPipeline {
  LogicalPlan::LogicalPlan(const DataTypes::Guid &sessionId, const int32_t &databaseId)
      : sessionId(sessionId), databaseId(databaseId) {}

  LogicalPlan::LogicalPlan(const DataTypes::Guid &sessionId)
    : sessionId(sessionId), databaseId(INVALID_DATABASE_ID) {}

  LogicalPlan::LogicalPlan(){
      this->sessionId = DataTypes::Guid();
      this->databaseId = INVALID_DATABASE_ID;
   }

  LogicalPlan::~LogicalPlan() = default;

  LogicalDeclareVariable::LogicalDeclareVariable(const DataTypes::Guid &sessionId, Variable& variable, Expressions::Expression* expression)
    : LogicalPlan(sessionId), variable(std::move(variable)), expression(expression){}

  PhysicalPlan::ExecutionNode * LogicalDeclareVariable::ToPhysical() {
    return new PhysicalPlan::PhysicalDeclareVariable(this->sessionId, this->variable, this->expression);
  }

  LogicalProject::LogicalProject(
    LogicalPlan *child,
    std::vector<Expressions::Expression*> &resultExpressions,
    std::vector<Headers::ColumnHeader>& columnsHeaders)
  : child(child), resultExpressions(std::move(resultExpressions)), columnsHeaders(std::move(columnsHeaders)) {}

  LogicalProject::~LogicalProject(){
      delete this->child;
  }

  PhysicalPlan::PhysicalProject * LogicalProject::ToPhysical(){
     return new PhysicalPlan::PhysicalProject(
         (this->child != nullptr) ? this->child->ToPhysical() : nullptr,
         this->resultExpressions,
         this->columnsHeaders
    );
  }

  bool LogicalTableScan::HasPredicate() const{
    return this->expression != nullptr;
  }

  LogicalTableScan::LogicalTableScan(Statements::DataSource* table, Expressions::Expression* expression)
  : table(table), expression(expression) {}

  PhysicalPlan::ExecutionNode * LogicalTableScan::ToPhysical(){
      auto indexes = DatabaseEngine::SystemCatalog::Get().SelectIndexes(this->table->tableId);

      // If no indexes are available, use heap scan
      if (indexes.empty())
        return new PhysicalPlan::PhysicalTableScan(this->table, this->expression);

      // If no filter expression, choose the best index for scanning
      if (!this->HasPredicate()) {
        const auto& firstIndex = indexes.front();

        if (firstIndex.isClustered)
          return new PhysicalPlan::PhysicalIndexScan(this->table, this->expression, true);

        // Otherwise use heap scan
        return new PhysicalPlan::PhysicalTableScan(this->table, this->expression);
      }

      const auto tableStats = DatabaseEngine::StatisticsManager::Get().GetTableStatistics(this->table->tableId);

    //Small table use heap Scan
      if (tableStats.rowCount < PipelineConstants::SMALL_TABLE)
        return new PhysicalPlan::PhysicalTableScan(this->table, this->expression);

      auto result = Optimizer::DetermineIndexSeekAnalyze(indexes, this->expression, tableStats);

      //scan the first index
      if (!result.canSeek)
        return new PhysicalPlan::PhysicalIndexScan(this->table, this->expression, indexes.front().isClustered);

      if (result.hasRange)
        return new PhysicalPlan::PhysicalIndexSeekRange(this->table, result.start, result.end, result.remainingPredicate);

      return new PhysicalPlan::PhysicalIndexSeek(this->table, result.start, result.remainingPredicate);
  }

   LogicalCreateUser::LogicalCreateUser(const DataTypes::Guid& sessionId, std::string& username, std::string& password, std::string& role)
     : LogicalPlan(sessionId), username(std::move(username)), password(std::move(password)), role(std::move(role)) {}

  LogicalCreateUser::~LogicalCreateUser() = default;

  PhysicalPlan::ExecutionNode * LogicalCreateUser::ToPhysical() {
    return new PhysicalPlan::PhysicalCreateUser(this->username, this->password, this->role);
  }

  LogicalGrantRole::LogicalGrantRole(const DataTypes::Guid& sessionId, std::string &username, std::string &role)
    : LogicalPlan(sessionId), username(std::move(username)), role(std::move(role)) {}

  PhysicalPlan::ExecutionNode * LogicalGrantRole::ToPhysical() {
    return new PhysicalPlan::PhysicalGrantRole(this->sessionId, this->username, this->role);
  }

  LogicalCreateDatabase::LogicalCreateDatabase(const DataTypes::Guid& sessionId, std::string& dbName) : LogicalPlan(sessionId), dbName(std::move(dbName)) {}

  PhysicalPlan::PhysicalCreateDatabase * LogicalCreateDatabase::ToPhysical(){
     return new PhysicalPlan::PhysicalCreateDatabase(this->sessionId, this->dbName);
  }

  LogicalUseDatabase::LogicalUseDatabase(const DataTypes::Guid &sessionId, const int32_t &databaseId)
    : databaseId(databaseId), sessionId(sessionId) {}

  PhysicalPlan::PhysicalUseDatabase * LogicalUseDatabase::ToPhysical(){
    return new PhysicalPlan::PhysicalUseDatabase(this->sessionId, this->databaseId);
  }

  LogicalJoin::LogicalJoin(
   LogicalPlan *left,
   LogicalPlan *right,
   Expressions::Expression *condition,
   const JoinType &type
  )
   : left(left), right(right), condition(condition), type(type) {}

  LogicalJoin::~LogicalJoin(){
    delete this->left;
    delete this->right;
  }

  PhysicalPlan::ExecutionNode * LogicalJoin::ToPhysical(){
    switch (this->type) {
      case JoinType::Inner:
        return new PhysicalPlan::PhysicalNestedLoopInnerJoin(
         left->ToPhysical(),
         right->ToPhysical(),
         this->condition
       );
      case JoinType::Left:
        return new PhysicalPlan::PhysicalNestedLoopLeftJoin(
          left->ToPhysical(),
          right->ToPhysical(),
        this->condition
        );
      case JoinType::Right:
        return new PhysicalPlan::PhysicalNestedLoopLeftJoin(
          right->ToPhysical(),
          left->ToPhysical(),
          this->condition
        );
      case JoinType::Full:
        return new PhysicalPlan::PhysicalNestedLoopFullJoin(
          left->ToPhysical(),
          right->ToPhysical(),
            this->condition
          );
      default:
          throw std::runtime_error("Unknown JoinType");
    }
  }

LogicalFilter::LogicalFilter(LogicalPlan* child, Expressions::Expression* filter)
  : child(child), filter(filter) {}

  PhysicalPlan::PhysicalFilter * LogicalFilter::ToPhysical(){
    return new PhysicalPlan::PhysicalFilter(this->child->ToPhysical(), this->filter);
  }

  LogicalInsert::LogicalInsert(
    Statements::DataSource* table,
    std::vector<Statements::Inserts> &fields,
    LogicalPlan* child,
    std::vector<column_index_t>& columnIndices
  ) : table(table), fields(std::move(fields)), child(child), columnsIndices(std::move(columnIndices)) {}

  LogicalInsert::~LogicalInsert(){
    delete this->child;
  }

  PhysicalPlan::PhysicalInsert * LogicalInsert::ToPhysical(){
    auto* physicalSelect = this->child != nullptr
        ? this->child->ToPhysical()
        : nullptr;

    return new PhysicalPlan::PhysicalInsert(this->table, this->fields, physicalSelect, this->columnsIndices);
  }

  LogicalSchemaCreate::LogicalSchemaCreate(const DataTypes::Guid& sessionId, const int32_t& databaseId, std::string &schemaName)
    : LogicalPlan(sessionId), schemaName(std::move(schemaName)), databaseId(databaseId) {}

  PhysicalPlan::PhysicalSchemaCreate * LogicalSchemaCreate::ToPhysical(){
    return new PhysicalPlan::PhysicalSchemaCreate(this->sessionId, this->databaseId, this->schemaName);
  }

  LogicalDelete::LogicalDelete(Statements::DataSource *table, Expressions::Expression *expression)
    : table(table), expression(expression) {}

  PhysicalPlan::ExecutionNode * LogicalDelete::ToPhysical(){
    const auto indexes = DatabaseEngine::SystemCatalog::Get().SelectIndexes(this->table->tableId);

    //if no indexes are available heap scan
    if (indexes.empty())
      return new PhysicalPlan::PhysicalHeapDelete(this->table, this->expression);

    //if expression is complex defer from index seek
    const bool canIndexSeek = expression != nullptr;// && !expression->IsComplex();

    // HashSet<column_index_t> expressionColumns;
    //
    // if (expression != nullptr)
    //   expression->GetColumns(expressionColumns);

    for (const auto& index: indexes) {
        const auto indexHeader = DatabaseEngine::SystemCatalog::Get().SelectIndexById(index.id);

          if (canIndexSeek) {
            for (const auto& column: index.columns) {
                //if columns is first prefer it, else break because index scan will occur
                //index seek
    //              if (!expressionColumns.Contains(column))
                  break;

            }
          }

        //find the first non clustered and use it
        return new PhysicalPlan::PhysicalIndexScanDelete(this->table, this->expression);
    }

    return new PhysicalPlan::PhysicalHeapDelete(this->table, this->expression);
  }

  LogicalTableCreate::LogicalTableCreate(
        const DataTypes::Guid& sessionId,
        Statements::DataSource*  table,
        std::vector<Statements::NewColumn*>& columns,
        std::vector<column_index_t> primaryKey,
        std::string  constraintName)
    : LogicalPlan(sessionId), table(table), constraintName(std::move(constraintName)),
      columns(std::move(columns)), primaryKey(std::move(primaryKey)) {}

  PhysicalPlan::PhysicalTableCreate * LogicalTableCreate::ToPhysical(){
    Headers::Index index;

    index.columns = std::move(primaryKey);

    return new PhysicalPlan::PhysicalTableCreate(this->sessionId, this->table, this->columns, index, this->constraintName);
  }

  LogicalUpdate::LogicalUpdate(Statements::DataSource *table, std::vector<Statements::UpdateColumn*>& updates, Expressions::Expression *expression)
  : table(table), updates(std::move(updates)), expression(expression) {}

  PhysicalPlan::ExecutionNode* LogicalUpdate::ToPhysical(){
      const auto indexes = DatabaseEngine::SystemCatalog::Get().SelectIndexes(this->table->tableId);

      //if no indexes are available heap scan
      if (indexes.empty())
        return new PhysicalPlan::PhysicalHeapUpdate(this->table, this->expression, this->updates);

      //if expression is complex defer from index seek
      const bool canIndexSeek = expression != nullptr; //&& !expression->IsComplex();
      
      HashSet<column_index_t> expressionColumns;
      //
      // if (expression != nullptr)
      //   expression->GetColumns(expressionColumns);

      for (const auto& index: indexes) {
          const auto indexHeader = DatabaseEngine::SystemCatalog::Get().SelectIndexById(index.id);

            if (canIndexSeek) {
              for (const auto& column: indexHeader.columns) {
                  //if columns is first prefer it, else break because index scan will occur
                  //index seek
                    if (expressionColumns.Contains(column.ordinalPosition))
                      return new PhysicalPlan::PhysicalIndexSeekUpdate(this->table, this->expression, this->updates);

                    break;
              }
            }

          //find the first non clustered and use it
          return new PhysicalPlan::PhysicalIndexScanUpdate(this->table, this->expression, this->updates);
      }

      return new PhysicalPlan::PhysicalHeapUpdate(this->table, this->expression, this->updates);
  }

  LogicalOrder::LogicalOrder(LogicalPlan *child, std::vector<Statements::OrderColumn*>& expressions)
    : child(child), expressions(std::move(expressions)) {}

  PhysicalPlan::ExecutionNode* LogicalOrder::ToPhysical(){
    return new PhysicalPlan::PhysicalOrderBy(this->child->ToPhysical(), this->expressions);
  }

  LogicalTop::LogicalTop(LogicalPlan *child, const int64_t &top)
    : child(child), top(top){}

  LogicalTop::~LogicalTop() {
    delete this->child;
  }

  PhysicalPlan::PhysicalTop * LogicalTop::ToPhysical(){
    return new PhysicalPlan::PhysicalTop(this->child->ToPhysical(), this->top);
  }

  LogicalDistinct::LogicalDistinct(LogicalPlan *child)
    : child(child) {}

  LogicalDistinct::~LogicalDistinct() {
    delete this->child;
  }

  PhysicalPlan::PhysicalDistinct * LogicalDistinct::ToPhysical(){
    return new PhysicalPlan::PhysicalDistinct(this->child->ToPhysical());
  }

  LogicalIndexCreate::LogicalIndexCreate(
    const DataTypes::Guid& sessionId,
    Statements::DataSource *table,
    std::string &constraintName,
    std::vector<column_index_t> &columns)
      : LogicalPlan(sessionId), table(table), constraintName(std::move(constraintName)), columns(std::move(columns)) {}

  PhysicalPlan::ExecutionNode * LogicalIndexCreate::ToPhysical(){
    return new PhysicalPlan::PhysicalIndexCreate(this->sessionId, this->table, this->constraintName, this->columns);
  }

  LogicalAlterTable::LogicalAlterTable(
    const DataTypes::Guid& sessionId,
    Statements::DataSource *table,
    const AlterTableType& type,
    Statements::NewColumn *column
  ): LogicalPlan(sessionId), table(table), type(type) {
      this->column = {
        .addColumn = column
      };
  }

  LogicalAlterTable::LogicalAlterTable(
    const DataTypes::Guid& sessionId,
    Statements::DataSource *table,
    const AlterTableType& type,
    Statements::AlterColumn *column
  ): LogicalPlan(sessionId), table(table), type(type) {
      this->column = {
        .alterColumn = column
      };
    }

  LogicalAlterTable::LogicalAlterTable(
    const DataTypes::Guid& sessionId,
    Statements::DataSource *table,
    const AlterTableType& type,
    Statements::RenameColumn *column
  ): LogicalPlan(sessionId), table(table), type(type) {
      this->column = {
        .renameColumn = column
      };
    }

  LogicalAlterTable::LogicalAlterTable(
    const DataTypes::Guid& sessionId,
    Statements::DataSource *table,
    const AlterTableType& type,
    Statements::DropColumn *column
  ): LogicalPlan(sessionId), table(table), type(type) {
      this->column = {
        .dropColumn = column
      };
    }

    PhysicalPlan::ExecutionNode * LogicalAlterTable::ToPhysical(){
      switch (this->type) {
        case AlterTableType::AddColumn:
          return new PhysicalPlan::PhysicalAddColumn(this->sessionId, this->table, this->column.addColumn);
        case AlterTableType::AlterColumn:
          return new PhysicalPlan::PhysicalAlterColumn(this->sessionId, this->table, this->column.alterColumn);
        case AlterTableType::RenameColumn:
          return new PhysicalPlan::PhysicalRenameColumn(this->sessionId, this->table, this->column.renameColumn);
        case AlterTableType::DropColumn:
          return new PhysicalPlan::PhysicalDropColumn(this->sessionId, this->table, this->column.dropColumn);
        default:
          return nullptr;
      }
    }
}

