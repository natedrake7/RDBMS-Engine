#include "../include/LogicalPlan.h"
#include "Managers/StatisticsManager.h"
#include "../include/Optimizer.h"
#include "../include/Statements.h"
#include "../../DatabaseEngine/include/SystemDatabases/SystemCatalog.h"

#include <utility>

#include "DatabaseConstants.h"
#include "Parser.h"


namespace QueryPipeline {
  LogicalPlan::LogicalPlan(const DataTypes::Guid &sessionId, const Int databaseId)
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

  PhysicalPlan::ExecutionNode * LogicalDeclareVariable::ToPhysical(QueryContext& context) {
    return context._context.Allocate<PhysicalPlan::PhysicalDeclareVariable>(this->sessionId, this->variable, this->expression);
  }

  LogicalCreateUser::LogicalCreateUser(const DataTypes::Guid& sessionId,DataTypes::String& username,DataTypes::String& password,DataTypes::String& role)
    : LogicalPlan(sessionId), username(std::move(username)), password(std::move(password)), role(std::move(role)) {}

  LogicalCreateUser::~LogicalCreateUser() = default;

  PhysicalPlan::ExecutionNode * LogicalCreateUser::ToPhysical(QueryContext& context) {
    return context._context.Allocate<PhysicalPlan::PhysicalCreateUser>(this->username, this->password, this->role);
  }

  LogicalGrantRole::LogicalGrantRole(const DataTypes::Guid& sessionId, DataTypes::String& username, DataTypes::String& role)
    : LogicalPlan(sessionId), username(std::move(username)), role(std::move(role)) {}

  PhysicalPlan::ExecutionNode * LogicalGrantRole::ToPhysical(QueryContext& context) {
    return context._context.Allocate<PhysicalPlan::PhysicalGrantRole>(this->sessionId, this->username, this->role);
  }

  LogicalCreateDatabase::LogicalCreateDatabase(const DataTypes::Guid& sessionId,DataTypes::String& dbName) : LogicalPlan(sessionId), dbName(std::move(dbName)) {}

  PhysicalPlan::PhysicalCreateDatabase * LogicalCreateDatabase::ToPhysical(QueryContext& context){
    return context._context.Allocate<PhysicalPlan::PhysicalCreateDatabase>(this->sessionId, this->dbName);
  }

  LogicalUseDatabase::LogicalUseDatabase(const DataTypes::Guid &sessionId, const Int databaseId)
    : databaseId(databaseId), sessionId(sessionId) {}

  PhysicalPlan::PhysicalUseDatabase * LogicalUseDatabase::ToPhysical(QueryContext& context){
    return context._context.Allocate<PhysicalPlan::PhysicalUseDatabase>(this->sessionId, this->databaseId);
  }

  LogicalProject::LogicalProject(
    LogicalPlan *child,
    std::vector<Expressions::Expression*> &resultExpressions,
    std::vector<Headers::ColumnHeader>& columnsHeaders)
    : child(child), resultExpressions(std::move(resultExpressions)), columnsHeaders(std::move(columnsHeaders)) {}

  LogicalProject::~LogicalProject(){
    delete this->child;
  }

  PhysicalPlan::PhysicalProject * LogicalProject::ToPhysical(QueryContext& context){
    return context._context.Allocate<PhysicalPlan::PhysicalProject>(
      (this->child != nullptr) ? this->child->ToPhysical(context) : nullptr,
      this->resultExpressions,
      this->columnsHeaders
    );
  }

  bool LogicalTableScan::HasPredicate() const{
    return this->expression != nullptr;
  }

  LogicalTableScan::LogicalTableScan(Statements::DataSource* table, Expressions::Expression* expression)
    : table(table), expression(expression) {}

  PhysicalPlan::ExecutionNode* LogicalTableScan::ToPhysical(QueryContext& context){
    auto indexes = DatabaseEngine::SystemCatalog::Get().SelectIndexes(context._context.GetAllocator(), this->table->tableId);

    // If no indexes are available, use heap scan
    if (indexes.empty())
      return context._context.Allocate<PhysicalPlan::PhysicalTableScan>(this->table, this->expression);

    // If no filter expression, choose the best index for scanning
    const auto& firstIndex = indexes.front();
    if (!this->HasPredicate()) {
      if (firstIndex.isClustered)
        return context._context.Allocate<PhysicalPlan::PhysicalIndexScan>(this->table, this->expression, true);

      // Otherwise use heap scan
      return context._context.Allocate<PhysicalPlan::PhysicalTableScan>(this->table, this->expression);
    }

    const auto tableStats = DatabaseEngine::StatisticsManager::Get().GetTableStatistics(this->table->tableId);

    //no table stats yet, or small table
    if (tableStats.tableId == INVALID_TABLE_ID || tableStats.rowCount < PipelineConstants::SMALL_TABLE){
      if (firstIndex.isClustered)
        return context._context.Allocate<PhysicalPlan::PhysicalIndexScan>(this->table, this->expression, true);

      return context._context.Allocate<PhysicalPlan::PhysicalTableScan>(this->table, this->expression);
    }

    Optimizer optimizer(context);
    //else use optimizer to choose index seek/scan
    auto result = optimizer.PerformIndexAnalysis(indexes, this->expression, tableStats);

    if (result.hasRange)
      return context._context.Allocate<PhysicalPlan::PhysicalIndexSeekRange>(this->table, result.start, result.end, result.remainingPredicate);

    //scan the first index
    if (!result.canSeek)
      return context._context.Allocate<PhysicalPlan::PhysicalIndexScan>(this->table, result.remainingPredicate, indexes.front().isClustered);

    return context._context.Allocate<PhysicalPlan::PhysicalIndexSeek>(this->table, result.start, result.remainingPredicate);
  }

  PhysicalPlan::ExecutionNode* LogicalJoin::CreateInnerJoinPhysicalPlan(QueryContext& context, JoinAlgorithmAnalysisResult& analysis) const{
    switch (analysis.algorithm) {
      case PipelineConstants::JoinAlgorithm::NestedLoopJoin:
      case PipelineConstants::JoinAlgorithm::HashJoin:
        return context._context.Allocate<PhysicalPlan::PhysicalNestedLoopInnerJoin>(
          left->ToPhysical(context),
          right->ToPhysical(context),
          analysis.remainingPredicate
        );
      case PipelineConstants::JoinAlgorithm::MergeJoin:
        return context._context.Allocate<PhysicalPlan::PhysicalMergeInnerJoin>(
          left->ToPhysical(context),
          right->ToPhysical(context),
          analysis.remainingPredicate,
          analysis.leftKeyColumns,
          analysis.rightKeyColumns
        );
    }

     throw std::runtime_error("LogicalJoin::ToPhysical(CompileResult& context): Unknown Join Algorithm");
  }

  PhysicalPlan::ExecutionNode* LogicalJoin::CreateLeftJoinPhysicalPlan(QueryContext& context, JoinAlgorithmAnalysisResult& analysis) const{
    switch (analysis.algorithm) {
      case PipelineConstants::JoinAlgorithm::NestedLoopJoin:
      case PipelineConstants::JoinAlgorithm::HashJoin:
      return context._context.Allocate<PhysicalPlan::PhysicalNestedLoopLeftJoin>(
        left->ToPhysical(context),
        right->ToPhysical(context),
        analysis.remainingPredicate
      );
      case PipelineConstants::JoinAlgorithm::MergeJoin:
        return context._context.Allocate<PhysicalPlan::PhysicalMergeLeftJoin>(
          left->ToPhysical(context),
          right->ToPhysical(context),
          analysis.remainingPredicate,
          analysis.leftKeyColumns,
          analysis.rightKeyColumns
        );
    }

     throw std::runtime_error("LogicalJoin::ToPhysical(CompileResult& context): Unknown Join Algorithm");
  }

  PhysicalPlan::ExecutionNode* LogicalJoin::CreateRightJoinPhysicalPlan(QueryContext& context, JoinAlgorithmAnalysisResult& analysis) const{
    switch (analysis.algorithm) {
    case PipelineConstants::JoinAlgorithm::NestedLoopJoin:
    case PipelineConstants::JoinAlgorithm::HashJoin:
      return context._context.Allocate<PhysicalPlan::PhysicalNestedLoopLeftJoin>(
        right->ToPhysical(context),
        left->ToPhysical(context),
        analysis.remainingPredicate
      );
    case PipelineConstants::JoinAlgorithm::MergeJoin:
      return context._context.Allocate<PhysicalPlan::PhysicalMergeLeftJoin>(
        right->ToPhysical(context),
        left->ToPhysical(context),
          analysis.remainingPredicate,
        analysis.leftKeyColumns,
        analysis.rightKeyColumns
      );
    }

     throw std::runtime_error("LogicalJoin::ToPhysical(CompileResult& context): Unknown Join Algorithm");
  }

  PhysicalPlan::ExecutionNode* LogicalJoin::CreateFullJoinPhysicalPlan(QueryContext& context, JoinAlgorithmAnalysisResult& analysis) const{
    switch (analysis.algorithm) {
      case PipelineConstants::JoinAlgorithm::NestedLoopJoin:
      case PipelineConstants::JoinAlgorithm::HashJoin:
        return context._context.Allocate<PhysicalPlan::PhysicalNestedLoopFullJoin>(
          left->ToPhysical(context),
          right->ToPhysical(context),
          analysis.remainingPredicate
        );
      case PipelineConstants::JoinAlgorithm::MergeJoin:
        return context._context.Allocate<PhysicalPlan::PhysicalMergeFullJoin>(
          left->ToPhysical(context),
        right->ToPhysical(context),
            analysis.remainingPredicate,
          analysis.leftKeyColumns,
          analysis.rightKeyColumns
        );
    }

     throw std::runtime_error("LogicalJoin::ToPhysical(CompileResult& context): Unknown Join Algorithm");
  }

  LogicalJoin::LogicalJoin(
    LogicalPlan *left,
    LogicalPlan *right,
    Expressions::Expression *condition,
    const JoinType type,
    const Int leftTableId,
    const Int rightTableId
  ) : leftTableId(leftTableId), rightTableId(rightTableId),
      left(left), right(right),
      condition(condition), type(type){}

  LogicalJoin::~LogicalJoin(){
    delete this->left;
    delete this->right;
  }

  PhysicalPlan::ExecutionNode * LogicalJoin::ToPhysical(QueryContext& context){
    Optimizer optimizer(context);

    auto analysisResult = optimizer.ChooseJoinAlgorithm(
      this->leftTableId,
      this->rightTableId,
      this->condition
    );

    switch (this->type) {
      case JoinType::Inner:
        return this->CreateInnerJoinPhysicalPlan(context, analysisResult);
      case JoinType::Left:
        return this->CreateLeftJoinPhysicalPlan(context, analysisResult);
      case JoinType::Right:
        return this->CreateRightJoinPhysicalPlan(context, analysisResult);
      case JoinType::Full:
        return this->CreateFullJoinPhysicalPlan(context, analysisResult);
      default:
        throw std::runtime_error("Unknown JoinType");
    }
  }

LogicalFilter::LogicalFilter(LogicalPlan* child, Expressions::Expression* filter)
  : child(child), filter(filter) {}

  PhysicalPlan::PhysicalFilter * LogicalFilter::ToPhysical(QueryContext& context){
    return context._context.Allocate<PhysicalPlan::PhysicalFilter>(this->child->ToPhysical(context), this->filter);
  }

  LogicalOrder::LogicalOrder(LogicalPlan *child, std::vector<Statements::OrderColumn*>& expressions)
    : child(child), expressions(std::move(expressions)) {}

  PhysicalPlan::ExecutionNode* LogicalOrder::ToPhysical(QueryContext& context){
    return context._context.Allocate<PhysicalPlan::PhysicalOrderBy>(this->child->ToPhysical(context), this->expressions);
  }

  LogicalTop::LogicalTop(LogicalPlan *child, const BigInt top)
    : child(child), top(top){}

  LogicalTop::~LogicalTop() {
    delete this->child;
  }

  PhysicalPlan::PhysicalTop * LogicalTop::ToPhysical(QueryContext& context){
    return context._context.Allocate<PhysicalPlan::PhysicalTop>(this->child->ToPhysical(context), this->top);
  }

  LogicalDistinct::LogicalDistinct(LogicalPlan *child)
    : child(child) {}

  LogicalDistinct::~LogicalDistinct() {
    delete this->child;
  }

  PhysicalPlan::PhysicalDistinct * LogicalDistinct::ToPhysical(QueryContext& context){
    return context._context.Allocate<PhysicalPlan::PhysicalDistinct>(this->child->ToPhysical(context));
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

  PhysicalPlan::PhysicalInsert * LogicalInsert::ToPhysical(QueryContext& context){
    auto* physicalSelect = this->child != nullptr
                             ? this->child->ToPhysical(context)
                             : nullptr;

    return context._context.Allocate<PhysicalPlan::PhysicalInsert>(this->table, this->fields, physicalSelect, this->columnsIndices);
  }

  LogicalSchemaCreate::LogicalSchemaCreate(const DataTypes::Guid& sessionId, const Int databaseId, DataTypes::String& schemaName)
    : LogicalPlan(sessionId), schemaName(std::move(schemaName)), databaseId(databaseId) {}

  PhysicalPlan::PhysicalSchemaCreate * LogicalSchemaCreate::ToPhysical(QueryContext& context){
    return context._context.Allocate<PhysicalPlan::PhysicalSchemaCreate>(this->sessionId, this->databaseId, this->schemaName);
  }

  LogicalDelete::LogicalDelete(Statements::DataSource *table, Expressions::Expression *expression)
    : table(table), expression(expression) {}

  PhysicalPlan::ExecutionNode * LogicalDelete::ToPhysical(QueryContext& context){
    const auto indexes = DatabaseEngine::SystemCatalog::Get().SelectIndexes(context._context.GetAllocator(), this->table->tableId);

    //if no indexes are available heap scan
    if (indexes.empty())
      return context._context.Allocate<PhysicalPlan::PhysicalHeapDelete>(this->table, this->expression);

    //if expression is complex defer from index seek
    const bool canIndexSeek = expression != nullptr;// && !expression->IsComplex();

    // HashSet<column_index_t> expressionColumns;
    //
    // if (expression != nullptr)
    //   expression->GetColumns(expressionColumns);

    for (const auto& index: indexes) {
      const auto indexHeader = DatabaseEngine::SystemCatalog::Get().SelectIndexById(context._context.GetAllocator(), index.id);

      if (canIndexSeek) {
        for (const auto& column: index.columns) {
          //if columns is first prefer it, else break because index scan will occur
          //index seek
          //              if (!expressionColumns.Contains(column))
          break;

        }
      }

      //find the first non clustered and use it
      return context._context.Allocate<PhysicalPlan::PhysicalIndexScanDelete>(this->table, this->expression);
    }

    return context._context.Allocate<PhysicalPlan::PhysicalHeapDelete>(this->table, this->expression);
  }

    LogicalUpdate::LogicalUpdate(
        Statements::DataSource *table,
        std::vector<Expressions::Expression*>& updates,
        Expressions::Expression *expression
    ): table(table), updates(std::move(updates)), expression(expression) {}

    PhysicalPlan::ExecutionNode* LogicalUpdate::ToPhysical(QueryContext& context){
        const auto indexes = DatabaseEngine::SystemCatalog::Get().SelectIndexes(context._context.GetAllocator(), this->table->tableId);

        //if no indexes are available heap scan
        if (indexes.empty())
            return context._context.Allocate<PhysicalPlan::PhysicalHeapUpdate>(this->table, this->expression, this->updates);

        //if expression is complex defer from index seek
        const bool canIndexSeek = expression != nullptr; //&& !expression->IsComplex();

        HashSet<column_index_t> expressionColumns;
        //
        // if (expression != nullptr)
        //   expression->GetColumns(expressionColumns);

        for (const auto& index: indexes) {
            const auto indexHeader = DatabaseEngine::SystemCatalog::Get().SelectIndexById(context._context.GetAllocator(), index.id);

            if (canIndexSeek) {
                for (const auto& column: indexHeader.columns) {
                    //if columns is first prefer it, else break because index scan will occur
                    //index seek
                    if (expressionColumns.Contains(column.ordinalPosition))
                    return context._context.Allocate<PhysicalPlan::PhysicalIndexSeekUpdate>(this->table, this->expression, this->updates);

                    break;
                }
            }

        //find the first non clustered and use it
            return context._context.Allocate<PhysicalPlan::PhysicalIndexScanUpdate>(this->table, this->expression, this->updates);
        }

        return context._context.Allocate<PhysicalPlan::PhysicalHeapUpdate>(this->table, this->expression, this->updates);
    }

    LogicalTableCreate::LogicalTableCreate(
        const DataTypes::Guid& sessionId,
        Statements::DataSource*  table,
        std::vector<Statements::NewColumn*>& columns,
        std::vector<column_index_t> primaryKey,
        DataTypes::String& constraintName
    ): LogicalPlan(sessionId), table(table), constraintName(std::move(constraintName)),
      columns(std::move(columns)), primaryKey(std::move(primaryKey)) {}

    PhysicalPlan::PhysicalTableCreate * LogicalTableCreate::ToPhysical(QueryContext& context){
        Headers::Index index(this->primaryKey.data(), this->primaryKey.size());
        return context._context.Allocate<PhysicalPlan::PhysicalTableCreate>(
            this->sessionId,
            this->table,
            this->columns,
            index,
            this->constraintName
        );
    }

    LogicalIndexCreate::LogicalIndexCreate(
        const DataTypes::Guid& sessionId,
        Statements::DataSource *table,
        DataTypes::String& constraintName,
        std::vector<column_index_t> &columns
    ): LogicalPlan(sessionId), table(table), constraintName(std::move(constraintName)), columns(std::move(columns)) {}

    PhysicalPlan::ExecutionNode * LogicalIndexCreate::ToPhysical(QueryContext& context){
        return context._context.Allocate<PhysicalPlan::PhysicalIndexCreate>(this->sessionId, this->table, this->constraintName, this->columns);
    }

    LogicalAlterTable::LogicalAlterTable(
        const DataTypes::Guid& sessionId,
        Statements::DataSource *table,
        const Constants::AlterTableType& type,
        Statements::NewColumn *column
    ): LogicalPlan(sessionId), table(table), type(type) {
        this->column = {
          .addColumn = column
        };
    }

    LogicalAlterTable::LogicalAlterTable(
        const DataTypes::Guid& sessionId,
        Statements::DataSource *table,
        const Constants::AlterTableType& type,
        Statements::AlterColumn *column
    ): LogicalPlan(sessionId), table(table), type(type) {
        this->column = {
          .alterColumn = column
        };
    }

    LogicalAlterTable::LogicalAlterTable(
        const DataTypes::Guid& sessionId,
        Statements::DataSource *table,
        const Constants::AlterTableType& type,
        Statements::RenameColumn *column
    ): LogicalPlan(sessionId), table(table), type(type) {
        this->column = {
          .renameColumn = column
        };
    }

    LogicalAlterTable::LogicalAlterTable(
        const DataTypes::Guid& sessionId,
        Statements::DataSource *table,
        const Constants::AlterTableType& type,
        Statements::DropColumn *column
    ): LogicalPlan(sessionId), table(table), type(type) {
        this->column = {
          .dropColumn = column
        };
    }

    PhysicalPlan::ExecutionNode * LogicalAlterTable::ToPhysical(QueryContext& context){
      switch (this->type) {
      case Constants::AlterTableType::AddColumn:
        return context._context.Allocate<PhysicalPlan::PhysicalAddColumn>(this->sessionId, this->table, this->column.addColumn);
      case Constants::AlterTableType::AlterColumn:
        return context._context.Allocate<PhysicalPlan::PhysicalAlterColumn>(this->sessionId, this->table, this->column.alterColumn);
      case Constants::AlterTableType::RenameColumn:
        return context._context.Allocate<PhysicalPlan::PhysicalRenameColumn>(this->sessionId, this->table, this->column.renameColumn);
      case Constants::AlterTableType::DropColumn:
        return context._context.Allocate<PhysicalPlan::PhysicalDropColumn>(this->sessionId, this->table, this->column.dropColumn);
      default:
        return nullptr;
      }
    }
}

