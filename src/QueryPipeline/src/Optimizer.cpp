#include "../include/Optimizer.h"

#include "../include/LogicalPlan.h"
#include "Managers/StatisticsManager.h"

namespace QueryPipeline {
  SeekRange::SeekRange() {
    this->endInclusive = false;
    this->startInclusive = false;
  }

  SeekRange::SeekRange(
    const Value &otherStart,
    const Value &otherEnd,
    const bool &includeStart,
    const bool &includeEnd
  ) {
    this->start = otherStart;
    this->end = otherEnd;
    this->startInclusive = includeStart;
    this->endInclusive = includeEnd;
  }

  IndexSeekAnalyzeResults::IndexSeekAnalyzeResults() {
    this->expression = nullptr;
    this->canIndexSeek = false;
  }

  IndexSeekAnalyzeResults::IndexSeekAnalyzeResults(Expressions::Expression *otherExpr) {
    this->expression = otherExpr;
    this->canIndexSeek = false;
  }

  JoinOrderAnalyzeResult::JoinOrderAnalyzeResult(){
    this->isReordered = false;
  }

  bool Optimizer::Analyze(
    const Expressions::ColumnExpression *columnExpression,
    const Expressions::Expression* otherExpression,
    const Expressions::BinaryOperator& operation,
    const std::vector<Headers::IndexColumnsHeader> &indexColumns,
    std::vector<IndexSeekAnalyzeResults> &results
  ) {
    //can use index seek
    if (columnExpression->columnId != indexColumns[0].columnId)
      return false;

    if (otherExpression->IsConstant()) {
      auto* constantExpr = otherExpression->AsConstant();

      const auto includeStart =
        operation == Expressions::BinaryOperator::GreaterEqual
        || operation == Expressions::BinaryOperator::LessEqual
        ||  operation == Expressions::BinaryOperator::Equal;

      auto result = IndexSeekAnalyzeResults();

      result.canIndexSeek = true;
      result.range = SeekRange(constantExpr->value, constantExpr->value, includeStart, includeStart);
      results.emplace_back(result);

      return true;
    }

    return false;
  }

  bool Optimizer::Analyze(
    const Expressions::BinaryExpression *expression,
    const std::vector<Headers::IndexColumnsHeader> &indexColumns,
    std::vector<IndexSeekAnalyzeResults> &results
  ){
    switch (expression->operation) {
    case Expressions::BinaryOperator::Equal:
    case Expressions::BinaryOperator::Greater:
    case Expressions::BinaryOperator::GreaterEqual:
    case Expressions::BinaryOperator::Less:
    case Expressions::BinaryOperator::LessEqual: {
      if (expression->left->IsColumn()) {
        return Analyze(expression->left->AsColumn(), expression->right, expression->operation, indexColumns, results);
      }
      if (expression->right->IsColumn()) {
        return Analyze(expression->left->AsColumn(), expression->right, expression->operation, indexColumns, results);
      }

      break;
    }
    case Expressions::BinaryOperator::EqualIgnoreOrdinalCase:
    case Expressions::BinaryOperator::NotEqual:
    case Expressions::BinaryOperator::Add:
    case Expressions::BinaryOperator::Subtract:
    case Expressions::BinaryOperator::Multiply:
    case Expressions::BinaryOperator::Divide:
    case Expressions::BinaryOperator::Modulo:
    default:
      break;
    }

    return false;
  }

  bool Optimizer::Analyze(
    const Expressions::LogicalExpression *expression,
    const std::vector<Headers::IndexColumnsHeader> &indexColumns,
    std::vector<IndexSeekAnalyzeResults> &results
  ){
    if (expression->IsAnd()) {
      return Analyze(expression->left, indexColumns, results)
        && Analyze(expression->right, indexColumns, results);
    }

    if (expression->IsOr()) {
      return Analyze(expression->left, indexColumns, results)
        && Analyze(expression->right, indexColumns, results);
    }

    return false;
  }

  bool Optimizer::Analyze(
    Expressions::Expression *expression,
    const std::vector<Headers::IndexColumnsHeader> &indexColumns,
    std::vector<IndexSeekAnalyzeResults> &results
  ){
    switch (expression->expressionType) {
    case Expressions::ExpressionType::Binary:
      return Analyze(expression->AsBinary(), indexColumns, results);
    case Expressions::ExpressionType::Logical:
      return Analyze(expression->AsLogical(), indexColumns, results);
    case Expressions::ExpressionType::Expression:
    case Expressions::ExpressionType::Column:
    case Expressions::ExpressionType::Constant:
    case Expressions::ExpressionType::Variable:
    case Expressions::ExpressionType::Branch:
    case Expressions::ExpressionType::Function:
    default:
      return false;
    }
  }

  void Optimizer::SplitConjunctions(Expressions::Expression* expression, std::vector<Expressions::Expression*>& conjunctions){
    if (!expression->IsLogical()){
      conjunctions.push_back(expression);
      return;
    }

    auto* logicalExpr = expression->AsLogical();
    if (logicalExpr->IsAnd()){
      SplitConjunctions(logicalExpr->left, conjunctions);
      SplitConjunctions(logicalExpr->right, conjunctions);

      //should delete logical expression?
      logicalExpr->left = nullptr;
      logicalExpr->right = nullptr;
      delete logicalExpr;
      return;
    }

    conjunctions.push_back(expression);
  }

  void Optimizer::GetInvolvedTables(const Expressions::Expression* expression, HashSet<table_id_t>& involvedTables){
    if (expression->IsBinary()){
      const auto* binaryExpr = expression->AsBinary();
      GetInvolvedTables(binaryExpr->left, involvedTables);
      GetInvolvedTables(binaryExpr->right, involvedTables);
      return;
    }

    if (expression->IsLogical()){
      const auto* logicalExpr = expression->AsLogical();
      GetInvolvedTables(logicalExpr->left, involvedTables);
      GetInvolvedTables(logicalExpr->right, involvedTables);
      return;
    }

    if (!expression->IsColumn())
      return;

    const auto* columnExpr = expression->AsColumn();
    involvedTables.Add(columnExpr->tableId);
  }

  std::vector<table_id_t> Optimizer::GetInvolvedTables(const Expressions::Expression* expression){
    HashSet<table_id_t> involvedTablesSet;

    Optimizer::GetInvolvedTables(expression, involvedTablesSet);

    return involvedTablesSet.ToVector();
  }

  void Optimizer::CombineExpressionsWithAnd(
    Expressions::Expression*& baseExpression,
    Expressions::Expression* newExpression
  ){

    if (baseExpression == nullptr){
        baseExpression = newExpression;
        return;
    }

    auto* left = baseExpression;

    baseExpression =  new Expressions::LogicalExpression(
      left,
      newExpression,
      Expressions::LogicalType::And
    );
  }

  void Optimizer::ProcessPredicate(
    Expressions::Expression* baseExpression,
    Expressions::Expression*& remainingPredicate,
    Dictionary<table_id_t, Expressions::Expression*>& tablePredicatesDictionary
  ){
    std::vector<Expressions::Expression*> expressions;
    Optimizer::SplitConjunctions(baseExpression, expressions);

    for (auto*& condition : expressions){
      const auto involvedTables = Optimizer::GetInvolvedTables(condition);

      if (involvedTables.size() == 1){
        auto& existingCondition = tablePredicatesDictionary[involvedTables[0]];
        Optimizer::CombineExpressionsWithAnd(existingCondition, condition);
        continue;
      }

      Optimizer::CombineExpressionsWithAnd(remainingPredicate, condition);
    }
  }

  std::vector<IndexSeekAnalyzeResults> Optimizer::AnalyzeTableScan(
    const LogicalTableScan *plan,
    const std::vector<Headers::IndexColumnsHeader> &indexColumns
  ){
    std::vector<IndexSeekAnalyzeResults> results;

    const auto _ = Analyze(plan->expression, indexColumns, results);

    return results;
  }

  JoinOrderAnalyzeResult Optimizer::DetermineJoinOrder(Statements::SelectStatement* statement){
    JoinOrderAnalyzeResult result;

    if (statement->IsConstant())
      return result;

    if (!statement->HasJoins()){
      result.order.push_back(statement->table->tableId);
      return result;
    }

    std::vector<JoinOrderAnalyzeInfo> infoVector;

    const auto baseSourceStats = DatabaseEngine::StatisticsManager::Get().GetTableStatistics(statement->table->tableId);

    //optimize by using hasIndex bool on tableStats to avoid lookups
    const auto baseSourceIndexStats = DatabaseEngine::StatisticsManager::Get().GetIndexStatistics(statement->table->tableId);

    infoVector.emplace_back(
      statement->table->tableId,
      baseSourceStats.rowCount,
      !baseSourceIndexStats.empty()
    );

    for (const auto& join : statement->joins) {
      if (join->IsRightJoin()){
        result.order.insert(result.order.begin(), join->table->tableId);
        result.orderedJoins.insert(result.orderedJoins.begin(), join);
        continue;
      }

      if (!join->IsInnerJoin()) {
        result.order.push_back(join->table->tableId);
        result.orderedJoins.push_back(join);
        continue;
      }

      const auto joinSourceStats = DatabaseEngine::StatisticsManager::Get().GetTableStatistics(join->table->tableId);
      const auto joinSourceIndexStats = DatabaseEngine::StatisticsManager::Get().GetIndexStatistics(join->table->tableId);

      infoVector.emplace_back(
        join->table->tableId,
        joinSourceStats.rowCount,
        !joinSourceIndexStats.empty(),
        join
      );
    }

    ranges::sort(infoVector, JoinOrderAnalyzeInfo());

    auto* baseSource = statement->table;

    Statements::JoinStatement* reorderedJoin = nullptr;

    for (int i = 0;i < infoVector.size(); i++){
      auto& info = infoVector[i];
      //swap occurred
      if (i == 0 && info.joinStatement != nullptr){
        statement->table = info.joinStatement->table;
        info.joinStatement->table = baseSource;
        baseSource = statement->table;

        reorderedJoin = info.joinStatement;
        result.order.push_back(info.tableId);
        result.orderedJoins.push_back(nullptr);
        continue;
      }

      result.order.push_back(info.tableId);

      if (info.joinStatement == nullptr){
        result.orderedJoins.push_back(reorderedJoin);
        continue;
      }

      result.orderedJoins.push_back(info.joinStatement);
    }

    //remove the first which is always null
    result.orderedJoins.erase(result.orderedJoins.begin());

    result.isReordered = result.order[0] != statement->table->tableId;

    return result;
  }

  PredicatePushDownResult Optimizer::PushDownPredicates(
    const std::vector<table_id_t>& tables,
    Expressions::Expression* expression,
    const std::vector<Statements::JoinStatement*>& joins
  ){
    PredicatePushDownResult result;

    if (expression == nullptr && joins.empty())
      return result;

    result.tablePredicatesDictionary = Dictionary<table_id_t, Expressions::Expression*>::FromVector(tables, nullptr);

    Optimizer::ProcessPredicate(expression, result.remainingPredicate, result.tablePredicatesDictionary);
    for (const auto& join : joins) {
      if (!join->IsInnerJoin())
        continue;

      Expressions::Expression* joinRemainingPredicate = nullptr;
      Optimizer::ProcessPredicate(join->expression, joinRemainingPredicate, result.tablePredicatesDictionary);
      join->expression = joinRemainingPredicate;
    }

    return result;
  }
}
