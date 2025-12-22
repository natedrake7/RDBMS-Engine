#include "../include/Optimizer.h"

#include "CostEstimator.h"
#include "../include/LogicalPlan.h"
#include "Managers/StatisticsManager.h"
#include "SystemDatabases/SystemCatalog.h"

namespace QueryPipeline {
  Range::Range(){
    this->hasRange = false;
    this->canSeek = false;
    this->remainingPredicate = nullptr;
  }

  SeekRange::SeekRange() {
    this->endInclusive = false;
    this->startInclusive = false;
    this->hasRange = false;
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
    this->hasRange = (this->start < this->end).GetBool();
  }

  bool SeekRange::HasStart() const{ return !this->start.IsNull(); }

  bool SeekRange::HasEnd() const{ return !this->end.IsNull(); }

  IndexSeekColumnAnalysisResults::IndexSeekColumnAnalysisResults() {
    this->expression = nullptr;
    this->canIndexSeek = false;
    this->needsParameterBinding = false;
    this->columnId = INVALID_COLUMN_ID;
  }

  IndexSeekColumnAnalysisResults::IndexSeekColumnAnalysisResults(Expressions::Expression *otherExpr) {
    this->expression = otherExpr;
    this->canIndexSeek = false;
    this->needsParameterBinding = false;
    this->columnId = INVALID_COLUMN_ID;
  }

  JoinOrderAnalyzeResult::JoinOrderAnalyzeResult(){
    this->isReordered = false;
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

  void Optimizer::AnalyzeTableScan(
    Expressions::Expression* baseExpression,
    const Expressions::BinaryExpression* expression,
    Dictionary<column_id_t, std::vector<Expressions::Expression*>>& columnPredicatesDictionary
  ){
    if (expression->left->IsColumn()
      && (expression->right->IsConstant() || expression->right->IsVariable()))
    {
      const auto* columnExpr = expression->left->AsColumn();
      columnPredicatesDictionary[columnExpr->columnId].push_back(baseExpression);
      return;
    }

    if (expression->right->IsColumn()
      && (expression->left->IsConstant() || expression->left->IsVariable()))
    {
      const auto* columnExpr = expression->right->AsColumn();
      columnPredicatesDictionary[columnExpr->columnId].push_back(baseExpression);
    }
  }

  void Optimizer::DetermineCanSeekOnEquality(
    const Value& predicateValue,
    SeekRange& range,
    bool& canSeek
  ){
    range.start = predicateValue;
    range.end = predicateValue;
    range.hasRange = false;
    canSeek = true;
  }

  void Optimizer::DetermineCanSeekOnGreaterThan(
    const Value& predicateValue,
    SeekRange& range,
    bool& canSeek,
    const bool& inclusive
  ){
    if ((predicateValue <= range.start).GetBool()
      && !range.start.IsNull()
      && range.hasRange
    ) return;

    range.start = predicateValue;
    range.startInclusive = inclusive;
    canSeek = true;
    range.hasRange = true;
  }

  void Optimizer::DetermineCanSeekOnLessThan(
    const Value& predicateValue,
    SeekRange& range,
    bool& canSeek,
    const bool& inclusive
  ){
    if ((predicateValue >= range.end).GetBool()
      && !range.end.IsNull()
      && range.hasRange
    ) return;

    range.end = predicateValue;
    range.endInclusive = inclusive;
    range.hasRange = true;
    canSeek = true;
  }

  void Optimizer::DetermineSeekRange(
    const Expressions::BinaryExpression* expression,
    const Value& predicateValue,
    SeekRange& range,
    bool& canSeek
  ){
    switch (expression->operation){
      case Expressions::BinaryOperator::Equal:
        Optimizer::DetermineCanSeekOnEquality(predicateValue, range, canSeek);
        break;
      case Expressions::BinaryOperator::Greater:
        Optimizer::DetermineCanSeekOnGreaterThan(predicateValue, range, canSeek, false);
        break;
      case Expressions::BinaryOperator::GreaterEqual:
        Optimizer::DetermineCanSeekOnGreaterThan(predicateValue, range, canSeek, true);
        break;
      case Expressions::BinaryOperator::Less:
        Optimizer::DetermineCanSeekOnLessThan(predicateValue, range, canSeek, false);
        break;
      case Expressions::BinaryOperator::LessEqual:
        Optimizer::DetermineCanSeekOnLessThan(predicateValue, range, canSeek, true);
        break;
      default:
        canSeek = false;
        break;
    }
  }

  IndexSeekAnalysisResult Optimizer::AnalyzeTableScan(
    const Headers::IndexHeader& index,
    Expressions::Expression* expression
  ){
    IndexSeekAnalysisResult result;

    if (expression == nullptr)
      return result;

    Optimizer::SplitConjunctions(expression, result.conjunctions);

    Dictionary<column_id_t, std::vector<Expressions::Expression*>> columnPredicates;
    for (const auto& column : index.columns)
      columnPredicates.Add(column.columnId, {});

    for (auto*& condition: result.conjunctions){
      if (!condition->IsBinary())
        continue;

      Optimizer::AnalyzeTableScan(condition, condition->AsBinary(), columnPredicates);
    }

    for (const auto& column : index.columns) {
      std::vector<Expressions::Expression*> predicates;
      if (!columnPredicates.TryGetValue(column.columnId, predicates))
        break;

      IndexSeekColumnAnalysisResults analyzeResult;
      analyzeResult.canIndexSeek = true;

      for (auto*& predicate : predicates) {
        if (!predicate->IsBinary())
          continue;

        Value value;
        const auto* binaryExpr = predicate->AsBinary();

        if (binaryExpr->left->IsColumn()){
          const auto* columnExpr = binaryExpr->left->AsColumn();

          if (binaryExpr->right->IsConstant()){
            const auto* constantExpr = binaryExpr->right->AsConstant();

            Optimizer::DetermineSeekRange(
              binaryExpr,
              constantExpr->value,
            analyzeResult.range,
            analyzeResult.canIndexSeek
            );
          }
          else if (binaryExpr->right->IsVariable()){
            analyzeResult.expression = binaryExpr->right;
            analyzeResult.needsParameterBinding = true;
          }

          analyzeResult.columnId = columnExpr->columnId;
          continue;
        }

        if (binaryExpr->right->IsColumn()){
          const auto* columnExpr = binaryExpr->right->AsColumn();

          if (binaryExpr->left->IsConstant()){
            const auto* constantExpr = binaryExpr->left->AsConstant();

            Optimizer::DetermineSeekRange(
              binaryExpr,
              constantExpr->value,
            analyzeResult.range,
            analyzeResult.canIndexSeek
            );
          }
          else if (binaryExpr->left->IsVariable()){
            analyzeResult.expression = binaryExpr->left;
            analyzeResult.needsParameterBinding = true;
          }

          analyzeResult.columnId = columnExpr->columnId;
        }
      }

      if (!analyzeResult.canIndexSeek)
        break;

      result.analyzeResults.push_back(analyzeResult);
    }

    return result;
  }

  Range Optimizer::BuildSeekKeys(
    const std::vector<IndexSeekColumnAnalysisResults>& analyzeResults,
    std::vector<Expressions::Expression*>& conjunctions
  ){
    Range range;
    bool canSeek = true;
    int counter = 0;
    for (const auto& info : analyzeResults){
      range.start.InsertKey(DataTypes::Indexing::Key(info.range.start));
      range.end.InsertKey(DataTypes::Indexing::Key(info.range.end));

      for (auto*& expression : conjunctions) {
        if (expression != info.expression)
          continue;

        expression = nullptr;
        break;
      }

      if (counter == 0)
        canSeek = info.range.HasStart() && info.range.HasEnd();

      counter++;
    }

    for (auto*& expression : conjunctions) {
      if (expression == nullptr)
        continue;

      Optimizer::CombineExpressionsWithAnd(range.remainingPredicate, expression);
    }

    range.hasRange = range.start < range.end;
    range.canSeek = canSeek;

    return range;
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
      // Cannot push down predicates for FULL OUTER JOIN as it would break semantics
      // (unmatched rows from both sides must be preserved with NULLs)
      if (join->IsFullOuterJoin())
        continue;

      Expressions::Expression* joinRemainingPredicate = nullptr;
      Optimizer::ProcessPredicate(join->expression, joinRemainingPredicate, result.tablePredicatesDictionary);
      join->expression = joinRemainingPredicate;
    }

    return result;
  }

   Range Optimizer::PerformIndexAnalysis(
    std::vector<Headers::IndexHeader>& indexes,
    Expressions::Expression* expression,
    const Headers::TableStatistics& tableStatistics
  ){
    std::vector<IndexCandidate> candidates;

    for (auto& index : indexes) {
      index.columns = DatabaseEngine::SystemCatalog::Get().SelectIndexColumnsByIndexId(index.id);

      auto [analyzeResults, conjunctions] = Optimizer::AnalyzeTableScan(index, expression);

      IndexCandidate candidate;

      candidate.header = index;
      candidate.analyzeInfo = std::move(analyzeResults);
      candidate.conjunctions = std::move(conjunctions);
      candidate.matchingColumns = static_cast<int>(candidate.analyzeInfo.size());
      candidate.estimatedCost = CostEstimator::EstimateIndexCost(index, candidate.analyzeInfo, tableStatistics);

      candidates.push_back(candidate);
    }

    if (candidates.empty())
      return {};

    for (auto& candidate : candidates){
      bool isPerfectSeek = true;
      for (const auto& info : candidate.analyzeInfo){
        if (info.canIndexSeek && !info.range.hasRange)
          continue;

        isPerfectSeek = false;
        break;
      }

      if (isPerfectSeek && candidate.header.isClustered)
        return Optimizer::BuildSeekKeys(candidate.analyzeInfo, candidate.conjunctions);
    }

    ranges::sort(candidates, IndexCandidate());

    auto& candidate = candidates[0];
    return Optimizer::BuildSeekKeys(candidate.analyzeInfo, candidate.conjunctions);
  }
}