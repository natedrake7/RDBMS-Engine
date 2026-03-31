#include "../include/Optimizer.h"

#include <algorithm>

#include "CostEstimator.h"
#include "DatabaseConstants.h"
#include "Parser.h"
#include "Managers/StatisticsManager.h"
#include "SystemDatabases/SystemCatalog.h"

namespace QueryPipeline {
  JoinAlgorithmAnalysisResult::JoinAlgorithmAnalysisResult(){
    this->algorithm = PipelineConstants::JoinAlgorithm::NestedLoopJoin;
    this->remainingPredicate = nullptr;
  }

  JoinAlgorithmAnalysisResult::JoinAlgorithmAnalysisResult(const PipelineConstants::JoinAlgorithm& algorithm){
    this->algorithm = algorithm;
    this->remainingPredicate = nullptr;
  }

  JoinAlgorithmAnalysisResult::JoinAlgorithmAnalysisResult(
    const PipelineConstants::JoinAlgorithm& algorithm,
    Expressions::Expression* expression,
    std::vector<column_index_t>& leftKeyColumns,
    std::vector<column_index_t>& rightKeyColumns
  ){
    this->remainingPredicate = expression;
    this->algorithm = algorithm;
    this->leftKeyColumns = std::move(leftKeyColumns);
    this->rightKeyColumns = std::move(rightKeyColumns);
  }

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
    const bool includeStart,
    const bool includeEnd
  ) {
    this->start = otherStart;
    this->end = otherEnd;
    this->startInclusive = includeStart;
    this->endInclusive = includeEnd;
    this->hasRange = (this->start < this->end).AsBool();
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
    if (expression == nullptr)
      return;

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
      // delete logicalExpr;
      return;
    }

    conjunctions.push_back(expression);
  }

  void Optimizer::GetInvolvedTables(const Expressions::Expression* expression, HashSet<table_id_t>& involvedTables){
    if (expression->IsBinary()){
      const auto* binaryExpr = expression->AsBinary();
      Optimizer::GetInvolvedTables(binaryExpr->left, involvedTables);
      Optimizer::GetInvolvedTables(binaryExpr->right, involvedTables);
      return;
    }

    if (expression->IsLogical()){
      const auto* logicalExpr = expression->AsLogical();
      Optimizer::GetInvolvedTables(logicalExpr->left, involvedTables);
      Optimizer::GetInvolvedTables(logicalExpr->right, involvedTables);
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
  ) const{
    if (baseExpression == nullptr){
        baseExpression = newExpression;
        return;
    }

    auto* left = baseExpression;

    baseExpression =  this->context->_context.Allocate<Expressions::LogicalExpression>(
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
    const bool inclusive
  ){
    if ((predicateValue <= range.start).AsBool()
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
    const bool inclusive
  ){
    if ((predicateValue >= range.end).AsBool()
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

  void Optimizer::AnalyzeTableScan(
    IndexSeekColumnAnalysisResults& analyzeResult,
    Expressions::BinaryExpression* binaryExpr,
    const Expressions::ColumnExpression* columnExpr,
    Expressions::Expression* otherExpression
  ){
    if (otherExpression->IsConstant()){
        const auto* constantExpr = otherExpression->AsConstant();

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

        analyzeResult.expression = binaryExpr;
      }

      analyzeResult.columnId = columnExpr->columnId;
      analyzeResult.expression = binaryExpr;
  }

  std::vector<IndexSeekColumnAnalysisResults> Optimizer::AnalyzeTableScan(
    const Headers::IndexHeader& index,
    const std::vector<Expressions::Expression*>& conjunctions
  ){
    std::vector<IndexSeekColumnAnalysisResults> result;

    Dictionary<column_id_t, std::vector<Expressions::Expression*>> columnPredicates;
    for (const auto& column : index.columns)
      columnPredicates.Add(column.columnId, {});

    for (auto& condition : conjunctions){
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
        auto* binaryExpr = predicate->AsBinary();

        if (binaryExpr->left->IsColumn()){
          Optimizer::AnalyzeTableScan(analyzeResult, binaryExpr, binaryExpr->left->AsColumn(), binaryExpr->right);
          continue;
        }

        if (binaryExpr->right->IsColumn())
          Optimizer::AnalyzeTableScan(analyzeResult, binaryExpr, binaryExpr->right->AsColumn(), binaryExpr->left);
      }

      if (!analyzeResult.canIndexSeek)
        break;

      result.push_back(analyzeResult);
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


        // //if expression is used in range, remove it from conjunctions
        // delete expression;
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

  void Optimizer::ProcessJoinCondition(
    Expressions::Expression* expression,
    std::vector<JoinConditionInfo>& conditionsInfo,
    bool& isEqualityJoin
  ){
    if (!expression->IsBinary())
      return;

    const auto* binaryExpr = expression->AsBinary();
    if (!binaryExpr->left->IsColumn() || !binaryExpr->right->IsColumn())
      return;

    JoinConditionInfo info;

    const auto* leftColumnExpr = binaryExpr->left->AsColumn();
    const auto* rightColumnExpr = binaryExpr->right->AsColumn();

    isEqualityJoin = !isEqualityJoin && (binaryExpr->operation == Expressions::BinaryOperator::Equal);
    info.leftColumnId = leftColumnExpr->columnId;
    info.leftColumnIndex = leftColumnExpr->index;
    info.leftTableId = leftColumnExpr->tableId;

    info.rightColumnId = rightColumnExpr->columnId;
    info.rightColumnIndex = rightColumnExpr->index;
    info.rightTableId = rightColumnExpr->tableId;

    info.expression = expression;

    conditionsInfo.push_back(info);
  }

  std::vector<Int> Optimizer::CheckPredicatesSorting(
    const Headers::TableStatistics& tableStats,
    const std::vector<JoinConditionInfo>& joinConditions
  ){
    const auto indexes = CoreEngine::StatisticsManager::Get().GetIndexStatistics(tableStats.tableId);

    if (indexes.empty())
      return {};

    std::vector<Int> bestMatch;
    for (const auto& index : indexes){
      const auto columns = CoreEngine::SystemCatalog::Get().SelectIndexColumnsByIndexId(this->context->_context.GetAllocator(), index.indexId);

      std::vector<Int> matches;
      for (const auto& column : columns){

        for (int i = 0;i < joinConditions.size();i++){
          const auto& joinCondition = joinConditions[i];

          const auto columnId = (joinCondition.leftTableId == tableStats.tableId)
                  ? joinCondition.leftColumnId
                  : joinCondition.rightColumnId;

          if (column.columnId == columnId){
            matches.push_back(i);
            continue;
          }

          break;
        }

        if (bestMatch.size() < matches.size())
          bestMatch = std::move(matches);
      }
    }

    return bestMatch;
  }

  JoinAlgorithmAnalysisResult Optimizer::CreateMergeJoinKeys(
    const std::vector<JoinConditionInfo>& conditionsInfo,
    const std::vector<Int>& leftKeyColumns,
    const std::vector<Int>& rightKeyColumns,
    const Int leftTableId,
    const Int rightTableId
  ) const{
    JoinAlgorithmAnalysisResult result(PipelineConstants::JoinAlgorithm::MergeJoin);

    const auto leftSize = leftKeyColumns.size();
    const auto rightSize = rightKeyColumns.size();

    const auto min = std::min(leftSize, rightSize);

    for (int i = 0;i < min;i++){
      const auto& joinCondition = conditionsInfo[leftKeyColumns[i]];

      const auto leftExprIndex = (joinCondition.leftTableId == leftTableId)
          ? joinCondition.leftColumnIndex
          : joinCondition.rightColumnIndex;

      const auto rightExprIndex = (joinCondition.leftTableId == rightTableId)
          ? joinCondition.leftColumnIndex
          : joinCondition.rightColumnIndex;

      result.leftKeyColumns.push_back(leftExprIndex);
      result.rightKeyColumns.push_back(rightExprIndex);
    }

    if (leftSize > min){
      for (int i = min;i < leftSize;i++){
        const auto& joinCondition = conditionsInfo[leftKeyColumns[i]];
        Optimizer::CombineExpressionsWithAnd(result.remainingPredicate, joinCondition.expression);
      }

      return result;
    }

    if (rightSize > min){
      for (int i = min;i < rightSize;i++){
        const auto& joinCondition = conditionsInfo[rightKeyColumns[i]];
        Optimizer::CombineExpressionsWithAnd(result.remainingPredicate, joinCondition.expression);
      }
    }

    return result;
  }

  Optimizer::Optimizer(QueryContext& context){
      this->context = &context;
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

    const auto baseSourceStats = CoreEngine::StatisticsManager::Get().GetTableStatistics(statement->table->tableId);

    //optimize by using hasIndex bool on tableStats to avoid lookups
    const auto baseSourceIndexStats = CoreEngine::StatisticsManager::Get().GetIndexStatistics(statement->table->tableId);

    //base table info
    infoVector.emplace_back(
      statement->table->tableId,
      baseSourceStats.rowCount,
      !baseSourceIndexStats.empty()
    );

    //each join info
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

      const auto joinSourceStats = CoreEngine::StatisticsManager::Get().GetTableStatistics(join->table->tableId);
      const auto joinSourceIndexStats = CoreEngine::StatisticsManager::Get().GetIndexStatistics(join->table->tableId);

      infoVector.emplace_back(
        join->table->tableId,
        joinSourceStats.rowCount,
        !joinSourceIndexStats.empty(),
        join
      );
    }

    std::ranges::sort(infoVector, JoinOrderAnalyzeInfo());

    auto* baseSource = statement->table;
    Statements::JoinStatement* reorderedJoin = nullptr;

    const auto preReorderSize = result.orderedJoins.size();

    for (int i = 0;i < infoVector.size(); i++){
      auto& info = infoVector[i];
      //swap occurred
      if (i == 0 && info.joinStatement != nullptr){
        statement->table = info.joinStatement->table;
        info.joinStatement->table = baseSource;
        baseSource = statement->table;

        reorderedJoin = info.joinStatement;

        result.order.insert(result.order.end() - preReorderSize, info.tableId);
        result.orderedJoins.insert(result.orderedJoins.end() - preReorderSize, nullptr);
        continue;
      }

      result.order.insert(result.order.end() - preReorderSize, info.tableId);

      if (info.joinStatement == nullptr){
        result.orderedJoins.insert(result.orderedJoins.end() - preReorderSize, reorderedJoin);
        continue;
      }

      result.orderedJoins.insert(result.orderedJoins.end() - preReorderSize, info.joinStatement);
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
    candidates.reserve(indexes.size());

    std::vector<Expressions::Expression*> conjunctions;
    Optimizer::SplitConjunctions(expression, conjunctions);

    for (auto& index : indexes) {
      index.columns = CoreEngine::SystemCatalog::Get().SelectIndexColumnsByIndexId(this->context->_context.GetAllocator(), index.id);

      auto analyzeResults = Optimizer::AnalyzeTableScan(index, conjunctions);

      IndexCandidate candidate;

      candidate.header = &index;
      candidate.analyzeInfo = std::move(analyzeResults);
      candidate.conjunctions = &conjunctions;
      candidate.matchingColumns = static_cast<int>(candidate.analyzeInfo.size());
      CostEstimator::EstimateIndexCost(this->context, candidate, tableStatistics);

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

      if (isPerfectSeek && candidate.header->isClustered)
        return Optimizer::BuildSeekKeys(candidate.analyzeInfo, conjunctions);
    }

    std::ranges::sort(candidates, IndexCandidate());

    const auto& candidate = candidates[0];
    return Optimizer::BuildSeekKeys(candidate.analyzeInfo, conjunctions);
  }

   JoinAlgorithmAnalysisResult Optimizer::ChooseJoinAlgorithm(
      const Int leftTableId,
      const Int rightTableId,
      Expressions::Expression* joinCondition
    ){
      //if left or right table is a subquery or derived table, use nested loop join
      if (leftTableId == INVALID_TABLE_ID || rightTableId == INVALID_TABLE_ID)
        return JoinAlgorithmAnalysisResult(PipelineConstants::JoinAlgorithm::NestedLoopJoin);

      static auto& statisticsManager = CoreEngine::StatisticsManager::Get();

      const auto leftInfo = statisticsManager.GetTableStatistics(leftTableId);
      const auto rightInfo = statisticsManager.GetTableStatistics(rightTableId);

      std::vector<Expressions::Expression*> conjunctions;
      SplitConjunctions(joinCondition, conjunctions);

      std::vector<JoinConditionInfo> conditionsInfo;
      bool isEqualityJoin = false;
      for (const auto& conjunction : conjunctions)
        Optimizer::ProcessJoinCondition(conjunction, conditionsInfo, isEqualityJoin);

      if (!isEqualityJoin)
        return JoinAlgorithmAnalysisResult(PipelineConstants::JoinAlgorithm::NestedLoopJoin);

      if (leftInfo.rowCount < PipelineConstants::SMALL_TABLE
          && rightInfo.rowCount < PipelineConstants::SMALL_TABLE)
        return JoinAlgorithmAnalysisResult(PipelineConstants::JoinAlgorithm::NestedLoopJoin);

      const auto leftBestMatch = Optimizer::CheckPredicatesSorting(leftInfo, conditionsInfo);
      const auto rightBestMatch = Optimizer::CheckPredicatesSorting(rightInfo, conditionsInfo);

      if (!leftBestMatch.empty() && !rightBestMatch.empty()){

        //expression should be consumed by the best index as keys will be used match
        return this->CreateMergeJoinKeys(
          conditionsInfo,
          leftBestMatch,
          rightBestMatch,
          leftInfo.tableId,
          rightInfo.tableId
        );
      }

    if (leftInfo.rowCount > PipelineConstants::HASH_JOIN_THRESHOLD
      || rightInfo.rowCount > PipelineConstants::HASH_JOIN_THRESHOLD)
      return JoinAlgorithmAnalysisResult(PipelineConstants::JoinAlgorithm::HashJoin);

    return JoinAlgorithmAnalysisResult(PipelineConstants::JoinAlgorithm::NestedLoopJoin);
  }
}
