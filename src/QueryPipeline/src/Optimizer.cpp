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
    DataStructures::PolymorphicArray<column_index_t>& leftKeyColumns,
    DataStructures::PolymorphicArray<column_index_t>& rightKeyColumns
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

  JoinOrderAnalyzeResult::JoinOrderAnalyzeResult(const ::Memory::IAllocator* allocator)
    : order(allocator), orderedJoins(allocator) {
    this->isReordered = false;
  }

  void Optimizer::SplitConjunctions(Expressions::Expression* expression, DataStructures::PolymorphicArray<Expressions::Expression*>& conjunctions){
    if (expression == nullptr) return;

    if (!expression->IsLogical()){
      conjunctions.Push(expression);
      return;
    }

    auto* logicalExpr = expression->AsLogical();
    if (logicalExpr->IsAnd()){
      SplitConjunctions(logicalExpr->left, conjunctions);
      SplitConjunctions(logicalExpr->right, conjunctions);

      //should delete logical expression?
      // logicalExpr->left = nullptr;
      // logicalExpr->right = nullptr;
      return;
    }

    //or expression
    conjunctions.Push(expression);
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

  DataStructures::PolymorphicArray<table_id_t> Optimizer::GetInvolvedTables(const Expressions::Expression* expression) const{
    HashSet<table_id_t> involvedTablesSet;

    Optimizer::GetInvolvedTables(expression, involvedTablesSet);

    return involvedTablesSet.ToPolymorphicArray(this->context->_context.GetAllocator());
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
        Dictionary<table_id_t, Expressions::Expression*>& tablePredicatesDictionary,
        Expressions::Expression*& remainingPredicate
    ) const{
        DataStructures::PolymorphicArray<Expressions::Expression*> expressions(
            this->context->_context.GetAllocator()
        );
        Optimizer::SplitConjunctions(baseExpression, expressions);

        for (auto* expression : expressions){
            const auto involvedTables = this->GetInvolvedTables(expression);

            if (involvedTables.Size() == 1){
                Expressions::Expression* existing = nullptr;
                tablePredicatesDictionary.TryGetValue(involvedTables[0], existing);
                Optimizer::CombineExpressionsWithAnd(existing, expression);
                tablePredicatesDictionary[involvedTables[0]] = existing;
                continue;
            }

            Optimizer::CombineExpressionsWithAnd(remainingPredicate, expression);
        }
    }

  void Optimizer::AnalyzeTableScan(
    Expressions::Expression* baseExpression,
    const Expressions::BinaryExpression* expression,
    Dictionary<column_id_t, DataStructures::PolymorphicArray<Expressions::Expression*>>& columnPredicatesDictionary
  ){
    if (expression->left->IsColumn()
      && (expression->right->IsConstant() || expression->right->IsVariable()))
    {
      const auto* columnExpr = expression->left->AsColumn();
      columnPredicatesDictionary[columnExpr->columnId].Push(baseExpression);
      return;
    }

    if (expression->right->IsColumn()
      && (expression->left->IsConstant() || expression->left->IsVariable()))
    {
      const auto* columnExpr = expression->right->AsColumn();
      columnPredicatesDictionary[columnExpr->columnId].Push(baseExpression);
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

  DataStructures::PolymorphicArray<IndexSeekColumnAnalysisResults> Optimizer::AnalyzeTableScan(
    const Headers::IndexHeader& index,
    const DataStructures::PolymorphicArray<Expressions::Expression*>& conjunctions
  ){
    DataStructures::PolymorphicArray<IndexSeekColumnAnalysisResults> result;

    Dictionary<column_id_t, DataStructures::PolymorphicArray<Expressions::Expression*>> columnPredicates;
    for (const auto& column : index.columns)
      columnPredicates.Add(column.columnId, {});

    for (auto& condition : conjunctions){
      if (!condition->IsBinary())
        continue;

      Optimizer::AnalyzeTableScan(condition, condition->AsBinary(), columnPredicates);
    }

    for (const auto& column : index.columns) {
      DataStructures::PolymorphicArray<Expressions::Expression*> predicates;
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

      result.Push(analyzeResult);
    }

    return result;
  }

  Range Optimizer::BuildSeekKeys(
    const DataStructures::PolymorphicArray<IndexSeekColumnAnalysisResults>& analyzeResults,
    DataStructures::PolymorphicArray<Expressions::Expression*>& conjunctions
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
    DataStructures::PolymorphicArray<JoinConditionInfo>& conditionsInfo,
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

    conditionsInfo.Push(info);
  }

  DataStructures::PolymorphicArray<Int> Optimizer::CheckPredicatesSorting(
    const Headers::TableStatistics& tableStats,
    const DataStructures::PolymorphicArray<JoinConditionInfo>& joinConditions
  ) const{
    const auto indexes = CoreEngine::StatisticsManager::Get().GetIndexStatistics(tableStats.tableId);

    if (indexes.Empty())
      return {};

    DataStructures::PolymorphicArray<Int> bestMatch;
    for (const auto& index : indexes){
      const auto columns = CoreEngine::SystemCatalog::Get().SelectIndexColumnsByIndexId(this->context->_context.GetAllocator(), index.indexId);

      DataStructures::PolymorphicArray<Int> matches;
      for (const auto& column : columns){

        for (int i = 0;i < joinConditions.Size();i++){
          const auto& joinCondition = joinConditions[i];

          const auto columnId = (joinCondition.leftTableId == tableStats.tableId)
                  ? joinCondition.leftColumnId
                  : joinCondition.rightColumnId;

          if (column.columnId == columnId){
            matches.Push(i);
            continue;
          }

          break;
        }

        if (bestMatch.Size() < matches.Size())
          bestMatch = std::move(matches);
      }
    }

    return bestMatch;
  }

  JoinAlgorithmAnalysisResult Optimizer::CreateMergeJoinKeys(
    const DataStructures::PolymorphicArray<JoinConditionInfo>& conditionsInfo,
    const DataStructures::PolymorphicArray<Int>& leftKeyColumns,
    const DataStructures::PolymorphicArray<Int>& rightKeyColumns,
    const Int leftTableId,
    const Int rightTableId
  ) const{
    JoinAlgorithmAnalysisResult result(PipelineConstants::JoinAlgorithm::MergeJoin);

    const auto leftSize = leftKeyColumns.Size();
    const auto rightSize = rightKeyColumns.Size();

    const auto min = std::min(leftSize, rightSize);

    for (int i = 0;i < min;i++){
      const auto& joinCondition = conditionsInfo[leftKeyColumns[i]];

      const auto leftExprIndex = (joinCondition.leftTableId == leftTableId)
          ? joinCondition.leftColumnIndex
          : joinCondition.rightColumnIndex;

      const auto rightExprIndex = (joinCondition.leftTableId == rightTableId)
          ? joinCondition.leftColumnIndex
          : joinCondition.rightColumnIndex;

      result.leftKeyColumns.Push(leftExprIndex);
      result.rightKeyColumns.Push(rightExprIndex);
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

    JoinOrderAnalyzeResult Optimizer::DetermineJoinOrder(Statements::SelectStatement* statement) const{
        JoinOrderAnalyzeResult result(this->context->_context.GetAllocator());

        if (statement->IsConstant()) return result;

        if (!statement->HasJoins()){
            result.order.Push(statement->table->tableId);
            return result;
        }

        DataStructures::PolymorphicArray<JoinOrderAnalyzeInfo> infoVector(this->context->_context.GetAllocator());
        const auto baseSourceStats = CoreEngine::StatisticsManager::Get().GetTableStatistics(statement->table->tableId);

        //optimize by using hasIndex bool on tableStats to avoid lookups
        const auto baseSourceIndexStats = CoreEngine::StatisticsManager::Get().GetIndexStatistics(statement->table->tableId);

        const JoinOrderAnalyzeInfo baseInfo(
            statement->table->tableId,
            baseSourceStats.rowCount,
            !baseSourceIndexStats.Empty()
        );

        //base table info
        infoVector.Push(baseInfo);

        //each join info
        for (const auto& join : statement->joins) {
            if (join->IsRightJoin()){
                result.order.Insert(join->table->tableId, 0);
                result.orderedJoins.Insert(join, 0);
                continue;
            }

            if (!join->IsInnerJoin()) {
                result.order.Push(join->table->tableId);
                result.orderedJoins.Push(join);
                continue;
            }

            const auto joinSourceStats = CoreEngine::StatisticsManager::Get().GetTableStatistics(join->table->tableId);
            const auto joinSourceIndexStats = CoreEngine::StatisticsManager::Get().GetIndexStatistics(join->table->tableId);

            const JoinOrderAnalyzeInfo joinInfo(
                join->table->tableId,
                joinSourceStats.rowCount,
                !joinSourceIndexStats.Empty(),
                join
            );

            infoVector.Push(joinInfo);
        }

        std::ranges::sort(infoVector, JoinOrderAnalyzeInfo());

        auto* baseSource = statement->table;
        Statements::JoinStatement* reorderedJoin = nullptr;

        const auto preReorderSize = result.orderedJoins.Size();

        for (int i = 0;i < infoVector.Size(); i++){
            auto& info = infoVector[i];
            //swap occurred
            if (i == 0 && info.joinStatement != nullptr){
                statement->table = info.joinStatement->table;
                info.joinStatement->table = baseSource;
                baseSource = statement->table;

                reorderedJoin = info.joinStatement;

                result.order.Insert(info.tableId, result.order.Size() - preReorderSize);
                result.orderedJoins.Insert(nullptr, result.orderedJoins.Size() - preReorderSize);
                continue;
            }

            result.order.Insert(info.tableId, result.order.Size() - preReorderSize);

            if (info.joinStatement == nullptr){
                result.orderedJoins.Insert(reorderedJoin, result.orderedJoins.Size() - preReorderSize);
                continue;
            }

            result.orderedJoins.Insert(info.joinStatement, result.orderedJoins.Size() - preReorderSize);
        }

        //remove the first which is always null
        result.orderedJoins.erase(result.orderedJoins.begin());
        result.isReordered = result.order[0] != statement->table->tableId;

        return result;
    }

  PredicatePushDownResult Optimizer::PushDownPredicates(
    const DataStructures::PolymorphicArray<table_id_t>& tables,
    Expressions::Expression* whereClause,
    const DataStructures::PolymorphicArray<Statements::JoinStatement*>& joins
  ) const{
    PredicatePushDownResult result;

    if (whereClause == nullptr && joins.Empty()) return result;

    result.tablePredicatesDictionary = Dictionary<table_id_t, Expressions::Expression*>::FromArray(tables, nullptr);
    Optimizer::ProcessPredicate(whereClause, result.tablePredicatesDictionary, result.remainingPredicate);

    for (const auto& join : joins) {
      // Cannot push down predicates for FULL OUTER JOIN as it would break semantics
      // (unmatched rows from both sides must be preserved with NULLs)
      if (join->IsFullOuterJoin()) continue;

      Expressions::Expression* joinRemainingPredicate = nullptr;
      Optimizer::ProcessPredicate(join->expression, result.tablePredicatesDictionary, joinRemainingPredicate);
      join->expression = joinRemainingPredicate;
    }

    return result;
  }

   Range Optimizer::PerformIndexAnalysis(
    DataStructures::PolymorphicArray<Headers::IndexHeader>& indexes,
    Expressions::Expression* expression,
    const Headers::TableStatistics& tableStatistics
  ){
    DataStructures::PolymorphicArray<IndexCandidate> candidates(this->context->_context.GetAllocator());
    candidates.Reserve(indexes.Size());

    DataStructures::PolymorphicArray<Expressions::Expression*> conjunctions;
    Optimizer::SplitConjunctions(expression, conjunctions);

    for (auto& index : indexes) {
      index.columns = CoreEngine::SystemCatalog::Get().SelectIndexColumnsByIndexId(this->context->_context.GetAllocator(), index.id);

      auto analyzeResults = Optimizer::AnalyzeTableScan(index, conjunctions);

      IndexCandidate candidate;

      candidate.header = &index;
      candidate.analyzeInfo = std::move(analyzeResults);
      candidate.conjunctions = &conjunctions;
      candidate.matchingColumns = static_cast<int>(candidate.analyzeInfo.Size());
      CostEstimator::EstimateIndexCost(this->context, candidate, tableStatistics);

      candidates.Push(candidate);
    }

    if (candidates.Empty())
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

      DataStructures::PolymorphicArray<Expressions::Expression*> conjunctions;
      SplitConjunctions(joinCondition, conjunctions);

      DataStructures::PolymorphicArray<JoinConditionInfo> conditionsInfo;
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

      if (!leftBestMatch.Empty() && !rightBestMatch.Empty()){

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
