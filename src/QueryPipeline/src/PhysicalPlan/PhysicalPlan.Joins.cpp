#include "Database.h"
#include "../../include/PhysicalPlan.h"

namespace QueryPipeline::PhysicalPlan {
  ExecutionResult PhysicalNestedLoopInnerJoin::ExecuteBatchJoin(
    const DatabaseEngine::ExecutionContext& context,
    const ExecutionResult& leftResult
  ) const{
    auto result = ExecutionResult();

    Expressions::EvaluationContext evaluationContext(
        Expressions::EvaluationContext::EvaluationContextType::Join,
        context
    );
    bool canFetchMore = true;

    while (canFetchMore){
      auto rightResult = this->right->Execute(context);
      canFetchMore = rightResult.canFetchMore;

      for (auto& outerRow: leftResult.rows) {
        for (auto& innerRow: rightResult.rows) {

          evaluationContext.outerRow = &outerRow;
          evaluationContext.innerRow = &innerRow;
          if (!this->expression->Evaluate(evaluationContext).AsBool())
            continue;

          outerRow.Join(innerRow);
          result.rows.Push(outerRow);
        }
      }
    }

    return result;
  }

  PhysicalNestedLoopInnerJoin::PhysicalNestedLoopInnerJoin(
    ExecutionNode* left,
    ExecutionNode* right,
    Expressions::Expression *joinCondition)
    : left(left), right(right), expression(joinCondition){}

  PhysicalNestedLoopInnerJoin::~PhysicalNestedLoopInnerJoin() {
    delete this->left;
    delete this->right;
  }


  ExecutionResult PhysicalNestedLoopInnerJoin::Execute(const DatabaseEngine::ExecutionContext& context){
    const auto leftResult = this->left->Execute(context);
    auto result = this->ExecuteBatchJoin(context, leftResult);
    result.canFetchMore = leftResult.canFetchMore;
    return result;
  }

  ExecutionResult PhysicalMergeInnerJoin::ExecuteBatchJoin(
    const DatabaseEngine::ExecutionContext& context,
    ExecutionResult& leftResult
  ) const
  {
    using CompOperator = DataTypes::Indexing::Key::ComparisonResult;

    auto result = ExecutionResult();

    Expressions::EvaluationContext evaluationContext(
      Expressions::EvaluationContext::EvaluationContextType::Join,
      context
    );

    auto rightResult = this->right->Execute(context);

    // Int leftIndex = 0;
    // Int rightIndex = 0;
    //
    // const auto leftRowsCount = leftResult->rows.Size();
    // const auto rightRowsCount = rightResult->rows.Size();
    //
    // const auto outerRowSize = leftResult->columns.Size();
    //
    // while (leftIndex < leftRowsCount){
    //   auto& outerRow = leftResult->rows[leftIndex];
    //   const auto leftKey = DatabaseEngine::Database::CreateKey(this->leftKeyColumns, &outerRow);
    //
    //   while (rightIndex < rightRowsCount){
    //     const auto& innerRow = rightResult->rows[rightIndex];
    //
    //     const auto rightKey = DatabaseEngine::Database::CreateKey(this->rightKeyColumns, &innerRow, outerRowSize);
    //
    //     const auto comparison = leftKey.CompareCompositeKeys(rightKey);
    //
    //     if (comparison == CompOperator::Less)
    //       break;
    //
    //     if (comparison == CompOperator::Greater){
    //       rightIndex++;
    //       continue;
    //     }
    //
    //     outerRow.Join(&innerRow);
    //     result->rows.push_back(outerRow);
    //
    //     rightIndex++;
    //   }
    //
    //   if (rightIndex >= rightRowsCount
    //       && leftIndex < leftRowsCount
    //       && rightResult->canFetchMore
    //   ){
    //     delete rightResult;
    //     rightResult = this->right->Execute(properties);
    //     rightIndex = 0;
    //   }
    //
    //   leftIndex++;
    // }
    //
    // if (rightIndex < rightRowsCount){
    //   const auto& lastUsedRow = rightResult->rows[rightIndex];
    //   this->right->UpdateScanState(lastUsedRow.GetId());
    // }
    //
    // delete rightResult;
    // result->canFetchMore = leftResult->canFetchMore;

    return result;
  }

  PhysicalMergeInnerJoin::PhysicalMergeInnerJoin(
    ExecutionNode* left,
    ExecutionNode* right,
    Expressions::Expression* expression,
    std::vector<column_index_t>& leftKeyColumns,
    std::vector<column_index_t>& rightKeyColumns
  ) : left(left), right(right), expression(expression),
      leftKeyColumns(std::move(leftKeyColumns)), rightKeyColumns(std::move(rightKeyColumns)){}

  PhysicalMergeInnerJoin::~PhysicalMergeInnerJoin(){
    delete this->left;
    delete this->right;
    delete this->expression;
  }

  ExecutionResult PhysicalMergeInnerJoin::Execute(const DatabaseEngine::ExecutionContext& context){
    auto leftResult = this->left->Execute(context);
    auto result = this->ExecuteBatchJoin(context, leftResult);

    result.canFetchMore = leftResult.canFetchMore;
    return result;
  }

  ExecutionResult PhysicalMergeLeftJoin::ExecuteBatchJoin(
    const DatabaseEngine::ExecutionContext& context,
    ExecutionResult& leftResult
  ) const{
    // using CompOperator = DataTypes::Indexing::Key::ComparisonResult;
    //
    // auto* result = new ExecutionResult();
    //
    // Expressions::EvaluationContext context(
    //   Expressions::EvaluationContext::EvaluationContextType::Join,
    //   properties.variables
    // );
    //
    // auto* rightResult = this->right->Execute(properties);
    //
    // Int leftIndex = 0;
    // Int rightIndex = 0;
    //
    // const auto leftRowsCount = leftResult->rows.size();
    // const auto rightRowsCount = rightResult->rows.size();
    //
    // const auto outerRowSize = static_cast<Int>(leftResult->columns.size());
    //
    // while (leftIndex < leftRowsCount){
    //   auto& outerRow = leftResult->rows[leftIndex];
    //   const auto leftKey = DatabaseEngine::Database::CreateKey(this->leftKeyColumns, &outerRow);
    //
    //   bool hasMatch = false;
    //
    //   while (rightIndex < rightRowsCount){
    //     auto& innerRow = rightResult->rows[rightIndex];
    //
    //     const auto rightKey = DatabaseEngine::Database::CreateKey(this->rightKeyColumns, &innerRow, outerRowSize);
    //
    //     const auto comparison = leftKey.CompareCompositeKeys(rightKey);
    //
    //     if (comparison == CompOperator::Less)
    //       break;
    //
    //     if (comparison == CompOperator::Greater){
    //       rightIndex++;
    //       continue;
    //     }
    //
    //     hasMatch = true;
    //
    //     outerRow.Join(&innerRow);
    //     result.rows.push_back(outerRow);
    //
    //     rightIndex++;
    //   }
    //
    //   if (rightIndex >= rightRowsCount
    //       && leftIndex < leftRowsCount
    //       && rightResult->canFetchMore
    //   ){
    //     delete rightResult;
    //     rightResult = this->right->Execute(properties);
    //     rightIndex = 0;
    //   }
    //
    //   if (!hasMatch){
    //     outerRow.LeftJoin(rightResult->columns);
    //     result->rows.push_back(outerRow);
    //   }
    //
    //   leftIndex++;
    // }
    //
    // if (rightIndex < rightRowsCount){
    //   const auto& lastUsedRow = rightResult->rows[rightIndex];
    //   this->right->UpdateScanState(lastUsedRow.GetId());
    // }
    //
    // delete rightResult;
    // result->canFetchMore = leftResult->canFetchMore;
    //
    // return result;
  }

  PhysicalMergeLeftJoin::PhysicalMergeLeftJoin(
    ExecutionNode* left,
    ExecutionNode* right,
    Expressions::Expression* expression,
    std::vector<column_index_t>& leftKeyColumns,
    std::vector<column_index_t>& rightKeyColumns
    ): left(left), right(right), expression(expression),
        leftKeyColumns(std::move(leftKeyColumns)), rightKeyColumns(std::move(rightKeyColumns)){}

  PhysicalMergeLeftJoin::~PhysicalMergeLeftJoin(){
    delete this->left;
    delete this->right;
    delete this->expression;
  }

  ExecutionResult PhysicalMergeLeftJoin::Execute(const DatabaseEngine::ExecutionContext& context){
    auto leftResult = this->left->Execute(context);
    auto result = this->ExecuteBatchJoin(context, leftResult);

    result.canFetchMore = leftResult.canFetchMore;
    return result;
  }

  ExecutionResult PhysicalMergeFullJoin::ExecuteBatchJoin(
    const DatabaseEngine::ExecutionContext& context,
    ExecutionResult& leftResult
  ) const{
    // using CompOperator = DataTypes::Indexing::Key::ComparisonResult;
    //
    // auto* result = new ExecutionResult();
    //
    // Expressions::EvaluationContext context(
    //   Expressions::EvaluationContext::EvaluationContextType::Join,
    //   properties.variables
    // );
    //
    // auto* rightResult = this->right->Execute(properties);
    //
    // Int leftIndex = 0;
    // Int rightIndex = 0;
    //
    // const auto leftRowsCount = leftResult->rows.size();
    // const auto rightRowsCount = rightResult->rows.size();
    //
    // const auto outerRowSize = static_cast<Int>(leftResult->columns.size());
    //
    // while (leftIndex < leftRowsCount){
    //   auto& outerRow = leftResult->rows[leftIndex];
    //   const auto leftKey = DatabaseEngine::Database::CreateKey(this->leftKeyColumns, &outerRow);
    //
    //   bool hasMatch = false;
    //
    //   while (rightIndex < rightRowsCount){
    //     auto& innerRow = rightResult->rows[rightIndex];
    //
    //     const auto rightKey = DatabaseEngine::Database::CreateKey(this->rightKeyColumns, &innerRow, outerRowSize);
    //
    //     const auto comparison = leftKey.CompareCompositeKeys(rightKey);
    //
    //     if (comparison == CompOperator::Less)
    //       break;
    //
    //     if (comparison == CompOperator::Greater){
    //       innerRow.RightJoin(leftResult->columns);
    //       result->rows.push_back(innerRow);
    //       rightIndex++;
    //       continue;
    //     }
    //
    //     hasMatch = true;
    //
    //     outerRow.Join(&innerRow);
    //     result->rows.push_back(outerRow);
    //     rightIndex++;
    //   }
    //
    //   if (rightIndex >= rightRowsCount
    //       && leftIndex < leftRowsCount
    //       && rightResult->canFetchMore
    //   ){
    //     delete rightResult;
    //     rightResult = this->right->Execute(properties);
    //     rightIndex = 0;
    //   }
    //
    //   if (!hasMatch){
    //     outerRow.LeftJoin(rightResult->columns);
    //     result->rows.push_back(outerRow);
    //   }
    //
    //   leftIndex++;
    // }
    //
    // if (rightIndex < rightRowsCount){
    //   const auto& lastUsedRow = rightResult->rows[rightIndex];
    //   this->right->UpdateScanState(lastUsedRow.GetId());
    // }
    //
    // delete rightResult;
    // result->canFetchMore = leftResult->canFetchMore;
    //
    // return result;
  }

  PhysicalMergeFullJoin::PhysicalMergeFullJoin(
    ExecutionNode* left,
    ExecutionNode* right,
    Expressions::Expression* expression,
    std::vector<column_index_t>& leftKeyColumns,
    std::vector<column_index_t>& rightKeyColumns
  ): left(left), right(right), expression(expression),
      leftKeyColumns(std::move(leftKeyColumns)), rightKeyColumns(std::move(rightKeyColumns)){}

  PhysicalMergeFullJoin::~PhysicalMergeFullJoin(){
    delete this->left;
    delete this->right;
    delete this->expression;
  }

  ExecutionResult PhysicalMergeFullJoin::Execute(const DatabaseEngine::ExecutionContext& context){
    auto leftResult = this->left->Execute(context);
    auto result = this->ExecuteBatchJoin(context, leftResult);

    result.canFetchMore = leftResult.canFetchMore;
    return result;
  }

  PhysicalNestedLoopLeftJoin::PhysicalNestedLoopLeftJoin(
    ExecutionNode* left,
    ExecutionNode* right,
    Expressions::Expression *expression)
    : left(left), right(right), expression(expression){}

  PhysicalNestedLoopLeftJoin::~PhysicalNestedLoopLeftJoin() {
      delete this->left;
      delete this->right;
  }

  ExecutionResult PhysicalNestedLoopLeftJoin::Execute(const DatabaseEngine::ExecutionContext& context){
      // auto* result = new ExecutionResult();
      //
      // auto* leftResult = this->left->Execute(properties);
      // auto* rightResult = this->right->Execute(properties);
      //
      // Expressions::EvaluationContext context(Expressions::EvaluationContext::EvaluationContextType::Join, properties.variables);
      // //create new row
      // for (auto& outerRow: leftResult->rows) {
      //
      //   bool hasMatched = false;
      //   for (auto& innerRow: rightResult->rows) {
      //
      //     context.outerRow = &outerRow;
      //     context.innerRow = &innerRow;
      //
      //     if (!this->expression->Evaluate(context).AsBool())
      //       continue;
      //
      //     outerRow.Join(&innerRow);
      //     result->rows.push_back(outerRow);
      //     hasMatched = true;
      //   }
      //
      //   if (!hasMatched){
      //     outerRow.LeftJoin(rightResult->columns);
      //     result->rows.push_back(outerRow);
      //   }
      // }
      //
      // result->columns.reserve(leftResult->columns.size() + rightResult->columns.size());
      // result->columns.insert(result->columns.end(),
      //                        std::make_move_iterator(leftResult->columns.begin()),
      //                        std::make_move_iterator(leftResult->columns.end()));
      // result->columns.insert(result->columns.end(),
      //                        std::make_move_iterator(rightResult->columns.begin()),
      //                        std::make_move_iterator(rightResult->columns.end()));
      //
      // delete leftResult;
      // delete rightResult;
      //
      // return result;
  }

  PhysicalNestedLoopFullJoin::PhysicalNestedLoopFullJoin(
    ExecutionNode* left,
    ExecutionNode* right,
    Expressions::Expression *joinCondition)
    : left(left), right(right), joinCondition(joinCondition){}

  PhysicalNestedLoopFullJoin::~PhysicalNestedLoopFullJoin() {
      delete this->left;
      delete this->right;
  }

  ExecutionResult PhysicalNestedLoopFullJoin::Execute(const DatabaseEngine::ExecutionContext& context){
      // auto* result = new ExecutionResult();
      //
      // auto* leftResult = this->left->Execute(properties);
      // auto* rightResult = this->right->Execute(properties);
      //
      // std::vector leftMatched(leftResult->rows.size(), false);
      // std::vector rightMatched(rightResult->rows.size(), false);
      //
      // Expressions::EvaluationContext context(Expressions::EvaluationContext::EvaluationContextType::Join, properties.variables);
      //
      // for (int i = 0;i < leftResult->rows.size();i++) {
      //   for (int j = 0;j < rightResult->rows.size();j++) {
      //     auto& outerRow = leftResult->rows[i];
      //     auto& innerRow = rightResult->rows[j];
      //
      //     context.outerRow = &outerRow;
      //     context.innerRow = &innerRow;
      //
      //     if (!this->joinCondition->Evaluate(context).AsBool())
      //       continue;
      //
      //     outerRow.Join(&innerRow);
      //     result->rows.push_back(outerRow);
      //     leftMatched[i] = true;
      //     rightMatched[j] = true;
      //   }
      // }
      //
      // for (int i = 0;i < leftResult->rows.size();i++) {
      //   if (leftMatched[i])
      //     continue;
      //
      //   auto& outerRow = leftResult->rows[i];
      //   outerRow.LeftJoin(rightResult->columns);
      //   result->rows.push_back(outerRow);
      // }
      //
      // for (int i = 0;i < rightResult->rows.size(); i++) {
      //   if (rightMatched[i])
      //     continue;
      //
      //   auto& innerRow = rightResult->rows[i];
      //   innerRow.RightJoin(rightResult->columns);
      //   result->rows.push_back(innerRow);
      // }
      //
      //
      // delete leftResult;
      // delete rightResult;
      //
      // return result;
  }

}