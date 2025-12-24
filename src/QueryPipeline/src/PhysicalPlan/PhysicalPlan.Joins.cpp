#include "Database.h"
#include "../../include/PhysicalPlan.h"

namespace QueryPipeline::PhysicalPlan {
  ExecutionResult* PhysicalNestedLoopInnerJoin::ExecuteBatchJoin(
    const DatabaseEngine::ExecutionProperties& properties,
    const ExecutionResult* leftResult
  ) const
  {
    auto* result = new ExecutionResult();

    Expressions::EvaluationContext context(Expressions::EvaluationContext::EvaluationContextType::Join, properties.variables);
    bool canFetchMore = true;

    while (canFetchMore){
      const auto* rightResult = this->right->Execute(properties);
      canFetchMore = rightResult->canFetchMore;

      for (const auto* outerRow: leftResult->rows) {
        for (const auto* innerRow: rightResult->rows) {

          context.outerRow = outerRow;
          context.innerRow = innerRow;
          if (!this->joinCondition->Evaluate(context).GetBool())
            continue;

          result->rows.push_back(outerRow->Join(innerRow));
        }
      }

      delete rightResult;
    }

    return result;
  }

  PhysicalNestedLoopInnerJoin::PhysicalNestedLoopInnerJoin(
    ExecutionNode* left,
    ExecutionNode* right,
    Expressions::Expression *joinCondition)
    : left(left), right(right), joinCondition(joinCondition){}

  PhysicalNestedLoopInnerJoin::~PhysicalNestedLoopInnerJoin() {
    delete this->left;
    delete this->right;
  }


  ExecutionResult * PhysicalNestedLoopInnerJoin::Execute(const DatabaseEngine::ExecutionProperties& properties){
    const auto* leftResult = this->left->Execute(properties);

    auto * result = this->ExecuteBatchJoin(properties, leftResult);

    result->canFetchMore = leftResult->canFetchMore;

    delete leftResult;
    return result;
  }

  ExecutionResult* PhysicalMergeInnerJoin::ExecuteBatchJoin(
    const DatabaseEngine::ExecutionProperties& properties,
    const ExecutionResult* leftResult
  ) const
  {
    using CompOperator = DataTypes::Indexing::Key::ComparisonResult;

    auto* result = new ExecutionResult();

    Expressions::EvaluationContext context(
      Expressions::EvaluationContext::EvaluationContextType::Join,
      properties.variables
    );

    const auto* rightResult = this->right->Execute(properties);

    Int leftIndex = 0;
    Int rightIndex = 0;

    const auto leftRowsCount = leftResult->rows.size();
    const auto rightRowsCount = rightResult->rows.size();

    const auto outerRowSize = leftResult->columns.size();

    while (leftIndex < leftRowsCount && rightIndex < rightRowsCount){
      const auto& outerRow = leftResult->rows[leftIndex];
      const auto& innerRow = rightResult->rows[rightIndex];

      const auto leftKey = DatabaseEngine::Database::CreateKey(this->leftKeyColumns, outerRow);
      const auto rightKey = DatabaseEngine::Database::CreateKey(this->rightKeyColumns, innerRow, outerRowSize);

      const auto comparison = leftKey.CompareCompositeKeys(rightKey);

      if (comparison == CompOperator::Less){
        leftIndex++;
        continue;
      }

      if (comparison == CompOperator::Greater){
        rightIndex++;
        continue;
      }

      result->rows.push_back(outerRow->Join(innerRow));
      rightIndex++;

      if (rightIndex >= rightRowsCount
          && leftIndex < leftRowsCount
          && rightResult->canFetchMore
      ){
        delete rightResult;
        rightResult = this->right->Execute(properties);
        rightIndex = 0;
      }
    }

    delete rightResult;
    result->canFetchMore = leftResult->canFetchMore;

    return result;
  }

  PhysicalMergeInnerJoin::PhysicalMergeInnerJoin(
    ExecutionNode* left,
    ExecutionNode* right,
    Expressions::Expression* joinCondition,
    std::vector<column_index_t>& leftKeyColumns,
    std::vector<column_index_t>& rightKeyColumns
  ) : left(left), right(right), joinCondition(joinCondition),
      leftKeyColumns(std::move(leftKeyColumns)), rightKeyColumns(std::move(rightKeyColumns)){}

  PhysicalMergeInnerJoin::~PhysicalMergeInnerJoin(){
    delete this->left;
    delete this->right;
    delete this->joinCondition;
  }

  ExecutionResult* PhysicalMergeInnerJoin::Execute(const DatabaseEngine::ExecutionProperties& properties){
    const auto* leftResult = this->left->Execute(properties);

    auto* result = this->ExecuteBatchJoin(properties, leftResult);

    result->canFetchMore = leftResult->canFetchMore;

    delete leftResult;
    return result;
  }

  PhysicalNestedLoopLeftJoin::PhysicalNestedLoopLeftJoin(
    ExecutionNode* left,
    ExecutionNode* right,
    Expressions::Expression *joinCondition)
    : left(left), right(right), joinCondition(joinCondition){}

  PhysicalNestedLoopLeftJoin::~PhysicalNestedLoopLeftJoin() {
      delete this->left;
      delete this->right;
  }

  ExecutionResult * PhysicalNestedLoopLeftJoin::Execute(const DatabaseEngine::ExecutionProperties& properties){
      auto* result = new ExecutionResult();

      const auto* leftResult = this->left->Execute(properties);
      const auto* rightResult = this->right->Execute(properties);

      Expressions::EvaluationContext context(Expressions::EvaluationContext::EvaluationContextType::Join, properties.variables);
      //create new row
      for (const auto* outerRow: leftResult->rows) {

        bool hasMatched = false;
        for (const auto* innerRow: rightResult->rows) {

          context.outerRow = outerRow;
          context.innerRow = innerRow;

          if (!this->joinCondition->Evaluate(context).GetBool())
            continue;

          const auto* joinedRow = outerRow->Join(innerRow);

          result->rows.push_back(joinedRow);

          hasMatched = true;
        }

        if (!hasMatched)
          result->rows.push_back(outerRow->LeftJoin(rightResult->columns));
      }

      result->columns.reserve(leftResult->columns.size() + rightResult->columns.size());
      result->columns.insert(result->columns.end(),
                             std::make_move_iterator(leftResult->columns.begin()),
                             std::make_move_iterator(leftResult->columns.end()));
      result->columns.insert(result->columns.end(),
                             std::make_move_iterator(rightResult->columns.begin()),
                             std::make_move_iterator(rightResult->columns.end()));

      delete leftResult;
      delete rightResult;

      return result;
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

  ExecutionResult * PhysicalNestedLoopFullJoin::Execute(const DatabaseEngine::ExecutionProperties& properties){
      auto* result = new PhysicalPlan::ExecutionResult();

      const auto* leftResult = this->left->Execute(properties);
      const auto* rightResult = this->right->Execute(properties);

      std::vector<bool> leftMatched(leftResult->rows.size(), false);
      std::vector<bool> rightMatched(rightResult->rows.size(), false);

      Expressions::EvaluationContext context(Expressions::EvaluationContext::EvaluationContextType::Join, properties.variables);

      for (int i = 0;i < leftResult->rows.size();i++) {
        for (int j = 0;j < rightResult->rows.size();j++) {
          const auto* outerRow = leftResult->rows[i];
          const auto* innerRow = rightResult->rows[j];

          context.outerRow = outerRow;
          context.innerRow = innerRow;

          if (!this->joinCondition->Evaluate(context).GetBool())
            continue;

          result->rows.push_back(outerRow->Join(innerRow));
          leftMatched[i] = true;
          rightMatched[j] = true;
        }
      }

      for (int i = 0;i < leftResult->rows.size();i++) {
        if (leftMatched[i])
          continue;

        const auto* outerRow = leftResult->rows[i];

        result->rows.push_back(outerRow->LeftJoin(rightResult->columns));
      }

      for (int i = 0;i < rightResult->rows.size(); i++) {
        if (rightMatched[i])
          continue;

        const auto* innerRow = rightResult->rows[i];

        result->rows.push_back(innerRow->RightJoin(rightResult->columns));
      }


      delete leftResult;
      delete rightResult;

      return result;
  }

}