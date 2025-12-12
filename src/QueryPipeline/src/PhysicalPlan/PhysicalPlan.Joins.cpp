#include "../../include/PhysicalPlan.h"

namespace QueryPipeline::PhysicalPlan {

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
    auto* result = new PhysicalPlan::ExecutionResult();

    const auto* leftResult = this->left->Execute(properties);
    const auto* rightResult = this->right->Execute(properties);

    Expressions::EvaluationContext context(Expressions::EvaluationContext::EvaluationContextType::Join, properties.variables);

    //create new row
    for (const auto* outerRow: leftResult->rows) {
      for (const auto* innerRow: rightResult->rows) {

        context.outerRow = outerRow;
        context.innerRow = innerRow;
        if (!this->joinCondition->Evaluate(context).GetBool())
          continue;

        result->rows.push_back(outerRow->Join(innerRow));
      }
    }

    delete leftResult;
    delete rightResult;

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
      auto* result = new PhysicalPlan::ExecutionResult();

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