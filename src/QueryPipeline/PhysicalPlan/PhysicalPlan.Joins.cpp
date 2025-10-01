#include "PhysicalPlan.h"

namespace QueryPipeline::PhysicalPlan {

  PhysicalNestedLoopInnerJoin::PhysicalNestedLoopInnerJoin(
    PhysicalOperator* left,
    PhysicalOperator* right,
    Expressions::Expression *joinCondition)
    : left(left), right(right), joinCondition(joinCondition){}

  PhysicalNestedLoopInnerJoin::~PhysicalNestedLoopInnerJoin() {
    delete this->left;
    delete this->right;
  }


  PhysicalPlanResult * PhysicalNestedLoopInnerJoin::Execute(const int &batchSize){
    auto* result = new PhysicalPlan::PhysicalPlanResult();

    const auto* leftResult = this->left->Execute(batchSize);
    const auto* rightResult = this->right->Execute(batchSize);

    //create new row
    for (const auto* outerRow: leftResult->rows) {
      for (const auto* innerRow: rightResult->rows) {

        if (!this->joinCondition->Evaluate(outerRow, innerRow).GetBool())
          continue;

        result->rows.push_back(outerRow->Join(innerRow));
      }
    }

    delete leftResult;
    delete rightResult;

    return result;
  }

  PhysicalNestedLoopLeftJoin::PhysicalNestedLoopLeftJoin(
    PhysicalOperator* left,
    PhysicalOperator* right,
    Expressions::Expression *joinCondition)
    : left(left), right(right), joinCondition(joinCondition){}

  PhysicalNestedLoopLeftJoin::~PhysicalNestedLoopLeftJoin() {
      delete this->left;
      delete this->right;
  }

  PhysicalPlanResult * PhysicalNestedLoopLeftJoin::Execute(const int &batchSize){
      auto* result = new PhysicalPlan::PhysicalPlanResult();

      const auto* leftResult = this->left->Execute(batchSize);
      const auto* rightResult = this->right->Execute(batchSize);

      //create new row
      for (const auto* outerRow: leftResult->rows) {

        bool hasMatched = false;
        for (const auto* innerRow: rightResult->rows) {

          if (!this->joinCondition->Evaluate(outerRow, innerRow).GetBool())
            continue;

          const auto* joinedRow = outerRow->Join(innerRow);

          result->rows.push_back(joinedRow);

          hasMatched = true;
        }

        if (!hasMatched)
          result->rows.push_back(outerRow->LeftJoin(rightResult->columns));
      }

      delete leftResult;
      delete rightResult;

      return result;
  }

  PhysicalNestedLoopFullJoin::PhysicalNestedLoopFullJoin(
    PhysicalOperator* left,
    PhysicalOperator* right,
    Expressions::Expression *joinCondition)
    : left(left), right(right), joinCondition(joinCondition){}

  PhysicalNestedLoopFullJoin::~PhysicalNestedLoopFullJoin() {
      delete this->left;
      delete this->right;
  }

  PhysicalPlanResult * PhysicalNestedLoopFullJoin::Execute(const int &batchSize){
      auto* result = new PhysicalPlan::PhysicalPlanResult();

      const auto* leftResult = this->left->Execute(batchSize);
      const auto* rightResult = this->right->Execute(batchSize);


      std::vector<bool> leftMatched(leftResult->rows.size(), false);
      std::vector<bool> rightMatched(rightResult->rows.size(), false);

      for (int i = 0;i < leftResult->rows.size();i++) {
        for (int j = 0;j < rightResult->rows.size();j++) {
          const auto* outerRow = leftResult->rows[i];
          const auto* innerRow = rightResult->rows[j];

          if (!this->joinCondition->Evaluate(outerRow, innerRow).GetBool())
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