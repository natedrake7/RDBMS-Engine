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

        const auto* joinedRow = outerRow->Join(innerRow);
        result->rows.push_back(joinedRow);
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

        // //TODO if no rows are returned?
        // if (!hasMatched)
        //   outerRow->LeftJoin(rightResult->rows.front());
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

      //create new row
      for (const auto* outerRow: leftResult->rows) {
        for (const auto* innerRow: rightResult->rows) {

          if (!this->joinCondition->Evaluate(outerRow, innerRow).GetBool())
            continue;

          outerRow->Join(innerRow);
          result->rows.push_back(std::move(outerRow));
        }
      }

      delete leftResult;
      delete rightResult;

      return result;
  }

}