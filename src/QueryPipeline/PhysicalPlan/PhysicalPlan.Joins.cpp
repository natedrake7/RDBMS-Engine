#include "PhysicalPlan.h"

namespace QueryPipeline::PhysicalPlan {

  PhysicalNestedLoopJoin::PhysicalNestedLoopJoin(
    PhysicalOperator* left,
    PhysicalOperator* right,
    Expressions::Expression *joinCondition)
    : left(left), right(right), joinCondition(joinCondition){}

  PhysicalNestedLoopJoin::~PhysicalNestedLoopJoin() {
    delete this->left;
    delete this->right;
  }


  PhysicalPlanResult * PhysicalNestedLoopJoin::Execute(const int &batchSize){
    auto* result = new PhysicalPlan::PhysicalPlanResult();

    const auto* leftResult = this->left->Execute(batchSize);
    const auto* rightResult = this->right->Execute(batchSize);

    //create new row
    for (const auto* outerRow: leftResult->rows) {
      for (const auto* innerRow: rightResult->rows) {

        const auto condResult = this->joinCondition->Evaluate(outerRow, innerRow);

        if (!condResult.GetBool())
          continue;

        // outerRow->Join(innerRow);

        result->rows.push_back(std::move(outerRow));
      }
    }

    delete leftResult;
    delete rightResult;

    return result;
  }

}