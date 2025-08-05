#include "PhysicalPlan.h"

namespace QueryPipeline::PhysicalPlan {

  PhysicalNestedLoopJoin::PhysicalNestedLoopJoin(
    const int32_t &databaseId,
    const table_id_t &leftTablePos,
    const table_id_t &rightTablePos,
    Expressions::LogicalExpression *joinCondition)
    : PhysicalOperator(databaseId), leftTablePos(leftTablePos), rightTablePos(rightTablePos), joinCondition(joinCondition){}


  PhysicalPlanResult * PhysicalNestedLoopJoin::Execute(const int &batchSize){
  }

}