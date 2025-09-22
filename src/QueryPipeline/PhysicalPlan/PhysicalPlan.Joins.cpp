#include "PhysicalPlan.h"

namespace QueryPipeline::PhysicalPlan {

  PhysicalNestedLoopJoin::PhysicalNestedLoopJoin(
    const table_id_t &leftTablePos,
    const table_id_t &rightTablePos,
    Expressions::Expression *joinCondition)
    : leftTablePos(leftTablePos), rightTablePos(rightTablePos), joinCondition(joinCondition){}


  PhysicalPlanResult * PhysicalNestedLoopJoin::Execute(const int &batchSize){
  }

}