#include "Cursor.h"

namespace QueryPipeline {
  Cursor::Cursor(
    const PipelineConstants::cursor_id_t& cursorId,
    const PhysicalPlan::PhysicalPlanExecutionProperties& properties,
    PhysicalPlan::PhysicalOperator *plan
  ) : id(cursorId), properties(properties), hasMoreRows(true), plan(plan) {}

  Cursor::~Cursor(){ delete this->plan; }

   PhysicalPlan::PhysicalPlanResult* Cursor::fetchNextBatch(){
    auto* result = this->plan->Execute(this->properties);

    this->hasMoreRows = result != nullptr && result->rows.size() == properties.batchSize;

    return result;
  }

  const bool & Cursor::hasMore() const{ return this->hasMoreRows; }

  const PhysicalPlan::Snapshot & Cursor::GetSnapshot() const{ return this->properties.snapshot; }

}