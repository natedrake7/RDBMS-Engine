#include "../include/Cursor.h"

namespace QueryPipeline {
  Cursor::Cursor(
    const PipelineConstants::cursor_id_t& cursorId,
    const DatabaseEngine::ExecutionProperties& properties,
    PhysicalPlan::ExecutionNode *plan
  ) : id(cursorId), properties(properties), hasMoreRows(true), plan(plan) {}

  Cursor::~Cursor(){ delete this->plan; }

   PhysicalPlan::ExecutionResult* Cursor::fetchNextBatch(){
    auto* result = this->plan->Execute(this->properties);

    this->hasMoreRows = result != nullptr && result->rows.size() == properties.batchSize;

    return result;
  }

  const bool & Cursor::hasMore() const{ return this->hasMoreRows; }

  const DatabaseEngine::Snapshot& Cursor::GetSnapshot() const{ return this->properties.snapshot; }

  const PipelineConstants::cursor_id_t & Cursor::GetId() const{ return this->id; }

}