#include "../include/Cursor.h"

namespace QueryPipeline {
  Cursor::Cursor(
    const PipelineConstants::cursor_id_t& cursorId,
    const DatabaseEngine::ExecutionProperties& properties,
    PhysicalPlan::ExecutionNode *plan
  ) : id(cursorId), properties(properties), canFetchMore(true), plan(plan) {}

  Cursor::~Cursor(){ delete this->plan; }

   PhysicalPlan::ExecutionResult* Cursor::fetchNextBatch(){
    auto* result = this->plan->Execute(this->properties);

    this->canFetchMore = result != nullptr && result->canFetchMore;

    return result;
  }

  const bool & Cursor::canFetch() const{ return this->canFetchMore; }

  const DatabaseEngine::Snapshot& Cursor::GetSnapshot() const{ return this->properties.snapshot; }

  const PipelineConstants::cursor_id_t & Cursor::GetId() const{ return this->id; }

}