#include "../include/Cursor.h"

namespace QueryPipeline {
  Cursor::Cursor(
    const PipelineConstants::cursor_id_t cursorId,
    DatabaseEngine::ExecutionProperties& properties,
    PhysicalPlan::ExecutionNode *plan
  ) : id(cursorId), properties(std::move(properties)), canFetchMore(true), plan(plan) {}

  Cursor::~Cursor(){ delete this->plan; }

   PhysicalPlan::ExecutionResult* Cursor::fetchNextBatch(){
    auto* result = this->plan->Execute(this->properties);

    this->canFetchMore = result != nullptr && result->canFetchMore;

    return result;
  }

  bool Cursor::canFetch() const{ return this->canFetchMore; }

  const DatabaseEngine::Snapshot& Cursor::GetSnapshot() const{ return this->properties.snapshot; }

  PipelineConstants::cursor_id_t Cursor::GetId() const{ return this->id; }

}