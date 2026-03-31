#include "../include/Cursor.h"

namespace QueryPipeline {
    Cursor::Cursor(
        const PipelineConstants::cursor_id_t cursorId,
        CoreEngine::ExecutionContext& executionContext,
        PhysicalPlan::ExecutionNode *plan
    ) : id(cursorId), executionContext(std::move(executionContext)), canFetchMore(true), plan(plan) {}

    Cursor::~Cursor(){ delete this->plan; }

    PhysicalPlan::ExecutionResult Cursor::FetchNextBatch(){
        auto result = this->plan->Execute(this->executionContext);
        this->canFetchMore = result.canFetchMore;
        return result;
    }

    bool Cursor::CanFetch() const{ return this->canFetchMore; }

    const CoreEngine::ExecutionContext& Cursor::GetExecutionContext()const{ return this->executionContext; }

    const CoreEngine::Snapshot& Cursor::GetSnapshot() const{ return this->executionContext.GetSnapshot(); }

    PipelineConstants::cursor_id_t Cursor::GetId() const{ return this->id; }
}