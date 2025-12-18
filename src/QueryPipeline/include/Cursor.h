#pragma once
#include "PipelineConstants.h"
#include "PhysicalPlan.h"

namespace QueryPipeline {
  class Cursor {
    PipelineConstants::cursor_id_t id;
    DatabaseEngine::ExecutionProperties properties;

    bool canFetchMore;

    PhysicalPlan::ExecutionNode* plan;

  public:
    Cursor(const PipelineConstants::cursor_id_t& cursorId, const DatabaseEngine::ExecutionProperties& properties, PhysicalPlan::ExecutionNode* plan);
    ~Cursor();

    [[nodiscard]] PhysicalPlan::ExecutionResult* fetchNextBatch();
    [[nodiscard]] const bool& canFetch()const;
    [[nodiscard]] const DatabaseEngine::Snapshot& GetSnapshot()const;
    [[nodiscard]] const PipelineConstants::cursor_id_t& GetId()const;
  };
}