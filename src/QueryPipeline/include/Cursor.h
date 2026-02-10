#pragma once
#include "DatabaseConstants.h"
#include "PhysicalPlan.h"

namespace QueryPipeline {
  class Cursor {
    PipelineConstants::cursor_id_t id;
    DatabaseEngine::ExecutionProperties properties;

    bool canFetchMore;

    PhysicalPlan::ExecutionNode* plan;

  public:
    Cursor(
      PipelineConstants::cursor_id_t cursorId,
      DatabaseEngine::ExecutionProperties& properties,
      PhysicalPlan::ExecutionNode* plan
    );
    ~Cursor();

    [[nodiscard]] PhysicalPlan::ExecutionResult* fetchNextBatch();
    [[nodiscard]] bool canFetch()const;
    [[nodiscard]] const DatabaseEngine::Snapshot& GetSnapshot()const;
    [[nodiscard]] PipelineConstants::cursor_id_t GetId()const;
  };
}