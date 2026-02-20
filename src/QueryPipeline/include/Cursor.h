#pragma once
#include "DatabaseConstants.h"
#include "PhysicalPlan.h"
#include "../../DatabaseEngine/include/Contexts/ExecutionContext.h"

namespace QueryPipeline {
  class Cursor {
    PipelineConstants::cursor_id_t id;
    DatabaseEngine::ExecutionContext executionContext;

    bool canFetchMore;

    PhysicalPlan::ExecutionNode* plan;

  public:
    Cursor(
      PipelineConstants::cursor_id_t cursorId,
      DatabaseEngine::ExecutionContext& executionContext,
      PhysicalPlan::ExecutionNode* plan
    );
    ~Cursor();

    [[nodiscard]] PhysicalPlan::ExecutionResult FetchNextBatch();
    [[nodiscard]] bool CanFetch()const;
    [[nodiscard]] const DatabaseEngine::Snapshot& GetSnapshot()const;
    [[nodiscard]] PipelineConstants::cursor_id_t GetId()const;
  };
}
