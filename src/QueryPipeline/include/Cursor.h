#pragma once
#include "DatabaseConstants.h"
#include "PhysicalPlan.h"
#include "../../CoreEngine/include/Contexts/ExecutionContext.h"

namespace QueryPipeline {
  class Cursor {
    PipelineConstants::cursor_id_t id;
    CoreEngine::ExecutionContext executionContext;

    bool canFetchMore;

    PhysicalPlan::ExecutionNode* plan;

  public:
    Cursor(
      PipelineConstants::cursor_id_t cursorId,
      CoreEngine::ExecutionContext& executionContext,
      PhysicalPlan::ExecutionNode* plan
    );
    ~Cursor();

    [[nodiscard]] PhysicalPlan::ExecutionResult FetchNextBatch();
    [[nodiscard]] bool CanFetch()const;
    [[nodiscard]] const CoreEngine::ExecutionContext& GetExecutionContext()const;
    [[nodiscard]] const CoreEngine::Snapshot& GetSnapshot()const;
    [[nodiscard]] PipelineConstants::cursor_id_t GetId()const;
  };
}
