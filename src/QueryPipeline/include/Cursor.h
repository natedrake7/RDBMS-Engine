#pragma once
#include "Constants.h"
#include "PhysicalPlan.h"

namespace QueryPipeline {
  class Cursor {
    PipelineConstants::cursor_id_t id;
    PhysicalPlan::ExecutionProperties properties;

    bool hasMoreRows;

    PhysicalPlan::ExecutionNode* plan;

  public:
    Cursor(const PipelineConstants::cursor_id_t& cursorId, const PhysicalPlan::ExecutionProperties& properties, PhysicalPlan::ExecutionNode* plan);
    ~Cursor();

    [[nodiscard]] PhysicalPlan::ExecutionResult* fetchNextBatch();
    [[nodiscard]] const bool& hasMore()const;
    [[nodiscard]] const PhysicalPlan::Snapshot& GetSnapshot()const;
    [[nodiscard]] const PipelineConstants::cursor_id_t& GetId()const;
  };
}