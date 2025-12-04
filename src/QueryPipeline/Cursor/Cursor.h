#pragma once
#include "../Constants.h"
#include "../PhysicalPlan/PhysicalPlan.h"

namespace QueryPipeline {
  class Cursor {
    PipelineConstants::cursor_id_t id;
    PhysicalPlan::PhysicalPlanExecutionProperties properties;

    bool hasMoreRows;

    PhysicalPlan::PhysicalOperator* plan;

  public:
    Cursor(const PipelineConstants::cursor_id_t& cursorId, const PhysicalPlan::PhysicalPlanExecutionProperties& properties, PhysicalPlan::PhysicalOperator* plan);
    ~Cursor();

    [[nodiscard]] PhysicalPlan::PhysicalPlanResult* fetchNextBatch();
    [[nodiscard]] const bool& hasMore()const;
    [[nodiscard]] const PhysicalPlan::Snapshot& GetSnapshot()const;
    [[nodiscard]] const PipelineConstants::cursor_id_t& GetId()const;
  };
}