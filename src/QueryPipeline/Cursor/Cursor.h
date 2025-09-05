#pragma once
#include "../Constants.h"
#include "../PhysicalPlan/PhysicalPlan.h"

namespace QueryPipeline {
  class Cursor {
    PipelineConstants::cursor_id_t id;
    int batchSize;
    bool hasMoreRows;
    PhysicalPlan::PhysicalOperator* plan;

  public:
    Cursor(const PipelineConstants::cursor_id_t& cursorId, PhysicalPlan::PhysicalOperator* plan, const int& batchSize = 100);
    ~Cursor();

    [[nodiscard]] PhysicalPlan::PhysicalPlanResult * fetchNextBatch();
    [[nodiscard]] const bool& hasMore()const;
  };
}