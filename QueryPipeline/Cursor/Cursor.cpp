#include "Cursor.h"

namespace QueryPipeline {
  Cursor::Cursor(const PipelineConstants::cursor_id_t& cursorId, PhysicalPlan::PhysicalOperator *plan, const int &batchSize)
    : id(cursorId), batchSize(batchSize), hasMoreRows(true), plan(plan) {}

  Cursor::~Cursor(){ delete plan; }

   PhysicalPlan::PhysicalPlanResult* Cursor::fetchNextBatch(){
    auto* result = this->plan->Execute(this->batchSize);

    if (result == nullptr
      || result->rows.size() == 0)
      this->hasMoreRows = false;

    return result;
  }

  const bool & Cursor::hasMore() const{ return this->hasMoreRows; }

}