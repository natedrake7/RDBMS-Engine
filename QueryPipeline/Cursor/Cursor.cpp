#include "Cursor.h"

namespace QueryPipeline {
  Cursor::Cursor(const PipelineConstants::cursor_id_t& cursorId, PhysicalPlan::PhysicalOperator *plan, const int &batchSize)
    : id(cursorId), batchSize(batchSize), hasMoreRows(true), plan(plan) {}

  Cursor::~Cursor(){ delete plan; }

   PhysicalPlan::PhysicalPlanResult* Cursor::fetchNextBatch(){
    auto* result = this->plan->Execute(this->batchSize);

    this->hasMoreRows = result != nullptr && result->rows.size() == batchSize;

    return result;
  }

  const bool & Cursor::hasMore() const{ return this->hasMoreRows; }

}