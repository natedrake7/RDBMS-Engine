#pragma once
#include "../Visitor.h"

namespace QueryPipeline {
  class LogicalOperator {
  public:
    virtual ~LogicalOperator() = default;
  };

  LogicalOperator* BuildLogicalPlan(const SelectStatement& selectStatement);
}

