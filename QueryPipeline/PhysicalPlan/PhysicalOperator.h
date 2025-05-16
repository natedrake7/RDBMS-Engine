#pragma once
#include <vector>
#include "../../Database/Row/Row.h"

namespace QueryPipeline {
  class LogicalOperator;
}

namespace DatabaseEngine::StorageTypes {
  class Row;
}

namespace QueryPipeline::PhysicalPlan {
  class PhysicalOperator {
    public:
      virtual ~PhysicalOperator() = default;
      virtual std::vector<DatabaseEngine::StorageTypes::Row> Execute() = 0;
  };

  PhysicalOperator* BuildPhysicalPlan(LogicalOperator* logicalOperator);
}
