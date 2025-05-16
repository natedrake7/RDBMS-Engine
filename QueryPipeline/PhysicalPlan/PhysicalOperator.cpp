#include "PhysicalOperator.h"
#include "PhysicalProject/PhysicalProject.h"
#include "PhysicalTableScan/PhysicalTableScan.h"
#include <stdexcept>
#include "../LogicalPlan/LogicalProject.h"
#include "../LogicalPlan/LogicalTableScan.h"

namespace QueryPipeline::PhysicalPlan {
  PhysicalOperator* BuildPhysicalPlan(LogicalOperator* logicalOperator) {
    if (auto project = dynamic_cast<LogicalProject*>(logicalOperator))
      return new PhysicalProject(BuildPhysicalPlan(project->child), project->columns);

    if (auto scan = dynamic_cast<LogicalTableScan*>(logicalOperator))
      return new PhysicalPlan::PhysicalTableScan(scan->tableName);

    throw std::runtime_error("Unknown logical operator.");
  }
}