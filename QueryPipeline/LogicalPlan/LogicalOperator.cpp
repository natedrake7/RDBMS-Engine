#include "LogicalOperator.h"
#include "LogicalProject.h"
#include "LogicalTableScan.h"
#include "../PhysicalPlan/PhysicalOperator.h"
#include "../PhysicalPlan/PhysicalProject/PhysicalProject.h"
#include "../PhysicalPlan/PhysicalTableScan/PhysicalTableScan.h"

namespace QueryPipeline {
 
  LogicalOperator * BuildLogicalPlan(const SelectStatement &selectStatement){
      const auto scanTable = new LogicalTableScan(selectStatement.table);

      LogicalOperator* current = scanTable;

    if (!selectStatement.columns.empty())
      current = new LogicalProject(current, selectStatement.columns);

    return current;
  }
}

