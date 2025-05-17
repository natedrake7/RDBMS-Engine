#include "LogicalPlan.h"

namespace QueryPipeline {
 
  LogicalPlan * BuildLogicalPlan(const SelectStatement &statement){
      const auto scanTable = new LogicalTableScan(statement.table);

      LogicalPlan* current = scanTable;

    if (!statement.columns.empty())
      current = new LogicalProject(current, statement.columns);

    return current;
  }

  LogicalPlan * BuildLogicalPlan(const CreateDbStatement &statement){
      return new LogicalCreateDatabase(statement.name);
    }
}

