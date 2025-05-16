#include "PhysicalProject.h"

namespace QueryPipeline::PhysicalPlan {
  std::vector<DatabaseEngine::StorageTypes::Row> PhysicalProject::Execute(){
    auto rows = child->Execute();

    return rows;    
  }

}
  
