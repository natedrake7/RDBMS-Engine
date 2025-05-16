#pragma once
#include "../PhysicalOperator.h"


#include <string>
#include <utility>
#include <vector>


namespace QueryPipeline::PhysicalPlan{
  class PhysicalProject : public PhysicalOperator{
    std::vector<std::string> columns;
    PhysicalOperator* child;

    public:
      PhysicalProject(PhysicalOperator* child, std::vector<std::string> columns) : columns(std::move(columns)), child(child) {}
      ~PhysicalProject() override {
          delete child;
      }
      std::vector<DatabaseEngine::StorageTypes::Row> Execute() override;
  };
}
