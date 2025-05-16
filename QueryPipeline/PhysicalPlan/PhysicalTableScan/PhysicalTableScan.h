#pragma once
#include "../PhysicalOperator.h"
#include <string>
#include <vector>

namespace DatabaseEngine::StorageTypes {
  class Row;
}

namespace QueryPipeline::PhysicalPlan {

  class PhysicalTableScan : public PhysicalOperator{
    std::string tableName;

  public:
    explicit PhysicalTableScan(const std::string& tableName): tableName(tableName) {}
    ~PhysicalTableScan()override = default;
    std::vector<DatabaseEngine::StorageTypes::Row> Execute() override;
  };

}