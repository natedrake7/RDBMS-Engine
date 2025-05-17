#pragma once
#include <string>
#include <vector>
#include "../../Database/Row/Row.h"

namespace QueryPipeline {
  class LogicalPlan;
}

namespace DatabaseEngine::StorageTypes {
  class Row;
}

namespace QueryPipeline::PhysicalPlan{

    class PhysicalOperator {
      public:
        virtual ~PhysicalOperator() = default;
        virtual std::vector<DatabaseEngine::StorageTypes::Row> Execute() = 0;
    };

  class PhysicalCreateDatabase final : public PhysicalOperator{
      std::string dbName;
    public:
      explicit PhysicalCreateDatabase(const std::string& name);
      ~PhysicalCreateDatabase() override = default;
      std::vector<DatabaseEngine::StorageTypes::Row> Execute() override;
  };

  class PhysicalTableScan final : public PhysicalOperator{
    std::string tableName;

    public:
      explicit PhysicalTableScan(const std::string& tableName);
      ~PhysicalTableScan()override = default;
      std::vector<DatabaseEngine::StorageTypes::Row> Execute() override;
  };

  class PhysicalProject final : public PhysicalOperator{
    std::vector<std::string> columns;
    PhysicalOperator* child;

    public:
      PhysicalProject(PhysicalOperator* child, std::vector<std::string> columns);
      ~PhysicalProject() override;
      std::vector<DatabaseEngine::StorageTypes::Row> Execute() override;
  };

}