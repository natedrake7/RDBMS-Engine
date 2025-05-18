#pragma once
#include <string>
#include <vector>
#include "../../Database/Row/Row.h"
#include "../../Server/Server.h"
#include "../Visitor/Visitor.h"

namespace QueryPipeline {
  class LogicalPlan;
}

namespace DatabaseEngine::StorageTypes {
  class Row;
}

namespace QueryPipeline::PhysicalPlan{

    typedef struct PhysicalPlanResult {
      std::vector<DatabaseEngine::StorageTypes::Row> rows;
      vector<column_index_t> columnIndices;
    }PhysicalPlanResult;

    class PhysicalOperator {
      public:
        virtual ~PhysicalOperator() = default;
        virtual PhysicalPlanResult Execute() = 0;
    };

  class PhysicalCreateDatabase final : public PhysicalOperator{
      std::string dbName;
    public:
      explicit PhysicalCreateDatabase(const std::string& name);
      ~PhysicalCreateDatabase() override = default;
      PhysicalPlanResult Execute() override;
  };

  class PhysicalTableScan final : public PhysicalOperator{
    std::string tableName;

    public:
      explicit PhysicalTableScan(const std::string& tableName);
      ~PhysicalTableScan()override = default;
      PhysicalPlanResult Execute() override;
  };

  class PhysicalProject final : public PhysicalOperator{
    std::vector<std::string> columns;
    PhysicalOperator* child;

    public:
      PhysicalProject(PhysicalOperator* child, std::vector<std::string> columns);
      ~PhysicalProject() override;
      PhysicalPlanResult Execute() override;
  };

  class PhysicalFilter final : public PhysicalOperator{
    Expression* filter;
    PhysicalOperator* child;

    static bool EvaluateExpression(const Expression* filter, const DatabaseEngine::StorageTypes::Row &row);

    public:
      PhysicalFilter(PhysicalOperator* child, Expression* filter);
      ~PhysicalFilter() override;
      PhysicalPlanResult Execute() override;
  };

}