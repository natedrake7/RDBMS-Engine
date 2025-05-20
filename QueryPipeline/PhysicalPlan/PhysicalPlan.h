#pragma once
#include "../../AdditionalLibraries/HashSet/HashSet.h"
#include <string>
#include <vector>
#include "../../Database/Row/Row.h"
#include "../Statements/Statements.h"

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
    HashSet<column_index_t> columns;
    PhysicalOperator* child;

    public:
      PhysicalProject(PhysicalOperator* child, const std::vector<column_index_t>& columns);
      ~PhysicalProject() override;
      PhysicalPlanResult Execute() override;
  };

  class PhysicalFilter final : public PhysicalOperator{
    Statements::Expression* filter;
    PhysicalOperator* child;

    static bool EvaluateExpression(const Statements::Expression* filter, const DatabaseEngine::StorageTypes::Row &row);

    public:
      PhysicalFilter(PhysicalOperator* child, Statements::Expression* filter);
      ~PhysicalFilter() override;
      PhysicalPlanResult Execute() override;
  };

  class PhysicalInsert final : public PhysicalOperator{
    std::string tableName;
    std::vector<Field> fields;

  public:
    PhysicalInsert(const std::string& tableName, const std::vector<Field>& fields);
    ~PhysicalInsert()override = default;
    PhysicalPlanResult Execute() override;
  };
}