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
        std::string dbName;
        explicit PhysicalOperator(const std::string& dbName) : dbName(dbName) {}
        PhysicalOperator() = default;
        virtual ~PhysicalOperator() = default;
        virtual PhysicalPlanResult Execute() = 0;
    };

  class PhysicalCreateDatabase final : public PhysicalOperator{
      std::string dbName;
    public:
      explicit PhysicalCreateDatabase(std::string  name);
      ~PhysicalCreateDatabase() override = default;
      PhysicalPlanResult Execute() override;
  };

  class PhysicalTableScan final : public PhysicalOperator{
    std::string tableName;

    public:
      explicit PhysicalTableScan(const std::string& dbName, std::string  tableName);
      ~PhysicalTableScan()override = default;
      PhysicalPlanResult Execute() override;
  };

  class PhysicalProject final : public PhysicalOperator{
    HashSet<column_index_t> columns;
    PhysicalOperator* child;

    public:
      PhysicalProject(const std::string& dbName, PhysicalOperator* child, const std::vector<column_index_t>& columns);
      ~PhysicalProject() override;
      PhysicalPlanResult Execute() override;
  };

  class PhysicalFilter final : public PhysicalOperator{
    Statements::Expression* filter;
    PhysicalOperator* child;

    static bool EvaluateExpression(const Statements::Expression* filter, const DatabaseEngine::StorageTypes::Row &row);

    public:
      PhysicalFilter(const std::string& dbName, PhysicalOperator* child, Statements::Expression* filter);
      ~PhysicalFilter() override;
      PhysicalPlanResult Execute() override;
  };

  class PhysicalInsert final : public PhysicalOperator{
    std::string tableName;
    std::vector<Field> fields;

  public:
    PhysicalInsert(const std::string& dbName, std::string  tableName, const std::vector<Field>& fields);
    ~PhysicalInsert()override = default;
    PhysicalPlanResult Execute() override;
  };

  class PhysicalTableCreate final : public PhysicalOperator{
      std::string name;
      std::vector<Statements::AddColumn> columns;
      std::vector<column_index_t> primaryKey;

    public:
      PhysicalTableCreate(const std::string& dbName, std::string& name, std::vector<Statements::AddColumn>& columns, std::vector<column_index_t>& primaryKey);
      ~PhysicalTableCreate()override = default;
      PhysicalPlanResult Execute() override;
  };
}