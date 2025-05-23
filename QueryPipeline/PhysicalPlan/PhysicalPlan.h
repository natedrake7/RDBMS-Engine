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
        virtual PhysicalPlanResult* Execute() = 0;
    };

  class PhysicalCreateDatabase final : public PhysicalOperator{
      std::string dbName;
    public:
      explicit PhysicalCreateDatabase(std::string  name);
      ~PhysicalCreateDatabase() override = default;
      PhysicalPlanResult* Execute() override;
  };

  class PhysicalSchemaCreate final : public PhysicalOperator{
    std::string dbName;
    std::string schemaName;
    public:
      explicit PhysicalSchemaCreate(const std::string& dbName, std::string& schemaName);
      ~PhysicalSchemaCreate() override = default;
      PhysicalPlanResult* Execute() override;
  };


  class PhysicalTableScan final : public PhysicalOperator{
    Statements::TableName* table;

    public:
      explicit PhysicalTableScan(const std::string& dbName, Statements::TableName* table);
      ~PhysicalTableScan()override = default;
      PhysicalPlanResult* Execute() override;
  };

  class PhysicalIndexScan final : public PhysicalOperator{
    Statements::TableName* table;
    bool isClustered;

  public:
    explicit PhysicalIndexScan(const std::string& dbName, Statements::TableName* table, const bool& isClustered = false);
    ~PhysicalIndexScan()override = default;
    PhysicalPlanResult* Execute() override;
  };

  class PhysicalIndexSeek final : public PhysicalOperator{
    Statements::TableName* table;
    Field minValue;
    Field maxValue;

    public:
      explicit PhysicalIndexSeek(const std::string& dbName, Statements::TableName* table, const Field& minValue, const Field& maxValue);
      ~PhysicalIndexSeek()override = default;
      PhysicalPlanResult* Execute() override;
  };

  class PhysicalProject final : public PhysicalOperator{
    HashSet<column_index_t> columns;
    PhysicalOperator* child;

    public:
      PhysicalProject(const std::string& dbName, PhysicalOperator* child, const std::vector<column_index_t>& columns);
      ~PhysicalProject() override;
      PhysicalPlanResult* Execute() override;
  };

  class PhysicalFilter final : public PhysicalOperator{
    Statements::Expression* filter;
    PhysicalOperator* child;

    static bool EvaluateExpression(const Statements::Expression* filter, const DatabaseEngine::StorageTypes::Row &row);

    public:
      PhysicalFilter(const std::string& dbName, PhysicalOperator* child, Statements::Expression* filter);
      ~PhysicalFilter() override;
      PhysicalPlanResult* Execute() override;
  };

  class PhysicalInsert final : public PhysicalOperator{
    Statements::TableName* table;
    std::vector<Field> fields;

  public:
    PhysicalInsert(const std::string& dbName, Statements::TableName* table, const std::vector<Field>& fields);
    ~PhysicalInsert()override = default;
    PhysicalPlanResult* Execute() override;
  };

  class PhysicalTableCreate final : public PhysicalOperator{
      Statements::TableName*  table;
      std::string constraintName;
      std::vector<Statements::AddColumn> columns;
      std::vector<column_index_t> primaryKey;

    public:
      PhysicalTableCreate(
        const std::string& dbName,
        Statements::TableName*  table,
        std::vector<Statements::AddColumn>& columns,
        std::vector<column_index_t>& primaryKey,
        std::string& constraintName);
      ~PhysicalTableCreate()override = default;
      PhysicalPlanResult* Execute() override;
  };
}