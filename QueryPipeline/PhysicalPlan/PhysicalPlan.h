#pragma once
#include "../../AdditionalLibraries/AdditionalDataTypes/ErrorHandling.h"
#include "../../AdditionalLibraries/HashSet/HashSet.h"
#include <string>
#include <utility>
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

    struct PhysicalPlanResult {
        std::vector<string> columns;
        std::vector<DatabaseEngine::StorageTypes::Row> rows;
        std::string message;
        AdditionalDataTypes::ResultCode code;
    };

    class PhysicalOperator {
      public:
        int32_t databaseId;
        explicit PhysicalOperator(const int32_t& databaseId) : databaseId(databaseId) {}
        PhysicalOperator(){
          this->databaseId = -1;
        }
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
    std::string schemaName;
    public:
      explicit PhysicalSchemaCreate(const int32_t & databaseId, std::string& schemaName);
      ~PhysicalSchemaCreate() override = default;
      PhysicalPlanResult* Execute() override;
  };

  class PhysicalTableScan final : public PhysicalOperator{
    Statements::TableName* table;

    public:
      explicit PhysicalTableScan(const int32_t & databaseId, Statements::TableName* table);
      ~PhysicalTableScan()override = default;
      PhysicalPlanResult* Execute() override;
  };

  class PhysicalIndexScan final : public PhysicalOperator{
    Statements::TableName* table;
    Expressions::Expression* expression;
    bool isClustered;

  public:
    explicit PhysicalIndexScan(const int32_t & databaseId, Statements::TableName* table, const bool& isClustered = false);
    explicit PhysicalIndexScan(const int32_t & databaseId, Statements::TableName* table, Expressions::Expression* expression, const bool& isClustered = false);
    ~PhysicalIndexScan()override = default;
    PhysicalPlanResult* Execute() override;
  };

  class PhysicalIndexSeek final : public PhysicalOperator{
    Statements::TableName* table;
    Field minValue;
    Field maxValue;

    public:
      explicit PhysicalIndexSeek(const int32_t & databaseId, Statements::TableName* table, const Field& minValue, const Field& maxValue);
      ~PhysicalIndexSeek()override = default;
      PhysicalPlanResult* Execute() override;
  };

  class PhysicalProject final : public PhysicalOperator{
    HashSet<column_index_t> columns;
    std::vector<std::string> columnLiterals;
    PhysicalOperator* child;

    public:
      PhysicalProject(const int32_t & databaseId, PhysicalOperator* child, const std::vector<column_index_t>& columns, std::vector<std::string>& columnLiterals);
      ~PhysicalProject() override;
      PhysicalPlanResult* Execute() override;
  };

  class PhysicalFilter final : public PhysicalOperator{
    Expressions::Expression* filter;
    PhysicalOperator* child;

    public:
      PhysicalFilter(const int32_t & databaseId, PhysicalOperator* child, Expressions::Expression* filter);
      ~PhysicalFilter() override;
      PhysicalPlanResult* Execute() override;
  };

  class PhysicalInsert final : public PhysicalOperator{
    Statements::TableName* table;
    std::vector<Field> fields;

  public:
    PhysicalInsert(const int32_t & databaseId, Statements::TableName* table, const std::vector<Field>& fields);
    ~PhysicalInsert()override = default;
    PhysicalPlanResult* Execute() override;
  };

  class PhysicalHeapDelete final : public PhysicalOperator{
    Statements::TableName* table;
    Expressions::Expression* expression;

  public:
    PhysicalHeapDelete(const int32_t & databaseId, Statements::TableName* table, Expressions::Expression* expression);
    ~PhysicalHeapDelete()override;
    PhysicalPlanResult* Execute() override;
  };

  class PhysicalIndexScanDelete final : public PhysicalOperator{
    Statements::TableName* table;
    Expressions::Expression* expression;

  public:
    PhysicalIndexScanDelete(const int32_t & databaseId, Statements::TableName* table, Expressions::Expression* expression);
    ~PhysicalIndexScanDelete()override;
    PhysicalPlanResult* Execute() override;
  };

  class PhysicalIndexSeekDelete final : public PhysicalOperator{
    Statements::TableName* table;
    Expressions::Expression* expression;

  public:
    PhysicalIndexSeekDelete(const int32_t & databaseId, Statements::TableName* table, Expressions::Expression* expression);
    ~PhysicalIndexSeekDelete()override;
    PhysicalPlanResult* Execute() override;
  };

  class PhysicalHeapUpdate final : public PhysicalOperator{
    Statements::TableName* table;
    Expressions::Expression* expression;
    std::vector<Field> fields;

  public:
    PhysicalHeapUpdate(const int32_t & databaseId, Statements::TableName* table, Expressions::Expression* expression, std::vector<Field>& fields);
    ~PhysicalHeapUpdate()override;
    PhysicalPlanResult* Execute() override;
  };

  class PhysicalIndexScanUpdate final : public PhysicalOperator{
    Statements::TableName* table;
    Expressions::Expression* expression;
    std::vector<Field> fields;

  public:
    PhysicalIndexScanUpdate(const int32_t & databaseId, Statements::TableName* table, Expressions::Expression* expression, std::vector<Field>& fields);
    ~PhysicalIndexScanUpdate()override;
    PhysicalPlanResult* Execute() override;
  };

  class PhysicalIndexSeekUpdate final : public PhysicalOperator{
    Statements::TableName* table;
    Expressions::Expression* expression;
    std::vector<Field> fields;

  public:
    PhysicalIndexSeekUpdate(const int32_t & databaseId, Statements::TableName* table, Expressions::Expression* expression, std::vector<Field>& fields);
    ~PhysicalIndexSeekUpdate()override;
    PhysicalPlanResult* Execute() override;
  };

  class PhysicalTableCreate final : public PhysicalOperator{
      Statements::TableName*  table;
      std::string constraintName;
      std::vector<Statements::AddColumn> columns;
      Headers::Index primaryKey;

    public:
      PhysicalTableCreate(
        const int32_t & databaseId,
        Statements::TableName*  table,
        std::vector<Statements::AddColumn>& columns,
        Headers::Index& primaryKey,
        std::string& constraintName);
      ~PhysicalTableCreate()override = default;
      PhysicalPlanResult* Execute() override;
  };

  class PhysicalOrderBy final : public PhysicalOperator{
    PhysicalOperator* child;
    std::vector<column_index_t> columns;
    Constants::OrderType orderType;

  public:
    PhysicalOrderBy(const int32_t & databaseId, PhysicalOperator* child, std::vector<column_index_t>& columns, const Constants::OrderType& orderType);
    ~PhysicalOrderBy()override;
    PhysicalPlanResult* Execute() override;
  };
}