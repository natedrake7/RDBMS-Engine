#pragma once
#include "../../AdditionalLibraries/AdditionalDataTypes/ErrorHandling.h"
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

  struct PhysicalPlanResult {
      std::vector<Headers::ColumnHeader> columns;
      std::vector<DatabaseEngine::StorageTypes::Row> rows;
      std::string message;
      AdditionalDataTypes::ResultCode code;
  };

  struct TableScanState {
    extent_id_t extentId;
    Headers::RowIdentifier lastFetchedRowId;

    TableScanState(){
      this->extentId = 0;
    }
  };

  struct IndexState {
    page_id_t pageId;
    int32_t lastFetchedKeyIndex;

    IndexState() {
      this->pageId = INVALID_PAGE_ID;
      this->lastFetchedKeyIndex = -1;
    }
  };

  class PhysicalOperator {
    public:
      int32_t databaseId;
      explicit PhysicalOperator(const int32_t& databaseId) : databaseId(databaseId) {}
      PhysicalOperator(){
        this->databaseId = -1;
      }
      virtual ~PhysicalOperator() = default;
      virtual PhysicalPlanResult* Execute(const int& batchSize) = 0;
  };

  class PhysicalCreateDatabase final : public PhysicalOperator{
      std::string dbName;
    public:
      explicit PhysicalCreateDatabase(std::string  name);
      ~PhysicalCreateDatabase() override = default;
      PhysicalPlanResult* Execute(const int& batchSize) override;
  };

  class PhysicalSchemaCreate final : public PhysicalOperator{
    std::string schemaName;
    public:
      explicit PhysicalSchemaCreate(const int32_t & databaseId, std::string& schemaName);
      ~PhysicalSchemaCreate() override = default;
      PhysicalPlanResult* Execute(const int& batchSize) override;
  };

  class PhysicalTableScan final : public PhysicalOperator{
    Statements::TableName* table;
    TableScanState state;

    public:
      explicit PhysicalTableScan(const int32_t & databaseId, Statements::TableName* table);
      ~PhysicalTableScan()override = default;
      PhysicalPlanResult* Execute(const int& batchSize) override;
  };

  class PhysicalIndexScan final : public PhysicalOperator{
    Statements::TableName* table;
    Expressions::LogicalExpression* expression;
    IndexState state;
    bool isClustered;

  public:
    explicit PhysicalIndexScan(const int32_t & databaseId, Statements::TableName* table, const bool& isClustered = false);
    explicit PhysicalIndexScan(const int32_t & databaseId, Statements::TableName* table, Expressions::LogicalExpression* expression, const bool& isClustered = false);
    ~PhysicalIndexScan()override = default;
    PhysicalPlanResult* Execute(const int& batchSize) override;
  };

  class PhysicalIndexSeek final : public PhysicalOperator{
    Statements::TableName* table;
    Field minValue;
    Field maxValue;

    public:
      explicit PhysicalIndexSeek(const int32_t & databaseId, Statements::TableName* table, const Field& minValue, const Field& maxValue);
      ~PhysicalIndexSeek()override = default;
      PhysicalPlanResult* Execute(const int& batchSize) override;
  };

  class PhysicalProject final : public PhysicalOperator{
    std::vector<Expressions::Expression*> resultExpressions;
    std::vector<Headers::ColumnHeader> columnHeaders;
    PhysicalOperator* child;

    public:
      PhysicalProject(
        const int32_t & databaseId,
        PhysicalOperator* child,
        std::vector<Expressions::Expression*>& resultExpressions,
        std::vector<Headers::ColumnHeader>& columnHeaders);
      ~PhysicalProject() override;
      PhysicalPlanResult* Execute(const int& batchSize) override;
  };

  class PhysicalFilter final : public PhysicalOperator{
    Expressions::LogicalExpression* filter;
    PhysicalOperator* child;

    public:
      PhysicalFilter(const int32_t & databaseId, PhysicalOperator* child, Expressions::LogicalExpression* filter);
      ~PhysicalFilter() override;
      PhysicalPlanResult* Execute(const int& batchSize) override;
  };

  class PhysicalInsert final : public PhysicalOperator{
    Statements::TableName* table;
    std::vector<Field> fields;

  public:
    PhysicalInsert(const int32_t & databaseId, Statements::TableName* table, const std::vector<Field>& fields);
    ~PhysicalInsert()override = default;
    PhysicalPlanResult* Execute(const int& batchSize) override;
  };

  class PhysicalHeapDelete final : public PhysicalOperator{
    Statements::TableName* table;
    Expressions::LogicalExpression* expression;

  public:
    PhysicalHeapDelete(const int32_t & databaseId, Statements::TableName* table, Expressions::LogicalExpression* expression);
    ~PhysicalHeapDelete()override;
    PhysicalPlanResult* Execute(const int& batchSize) override;
  };

  class PhysicalIndexScanDelete final : public PhysicalOperator{
    Statements::TableName* table;
    Expressions::LogicalExpression* expression;
    IndexState state;

  public:
    PhysicalIndexScanDelete(const int32_t & databaseId, Statements::TableName* table, Expressions::LogicalExpression* expression);
    ~PhysicalIndexScanDelete()override;
    PhysicalPlanResult* Execute(const int& batchSize) override;
  };

  class PhysicalIndexSeekDelete final : public PhysicalOperator{
    Statements::TableName* table;
    Expressions::LogicalExpression* expression;
    IndexState state;

  public:
    PhysicalIndexSeekDelete(const int32_t & databaseId, Statements::TableName* table, Expressions::LogicalExpression* expression);
    ~PhysicalIndexSeekDelete()override;
    PhysicalPlanResult* Execute(const int& batchSize) override;
  };

  class PhysicalHeapUpdate final : public PhysicalOperator{
    Statements::TableName* table;
    Expressions::LogicalExpression* expression;
    std::vector<Field> fields;

  public:
    PhysicalHeapUpdate(const int32_t & databaseId, Statements::TableName* table, Expressions::LogicalExpression* expression, std::vector<Field>& fields);
    ~PhysicalHeapUpdate()override;
    PhysicalPlanResult* Execute(const int& batchSize) override;
  };

  class PhysicalIndexScanUpdate final : public PhysicalOperator{
    Statements::TableName* table;
    Expressions::LogicalExpression* expression;
    std::vector<Field> fields;

  public:
    PhysicalIndexScanUpdate(const int32_t & databaseId, Statements::TableName* table, Expressions::LogicalExpression* expression, std::vector<Field>& fields);
    ~PhysicalIndexScanUpdate()override;
    PhysicalPlanResult* Execute(const int& batchSize) override;
  };

  class PhysicalIndexSeekUpdate final : public PhysicalOperator{
    Statements::TableName* table;
    Expressions::LogicalExpression* expression;
    std::vector<Field> fields;

  public:
    PhysicalIndexSeekUpdate(const int32_t & databaseId, Statements::TableName* table, Expressions::LogicalExpression* expression, std::vector<Field>& fields);
    ~PhysicalIndexSeekUpdate()override;
    PhysicalPlanResult* Execute(const int& batchSize) override;
  };

  class PhysicalTableCreate final : public PhysicalOperator{
      Statements::TableName*  table;
      std::string constraintName;
      std::vector<Statements::AddColumn*> columns;
      Headers::Index primaryKey;

    public:
      PhysicalTableCreate(
        const int32_t & databaseId,
        Statements::TableName*  table,
        std::vector<Statements::AddColumn*>& columns,
        Headers::Index& primaryKey,
        std::string& constraintName);
      ~PhysicalTableCreate()override;
      PhysicalPlanResult* Execute(const int& batchSize) override;
  };

  class PhysicalOrderBy final : public PhysicalOperator{
    PhysicalOperator* child;
    std::vector<column_index_t> columns;
    Constants::OrderType orderType;

  public:
    PhysicalOrderBy(const int32_t & databaseId, PhysicalOperator* child, std::vector<column_index_t>& columns, const Constants::OrderType& orderType);
    ~PhysicalOrderBy()override;
    PhysicalPlanResult* Execute(const int& batchSize) override;
  };

  class PhysicalIndexCreate final : public PhysicalOperator {
    Statements::TableName* table;
    std::string constraintName;
    std::vector<Constants::column_index_t> columns;

    public:
    PhysicalIndexCreate(
        const int32_t & databaseId,
        Statements::TableName*  table,
        std::string& constraintName,
        vector<Constants::column_index_t>& columns);
    PhysicalPlanResult * Execute(const int& batchSize) override;
  };

  class PhysicalAddColumn final : public PhysicalOperator {
    Statements::TableName* table;
    Statements::AddColumn* column;

    public:
    PhysicalAddColumn(const int32_t & databaseId, Statements::TableName* table, Statements::AddColumn* column);
    ~PhysicalAddColumn()override;
    PhysicalPlanResult* Execute(const int& batchSize) override;
  };

  class PhysicalDropColumn final : public PhysicalOperator {
    Statements::TableName* table;
    Statements::DropColumn* column;
    public:
    PhysicalDropColumn(const int32_t & databaseId, Statements::TableName* table, Statements::DropColumn* column);
    ~PhysicalDropColumn()override;
    PhysicalPlanResult* Execute(const int& batchSize) override;
  };

  class PhysicalRenameColumn final : public PhysicalOperator {
    Statements::TableName* table;
    Statements::RenameColumn* column;
    public:
    PhysicalRenameColumn(const int32_t & databaseId, Statements::TableName* table, Statements::RenameColumn* column);
    ~PhysicalRenameColumn()override;
    PhysicalPlanResult* Execute(const int& batchSize) override;
  };

  class PhysicalAlterColumn final : public PhysicalOperator {
    Statements::TableName* table;
    Statements::AlterColumn* column;
    public:
    PhysicalAlterColumn(const int32_t & databaseId, Statements::TableName* table, Statements::AlterColumn* column);
    ~PhysicalAlterColumn()override;
    PhysicalPlanResult* Execute(const int& batchSize) override;
  };

  class PhysicalNestedLoopJoin final : public PhysicalOperator {
    table_id_t leftTablePos;
    table_id_t rightTablePos;
    Expressions::LogicalExpression* joinCondition;

    public:
      PhysicalNestedLoopJoin(
        const int32_t& databaseId,
        const table_id_t& leftTablePos,
        const table_id_t& rightTablePos,
        Expressions::LogicalExpression* joinCondition);
      PhysicalPlanResult* Execute(const int& batchSize) override;
  };

}