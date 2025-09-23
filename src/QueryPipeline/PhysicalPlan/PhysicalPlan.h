#pragma once
#include "../../AdditionalLibraries/DataTypes/ErrorHandling.h"
#include "../../AdditionalLibraries/HashSet/HashSet.h"
#include <string>
#include <vector>
#include "../../Database/Row/Row.h"
#include "../QueryResult/QueryResult.h"
#include "../Statements/Statements.h"

namespace QueryPipeline {
  class LogicalPlan;
}

namespace DatabaseEngine::StorageTypes {
  class Row;
}

namespace QueryPipeline::PhysicalPlan{

  struct PhysicalPlanResult {
      std::vector<std::string> columns;
      std::vector<DatabaseEngine::StorageTypes::Row> rows;

      std::vector<QueryResult> results;

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
      explicit PhysicalOperator() = default;
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
    int32_t databaseId;

    public:
      explicit PhysicalSchemaCreate(const int32_t& databaseId, std::string& schemaName);
      ~PhysicalSchemaCreate() override = default;
      PhysicalPlanResult* Execute(const int& batchSize) override;
  };

  class PhysicalTableScan final : public PhysicalOperator{
    Statements::TableName* table;
    TableScanState state;

    public:
      explicit PhysicalTableScan(Statements::TableName* table);
      ~PhysicalTableScan()override = default;
      PhysicalPlanResult* Execute(const int& batchSize) override;
  };

  class PhysicalIndexScan final : public PhysicalOperator{
    Statements::TableName* table;
    Expressions::Expression* expression;
    IndexState state;
    bool isClustered;

  public:
    explicit PhysicalIndexScan(Statements::TableName* table, const bool& isClustered = false);
    explicit PhysicalIndexScan(Statements::TableName* table, Expressions::Expression* expression, const bool& isClustered = false);
    ~PhysicalIndexScan()override = default;
    PhysicalPlanResult* Execute(const int& batchSize) override;
  };

  class PhysicalIndexSeek final : public PhysicalOperator{
    Statements::TableName* table;
    Value minValue;
    Value maxValue;

    public:
      explicit PhysicalIndexSeek(Statements::TableName* table, const Value& minValue, const Value& maxValue);
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
    Expressions::Expression* filter;
    PhysicalOperator* child;

    public:
      PhysicalFilter(PhysicalOperator* child, Expressions::Expression* filter);
      ~PhysicalFilter() override;
      PhysicalPlanResult* Execute(const int& batchSize) override;
  };

  class PhysicalInsert final : public PhysicalOperator{
    Statements::TableName* table;
    std::vector<Statements::InsertColumns> fields;

    PhysicalOperator* child;
    std::vector<column_index_t> columnsIndices;


    void InsertFromChild(DatabaseEngine::StorageTypes::Table* tablePtr, const transaction_id_t& transactionId, const int& batchSize)const;
    void InsertFromFields(DatabaseEngine::StorageTypes::Table* tablePtr, const transaction_id_t& transactionId);
  public:
    PhysicalInsert(
      Statements::TableName* table,
      std::vector<Statements::InsertColumns>& fields,
      PhysicalOperator* child,
      std::vector<column_index_t>& columnsIndices
    );
    ~PhysicalInsert()override;
    PhysicalPlanResult* Execute(const int& batchSize) override;
  };

  class PhysicalHeapDelete final : public PhysicalOperator{
    Statements::TableName* table;
    Expressions::Expression* expression;

  public:
    PhysicalHeapDelete(Statements::TableName* table, Expressions::Expression* expression);
    ~PhysicalHeapDelete()override;
    PhysicalPlanResult* Execute(const int& batchSize) override;
  };

  class PhysicalIndexScanDelete final : public PhysicalOperator{
    Statements::TableName* table;
    Expressions::Expression* expression;
    IndexState state;

  public:
    PhysicalIndexScanDelete(Statements::TableName* table, Expressions::Expression* expression);
    ~PhysicalIndexScanDelete()override;
    PhysicalPlanResult* Execute(const int& batchSize) override;
  };

  class PhysicalIndexSeekDelete final : public PhysicalOperator{
    Statements::TableName* table;
    Expressions::Expression* expression;
    IndexState state;

  public:
    PhysicalIndexSeekDelete(Statements::TableName* table, Expressions::Expression* expression);
    ~PhysicalIndexSeekDelete()override;
    PhysicalPlanResult* Execute(const int& batchSize) override;
  };

  class PhysicalHeapUpdate final : public PhysicalOperator{
    Statements::TableName* table;
    std::vector<Statements::UpdateColumn*>  updates;
    Expressions::Expression* expression;

  public:
    PhysicalHeapUpdate(Statements::TableName* table, Expressions::Expression* expression, std::vector<Statements::UpdateColumn*> & updates);
    ~PhysicalHeapUpdate()override;
    PhysicalPlanResult* Execute(const int& batchSize) override;
  };

  class PhysicalIndexScanUpdate final : public PhysicalOperator{
    Statements::TableName* table;
    std::vector<Statements::UpdateColumn*>  updates;
    Expressions::Expression* expression;

  public:
    PhysicalIndexScanUpdate(Statements::TableName* table, Expressions::Expression* expression, std::vector<Statements::UpdateColumn*> & updates);
    ~PhysicalIndexScanUpdate()override;
    PhysicalPlanResult* Execute(const int& batchSize) override;
  };

  class PhysicalIndexSeekUpdate final : public PhysicalOperator{
    Statements::TableName* table;
    std::vector<Statements::UpdateColumn*>  updates;
    Expressions::Expression* expression;

  public:
    PhysicalIndexSeekUpdate(Statements::TableName* table, Expressions::Expression* expression, std::vector<Statements::UpdateColumn*> & updates);
    ~PhysicalIndexSeekUpdate()override;
    PhysicalPlanResult* Execute(const int& batchSize) override;
  };

  class PhysicalTableCreate final : public PhysicalOperator{
      Statements::TableName*  table;
      std::string constraintName;
      std::vector<Statements::NewColumn*> columns;
      Headers::Index primaryKey;

    public:
      PhysicalTableCreate(
        const int32_t & databaseId,
        Statements::TableName*  table,
        std::vector<Statements::NewColumn*>& columns,
        Headers::Index& primaryKey,
        std::string& constraintName);
      ~PhysicalTableCreate()override;
      PhysicalPlanResult* Execute(const int& batchSize) override;
  };

  class PhysicalOrderBy final : public PhysicalOperator{
    PhysicalOperator* child;
    std::vector<Statements::OrderColumn*> expressions;
  public:
    PhysicalOrderBy(PhysicalOperator* child, std::vector<Statements::OrderColumn*>& expressions);
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
    Statements::NewColumn* column;

    public:
    PhysicalAddColumn(Statements::TableName* table, Statements::NewColumn* column);
    ~PhysicalAddColumn()override;
    PhysicalPlanResult* Execute(const int& batchSize) override;
  };

  class PhysicalDropColumn final : public PhysicalOperator {
    Statements::TableName* table;
    Statements::DropColumn* column;
    public:
    PhysicalDropColumn(Statements::TableName* table, Statements::DropColumn* column);
    ~PhysicalDropColumn()override;
    PhysicalPlanResult* Execute(const int& batchSize) override;
  };

  class PhysicalRenameColumn final : public PhysicalOperator {
    Statements::TableName* table;
    Statements::RenameColumn* column;
    public:
    PhysicalRenameColumn(Statements::TableName* table, Statements::RenameColumn* column);
    ~PhysicalRenameColumn()override;
    PhysicalPlanResult* Execute(const int& batchSize) override;
  };

  class PhysicalAlterColumn final : public PhysicalOperator {
    Statements::TableName* table;
    Statements::AlterColumn* column;
    public:
    PhysicalAlterColumn(Statements::TableName* table, Statements::AlterColumn* column);
    ~PhysicalAlterColumn()override;
    PhysicalPlanResult* Execute(const int& batchSize) override;
  };

  class PhysicalNestedLoopJoin final : public PhysicalOperator {
    table_id_t leftTablePos;
    table_id_t rightTablePos;
    Expressions::Expression* joinCondition;

    public:
      PhysicalNestedLoopJoin(
        const table_id_t& leftTablePos,
        const table_id_t& rightTablePos,
        Expressions::Expression* joinCondition);
      PhysicalPlanResult* Execute(const int& batchSize) override;
  };

}