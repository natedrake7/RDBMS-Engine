#include "PhysicalPlan.h"
#include <utility>
#include "../../Database/Database.h"
#include "../../AdditionalLibraries/StringFunctions/StringFunctions.h"
#include "../../Database/AdditionalFunctions/SortingFunctions.h"
#include "../../Database/Block/Block.h"

namespace QueryPipeline::PhysicalPlan {
  PhysicalCreateDatabase::PhysicalCreateDatabase(std::string name) : dbName(std::move(name)){}

  PhysicalPlanResult* PhysicalCreateDatabase::Execute(){
    auto result = Server::ServerInstance::Get().InsertDbToMasterDb(this->dbName, this->dbName + ".db");

    Server::ServerInstance::Get().InsertSchemaToMasterDb(result.primaryKeyVal, "dbo");
    
    DatabaseEngine::CreateDatabase(this->dbName);

    return new PhysicalPlanResult();
  }

PhysicalSchemaCreate::PhysicalSchemaCreate(const int32_t & databaseId, std::string &schemaName)
  : PhysicalOperator(databaseId), schemaName(std::move(schemaName)) {}

  PhysicalPlanResult * PhysicalSchemaCreate::Execute(){
    Server::ServerInstance::Get().InsertSchemaToMasterDb(this->databaseId, this->schemaName);

    return new PhysicalPlanResult();
  }

  PhysicalProject:: PhysicalProject(const int32_t & databaseId, PhysicalOperator *child, const std::vector<column_index_t>& columns, std::vector<Headers::ColumnHeader>& columnHeaders)
    : PhysicalOperator(databaseId), columns(columns), child(child), columnHeaders(std::move(columnHeaders)) {}

  PhysicalProject::~PhysicalProject(){ delete this->child; }

  PhysicalPlanResult* PhysicalProject::Execute(){
      auto* result = this->child->Execute();

      for (auto& row: result->rows) {
          auto& data = row.GetData();

        vector<DatabaseEngine::StorageTypes::Block*> newData;

        for (int i = 0;i < data.size(); i++) {
          if (!columns.Contains(i)) {
            delete data[i];
            continue;
          }

          newData.push_back(data[i]);
        }

        data = std::move(newData);
      }

    ranges::sort(this->columnHeaders,
      [](const Headers::ColumnHeader& a, const Headers::ColumnHeader& b) {
          return a.ordinalPosition < b.ordinalPosition;
      }
    );

    result->columns = std::move(this->columnHeaders);

    return result;
  }

  PhysicalFilter::PhysicalFilter(const int32_t & databaseId, PhysicalOperator *child, Expressions::Expression* filter)
        : PhysicalOperator(databaseId), filter(filter) , child(child) {}

  PhysicalFilter::~PhysicalFilter(){
    delete this->child;
    delete this->filter;
  }


  PhysicalPlanResult* PhysicalFilter::Execute(){
    auto* result = child->Execute();

    if(dynamic_cast<PhysicalIndexScan*>(child) != nullptr
      || dynamic_cast<PhysicalIndexSeek*>(child) != nullptr)
      return result;

    for (const auto &row : result->rows) {
      
      if (!row.Evaluate(this->filter))
        continue;

      result->rows.emplace_back(row);
    }

    return result;
  }

  PhysicalTableScan::PhysicalTableScan(const int32_t & databaseId, Statements::TableName* table): PhysicalOperator(databaseId), table(table) {}

  PhysicalPlanResult* PhysicalTableScan::Execute(){
      using namespace DatabaseEngine::StorageTypes;
      const DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase(this->databaseId);

      Table* tablePtr = db->OpenTable(this->table->ordinalPosition);

      auto* result = new PhysicalPlanResult();

      tablePtr->HeapScan(&result->rows, -1);

      return result;
    }

  PhysicalIndexScan::PhysicalIndexScan(const int32_t & databaseId, Statements::TableName* table, const bool& isClustered)
    : PhysicalOperator(databaseId), table(table), isClustered(isClustered), expression(nullptr) {}

  PhysicalIndexScan::PhysicalIndexScan(const int32_t & databaseId, Statements::TableName *table, Expressions::Expression *expression, const bool & isClustered)
    : PhysicalOperator(databaseId), table(table), expression(expression), isClustered(isClustered) {}

  PhysicalPlanResult * PhysicalIndexScan::Execute(){
    using namespace DatabaseEngine::StorageTypes;

    auto* result = new PhysicalPlanResult();

    const DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase(this->databaseId);

    Table* tablePtr = db->OpenTable(this->table->ordinalPosition);

    if (this->isClustered) {
      tablePtr->ClusteredIndexScan(&result->rows, this->expression);
      return result;
    }

    tablePtr->NonClusteredIndexScan(&result->rows, 0, this->expression);


    return result;
  }

  PhysicalIndexSeek::PhysicalIndexSeek(const int32_t & databaseId, Statements::TableName* table, const Field& minValue, const Field& maxValue)
    : PhysicalOperator(databaseId), table(table), minValue(minValue), maxValue(maxValue) {}

  PhysicalPlanResult* PhysicalIndexSeek::Execute(){
    using namespace DatabaseEngine::StorageTypes;

    auto* result = new PhysicalPlanResult();

    const DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase(this->databaseId);

    Table* tablePtr = db->OpenTable(this->table->ordinalPosition);

    const Indexing::Key minKey(minValue);
    const Indexing::Key maxKey(maxValue);

    //select if to use clustered or non clustered index here

    tablePtr->ClusteredIndexSeek(&result->rows,&minKey, &maxKey);

    return result;
  }

  PhysicalInsert::PhysicalInsert(const int32_t & databaseId, Statements::TableName* table, const std::vector<Field> &fields)
    : PhysicalOperator(databaseId), table(table), fields(fields) {}

  PhysicalPlanResult* PhysicalInsert::Execute(){
    using namespace DatabaseEngine::StorageTypes;

    auto* result = new PhysicalPlanResult();

    const DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase(this->databaseId);
    
    Table* tablePtr = db->OpenTable(this->table->ordinalPosition);

    const auto insertResult = tablePtr->InsertRow(fields);

    // const auto insertResult = tablePtr->InsertRow(fields);
    //
    // result->code = insertResult.code;
    // result->message = insertResult.message;

    return result;
  }

  PhysicalHeapDelete::PhysicalHeapDelete(const int32_t & databaseId, Statements::TableName *table, Expressions::Expression *expression)
    : PhysicalOperator(databaseId), table(table), expression(expression) {}

  PhysicalHeapDelete::~PhysicalHeapDelete(){
    delete this->expression;
    delete this->table;
  }

  PhysicalPlanResult * PhysicalHeapDelete::Execute(){
    auto* result = new PhysicalPlanResult();

    const DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase(this->databaseId);

    const DatabaseEngine::StorageTypes::Table* tablePtr = db->OpenTable(this->table->ordinalPosition);

    tablePtr->HeapDelete(this->expression);

    return result;
  }

  PhysicalIndexScanDelete::PhysicalIndexScanDelete(const int32_t & databaseId, Statements::TableName *table, Expressions::Expression *expression)
    : PhysicalOperator(databaseId), table(table), expression(expression) {}

  PhysicalIndexScanDelete::~PhysicalIndexScanDelete(){
      delete this->expression;
      delete this->table;
  }

  PhysicalPlanResult * PhysicalIndexScanDelete::Execute(){
    auto* result = new PhysicalPlanResult();

    const DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase(this->databaseId);

    DatabaseEngine::StorageTypes::Table* tablePtr = db->OpenTable(this->table->ordinalPosition);

    tablePtr->ClusteredIndexScanDelete(this->expression);

    return result;
  }

  PhysicalIndexSeekDelete::PhysicalIndexSeekDelete(const int32_t & databaseId, Statements::TableName *table, Expressions::Expression *expression)
    : PhysicalOperator(databaseId), table(table), expression(expression) {}

  PhysicalIndexSeekDelete::~PhysicalIndexSeekDelete(){
    delete this->expression;
    delete this->table;
  }

  PhysicalPlanResult * PhysicalIndexSeekDelete::Execute(){
    auto* result = new PhysicalPlanResult();

    const DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase(this->databaseId);

    DatabaseEngine::StorageTypes::Table* tablePtr = db->OpenTable(this->table->ordinalPosition);

    tablePtr->ClusteredIndexSeekDelete(expression);

    return result;
  }

  PhysicalTableCreate::PhysicalTableCreate(
      const int32_t & databaseId,
      Statements::TableName*  table,
      std::vector<Statements::AddColumn*> &columns,
      Headers::Index& primaryKey,
      std::string& constraintName)
    : PhysicalOperator(databaseId), table(table), constraintName(std::move(constraintName)),
      columns(std::move(columns)), primaryKey(std::move(primaryKey)) {}

  PhysicalTableCreate::~PhysicalTableCreate(){
    for (const auto& column: this->columns)
      delete column;
  }

  PhysicalPlanResult* PhysicalTableCreate::Execute(){
    DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase(this->databaseId);

    vector<DatabaseEngine::StorageTypes::Column*> columnsPtrs;
    columnsPtrs.reserve(columns.size());
    
    for (const auto& column: this->columns)
      columnsPtrs.push_back(new DatabaseEngine::StorageTypes::Column(
        column->name.name,
        ColumnTypesDictionary.Get(column->type.name),
        column->type.size,
        column->index,
        column->isNullable
        ));

    const auto& tables = Server::ServerInstance::Get().SelectTables(this->databaseId);

    const auto schemas = Server::ServerInstance::Get().SelectSchemas(this->databaseId);

    const int16_t& index = tables.empty() ? 0 : tables[tables.size() - 1].ordinalPosition + 1;


    const auto tableResult = Server::ServerInstance::Get().InsertTableToMasterDb(
        this->databaseId,
        this->table->schemaId,
        this->table->name,
        index);

    auto* tablePtr = db->CreateTable(tableResult.primaryKeyVal, index, columnsPtrs, &this->primaryKey);

    Dictionary<int, int32_t> columnIdsDict;

    for (const auto& column: this->columns) {
      const auto columnResult =
          Server::ServerInstance::Get().InsertColumnToMasterDb(
            tableResult.primaryKeyVal,
            column->name.name,
            ColumnTypesDictionary.Get(AdditionalLibraries::NormalizeString(column->type.name)),
            column->type.size,
            column->isNullable,
            column->index
            );

      columnIdsDict.Add(column->index, columnResult.primaryKeyVal);

      //insert identity columns
      if (column->autoIncrementKey == nullptr)
        continue;

      Server::ServerInstance::Get().InsertIdentityColumnToMasterDb(
          tableResult.primaryKeyVal,
          columnResult.primaryKeyVal,
          column->autoIncrementKey->seed,
          column->autoIncrementKey->incrementFactor,
          column->autoIncrementKey->seed,
          true,
          column->autoIncrementKey->cacheBlock);
    }

    const bool isConstraintEmpty = this->constraintName.empty();

    std::vector<int32_t> primaryKeyColumnIds;

    for (const auto& column: this->primaryKey.columns) {
      if (isConstraintEmpty)
        this->constraintName += this->constraintName.empty() ? "PK_" + this->columns[column]->name.name :"_" + this->columns[column]->name.name;

      primaryKeyColumnIds.push_back(columnIdsDict.Get(column));
    }

    if (primaryKeyColumnIds.empty())
        return nullptr;

    const auto indexResult = Server::ServerInstance::Get().InsertIndexToMasterDb(
        tableResult.primaryKeyVal,
        this->constraintName,
        true,
        false);

    const auto indexId = static_cast<int32_t>(indexResult.primaryKeyVal);

    const auto constraintResult = Server::ServerInstance::Get().InsertConstraintToMasterDb(
      tableResult.primaryKeyVal,
      this->constraintName,
      Headers::ConstraintType::PrimaryKey,
      false,
      &indexId);

    for(int i = 0;i < primaryKeyColumnIds.size(); i++){
      Server::ServerInstance::Get().InsertIndexColumnToMasterDb(
        indexResult.primaryKeyVal,
        primaryKeyColumnIds[i],
        this->primaryKey.columns[i],
        true);

      Server::ServerInstance::Get().InsertConstraintColumnToMasterDb(
        constraintResult.primaryKeyVal,
        primaryKeyColumnIds[i],
    this->primaryKey.columns[i]);
    }

    tablePtr->GetColumnsHeaders();
    tablePtr->GetIdentityColumns();

    return nullptr;
  }

  PhysicalHeapUpdate::PhysicalHeapUpdate(const int32_t & databaseId, Statements::TableName *table, Expressions::Expression *expression, vector<Field> & fields)
  : PhysicalOperator(databaseId), table(table), expression(expression), fields(std::move(fields)) {}

  PhysicalHeapUpdate::~PhysicalHeapUpdate(){
    delete this->expression;
    delete this->table;
  }

  PhysicalPlanResult* PhysicalHeapUpdate::Execute(){
    using namespace DatabaseEngine::StorageTypes;

    auto* result = new PhysicalPlanResult();

    const DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase(this->databaseId);

    Table* tablePtr = db->OpenTable(this->table->ordinalPosition);

    tablePtr->HeapUpdate(this->expression, this->fields);

    return result;
  }

  PhysicalIndexScanUpdate::PhysicalIndexScanUpdate(const int32_t & databaseId, Statements::TableName *table, Expressions::Expression *expression, vector<Field> & fields)
  : PhysicalOperator(databaseId), table(table), expression(expression), fields(std::move(fields)) {}

  PhysicalIndexScanUpdate::~PhysicalIndexScanUpdate(){
    delete this->expression;
    delete this->table;
  }

  PhysicalPlanResult* PhysicalIndexScanUpdate::Execute(){
    using namespace DatabaseEngine::StorageTypes;

    auto* result = new PhysicalPlanResult();

    const DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase(this->databaseId);

    Table* tablePtr = db->OpenTable(this->table->ordinalPosition);

    tablePtr->ClusteredIndexScanUpdate(this->expression, this->fields);

    return result;
  }

  PhysicalIndexSeekUpdate::PhysicalIndexSeekUpdate(const int32_t & databaseId, Statements::TableName *table, Expressions::Expression *expression, vector<Field> & fields)
    : PhysicalOperator(databaseId), table(table), expression(expression), fields(std::move(fields)) {}

  PhysicalIndexSeekUpdate::~PhysicalIndexSeekUpdate(){
    delete this->expression;
    delete this->table;
  }

  PhysicalPlanResult* PhysicalIndexSeekUpdate::Execute(){
    using namespace DatabaseEngine::StorageTypes;

    auto* result = new PhysicalPlanResult();

    const DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase(this->databaseId);

    Table* tablePtr = db->OpenTable(this->table->ordinalPosition);

    Indexing::Key key;

    tablePtr->ClusteredIndexScanUpdate(this->expression, this->fields);

    // tablePtr->ClusteredIndexSeekUpdate(this->expression, &key, &key, this->fields);

    return result;
  }

  PhysicalOrderBy::PhysicalOrderBy(
      const int32_t & databaseId,
      PhysicalOperator *child,
      vector<column_index_t> & columns,
      const OrderType & orderType)
    : PhysicalOperator(databaseId), child(child), columns(std::move(columns)), orderType(orderType){}

  PhysicalOrderBy::~PhysicalOrderBy(){
    delete this->child;
  }

  PhysicalPlanResult* PhysicalOrderBy::Execute(){
    auto* result = this->child->Execute();

    std::vector<DatabaseEngine::StorageTypes::Row*> rowsPtrs;

    std::vector<SortCondition> conditions;
    for(const auto& column : this->columns){
      conditions.emplace_back(
      column,
      this->orderType,
    false
      );
    }

    SortingFunctions::OrderBy(result->rows, conditions);

    return result;
  }

  PhysicalIndexCreate::PhysicalIndexCreate(
    const int32_t &databaseId,
    Statements::TableName *table,
    std::string &constraintName,
    vector<Constants::column_index_t> &columns)
    : PhysicalOperator(databaseId), table(table), constraintName(std::move(constraintName)), columns(std::move(columns)) {}

  PhysicalPlanResult * PhysicalIndexCreate::Execute(){
    auto* result = new PhysicalPlanResult();

    const auto* db = Server::ServerInstance::Get().UseDatabase(this->databaseId);

    auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

    const auto columnsHeaders = Server::ServerInstance::Get().SelectColumns(this->table->tableId);

    const auto indexResult = Server::ServerInstance::Get().InsertIndexToMasterDb(
        this->table->tableId,
        this->constraintName,
        false,
        false);

    const auto indexId = static_cast<int32_t>(indexResult.primaryKeyVal);

    const auto constraintResult = Server::ServerInstance::Get().InsertConstraintToMasterDb(
      this->table->tableId,
      this->constraintName,
      Headers::ConstraintType::IndexKey,
      false,
      &indexId);

    for (const auto& columnPos : this->columns) {
      const auto& header = columnsHeaders.at(columnPos);

      const auto indexColumnResult =
        Server::ServerInstance::Get().InsertIndexColumnToMasterDb(
            indexResult.primaryKeyVal,
            header.id,
            columnPos,
            true);

      const auto constraintColumnResult =
        Server::ServerInstance::Get().InsertConstraintColumnToMasterDb(
            constraintResult.primaryKeyVal,
            header.id,
        columnPos);
    }

    const auto indexPos = tablePtr->CreateNonClusteredIndex(this->columns);

    tablePtr->NonClusteredIndexInsertExistingRows(indexPos);

    //if there are rows in the table update the index
    //do stuff here

    return result;
  }
}