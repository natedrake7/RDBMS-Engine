#include "PhysicalPlan.h"
#include <utility>
#include "../../Database/Database.h"
#include "../../Server/Server.h"
#include "../../AdditionalLibraries/Functions/StringFunctions.h"
#include "../../Database/AdditionalFunctions/SortingFunctions.h"
#include "../../Database/Block/Block.h"

namespace QueryPipeline::PhysicalPlan {

PhysicalPlanResult::PhysicalPlanResult(){
  this->code = AdditionalDataTypes::ResultCode::Ok;
}

PhysicalPlanResult::~PhysicalPlanResult(){
  for (const auto* row: this->rows)
    if (row->IsCopy())
      delete row;
}

PhysicalCreateDatabase::PhysicalCreateDatabase(std::string name) : dbName(std::move(name)){}

  PhysicalPlanResult* PhysicalCreateDatabase::Execute(const int& batchSize){
    const auto result = Server::ServerInstance::Get().InsertDbToMasterDb(this->dbName, this->dbName + ".db");

    Server::ServerInstance::Get().InsertSchemaToMasterDb(result.primaryKeyVal, "dbo");
    
    DatabaseEngine::CreateDatabase(this->dbName);

    return new PhysicalPlanResult();
  }

PhysicalSchemaCreate::PhysicalSchemaCreate(const int32_t& databaseId, std::string &schemaName)
  : schemaName(std::move(schemaName)) ,databaseId(databaseId) {}

  PhysicalPlanResult * PhysicalSchemaCreate::Execute(const int& batchSize){
    Server::ServerInstance::Get().InsertSchemaToMasterDb(this->databaseId, this->schemaName);

    return new PhysicalPlanResult();
  }

  PhysicalProject:: PhysicalProject(
    const int32_t & databaseId,
    PhysicalOperator *child,
    std::vector<Expressions::Expression*>& resultExpressions,
    std::vector<Headers::ColumnHeader>& columnHeaders)
    : resultExpressions(std::move(resultExpressions)), columnHeaders(std::move(columnHeaders)), child(child) {}

  PhysicalProject::~PhysicalProject() {
    for (const auto& expression : this->resultExpressions)
      delete expression;

    delete this->child;
  }

  PhysicalPlanResult* PhysicalProject::Execute(const int& batchSize){
      if (this->child == nullptr) {
        auto* result = new PhysicalPlanResult();

        QueryResult resultRow;

        for (const auto& expression : this->resultExpressions) {
          result->displayColumnNames.emplace_back(expression->name);

          auto field = expression->Evaluate(nullptr);
          resultRow.AddColumn(field);
        }

        result->results.push_back(std::move(resultRow));

        return result;
      }

      auto* result = this->child->Execute(batchSize);

      for (const auto& expression : this->resultExpressions)
        result->displayColumnNames.emplace_back(expression->name);

      for (const auto* row: result->rows) {
          QueryResult resultRow;

          for (const auto& expression : this->resultExpressions) {
            auto field = expression->Evaluate(row);
            resultRow.AddColumn(field);
          }

          result->results.push_back(std::move(resultRow));
      }

      ranges::sort(this->columnHeaders,
        [](const Headers::ColumnHeader& a, const Headers::ColumnHeader& b) {
            return a.ordinalPosition < b.ordinalPosition;
        }
      );

      return result;
  }

  PhysicalFilter::PhysicalFilter(PhysicalOperator *child, Expressions::Expression* filter)
        : filter(filter) , child(child) {}

  PhysicalFilter::~PhysicalFilter(){
    delete this->child;
    delete this->filter;
  }


  PhysicalPlanResult* PhysicalFilter::Execute(const int& batchSize){
    auto* result = child->Execute(batchSize);

    if(dynamic_cast<PhysicalIndexScan*>(child) != nullptr
      || dynamic_cast<PhysicalIndexSeek*>(child) != nullptr)
      return result;

    std::vector<const DatabaseEngine::StorageTypes::Row*> filteredRows;
    for (const auto* row : result->rows) {

      if (!this->filter->Evaluate(row).GetBool())
        continue;

      filteredRows.push_back(row);
    }

    result->rows = std::move(filteredRows);
    return result;
  }

  PhysicalTop::PhysicalTop(PhysicalOperator* child, int64_t& top)
    : top(std::move(top)), child(child){}

  PhysicalTop::~PhysicalTop(){
    delete this->child;
  }

  PhysicalPlanResult * PhysicalTop::Execute(const int &batchSize){
    auto* result = this->child->Execute(batchSize);

    if (this->top > result->results.size())
      return result;

    result->results.erase(result->results.begin() + this->top, result->results.end());

    return result;
  }

  PhysicalDistinct::PhysicalDistinct(PhysicalOperator *child)
    : child(child){}

  PhysicalDistinct::~PhysicalDistinct(){
    delete this->child;
  }

  PhysicalPlanResult * PhysicalDistinct::Execute(const int &batchSize){
    auto* result = this->child->Execute(batchSize);

    std::vector<QueryResult> results;

    HashSet<int64_t> computedHashes;

    for (auto& row : result->results) {

      const auto hash = row.ComputeHash();

      //if no collision occurs
      if (!computedHashes.Contains(hash)) {
        results.push_back(std::move(row));
        computedHashes.Add(hash);
        continue;
      }

      bool isDuplicate = false;
      for (const auto& distinctRow : results) {
        if (distinctRow == row) {
          isDuplicate = true;
          break;
        }
      }

      if (!isDuplicate)
        results.push_back(std::move(row));
    }

    result->results = std::move(results);

    return result;
  }

  PhysicalTableScan::PhysicalTableScan(Statements::TableName* table): table(table) {}

  PhysicalPlanResult* PhysicalTableScan::Execute(const int& batchSize){
      using namespace DatabaseEngine::StorageTypes;
      const DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase(this->table->databaseId);

      const auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

      auto* result = new PhysicalPlanResult();

      result->columns = tablePtr->GetConstantColumns();

      tablePtr->HeapScan(&result->rows, this->state, batchSize);

      return result;
    }

  PhysicalIndexScan::PhysicalIndexScan(Statements::TableName* table, const bool& isClustered)
    : table(table), expression(nullptr), isClustered(isClustered) {}

  PhysicalIndexScan::PhysicalIndexScan(Statements::TableName *table, Expressions::Expression *expression, const bool & isClustered)
    : table(table), expression(expression), isClustered(isClustered) {}

  PhysicalPlanResult * PhysicalIndexScan::Execute(const int& batchSize){
    using namespace DatabaseEngine::StorageTypes;

    auto* result = new PhysicalPlanResult();

    const DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase(this->table->databaseId);

    Table* tablePtr = db->OpenTable(this->table->ordinalPosition);

    result->columns = tablePtr->GetConstantColumns();

    if (this->isClustered) {
      tablePtr->ClusteredIndexScan(&result->rows, state, batchSize, this->expression);
      return result;
    }

    tablePtr->NonClusteredIndexScan(&result->rows, 0, state, batchSize, this->expression);

    return result;
  }

  PhysicalIndexSeek::PhysicalIndexSeek(Statements::TableName* table, const Value& minValue, const Value& maxValue)
    : table(table), minValue(minValue), maxValue(maxValue) {}

  PhysicalPlanResult* PhysicalIndexSeek::Execute(const int& batchSize){
    using namespace DatabaseEngine::StorageTypes;

    auto* result = new PhysicalPlanResult();

    const DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase(this->table->databaseId);

    Table* tablePtr = db->OpenTable(this->table->ordinalPosition);

    result->columns = tablePtr->GetConstantColumns();
    const Indexing::Key minKey(minValue);
    const Indexing::Key maxKey(maxValue);

    //select if to use clustered or non clustered index here

    tablePtr->ClusteredIndexSeek(&result->rows,&minKey, &maxKey);

    return result;
  }

  PhysicalPlanResult* PhysicalInsert::InsertFromChild(DatabaseEngine::StorageTypes::Table* tablePtr, const transaction_id_t& transactionId, const int& batchSize)const{
    auto* result = this->child->Execute(batchSize);

    for (auto& row : result->results) {
      const auto insertResult = tablePtr->InsertRow(transactionId, row.GetData(), this->columnsIndices);

      if (insertResult.code != AdditionalDataTypes::ResultCode::Ok) {
        result->message = insertResult.message;
        result->code = insertResult.code;
        return result;
      }
    }

    result->message = "Rows inserted: " + std::to_string(result->results.size());
    result->code = AdditionalDataTypes::ResultCode::Ok;
    return result;
  }

  PhysicalPlanResult* PhysicalInsert::InsertFromFields(DatabaseEngine::StorageTypes::Table* tablePtr, const transaction_id_t& transactionId){
    auto* result = new PhysicalPlanResult();

    for (const auto&[columns] : this->fields) {
      const auto insertResult = tablePtr->InsertRow(transactionId, columns, this->columnsIndices);

      if (insertResult.code != AdditionalDataTypes::ResultCode::Ok) {
        result->message = insertResult.message;
        result->code = insertResult.code;
        return result;
      }
    }

    result->message = "Rows inserted: " + std::to_string(this->fields.size());
    result->code = AdditionalDataTypes::ResultCode::Ok;
    return result;
  }

PhysicalInsert::PhysicalInsert(
  Statements::TableName* table,
  std::vector<Statements::Inserts> &fields,
  PhysicalOperator* child,
  std::vector<column_index_t>& columnsIndices)
    : table(table), fields(std::move(fields)), child(child), columnsIndices(std::move(columnsIndices)) {}

  PhysicalInsert::~PhysicalInsert(){
    for (auto&[columns] : this->fields) {
      for (const auto& value: columns)
        delete value;
    }

    delete this->table;
    delete this->child;
  }

  PhysicalPlanResult* PhysicalInsert::Execute(const int& batchSize){
    using namespace DatabaseEngine::StorageTypes;

    const DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase(this->table->databaseId);
    
    Table* tablePtr = db->OpenTable(this->table->ordinalPosition);

    const auto transactionId = db->StartLogTransaction();

    return (this->child != nullptr)
        ? this->InsertFromChild(tablePtr, transactionId, batchSize)
        : this->InsertFromFields(tablePtr, transactionId);
  }

  PhysicalHeapDelete::PhysicalHeapDelete(Statements::TableName *table, Expressions::Expression *expression)
    : table(table), expression(expression) {}

  PhysicalHeapDelete::~PhysicalHeapDelete(){
    delete this->expression;
    delete this->table;
  }

  PhysicalPlanResult * PhysicalHeapDelete::Execute(const int& batchSize){
    auto* result = new PhysicalPlanResult();

    const DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase(this->table->databaseId);

    const DatabaseEngine::StorageTypes::Table* tablePtr = db->OpenTable(this->table->ordinalPosition);

    tablePtr->HeapDelete(this->expression);

    return result;
  }

  PhysicalIndexScanDelete::PhysicalIndexScanDelete(Statements::TableName *table, Expressions::Expression *expression)
    : table(table), expression(expression) {}

  PhysicalIndexScanDelete::~PhysicalIndexScanDelete(){
      delete this->expression;
      delete this->table;
  }

  PhysicalPlanResult * PhysicalIndexScanDelete::Execute(const int& batchSize){
    auto* result = new PhysicalPlanResult();

    const DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase(this->table->databaseId);

    DatabaseEngine::StorageTypes::Table* tablePtr = db->OpenTable(this->table->ordinalPosition);

    tablePtr->ClusteredIndexScanDelete(this->expression, state, batchSize);

    return result;
  }

  PhysicalIndexSeekDelete::PhysicalIndexSeekDelete(Statements::TableName *table, Expressions::Expression *expression)
    : table(table), expression(expression) {}

  PhysicalIndexSeekDelete::~PhysicalIndexSeekDelete(){
    delete this->expression;
    delete this->table;
  }

  PhysicalPlanResult * PhysicalIndexSeekDelete::Execute(const int& batchSize){
    auto* result = new PhysicalPlanResult();

    const DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase(this->table->databaseId);

    DatabaseEngine::StorageTypes::Table* tablePtr = db->OpenTable(this->table->ordinalPosition);

    tablePtr->ClusteredIndexSeekDelete(expression, state, batchSize);

    return result;
  }

  PhysicalTableCreate::PhysicalTableCreate(
      const int32_t & databaseId,
      Statements::TableName*  table,
      std::vector<Statements::NewColumn*> &columns,
      Headers::Index& primaryKey,
      std::string& constraintName)
    : table(table), constraintName(std::move(constraintName)),
      columns(std::move(columns)), primaryKey(std::move(primaryKey)) {}

  PhysicalTableCreate::~PhysicalTableCreate(){
    for (const auto& column: this->columns)
      delete column;
  }

  PhysicalPlanResult* PhysicalTableCreate::Execute(const int& batchSize){
    DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase(this->table->databaseId);

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

    const auto& tables = Server::ServerInstance::Get().SelectTables(this->table->databaseId);

    const auto schemas = Server::ServerInstance::Get().SelectSchemas(this->table->databaseId);

    const int16_t& index = tables.empty() ? 0 : tables[tables.size() - 1].ordinalPosition + 1;

    const auto tableResult = Server::ServerInstance::Get().InsertTableToMasterDb(
        this->table->databaseId,
        this->table->schemaId,
        this->table->name,
        index);

    const auto tableStatsResult = Server::ServerInstance::Get().InsertTableStatisticsToMasterDb(
      tableResult.primaryKeyVal
    );

    const auto* tablePtr = db->CreateTable(tableResult.primaryKeyVal, index, columnsPtrs, &this->primaryKey);

    Dictionary<int, int32_t> columnIdsDict;

    for (const auto& column: this->columns) {
      const auto columnResult =
          Server::ServerInstance::Get().InsertColumnToMasterDb(
            static_cast<int32_t>(tableResult.primaryKeyVal),
            column->name.name,
            ColumnTypesDictionary.Get(AdditionalLibraries::StringFunctions::NormalizeString(column->type.name)),
            static_cast<int32_t>(column->type.size),
            column->type.decimal.precision,
            column->type.decimal.scale,
            column->isNullable,
            column->index
          );

      const auto columnStatsResult = Server::ServerInstance::Get().InsertColumnStatisticsToMasterDb(columnResult.primaryKeyVal);

      columnIdsDict.Add(column->index, static_cast<int32_t>(columnResult.primaryKeyVal));

      if (!column->defaultValue.GetIsNull() || column->defaultValue.GetSize() != 0) {
        Server::ServerInstance::Get().InsertDefaultValuesToMasterDb(
          static_cast<int32_t>(columnResult.primaryKeyVal),
          column->defaultValue
        );
      }

      //insert identity columns
      if (column->identity == nullptr)
        continue;

      Server::ServerInstance::Get().InsertIdentityColumnToMasterDb(
          static_cast<int32_t>(tableResult.primaryKeyVal),
          static_cast<int32_t>(columnResult.primaryKeyVal),
          column->identity->seed,
          column->identity->incrementFactor,
          column->identity->seed,
          true,
          static_cast<int32_t>(column->identity->cacheBlock)
          );
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
        static_cast<int32_t>(tableResult.primaryKeyVal),
        this->constraintName,
        true,
        false);

    const auto indexId = static_cast<int32_t>(indexResult.primaryKeyVal);

    const auto constraintResult = Server::ServerInstance::Get().InsertConstraintToMasterDb(
      static_cast<int32_t>(tableResult.primaryKeyVal),
      this->constraintName,
      Headers::ConstraintType::PrimaryKey,
      false,
      &indexId);

    for(int i = 0;i < primaryKeyColumnIds.size(); i++){
      Server::ServerInstance::Get().InsertIndexColumnToMasterDb(
        static_cast<int32_t>(indexResult.primaryKeyVal),
        primaryKeyColumnIds[i],
        this->primaryKey.columns[i],
        true);

      Server::ServerInstance::Get().InsertConstraintColumnToMasterDb(
        static_cast<int32_t>(constraintResult.primaryKeyVal),
        primaryKeyColumnIds[i],
    this->primaryKey.columns[i]);
    }

    tablePtr->GetColumnsHeaders();
    tablePtr->GetIdentityColumns();

    return nullptr;
  }

  PhysicalHeapUpdate::PhysicalHeapUpdate(Statements::TableName *table, Expressions::Expression *expression, std::vector<Statements::UpdateColumn*> & updates)
  : table(table), updates(std::move(updates)), expression(expression) {}

  PhysicalHeapUpdate::~PhysicalHeapUpdate(){
    delete this->expression;
    delete this->table;

    for (const auto* update : this->updates)
      delete update;
  }

  PhysicalPlanResult* PhysicalHeapUpdate::Execute(const int& batchSize){
    using namespace DatabaseEngine::StorageTypes;

    auto* result = new PhysicalPlanResult();

    const DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase(this->table->databaseId);

    Table* tablePtr = db->OpenTable(this->table->ordinalPosition);

    const auto insertResult = tablePtr->HeapUpdate(this->expression, this->updates);

    result->code = insertResult.code;
    result->message = insertResult.message;

    return result;
  }

  PhysicalIndexScanUpdate::PhysicalIndexScanUpdate(Statements::TableName *table, Expressions::Expression *expression, std::vector<Statements::UpdateColumn*> & updates)
  : table(table), updates(std::move(updates)), expression(expression) {}

  PhysicalIndexScanUpdate::~PhysicalIndexScanUpdate(){
    delete this->expression;
    delete this->table;

    for (const auto* update : this->updates)
      delete update;
  }

  PhysicalPlanResult* PhysicalIndexScanUpdate::Execute(const int& batchSize){
    using namespace DatabaseEngine::StorageTypes;

    auto* result = new PhysicalPlanResult();

    const DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase(this->table->databaseId);

    Table* tablePtr = db->OpenTable(this->table->ordinalPosition);

    tablePtr->ClusteredIndexScanUpdate(this->expression, this->updates);

    return result;
  }

  PhysicalIndexSeekUpdate::PhysicalIndexSeekUpdate(Statements::TableName *table, Expressions::Expression *expression, std::vector<Statements::UpdateColumn*> & updates)
    : table(table), updates(std::move(updates)), expression(expression) {}

  PhysicalIndexSeekUpdate::~PhysicalIndexSeekUpdate(){
    delete this->expression;
    delete this->table;

    for (const auto* update : this->updates)
      delete update;
  }

  PhysicalPlanResult* PhysicalIndexSeekUpdate::Execute(const int& batchSize){
    using namespace DatabaseEngine::StorageTypes;

    auto* result = new PhysicalPlanResult();

    const DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase(this->table->databaseId);

    Table* tablePtr = db->OpenTable(this->table->ordinalPosition);

    Indexing::Key key;

    tablePtr->ClusteredIndexScanUpdate(this->expression, this->updates);

    // tablePtr->ClusteredIndexSeekUpdate(this->expression, &key, &key, this->fields);

    return result;
  }

  PhysicalOrderBy::PhysicalOrderBy(
      PhysicalOperator *child,
      std::vector<Statements::OrderColumn*>& expressions)
    : child(child), expressions(std::move(expressions)){}

  PhysicalOrderBy::~PhysicalOrderBy(){
    for (const auto* column : this->expressions) {
      delete column;
    }

    delete this->child;
  }

  PhysicalPlanResult* PhysicalOrderBy::Execute(const int& batchSize){
    auto* result = this->child->Execute(batchSize);

    SortingFunctions::OrderBy(result->results, this->expressions);

    return result;
  }

  PhysicalIndexCreate::PhysicalIndexCreate(
    const int32_t &databaseId,
    Statements::TableName *table,
    std::string &constraintName,
    vector<Constants::column_index_t> &columns)
    : table(table), constraintName(std::move(constraintName)), columns(std::move(columns)) {}

  PhysicalPlanResult * PhysicalIndexCreate::Execute(const int& batchSize){
    auto* result = new PhysicalPlanResult();

    const auto* db = Server::ServerInstance::Get().UseDatabase(this->table->databaseId);

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