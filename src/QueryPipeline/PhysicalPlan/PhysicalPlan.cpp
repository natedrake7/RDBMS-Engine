#include "PhysicalPlan.h"
#include <utility>
#include "../../Database/Database.h"
#include "../../Server/Server.h"
#include "../../Systemic/Functions/StringFunctions.h"
#include "../../Database/AdditionalFunctions/SortingFunctions.h"
#include "../../Database/Block/Block.h"

#include <iostream>

namespace QueryPipeline::PhysicalPlan {
  PhysicalPlanResult::PhysicalPlanResult(){
    this->code = Errors::ResultCode::Ok;
  }

  PhysicalPlanResult::~PhysicalPlanResult(){
    for (const auto* row: this->rows)
      if (row->IsCopy())
        delete row;
  }

 PhysicalCreateUser::PhysicalCreateUser(std::string &username, std::string &password, std::string &role)
   : username(std::move(username)), password(std::move(password)), roleName(std::move(role)) {}

  PhysicalPlanResult * PhysicalCreateUser::Execute(const int &batchSize) {
    auto* result = new PhysicalPlanResult();

    auto& server = Server::ServerInstance::Get();

    if (server.CreateUser(this->username, this->password, this->roleName) == false) {
      result->code = Errors::ResultCode::Error;
      result->message = "Failed to create user";
    }

    return result;
  }

  PhysicalGrantRole::PhysicalGrantRole(std::string &username, std::string &roleName)
    : username(std::move(username)), roleName(std::move(roleName)) {}

  PhysicalPlanResult * PhysicalGrantRole::Execute(const int &batchSize) {
    auto* result = new PhysicalPlanResult();

    const auto& server = Server::ServerInstance::Get();

    const auto* role = server.GetRole(this->roleName);

    if (role == nullptr) {
      result->code = Errors::ResultCode::Error;
      result->message = "Failed to get role " + this->roleName;
      return result;
    }

    if (!server.GrantRole(this->username, role)) {
      result->code = Errors::ResultCode::Error;
      result->message = "Failed to grant role " + this->roleName + " to user: " + this->username;
    }

    return result;
  }

  PhysicalCreateDatabase::PhysicalCreateDatabase(std::string name) : dbName(std::move(name)){}

  PhysicalPlanResult* PhysicalCreateDatabase::Execute(const int& batchSize){
    const auto result = Server::ServerInstance::Get().InsertDbToMasterDb(this->dbName, this->dbName + ".db");

    const auto _ = Server::ServerInstance::Get().InsertSchemaToMasterDb(result.primaryKey.GetKeyAsInt(), "dbo");
    
    DatabaseEngine::CreateDatabase(this->dbName);

    return new PhysicalPlanResult();
  }

  PhysicalUseDatabase::PhysicalUseDatabase(const DataTypes::Guid &sessionId, const int32_t &databaseId)
    : sessionId(sessionId), databaseId(databaseId){}

  PhysicalPlanResult * PhysicalUseDatabase::Execute(const int &batchSize) {
    auto* result = new PhysicalPlanResult();

    const auto sessionUpdateResult = Server::ServerInstance::Get().UpdateSession(this->sessionId, this->databaseId);

    if (sessionUpdateResult) {
      result->code = Errors::ResultCode::Ok;
      result->message = "Database selected successfully";

      return result;
    }

    result->code = Errors::ResultCode::Error;
    result->message = "Failed to select Database";

    return result;
  }

PhysicalSchemaCreate::PhysicalSchemaCreate(const int32_t& databaseId, std::string &schemaName)
  : schemaName(std::move(schemaName)) ,databaseId(databaseId) {}

  PhysicalPlanResult * PhysicalSchemaCreate::Execute(const int& batchSize){
    Server::ServerInstance::Get().InsertSchemaToMasterDb(this->databaseId, this->schemaName);

    return new PhysicalPlanResult();
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
    const DataTypes::Indexing::Key minKey(minValue);
    const DataTypes::Indexing::Key maxKey(maxValue);

    //select if to use clustered or non clustered index here

    tablePtr->ClusteredIndexSeek(&result->rows,&minKey, &maxKey);

    return result;
  }


  PhysicalPlanResult * PhysicalProject::ExecuteStatement(const int &batchSize){
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

  PhysicalPlanResult * PhysicalProject::ExecuteConstantStatement()const{
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
      return (this->child == nullptr)
        ? this->ExecuteConstantStatement()
        : this->ExecuteStatement(batchSize);
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

  PhysicalTop::PhysicalTop(PhysicalOperator* child, const int64_t& top)
    : top(top), child(child){}

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

  PhysicalPlanResult* PhysicalInsert::InsertFromChild(DatabaseEngine::StorageTypes::Table* tablePtr, const transaction_id_t& transactionId, const int& batchSize)const{
    auto* result = this->child->Execute(batchSize);

    for (auto& row : result->results) {
      const auto insertResult = tablePtr->InsertRow(transactionId, row.GetData(), this->columnsIndices);

      if (insertResult.code != Errors::ResultCode::Ok) {
        result->message = insertResult.message;
        result->code = insertResult.code;
        return result;
      }
    }

    result->message = "Rows inserted: " + std::to_string(result->results.size());
    result->code = Errors::ResultCode::Ok;
    return result;
  }

  PhysicalPlanResult* PhysicalInsert::InsertFromFields(DatabaseEngine::StorageTypes::Table* tablePtr, const transaction_id_t& transactionId){
    auto* result = new PhysicalPlanResult();

    for (const auto&[columns] : this->fields) {
      const auto insertResult = tablePtr->InsertRow(transactionId, columns, this->columnsIndices);

      if (insertResult.code != Errors::ResultCode::Ok) {
        result->message = insertResult.message;
        result->code = insertResult.code;
        return result;
      }
    }

    result->message = "Rows inserted: " + std::to_string(this->fields.size());
    result->code = Errors::ResultCode::Ok;
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

    const auto updateResult = tablePtr->ClusteredIndexScanUpdate(this->expression, this->updates);

    result->code = updateResult.code;
    result->message = updateResult.message;

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

    DataTypes::Indexing::Key key;

    tablePtr->ClusteredIndexScanUpdate(this->expression, this->updates);

    // tablePtr->ClusteredIndexSeekUpdate(this->expression, &key, &key, this->fields);

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

    const int16_t& index = tables.empty() ? 0 : tables[tables.size() - 1].ordinalPosition + 1;

    const auto tableResult = Server::ServerInstance::Get().InsertTableToMasterDb(
        this->table->databaseId,
        this->table->schemaId,
        this->table->name,
        index
    );

    const auto tableStatsResult = Server::ServerInstance::Get().InsertTableStatisticsToMasterDb(
      tableResult.primaryKey.GetKeyAsInt()
    );

    auto* tablePtr = db->CreateTable(tableResult.primaryKey.GetKeyAsInt(), index, columnsPtrs, &this->primaryKey);

    Dictionary<int, int32_t> columnIdsDict;

    for (const auto& column: this->columns) {
      const auto columnResult =
          Server::ServerInstance::Get().InsertColumnToMasterDb(
            tableResult.primaryKey.GetKeyAsInt(),
            column->name.name,
            ColumnTypesDictionary.Get(Functions::String::NormalizeString(column->type.name)),
            column->type.size,
            column->type.decimal.precision,
            column->type.decimal.scale,
            column->isNullable,
            column->index
          );

      const auto columnStatsResult = Server::ServerInstance::Get().InsertColumnStatisticsToMasterDb(columnResult.primaryKey.GetKeyAsInt());

      columnIdsDict.Add(column->index, columnResult.primaryKey.GetKeyAsInt());

      if (!column->defaultValue.GetIsNull() || column->defaultValue.GetSize() != 0) {
        Server::ServerInstance::Get().InsertDefaultValuesToMasterDb(
          columnResult.primaryKey.GetKeyAsInt(),
          column->defaultValue
        );
      }

      //insert identity columns
      if (column->identity == nullptr)
        continue;

      Server::ServerInstance::Get().InsertIdentityColumnToMasterDb(
          tableResult.primaryKey.GetKeyAsInt(),
          columnResult.primaryKey.GetKeyAsInt(),
          column->identity->seed,
          column->identity->incrementFactor,
          column->identity->seed,
          true,
          column->identity->cacheBlock
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
        tableResult.primaryKey.GetKeyAsInt(),
        this->constraintName,
        true,
        false);

    const auto indexId = indexResult.primaryKey.GetKeyAsInt();

    const auto constraintResult = Server::ServerInstance::Get().InsertConstraintToMasterDb(
      tableResult.primaryKey.GetKeyAsInt(),
      this->constraintName,
      Headers::ConstraintType::PrimaryKey,
      false,
      &indexId);

    for(int i = 0;i < primaryKeyColumnIds.size(); i++){
      Server::ServerInstance::Get().InsertIndexColumnToMasterDb(
        indexResult.primaryKey.GetKeyAsInt(),
        primaryKeyColumnIds[i],
        this->primaryKey.columns[i],
        true);

      Server::ServerInstance::Get().InsertConstraintColumnToMasterDb(
        constraintResult.primaryKey.GetKeyAsInt(),
        primaryKeyColumnIds[i],
    this->primaryKey.columns[i]);
    }

    tablePtr->GetColumnsHeaders();
    tablePtr->GetIdentityColumns();
    tablePtr->GetStatistics();

    return nullptr;
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

    const auto indexId = indexResult.primaryKey.GetKeyAsInt();

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
            indexResult.primaryKey.GetKeyAsInt(),
            header.id,
            columnPos,
            true);

      const auto constraintColumnResult =
        Server::ServerInstance::Get().InsertConstraintColumnToMasterDb(
            constraintResult.primaryKey.GetKeyAsInt(),
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