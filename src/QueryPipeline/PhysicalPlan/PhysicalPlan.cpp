#include "PhysicalPlan.h"
#include <utility>
#include "../../Database/Database.h"
#include "../../Server/Server.h"
#include "../../Systemic/Functions/StringFunctions.h"
#include "../../Database/AdditionalFunctions/SortingFunctions.h"
#include "../../Database/Block/Block.h"

namespace QueryPipeline::PhysicalPlan {
  PhysicalPlanResult::PhysicalPlanResult(){
    this->code = Errors::RuntimeError::Ok;
  }

  PhysicalPlanResult::PhysicalPlanResult(const Errors::RuntimeError &code, const std::string &message) {
    this->code = code;
    this->message = message;
  }

  PhysicalPlanResult::~PhysicalPlanResult(){
    for (const auto* row: this->rows)
      if (row->IsCopy())
        delete row;
  }

  bool PhysicalPlanResult::IsOk() const {
    return this->code == Errors::RuntimeError::Ok;
  }

  PhysicalOperator::PhysicalOperator(const DataTypes::Guid &currentSessionId)
    : sessionId(currentSessionId){}

  PhysicalCreateUser::PhysicalCreateUser(std::string &username, std::string &password, std::string &role)
   : username(std::move(username)), password(std::move(password)), roleName(std::move(role)) {}

  PhysicalPlanResult * PhysicalCreateUser::Execute(const PhysicalPlanExecutionProperties& properties) {
    auto* result = new PhysicalPlanResult();

    auto& server = Server::ServerInstance::Get();

    if (server.CreateUser(properties.transactionId, this->username, this->password, this->roleName) == false) {
      result->code = Errors::RuntimeError::Error;
      result->message = "Failed to create user";
    }

    return result;
  }

  PhysicalGrantRole::PhysicalGrantRole(const DataTypes::Guid& sessionId, std::string &username, std::string &roleName)
    : PhysicalOperator(sessionId), username(std::move(username)), roleName(std::move(roleName)) {}

  PhysicalPlanResult * PhysicalGrantRole::Execute(const PhysicalPlanExecutionProperties& properties) {
    auto* result = new PhysicalPlanResult();

    const auto& server = Server::ServerInstance::Get();

    const auto* role = server.GetRole(this->roleName);

    if (role == nullptr) {
      result->code = Errors::RuntimeError::Error;
      result->message = "Failed to get role " + this->roleName;
      return result;
    }

    const auto grantRoleResult = server.GrantRole(this->sessionId, this->username, role);

    result->code = grantRoleResult.code;
    result->message = grantRoleResult.message;

    return result;
  }

  PhysicalCreateDatabase::PhysicalCreateDatabase(const DataTypes::Guid& sessionId, std::string& name) : PhysicalOperator(sessionId), dbName(std::move(name)){}

  PhysicalPlanResult* PhysicalCreateDatabase::Execute(const PhysicalPlanExecutionProperties& properties){
    auto& server = Server::ServerInstance::Get();

    const auto* session = server.GetSession(this->sessionId);

    if (session == nullptr || session->user == nullptr)
      return new PhysicalPlanResult{
        Errors::RuntimeError::Error,
        "Failed to retrieve user session"
      };

    const auto result = server.InsertDbToMasterDb(properties.transactionId, this->dbName, this->dbName + ".db", false, session->user->name);

    const auto _ = Server::ServerInstance::Get().InsertSchemaToMasterDb(properties.transactionId, result.primaryKey.GetKeyAsInt(), "dbo");
    
    DatabaseEngine::CreateDatabase(this->dbName);

    return new PhysicalPlanResult();
  }

  PhysicalUseDatabase::PhysicalUseDatabase(const DataTypes::Guid &sessionId, const int32_t &databaseId)
    : sessionId(sessionId), databaseId(databaseId){}

  PhysicalPlanResult * PhysicalUseDatabase::Execute(const PhysicalPlanExecutionProperties& properties) {
    auto* result = new PhysicalPlanResult();

    if (Server::ServerInstance::Get().UpdateSession(this->sessionId, this->databaseId)) {
      result->code = Errors::RuntimeError::Ok;
      result->message = "Database selected successfully";

      return result;
    }

    result->code = Errors::RuntimeError::Error;
    result->message = "Failed to select Database";

    return result;
  }

PhysicalSchemaCreate::PhysicalSchemaCreate(const DataTypes::Guid& sessionId, const int32_t& databaseId, std::string &schemaName)
  : PhysicalOperator(sessionId), schemaName(std::move(schemaName)) ,databaseId(databaseId) {}

  PhysicalPlanResult * PhysicalSchemaCreate::Execute(const PhysicalPlanExecutionProperties& properties){
    auto& server = Server::ServerInstance::Get();

    const auto* session = server.GetSession(this->sessionId);

    if (session == nullptr || session->user == nullptr)
      return new PhysicalPlanResult{
        Errors::RuntimeError::Error,
        "Failed to retrieve user session"
      };

    const auto insertResult = Server::ServerInstance::Get().InsertSchemaToMasterDb(properties.transactionId, this->databaseId, this->schemaName, session->user->name);

    return new PhysicalPlanResult{
      insertResult.code,
      insertResult.message,
    };
  }

  PhysicalTableScan::PhysicalTableScan(Statements::TableName* table): table(table) {}

  PhysicalPlanResult* PhysicalTableScan::Execute(const PhysicalPlanExecutionProperties& properties){
      using namespace DatabaseEngine::StorageTypes;
      const auto* db = Server::ServerInstance::Get().UseDatabase(this->table->databaseId);

      const auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

      auto* result = new PhysicalPlanResult();

      result->columns = tablePtr->GetConstantColumns();

      tablePtr->HeapScan(&result->rows, this->state, properties.batchSize);

      return result;
    }

  PhysicalIndexScan::PhysicalIndexScan(Statements::TableName* table, const bool& isClustered)
    : table(table), expression(nullptr), isClustered(isClustered) {}

  PhysicalIndexScan::PhysicalIndexScan(Statements::TableName *table, Expressions::Expression *expression, const bool & isClustered)
    : table(table), expression(expression), isClustered(isClustered) {}

  PhysicalPlanResult * PhysicalIndexScan::Execute(const PhysicalPlanExecutionProperties& properties){
    using namespace DatabaseEngine::StorageTypes;

    auto* result = new PhysicalPlanResult();

    const auto* db = Server::ServerInstance::Get().UseDatabase(this->table->databaseId);

    Table* tablePtr = db->OpenTable(this->table->ordinalPosition);

    result->columns = tablePtr->GetConstantColumns();

    if (this->isClustered) {
      tablePtr->ClusteredIndexScan(&result->rows, this->state, properties.batchSize, this->expression);
      return result;
    }

    tablePtr->NonClusteredIndexScan(&result->rows, 0, this->state, properties.batchSize, this->expression);

    return result;
  }

  PhysicalIndexSeek::PhysicalIndexSeek(Statements::TableName* table, const Value& minValue, const Value& maxValue)
    : table(table), minValue(minValue), maxValue(maxValue) {}

  PhysicalPlanResult* PhysicalIndexSeek::Execute(const PhysicalPlanExecutionProperties& properties){
    using namespace DatabaseEngine::StorageTypes;

    auto* result = new PhysicalPlanResult();

    const auto* db = Server::ServerInstance::Get().UseDatabase(this->table->databaseId);

    Table* tablePtr = db->OpenTable(this->table->ordinalPosition);

    result->columns = tablePtr->GetConstantColumns();
    const DataTypes::Indexing::Key minKey(minValue);
    const DataTypes::Indexing::Key maxKey(maxValue);

    //select if to use clustered or non clustered index here

    tablePtr->ClusteredIndexSeek(&result->rows,&minKey, &maxKey);

    return result;
  }


  PhysicalPlanResult * PhysicalProject::ExecuteStatement(const PhysicalPlanExecutionProperties& properties){
    auto* result = this->child->Execute(properties);

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
    PhysicalOperator *child,
    std::vector<Expressions::Expression*>& resultExpressions,
    std::vector<Headers::ColumnHeader>& columnHeaders)
    : resultExpressions(std::move(resultExpressions)), columnHeaders(std::move(columnHeaders)), child(child) {}

  PhysicalProject::~PhysicalProject() {
    for (const auto& expression : this->resultExpressions)
      delete expression;

    delete this->child;
  }

  PhysicalPlanResult* PhysicalProject::Execute(const PhysicalPlanExecutionProperties& properties){
      return (this->child == nullptr)
        ? this->ExecuteConstantStatement()
        : this->ExecuteStatement(properties);
  }

  PhysicalFilter::PhysicalFilter(PhysicalOperator *child, Expressions::Expression* filter)
        : filter(filter) , child(child) {}

  PhysicalFilter::~PhysicalFilter(){
    delete this->child;
    delete this->filter;
  }

  PhysicalPlanResult* PhysicalFilter::Execute(const PhysicalPlanExecutionProperties& properties){
    auto* result = child->Execute(properties);

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

  PhysicalPlanResult * PhysicalTop::Execute(const PhysicalPlanExecutionProperties& properties){
    auto* result = this->child->Execute(properties);

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

  PhysicalPlanResult * PhysicalDistinct::Execute(const PhysicalPlanExecutionProperties& properties){
    auto* result = this->child->Execute(properties);

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

  PhysicalPlanResult* PhysicalInsert::InsertFromChild(DatabaseEngine::StorageTypes::Table* tablePtr, const PhysicalPlanExecutionProperties& properties)const{
    auto* result = this->child->Execute(properties);

    for (auto& row : result->results) {
      const auto insertResult = tablePtr->InsertRow(properties.transactionId, row.GetData(), this->columnsIndices);

      if (insertResult.code != Errors::RuntimeError::Ok) {
        result->message = insertResult.message;
        result->code = insertResult.code;
        return result;
      }
    }

    result->message = "Rows inserted: " + std::to_string(result->results.size());
    result->code = Errors::RuntimeError::Ok;
    return result;
  }

  PhysicalPlanResult* PhysicalInsert::InsertFromFields(DatabaseEngine::StorageTypes::Table* tablePtr, const PhysicalPlanExecutionProperties& properties){
    auto* result = new PhysicalPlanResult();

    for (const auto&[columns] : this->fields) {
      const auto insertResult = tablePtr->InsertRow(properties.transactionId, columns, this->columnsIndices);

      if (insertResult.code != Errors::RuntimeError::Ok) {
        result->message = insertResult.message;
        result->code = insertResult.code;
        return result;
      }
    }

    result->message = "Rows inserted: " + std::to_string(this->fields.size());
    result->code = Errors::RuntimeError::Ok;
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

  PhysicalPlanResult* PhysicalInsert::Execute(const PhysicalPlanExecutionProperties& properties){
    using namespace DatabaseEngine::StorageTypes;

    const auto* db = Server::ServerInstance::Get().UseDatabase(this->table->databaseId);
    
    Table* tablePtr = db->OpenTable(this->table->ordinalPosition);

    const auto transactionId = db->StartLogTransaction();

    return (this->child != nullptr)
        ? this->InsertFromChild(tablePtr, properties)
        : this->InsertFromFields(tablePtr, properties);
  }

  PhysicalHeapDelete::PhysicalHeapDelete(Statements::TableName *table, Expressions::Expression *expression)
    : table(table), expression(expression) {}

  PhysicalHeapDelete::~PhysicalHeapDelete(){
    delete this->expression;
    delete this->table;
  }

  PhysicalPlanResult * PhysicalHeapDelete::Execute(const PhysicalPlanExecutionProperties& properties){
    auto* result = new PhysicalPlanResult();

    const auto* db = Server::ServerInstance::Get().UseDatabase(this->table->databaseId);

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

  PhysicalPlanResult * PhysicalIndexScanDelete::Execute(const PhysicalPlanExecutionProperties& properties){
    auto* result = new PhysicalPlanResult();

    const auto* db = Server::ServerInstance::Get().UseDatabase(this->table->databaseId);

    DatabaseEngine::StorageTypes::Table* tablePtr = db->OpenTable(this->table->ordinalPosition);

    tablePtr->ClusteredIndexScanDelete(this->expression, state, properties.batchSize);

    return result;
  }

  PhysicalIndexSeekDelete::PhysicalIndexSeekDelete(Statements::TableName *table, Expressions::Expression *expression)
    : table(table), expression(expression) {}

  PhysicalIndexSeekDelete::~PhysicalIndexSeekDelete(){
    delete this->expression;
    delete this->table;
  }

  PhysicalPlanResult * PhysicalIndexSeekDelete::Execute(const PhysicalPlanExecutionProperties& properties){
    auto* result = new PhysicalPlanResult();

    const auto* db = Server::ServerInstance::Get().UseDatabase(this->table->databaseId);

    DatabaseEngine::StorageTypes::Table* tablePtr = db->OpenTable(this->table->ordinalPosition);

    tablePtr->ClusteredIndexSeekDelete(expression, state, properties.batchSize);

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

  PhysicalPlanResult* PhysicalHeapUpdate::Execute(const PhysicalPlanExecutionProperties& properties){
    using namespace DatabaseEngine::StorageTypes;

    auto* result = new PhysicalPlanResult();

    const auto* db = Server::ServerInstance::Get().UseDatabase(this->table->databaseId);

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

  PhysicalPlanResult* PhysicalIndexScanUpdate::Execute(const PhysicalPlanExecutionProperties& properties){
    using namespace DatabaseEngine::StorageTypes;

    auto* result = new PhysicalPlanResult();

    const auto* db = Server::ServerInstance::Get().UseDatabase(this->table->databaseId);

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

  PhysicalPlanResult* PhysicalIndexSeekUpdate::Execute(const PhysicalPlanExecutionProperties& properties){
    using namespace DatabaseEngine::StorageTypes;

    const auto* db = Server::ServerInstance::Get().UseDatabase(this->table->databaseId);

    Table* tablePtr = db->OpenTable(this->table->ordinalPosition);

    DataTypes::Indexing::Key key;

    const auto updateResult = tablePtr->ClusteredIndexScanUpdate(this->expression, this->updates);

    // tablePtr->ClusteredIndexSeekUpdate(this->expression, &key, &key, this->fields);

    return new PhysicalPlanResult{
      updateResult.code,
      updateResult.message,
    };
  }

  PhysicalTableCreate::PhysicalTableCreate(
      const DataTypes::Guid& sessionId,
      Statements::TableName*  table,
      std::vector<Statements::NewColumn*> &columns,
      Headers::Index& primaryKey,
      std::string& constraintName)
    : PhysicalOperator(sessionId), table(table), constraintName(std::move(constraintName)),
      columns(std::move(columns)), primaryKey(std::move(primaryKey)) {}

  PhysicalTableCreate::~PhysicalTableCreate(){
    for (const auto& column: this->columns)
      delete column;
  }

  PhysicalPlanResult* PhysicalTableCreate::Execute(const PhysicalPlanExecutionProperties& properties){

    auto& server = Server::ServerInstance::Get();

    const auto* session = server.GetSession(this->sessionId);

    if (session == nullptr || session->user == nullptr)
      return new PhysicalPlanResult{
        Errors::RuntimeError::Error,
        "Failed to retrieve user session"
      };

    auto* db = server.UseDatabase(this->table->databaseId);

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

    const auto& tables = server.SelectTables(this->table->databaseId);

    const int16_t& index = static_cast<int16_t>(tables.empty() ? 0 : tables[tables.size() - 1].ordinalPosition + 1);

    const auto tableResult = server.InsertTableToMasterDb(
        properties.transactionId,
        this->table->databaseId,
        this->table->schemaId,
        this->table->name,
        index
    );

    const auto tableStatsResult = server.InsertTableStatisticsToMasterDb(
      properties.transactionId,
      tableResult.primaryKey.GetKeyAsInt()
    );

    auto* tablePtr = db->CreateTable(tableResult.primaryKey.GetKeyAsInt(), index, columnsPtrs, &this->primaryKey);

    Dictionary<int, int32_t> columnIdsDict;

    for (const auto& column: this->columns) {
      const auto columnResult =
          server.InsertColumnToMasterDb(
            properties.transactionId,
            tableResult.primaryKey.GetKeyAsInt(),
            column->name.name,
            ColumnTypesDictionary.Get(Functions::String::NormalizeString(column->type.name)),
            column->type.size,
            column->type.decimal.precision,
            column->type.decimal.scale,
            column->isNullable,
            column->index,
            false,
            session->user->name
          );

      const auto columnStatsResult = server.InsertColumnStatisticsToMasterDb(properties.transactionId, columnResult.primaryKey.GetKeyAsInt());

      columnIdsDict.Add(column->index, columnResult.primaryKey.GetKeyAsInt());

      if (!column->defaultValue.GetIsNull() || column->defaultValue.GetSize() != 0) {
        const auto _ = server.InsertDefaultValuesToMasterDb(
          properties.transactionId,
          columnResult.primaryKey.GetKeyAsInt(),
          column->defaultValue
        );
      }

      //insert identity columns
      if (column->identity == nullptr)
        continue;

      const auto _ = server.InsertIdentityColumnToMasterDb(
          properties.transactionId,
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

    const auto indexResult = server.InsertIndexToMasterDb(
         properties.transactionId,
        tableResult.primaryKey.GetKeyAsInt(),
        this->constraintName,
        true,
        false,
        session->user->name
      );

    const auto indexId = indexResult.primaryKey.GetKeyAsInt();

    const auto constraintResult = server.InsertConstraintToMasterDb(
        properties.transactionId,
      tableResult.primaryKey.GetKeyAsInt(),
        this->constraintName,
        Headers::ConstraintType::PrimaryKey,
        false,
        &indexId,
        session->user->name
      );

    for(int i = 0;i < primaryKeyColumnIds.size(); i++){
      auto _ = server.InsertIndexColumnToMasterDb(
        properties.transactionId,
        indexResult.primaryKey.GetKeyAsInt(),
        primaryKeyColumnIds[i],
        this->primaryKey.columns[i],
        true);

      _ = server.InsertConstraintColumnToMasterDb(
          properties.transactionId,
          constraintResult.primaryKey.GetKeyAsInt(),
          primaryKeyColumnIds[i],
      this->primaryKey.columns[i]
        );
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

  PhysicalPlanResult* PhysicalOrderBy::Execute(const PhysicalPlanExecutionProperties& properties){
    auto* result = this->child->Execute(properties);

    SortingFunctions::OrderBy(result->results, this->expressions);

    return result;
  }

  PhysicalIndexCreate::PhysicalIndexCreate(
    const DataTypes::Guid& sessionId,
    Statements::TableName *table,
    std::string &constraintName,
    vector<Constants::column_index_t> &columns)
    : PhysicalOperator(sessionId), table(table), constraintName(std::move(constraintName)), columns(std::move(columns)) {}

  PhysicalPlanResult * PhysicalIndexCreate::Execute(const PhysicalPlanExecutionProperties& properties){
    auto* result = new PhysicalPlanResult();

    const auto* db = Server::ServerInstance::Get().UseDatabase(this->table->databaseId);

    auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

    const auto columnsHeaders = Server::ServerInstance::Get().SelectColumns(this->table->tableId);

    const auto indexResult = Server::ServerInstance::Get().InsertIndexToMasterDb(
        properties.transactionId,
        this->table->tableId,
        this->constraintName,
        false,
        false);

    const auto indexId = indexResult.primaryKey.GetKeyAsInt();

    const auto constraintResult = Server::ServerInstance::Get().InsertConstraintToMasterDb(\
      properties.transactionId,
      this->table->tableId,
      this->constraintName,
      Headers::ConstraintType::IndexKey,
      false,
      &indexId);

    for (const auto& columnPos : this->columns) {
      const auto& header = columnsHeaders.at(columnPos);

      const auto indexColumnResult =
        Server::ServerInstance::Get().InsertIndexColumnToMasterDb(
             properties.transactionId,
            indexResult.primaryKey.GetKeyAsInt(),
            header.id,
            columnPos,
            true);

      const auto constraintColumnResult =
        Server::ServerInstance::Get().InsertConstraintColumnToMasterDb(
              properties.transactionId,
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