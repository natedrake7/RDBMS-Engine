#include "../../include/PhysicalPlan.h"
#include <utility>
#include "../../../DatabaseEngine/include/Database.h"
#include "../../../DatabaseEngine/include/SystemDatabases/SystemCatalog.h"
#include "../../../Server/include/Server.h"
#include "../../../Systemic/include/Functions/StringFunctions.h"
#include "../../../DatabaseEngine/include/Algorithms/Sort/SortingFunctions.h"
#include "../../../DatabaseEngine/include/ExecutionProperties.h"
#include "SystemDatabases/TemporaryDatabase.h"

namespace QueryPipeline::PhysicalPlan {
  ExecutionResult::ExecutionResult(){
    this->code = Errors::RuntimeError::Ok;
    this->canFetchMore = false;
  }

  ExecutionResult::ExecutionResult(const Errors::RuntimeError &code, const std::string &message) {
    this->code = code;
    this->message = message;
    this->canFetchMore = false;
  }

  ExecutionResult::~ExecutionResult() = default;

  bool ExecutionResult::IsOk() const {
    return this->code == Errors::RuntimeError::Ok;
  }

  ExecutionNode::ExecutionNode() {
    this->catalog = &DatabaseEngine::SystemCatalog::Get();
    this->server = &Network::Server::Get();
    this->session = nullptr;
    this->temporaryTableId = INVALID_TABLE_ID;
  }

  ExecutionNode::ExecutionNode(const DataTypes::Guid &currentSessionId){
    this->sessionId = currentSessionId;
    this->catalog = &DatabaseEngine::SystemCatalog::Get();
    this->server = &Network::Server::Get();
    this->session = this->server->GetSession(this->sessionId);
    this->temporaryTableId = INVALID_TABLE_ID;
  }

  void ExecutionNode::InsertToTemporaryDatabase(const std::vector<Pages::RowReference>& rows){

  }

  void ExecutionNode::InsertPostProjectionResultsToTemporaryDatabase(
    const DatabaseEngine::ExecutionProperties& properties,
    ExecutionResult*& result,
    DataTypes::RowIdentifier& firstRowId
  ){
    static auto& tempDb = DatabaseEngine::TemporaryDatabase::Get();

    auto* table = (this->temporaryTableId == INVALID_TABLE_ID)
          ? tempDb.CreateTable()
          : tempDb.OpenTable(this->temporaryTableId);

    //table ordinal and table id are the same rn
    this->temporaryTableId = table->GetTableId();

    const auto& columns = table->GetColumns();
    std::vector<column_index_t> columnIndices;
    columnIndices.reserve(columns.size());

    for (const auto& column : columns)
      columnIndices.push_back(column->OrdinalPosition());

    const auto batchResult = table->BatchInsert(properties, result->results, columnIndices);

    firstRowId = batchResult.rowId;

    result->code = batchResult.code;
    result->message = batchResult.message;
  }

  ExecutionResult* ExecutionNode::StreamFromTemporaryDatabase(
    const DatabaseEngine::ExecutionProperties& properties,
    DatabaseEngine::ScanState& state
  ) const
  {
    static auto& tempDb = DatabaseEngine::TemporaryDatabase::Get();

    auto* result = new ExecutionResult();

    if (this->temporaryTableId == INVALID_TABLE_ID)
      return result;

    const auto* table = tempDb.OpenTable(this->temporaryTableId);

    // table->TemporaryDatabaseHeapScan(&result->rows, state, properties.batchSize);

    result->results.reserve(result->rows.size());

    for (const auto& row: result->rows){
      result->results.emplace_back(row.Materialize());
    }

    return result;
  }


  void ExecutionNode::UpdateScanState(const DataTypes::RowIdentifier& rowId){ }

  bool ExecutionNode::UsesExternalStorage() const{ return this->temporaryTableId != INVALID_TABLE_ID; }

  PhysicalDeclareVariable::PhysicalDeclareVariable(const DataTypes::Guid &currentSessionId, Variable& variable, Expressions::Expression* expression)
    : ExecutionNode(currentSessionId), variable(std::move(variable)), expression(expression){}

  ExecutionResult * PhysicalDeclareVariable::Execute(const DatabaseEngine::ExecutionProperties &properties) {
    auto* result = new ExecutionResult();

    const Expressions::EvaluationContext context(Expressions::EvaluationContext::EvaluationContextType::Constant, properties.variables);

    auto value = this->expression->Evaluate(context);
    this->variable.SetValue(value);

    if (!this->server->AddOrSetVariable(this->sessionId, this->variable)) {
      result->code = Errors::RuntimeError::Error;
      result->message = "Failed to add variable";
    }

    result->code = Errors::RuntimeError::Ok;
    result->message = "Added variable with name: " + this->variable.GetName();

    return result;
  }

  PhysicalCreateUser::PhysicalCreateUser(std::string &username, std::string &password, std::string &role)
   : username(std::move(username)), password(std::move(password)), roleName(std::move(role)) {}

  ExecutionResult * PhysicalCreateUser::Execute(const DatabaseEngine::ExecutionProperties& properties) {
    auto* result = new ExecutionResult();

    if (!this->server->CreateUser(properties, this->username, this->password, this->roleName)) {
      result->code = Errors::RuntimeError::Error;
      result->message = "Failed to create user";
    }

    return result;
  }

  PhysicalGrantRole::PhysicalGrantRole(const DataTypes::Guid& sessionId, std::string &username, std::string &roleName)
    : ExecutionNode(sessionId), username(std::move(username)), roleName(std::move(roleName)) {}

  ExecutionResult * PhysicalGrantRole::Execute(const DatabaseEngine::ExecutionProperties& properties) {
    auto* result = new ExecutionResult();

    const auto* role = this->server->GetRole(this->roleName);

    if (role == nullptr) {
      result->code = Errors::RuntimeError::Error;
      result->message = "Failed to get role " + this->roleName;
      return result;
    }

    const auto grantRoleResult = this->server->GrantRole(this->sessionId, this->username, role);

    result->code = grantRoleResult.code;
    result->message = grantRoleResult.message;

    return result;
  }

  PhysicalCreateDatabase::PhysicalCreateDatabase(const DataTypes::Guid& sessionId, std::string& name) : ExecutionNode(sessionId), dbName(std::move(name)){}

  ExecutionResult* PhysicalCreateDatabase::Execute(const DatabaseEngine::ExecutionProperties& properties){
    if (this->session == nullptr || this->session->user == nullptr)
      return new ExecutionResult{
        Errors::RuntimeError::Error,
        "Failed to retrieve user session"
      };

    const auto result = this->catalog->InsertDbToMasterDb(properties, this->dbName, this->dbName + ".db", false, this->session->user->name);

    const auto _ = this->catalog->InsertSchemaToMasterDb(properties, result.primaryKey.AsInt(), "dbo");

    DatabaseEngine::CreateDatabase(this->dbName);

    return new ExecutionResult();
  }

  PhysicalUseDatabase::PhysicalUseDatabase(const DataTypes::Guid &sessionId, const Int databaseId)
    : sessionId(sessionId), databaseId(databaseId){}

  ExecutionResult * PhysicalUseDatabase::Execute(const DatabaseEngine::ExecutionProperties& properties) {
    auto* result = new ExecutionResult();

    if (this->server->UpdateSession(this->sessionId, this->databaseId)) {
      result->code = Errors::RuntimeError::Ok;
      result->message = "Database selected successfully";

      return result;
    }

    result->code = Errors::RuntimeError::Error;
    result->message = "Failed to select Database";

    return result;
  }

PhysicalSchemaCreate::PhysicalSchemaCreate(const DataTypes::Guid& sessionId, const Int databaseId, std::string &schemaName)
  : ExecutionNode(sessionId), schemaName(std::move(schemaName)) ,databaseId(databaseId) {}

  ExecutionResult * PhysicalSchemaCreate::Execute(const DatabaseEngine::ExecutionProperties& properties){
    if (this->session == nullptr || this->session->user == nullptr)
      return new ExecutionResult{
        Errors::RuntimeError::Error,
        "Failed to retrieve user session"
      };

    const auto insertResult = this->catalog->InsertSchemaToMasterDb(properties, this->databaseId, this->schemaName, this->session->user->name);

    return new ExecutionResult{
      insertResult.code,
      insertResult.message,
    };
  }

  PhysicalTableScan::PhysicalTableScan(Statements::DataSource* table, Expressions::Expression* expression)
    : table(table), expression(expression) {}

  PhysicalTableScan::~PhysicalTableScan(){
    delete this->table;
    delete this->expression;
  }

  ExecutionResult* PhysicalTableScan::Execute(const DatabaseEngine::ExecutionProperties& properties){
    auto* result = new ExecutionResult();

    const auto* db = this->server->UseDatabase(this->table->databaseId);

    const auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

    result->columns = tablePtr->GetConstantColumns();

    tablePtr->HeapScan(properties, &result->rows, this->state);

    result->canFetchMore = this->state.canFetchMore;

    if (result->canFetchMore == false)
      this->state.Reset();

    return result;
  }

  void PhysicalTableScan::UpdateScanState(const DataTypes::RowIdentifier& rowId){
    this->state.lastFetchedRowId = rowId;
  }

  PhysicalIndexScan::PhysicalIndexScan(Statements::DataSource* table, const bool isClustered)
    : table(table), expression(nullptr), isClustered(isClustered) {}

  PhysicalIndexScan::PhysicalIndexScan(Statements::DataSource *table, Expressions::Expression *expression, const bool isClustered)
    : table(table), expression(expression), isClustered(isClustered) {}

  PhysicalIndexScan::~PhysicalIndexScan(){
    delete this->table;
    delete this->expression;
  }

  ExecutionResult * PhysicalIndexScan::Execute(const DatabaseEngine::ExecutionProperties& properties){
    auto* result = new ExecutionResult();

    const auto* db =  this->server->UseDatabase(this->table->databaseId);

    auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

    result->columns = tablePtr->GetConstantColumns();

    if (this->isClustered) {
      tablePtr->ClusteredIndexScan(properties, &result->rows, this->state, this->expression);
      result->canFetchMore = this->state.canFetchMore;

      if (result->canFetchMore == false)
        this->state.Reset();

      return result;
    }

    tablePtr->NonClusteredIndexScan(properties, &result->rows, 0, this->state, this->expression);

    result->canFetchMore = this->state.canFetchMore;
    if (result->canFetchMore == false)
      this->state.Reset();

    return result;
  }

  void PhysicalIndexScan::UpdateScanState(const DataTypes::RowIdentifier& rowId){
    this->state.pageId = rowId.pageId;
    this->state.lastFetchedKeyIndex = rowId.indexId;
  }

  PhysicalIndexSeek::PhysicalIndexSeek(
    Statements::DataSource* table,
    DataTypes::Indexing::Key& key,
    Expressions::Expression* expression
  ) : table(table), expression(expression), key(std::move(key)) {}

  PhysicalIndexSeek::~PhysicalIndexSeek(){
    delete this->table;
    delete this->expression;
  }

  ExecutionResult* PhysicalIndexSeek::Execute(const DatabaseEngine::ExecutionProperties& properties){
    auto* result = new ExecutionResult();

    const auto* db = Network::Server::Get().UseDatabase(this->table->databaseId);

    auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

    result->columns = tablePtr->GetConstantColumns();

    //select if to use clustered or non clustered index here
    tablePtr->ClusteredIndexSeek(properties, &result->rows, this->key, this->expression);

    return result;
  }

  PhysicalIndexSeekRange::PhysicalIndexSeekRange(
    Statements::DataSource* table,
    DataTypes::Indexing::Key& minKey,
    DataTypes::Indexing::Key& maxKey,
    Expressions::Expression* expression
  )
    : table(table), expression(expression), minKey(std::move(minKey)), maxKey(std::move(maxKey)) {}

  PhysicalIndexSeekRange::~PhysicalIndexSeekRange(){
    delete this->table;
    delete this->expression;
  }

  ExecutionResult* PhysicalIndexSeekRange::Execute(const DatabaseEngine::ExecutionProperties& properties){
    auto* result = new ExecutionResult();

    const auto* db = Network::Server::Get().UseDatabase(this->table->databaseId);

    auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

    result->columns = tablePtr->GetConstantColumns();

    //select if to use clustered or non clustered index here
    tablePtr->ClusteredIndexSeekRange(properties, &result->rows, this->minKey, this->maxKey, this->expression);

    return result;
  }

  ExecutionResult * PhysicalProject::ExecuteStatement(const DatabaseEngine::ExecutionProperties& properties) const{
    auto* result = this->child->Execute(properties);

    for (const auto& expression : this->resultExpressions)
      result->displayColumnNames.emplace_back(expression->name);

    Expressions::EvaluationContext context(Expressions::EvaluationContext::EvaluationContextType::SingleRow, properties.variables);

    for (const auto& row: result->rows) {
      QueryResult resultRow;

      for (const auto& expression : this->resultExpressions) {
        context.row = &row;
        auto field = expression->Evaluate(context);
        resultRow.AddColumn(field);
      }

      result->results.push_back(std::move(resultRow));
    }

    // ranges::sort(this->columnHeaders,
    //   [](const Headers::ColumnHeader& a, const Headers::ColumnHeader& b) {
    //       return a.ordinalPosition < b.ordinalPosition;
    //   }
    // );

    return result;
  }

  ExecutionResult * PhysicalProject::ExecuteConstantStatement(const DatabaseEngine::ExecutionProperties& properties)const{
    auto* result = new ExecutionResult();

    QueryResult resultRow;

    const Expressions::EvaluationContext context(Expressions::EvaluationContext::EvaluationContextType::Constant, properties.variables);

    for (const auto& expression : this->resultExpressions) {
      result->displayColumnNames.emplace_back(expression->name);

      auto field = expression->Evaluate(context);
      resultRow.AddColumn(field);
    }

    result->results.push_back(std::move(resultRow));

    return result;
  }

  PhysicalProject:: PhysicalProject(
    ExecutionNode *child,
    std::vector<Expressions::Expression*>& resultExpressions,
    std::vector<Headers::ColumnHeader>& columnHeaders)
    : resultExpressions(std::move(resultExpressions)), columnHeaders(std::move(columnHeaders)), child(child) {}

  PhysicalProject::~PhysicalProject() {
    for (const auto& expression : this->resultExpressions)
      delete expression;

    delete this->child;
  }

  ExecutionResult* PhysicalProject::Execute(const DatabaseEngine::ExecutionProperties& properties){
      return (this->child == nullptr)
        ? this->ExecuteConstantStatement(properties)
        : this->ExecuteStatement(properties);
  }

  void PhysicalProject::UpdateScanState(const DataTypes::RowIdentifier& rowId){
    this->child->UpdateScanState(rowId);
  }

  PhysicalFilter::PhysicalFilter(ExecutionNode *child, Expressions::Expression* filter)
        : filter(filter) , child(child) {}

  PhysicalFilter::~PhysicalFilter(){
    delete this->child;
    delete this->filter;
  }

  ExecutionResult* PhysicalFilter::Execute(const DatabaseEngine::ExecutionProperties& properties){
    auto* result = child->Execute(properties);

    if(dynamic_cast<PhysicalIndexScan*>(this->child) != nullptr
      || dynamic_cast<PhysicalIndexSeekRange*>(this->child) != nullptr)
      return result;

    Expressions::EvaluationContext context(Expressions::EvaluationContext::EvaluationContextType::SingleRow, properties.variables);

    std::vector<Pages::RowReference> filteredRows;
    filteredRows.reserve(result->rows.size() / 2);
    for (auto& row : result->rows) {
      context.row = &row;

      if (!this->filter->Evaluate(context).AsBool())
        continue;

      filteredRows.push_back(std::move(row));
    }

    result->rows = std::move(filteredRows);
    return result;
  }

  void PhysicalFilter::UpdateScanState(const DataTypes::RowIdentifier& rowId){
    this->child->UpdateScanState(rowId);
  }

  PhysicalTop::PhysicalTop(ExecutionNode* child, const BigInt top)
    : top(top), child(child){}

  PhysicalTop::~PhysicalTop(){
    delete this->child;
  }

  ExecutionResult * PhysicalTop::Execute(const DatabaseEngine::ExecutionProperties& properties){
    auto* result = this->child->Execute(properties);

    if (this->top > result->results.size())
      return result;

    result->results.erase(result->results.begin() + this->top, result->results.end());

    return result;
  }

  void PhysicalTop::UpdateScanState(const DataTypes::RowIdentifier& rowId){
    this->child->UpdateScanState(rowId);
  }

  PhysicalDistinct::PhysicalDistinct(ExecutionNode *child)
    : child(child){}

  PhysicalDistinct::~PhysicalDistinct(){
    delete this->child;
  }

  ExecutionResult * PhysicalDistinct::Execute(const DatabaseEngine::ExecutionProperties& properties){
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

  void PhysicalDistinct::UpdateScanState(const DataTypes::RowIdentifier& rowId){
    this->child->UpdateScanState(rowId);
  }

  bool PhysicalInsert::SortInsertsAscending(const Value& lhs, const Value& rhs){
    return lhs.GetColumnIndex() < rhs.GetColumnIndex();
  }

    std::vector<Value> PhysicalInsert::ConvertExpressionsToValues(
        const DatabaseEngine::ExecutionProperties& properties,
        const Int index
    ) const{
        auto& [expressions] = this->fields.at(index);

        std::vector<Value> values;
        values.reserve(expressions.size());
        for (int i = 0; i < expressions.size(); i++) {
            const Expressions::EvaluationContext context(
                Expressions::EvaluationContext::EvaluationContextType::Constant,
                properties.variables
            );

            auto value = expressions[i]->Evaluate(context);
            value.SetColumnIndex(this->columnsIndices.at(index));
            values.push_back(value);
        }

        ranges::sort(values, SortInsertsAscending);
        return values;
    }

  ExecutionResult* PhysicalInsert::InsertFromChild(DatabaseEngine::StorageTypes::Table* tablePtr, const DatabaseEngine::ExecutionProperties& properties)const{
    ExecutionResult* result = nullptr;
    bool canFetchMore = true;
    int rowCount = 0;

    while (canFetchMore) {
      result = this->child->Execute(properties);

      auto insertResult = tablePtr->BatchInsert(properties, result->results, this->columnsIndices);

      if (insertResult.code != Errors::RuntimeError::Ok) {
        result->message = insertResult.message;
        result->code = insertResult.code;
        return result;
      }

      canFetchMore = result->canFetchMore;
      rowCount += result->results.size();
    }

    result->message = "Rows inserted: " + std::to_string(rowCount);
    result->code = Errors::RuntimeError::Ok;
    return result;
  }

    ExecutionResult* PhysicalInsert::InsertFromFields(
      DatabaseEngine::StorageTypes::Table* tablePtr,
      const DatabaseEngine::ExecutionProperties& properties
    ) const{
        auto* result = new ExecutionResult();

        for (int i = 0;i < this->fields.size(); i++){
            auto values = this->ConvertExpressionsToValues(properties, i);

            const auto insertResult = tablePtr->InsertRow(properties, values);

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
  Statements::DataSource* table,
  std::vector<Statements::Inserts> &fields,
  ExecutionNode* child,
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

  ExecutionResult* PhysicalInsert::Execute(const DatabaseEngine::ExecutionProperties& properties){
    const auto* db =  this->server->UseDatabase(this->table->databaseId);

    auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

    return (this->child != nullptr)
        ? this->InsertFromChild(tablePtr, properties)
        : this->InsertFromFields(tablePtr, properties);
  }

  PhysicalHeapDelete::PhysicalHeapDelete(Statements::DataSource *table, Expressions::Expression *expression)
    : table(table), expression(expression) {}

  PhysicalHeapDelete::~PhysicalHeapDelete(){
    delete this->expression;
    delete this->table;
  }

  ExecutionResult * PhysicalHeapDelete::Execute(const DatabaseEngine::ExecutionProperties& properties){
    auto* result = new ExecutionResult();

    const auto* db = Network::Server::Get().UseDatabase(this->table->databaseId);

    const DatabaseEngine::StorageTypes::Table* tablePtr = db->OpenTable(this->table->ordinalPosition);

    tablePtr->HeapDelete(properties, this->expression);

    return result;
  }

  PhysicalIndexScanDelete::PhysicalIndexScanDelete(Statements::DataSource *table, Expressions::Expression *expression)
    : table(table), expression(expression) {}

  PhysicalIndexScanDelete::~PhysicalIndexScanDelete(){
      delete this->expression;
      delete this->table;
  }

  ExecutionResult * PhysicalIndexScanDelete::Execute(const DatabaseEngine::ExecutionProperties& properties){
    auto* result = new ExecutionResult();

    const auto* db =  this->server->UseDatabase(this->table->databaseId);

    auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

    tablePtr->ClusteredIndexScanDelete(properties, this->expression, state);

    return result;
  }

  PhysicalIndexSeekDelete::PhysicalIndexSeekDelete(Statements::DataSource *table, Expressions::Expression *expression)
    : table(table), expression(expression) {}

  PhysicalIndexSeekDelete::~PhysicalIndexSeekDelete(){
    delete this->expression;
    delete this->table;
  }

  ExecutionResult * PhysicalIndexSeekDelete::Execute(const DatabaseEngine::ExecutionProperties& properties){
    auto* result = new ExecutionResult();

    const auto* db =  this->server->UseDatabase(this->table->databaseId);

    auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

    tablePtr->ClusteredIndexSeekDelete(properties, this->expression, this->state);

    return result;
  }

  PhysicalHeapUpdate::PhysicalHeapUpdate(Statements::DataSource *table, Expressions::Expression *expression, std::vector<Statements::UpdateColumn*> & updates)
  : table(table), updates(std::move(updates)), expression(expression) {}

  PhysicalHeapUpdate::~PhysicalHeapUpdate(){
    delete this->expression;
    delete this->table;

    for (const auto* update : this->updates)
      delete update;
  }

  ExecutionResult* PhysicalHeapUpdate::Execute(const DatabaseEngine::ExecutionProperties& properties){
    auto* result = new ExecutionResult();

    const auto* db =  this->server->UseDatabase(this->table->databaseId);

    auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

    const auto insertResult = tablePtr->HeapUpdate(properties, this->expression, this->updates);

    result->code = insertResult.code;
    result->message = insertResult.message;

    return result;
  }

  PhysicalIndexScanUpdate::PhysicalIndexScanUpdate(Statements::DataSource *table, Expressions::Expression *expression, std::vector<Statements::UpdateColumn*> & updates)
  : table(table), updates(std::move(updates)), expression(expression) {}

  PhysicalIndexScanUpdate::~PhysicalIndexScanUpdate(){
    delete this->expression;
    delete this->table;

    for (const auto* update : this->updates)
      delete update;
  }

  ExecutionResult* PhysicalIndexScanUpdate::Execute(const DatabaseEngine::ExecutionProperties& properties){
    auto* result = new ExecutionResult();

    const auto* db =  this->server->UseDatabase(this->table->databaseId);

    auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

    const auto updateResult = tablePtr->ClusteredIndexScanUpdate(properties, this->expression, this->updates);

    result->code = updateResult.code;
    result->message = updateResult.message;

    return result;
  }

  PhysicalIndexSeekUpdate::PhysicalIndexSeekUpdate(Statements::DataSource *table, Expressions::Expression *expression, std::vector<Statements::UpdateColumn*> & updates)
    : table(table), updates(std::move(updates)), expression(expression) {}

  PhysicalIndexSeekUpdate::~PhysicalIndexSeekUpdate(){
    delete this->expression;
    delete this->table;

    for (const auto* update : this->updates)
      delete update;
  }

  ExecutionResult* PhysicalIndexSeekUpdate::Execute(const DatabaseEngine::ExecutionProperties& properties){
    const auto* db = this->server->UseDatabase(this->table->databaseId);

    auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

    DataTypes::Indexing::Key key;

    const auto updateResult = tablePtr->ClusteredIndexScanUpdate(properties, this->expression, this->updates);

    // tablePtr->ClusteredIndexSeekUpdate(this->expression, &key, &key, this->fields);

    return new ExecutionResult{
      updateResult.code,
      updateResult.message,
    };
  }

  PhysicalTableCreate::PhysicalTableCreate(
      const DataTypes::Guid& sessionId,
      Statements::DataSource*  table,
      std::vector<Statements::NewColumn*> &columns,
      Headers::Index& primaryKey,
      std::string& constraintName)
    : ExecutionNode(sessionId), table(table), constraintName(std::move(constraintName)),
      columns(std::move(columns)), primaryKey(std::move(primaryKey)) {}

  PhysicalTableCreate::~PhysicalTableCreate(){
    for (const auto& column: this->columns)
      delete column;
  }

  ExecutionResult* PhysicalTableCreate::Execute(const DatabaseEngine::ExecutionProperties& properties){
    if (this->session == nullptr || this->session->user == nullptr)
      return new ExecutionResult(
        Errors::RuntimeError::Error,
        "Failed to retrieve user session"
      );

    auto* db =  this->server->UseDatabase(this->table->databaseId);

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

    const auto& tables = this->catalog->SelectTables(this->table->databaseId);

    const int16_t& index = static_cast<int16_t>(tables.empty() ? 0 : tables[tables.size() - 1].ordinalPosition + 1);

    const auto tableResult = this->catalog->InsertTableToMasterDb(
        properties,
        this->table->databaseId,
        this->table->schemaId,
        this->table->name,
        index,
        false,
        this->session->user->name
    );

    const auto tableId = tableResult.primaryKey.AsInt(1);

    const auto tableStatsResult = this->catalog->InsertTableStatisticsToMasterDb(
      properties,
      tableId
    );

    auto* tablePtr = db->CreateTable(tableId, index, columnsPtrs, &this->primaryKey);

    Dictionary<int, Int> columnIdsDict;

    for (const auto& column: this->columns) {
      const auto columnResult =
          this->catalog->InsertColumnToMasterDb(
            properties,
            tableId,
            column->name.name,
            ColumnTypesDictionary.Get(Functions::String::NormalizeString(column->type.name)),
            column->type.size,
            column->type.decimal.precision,
            column->type.decimal.scale,
            column->isNullable,
            column->index,
            false,
            this->session->user->name
          );

      const auto columnId = columnResult.primaryKey.AsInt(1);

      const auto columnStatsResult = this->catalog->InsertColumnStatisticsToMasterDb(properties, columnId);

      columnIdsDict.Add(column->index, columnId);

      if (!column->defaultValue.IsNull() || column->defaultValue.Size() != 0) {
        const auto _ = this->catalog->InsertDefaultValuesToMasterDb(
          properties,
          columnId,
          column->defaultValue
        );
      }

      //insert identity columns
      if (column->identity == nullptr)
        continue;

      const auto _ = this->catalog->InsertIdentityColumnToMasterDb(
          properties,
          tableId,
          columnId,
          column->identity->seed,
          column->identity->incrementFactor,
          column->identity->seed,
          true,
          column->identity->cacheBlock
          );
    }

    const bool isConstraintEmpty = this->constraintName.empty();

    std::vector<Int> primaryKeyColumnIds;

    for (const auto& column: this->primaryKey.columns) {
      if (isConstraintEmpty)
        this->constraintName += this->constraintName.empty()
          ? "PK_" + this->columns[column]->name.name
          : "_" + this->columns[column]->name.name;

      primaryKeyColumnIds.push_back(columnIdsDict.Get(column));
    }

    if (primaryKeyColumnIds.empty()) {
      tablePtr->RetrieveColumnHeadersFromCatalog();
      tablePtr->RetrieveIdentityColumnsFromCatalog();
      return nullptr;
    }

    const auto indexResult = this->catalog->InsertIndexToMasterDb(
         properties,
        tableId,
        this->constraintName,
        true,
        false,
        this->session->user->name
      );

    const auto indexId = indexResult.primaryKey.AsInt(1);

    const auto constraintResult = this->catalog->InsertConstraintToMasterDb(
        properties,
      tableResult.primaryKey.AsInt(),
        this->constraintName,
        Headers::ConstraintType::PrimaryKey,
        false,
        &indexId,
        this->session->user->name
    );

    const auto constraintId = constraintResult.primaryKey.AsInt(1);

    for(int i = 0;i < primaryKeyColumnIds.size(); i++){
      auto _ = this->catalog->InsertIndexColumnToMasterDb(
        properties,
        indexResult.primaryKey.AsInt(),
        primaryKeyColumnIds[i],
        this->primaryKey.columns[i],
        true
      );


      _ = this->catalog->InsertConstraintColumnToMasterDb(
          properties,
          constraintId,
          primaryKeyColumnIds[i],
      this->primaryKey.columns[i]
        );
    }

    const auto indexStatsResult = this->catalog->InsertIndexStatisticsToMasterDb(
      properties,
      tableId,
      indexId
    );

    tablePtr->RetrieveColumnHeadersFromCatalog();
    tablePtr->RetrieveIdentityColumnsFromCatalog();

    return nullptr;
  }

  bool PhysicalOrderBy::CanBeSortedInMemory(const bool canFetchMore) const{
    return !canFetchMore && !this->UsesExternalStorage();
  }

  PhysicalOrderBy::PhysicalOrderBy(
      ExecutionNode *child,
      std::vector<Statements::OrderColumn*>& expressions
  ) : child(child),
      expressions(std::move(expressions)),
      priorityQueue(MergeComparator(&this->expressions)) {}

  PhysicalOrderBy::~PhysicalOrderBy(){
    for (const auto* column : this->expressions) {
      delete column;
    }

    delete this->child;
  }

  ExecutionResult* PhysicalOrderBy::Execute(const DatabaseEngine::ExecutionProperties& properties){
    auto* result = this->child->Execute(properties);

    // Case 1: In-memory sort (no external storage needed)
    if (!result->canFetchMore && !this->UsesExternalStorage()){
        SortingFunctions::OrderBy(result->results, this->expressions);
        return result;
    }

    // Case 2: Build phase - collect and sort batches
    while (result->canFetchMore || (result->canFetchMore == false && this->priorityQueue.Empty())) {
        SortingFunctions::OrderBy(result->results, this->expressions);

        DataTypes::RowIdentifier rowId;
        this->InsertPostProjectionResultsToTemporaryDatabase(properties, result, rowId);

        auto element = MergeElement(
            result->results.front(),
            static_cast<int>(this->priorityQueue.Size()),
            rowId
        );

        this->priorityQueue.Add(std::move(element));

        if (!result->canFetchMore)
            break;

        result = this->child->Execute(properties);
    }

    // Case 3: Merge phase - k-way merge
    result->results.clear();

    std::vector<std::vector<QueryResult>> batches;
    batches.resize(this->priorityQueue.Size());

    while (!this->priorityQueue.Empty()){
        auto top = this->priorityQueue.Top();
        this->priorityQueue.Remove();

        result->results.push_back(std::move(top.value));

        const auto batchId = top.batchId;

        // Lazy load batch if needed
        if (batches[batchId].empty()) {
            auto state = DatabaseEngine::ScanState();
            state.lastFetchedRowId = top.rowId;
            state.extentId = DatabaseEngine::Database::CalculateExtentId(state.lastFetchedRowId.pageId);

            auto* batchResult = this->StreamFromTemporaryDatabase(properties, state);
            batches[batchId] = std::move(batchResult->results);
            delete batchResult;
        }

        // Remove consumed element
        batches[batchId].erase(batches[batchId].begin());

        // Add next element from same batch if available
        if (!batches[batchId].empty()) {
            auto nextElement = MergeElement(
                batches[batchId].front(),
                batchId,
                top.rowId // Update with proper next rowId if needed
            );
            this->priorityQueue.Add(std::move(nextElement));
        }
    }

    return result;
  }

  void PhysicalOrderBy::UpdateScanState(const DataTypes::RowIdentifier& rowId){
    this->child->UpdateScanState(rowId);
  }

  PhysicalIndexCreate::PhysicalIndexCreate(
    const DataTypes::Guid& sessionId,
    Statements::DataSource *table,
    std::string &constraintName,
    vector<column_index_t> &columns)
    : ExecutionNode(sessionId), table(table), constraintName(std::move(constraintName)), columns(std::move(columns)) {}

  ExecutionResult * PhysicalIndexCreate::Execute(const DatabaseEngine::ExecutionProperties& properties){
    auto* result = new ExecutionResult();

    const auto* db = this->server->UseDatabase(this->table->databaseId);

    auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

    const auto columnsHeaders =this->catalog->SelectColumns(this->table->tableId);

    const auto indexResult =this->catalog->InsertIndexToMasterDb(
        properties,
        this->table->tableId,
        this->constraintName,
        false,
        false,
        this->session->user->name
    );

    const auto indexId = indexResult.primaryKey.AsInt(1);

    const auto constraintResult =this->catalog->InsertConstraintToMasterDb(
      properties,
      this->table->tableId,
      this->constraintName,
      Headers::ConstraintType::IndexKey,
      false,
      &indexId,
      this->session->user->name
    );

    const auto constraintId = constraintResult.primaryKey.AsInt(1);

    for (const auto& columnPos : this->columns) {
      const auto& header = columnsHeaders.at(columnPos);

      const auto indexColumnResult =
       this->catalog->InsertIndexColumnToMasterDb(
             properties,
            indexId,
            header.id,
            columnPos,
            true
      );

      const auto constraintColumnResult =
         this->catalog->InsertConstraintColumnToMasterDb(
                properties,
              constraintId,
              header.id,
          columnPos
        );
    }

    const auto indexStatsResult = this->catalog->InsertIndexStatisticsToMasterDb(
      properties,
      this->table->tableId,
      indexId
    );

    const auto indexPos = tablePtr->CreateNonClusteredIndex(this->columns);

    const auto pages = 1;
    tablePtr->NonClusteredIndexInsertExistingRows(indexPos, pages);

    //if there are rows in the table update the index
    //do stuff here

    return result;
  }
}
