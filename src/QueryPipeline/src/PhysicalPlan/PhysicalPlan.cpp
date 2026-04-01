#include "../../include/PhysicalPlan.h"

#include <iostream>
#include <utility>

#include "ValidationMessages.h"
#include "../../../CoreEngine/include/Database.h"
#include "../../../CoreEngine/include/SystemDatabases/SystemCatalog.h"
#include "../../../Server/include/Server.h"
#include "../../../Systemic/include/Functions/StringFunctions.h"
#include "../../../CoreEngine/include/Algorithms/Sort/SortingFunctions.h"
#include "../../../CoreEngine/include/ScanState.h"
#include "../../../CoreEngine/include/DataStorage/Table.h"
#include "../../../Systemic/include/DataTypes/DataTypes.StaticData.h"
#include "Contexts/ExecutionContext.h"
#include "SystemDatabases/TemporaryDatabase.h"

namespace QueryPipeline::PhysicalPlan {
  // ExecutionResult::ExecutionResult(){
  //   this->status.code = Errors::RuntimeError::Ok;
  //   this->canFetchMore = false;
  // }

  ExecutionResult::ExecutionResult(const CoreEngine::ExecutionContext& context)
      : status(context.GetAllocator()){
      this->canFetchMore = false;
      this->results.SetAllocator(context.GetAllocator());
      this->rows.SetAllocator(context.GetAllocator());
      this->columns.SetAllocator(context.GetAllocator());
      this->displayColumnNames.SetAllocator(context.GetAllocator());
  }

  ExecutionResult::ExecutionResult(const Errors::RuntimeError &code, const DataTypes::String& message) {
    this->status.code = code;
    this->status.message = message;
    this->canFetchMore = false;
  }

  ExecutionResult::ExecutionResult(
      const Errors::RuntimeError& code,
      const DataTypes::StringView& message,
      const ::Memory::IAllocator* allocator
    ){
      this->status.code = code;
      this->status.message = DataTypes::String(message, allocator);
      this->canFetchMore = false;
  }

  ExecutionResult::ExecutionResult(ExecutionResult&& other) noexcept{
      this->status = std::move(other.status);
      this->canFetchMore = other.canFetchMore;
      this->results = std::move(other.results);
      this->rows = std::move(other.rows);
      this->columns = std::move(other.columns);
      this->displayColumnNames = std::move(other.displayColumnNames);
  }

  ExecutionResult& ExecutionResult::operator=(ExecutionResult&& other) noexcept{
      if (this == &other) return *this;
      this->status = std::move(other.status);
      this->canFetchMore = other.canFetchMore;
      this->results = std::move(other.results);
      this->rows = std::move(other.rows);
      this->columns = std::move(other.columns);
      this->displayColumnNames = std::move(other.displayColumnNames);
      return *this;
  }

  ExecutionResult::~ExecutionResult() = default;

  bool ExecutionResult::IsOk() const {
    return this->status.code == Errors::RuntimeError::Ok;
  }

  ExecutionNode::ExecutionNode() {
    this->catalog = &CoreEngine::SystemCatalog::Get();
    this->server = &Network::Server::Get();
    this->session = nullptr;
    this->temporaryTableId = INVALID_TABLE_ID;
  }

  ExecutionNode::ExecutionNode(const DataTypes::Guid &currentSessionId){
    this->sessionId = currentSessionId;
    this->catalog = &CoreEngine::SystemCatalog::Get();
    this->server = &Network::Server::Get();
    this->session = this->server->GetSession(this->sessionId);
    this->temporaryTableId = INVALID_TABLE_ID;
  }

  void ExecutionNode::InsertToTemporaryDatabase(const std::vector<Pages::RowReference>& rows){

  }

  void ExecutionNode::InsertPostProjectionResultsToTemporaryDatabase(
    const CoreEngine::ExecutionContext& context,
    ExecutionResult& result,
    DataTypes::RowIdentifier& firstRowId
  ){
    static auto& tempDb = CoreEngine::TemporaryDatabase::Get();

    auto* table = (this->temporaryTableId == INVALID_TABLE_ID)
          ? tempDb.CreateTable()
          : tempDb.OpenTable(this->temporaryTableId);

    //table ordinal and table id are the same rn
    this->temporaryTableId = table->GetTableId();

    const auto& columns = table->GetColumns();
    result.status = table->BatchInsert(context, result.results);

    firstRowId = result.status.rowId;
  }

  ExecutionResult ExecutionNode::StreamFromTemporaryDatabase(
    const CoreEngine::ExecutionContext& context,
    CoreEngine::ScanState& state
  ) const
  {
    static auto& tempDb = CoreEngine::TemporaryDatabase::Get();

    auto result = ExecutionResult(context);
    result.rows.TrySetAllocator(context.GetAllocator());

    if (this->temporaryTableId == INVALID_TABLE_ID)
      return result;

    const auto* table = tempDb.OpenTable(this->temporaryTableId);

    table->TemporaryDatabaseHeapScan(&result.rows, state, context.GetBatchSize());

    result.results.Reserve(result.rows.Size());

    for (const auto& row: result.rows){
      result.results.Push(row.Materialize(context.GetAllocator()));
    }

    return result;
  }

  void ExecutionNode::UpdateScanState(const DataTypes::RowIdentifier& rowId){ }

  bool ExecutionNode::UsesExternalStorage() const{ return this->temporaryTableId != INVALID_TABLE_ID; }

  PhysicalDeclareVariable::PhysicalDeclareVariable(const DataTypes::Guid &currentSessionId, Variable& variable, Expressions::Expression* expression)
    : ExecutionNode(currentSessionId), variable(std::move(variable)), expression(expression){}

  ExecutionResult PhysicalDeclareVariable::Execute(const CoreEngine::ExecutionContext& context) {
    auto result = ExecutionResult(context);

    const Expressions::EvaluationContext evaluationContext(
        Expressions::EvaluationContext::EvaluationContextType::Constant,
        context
    );

    auto value = this->expression->Evaluate(evaluationContext);
    this->variable.SetValue(value);

    if (!this->server->AddOrSetVariable(this->sessionId, this->variable)){
        result.status = Errors::RuntimeStatus(
            Errors::RuntimeError::Error,
            Messages::FAILED_TO_ADD_VARIABLE,
            context.GetAllocator()
        );
        return result;
    }

    result.status = Errors::RuntimeStatus(
        Errors::RuntimeError::Ok,
        Messages::ADDED_VARIABLE(this->variable.GetName().ToView(), context.GetAllocator())
    );
    return result;
  }

  PhysicalCreateUser::PhysicalCreateUser(DataTypes::String& username, DataTypes::String& password, DataTypes::String& role)
   : username(std::move(username)), password(std::move(password)), roleName(std::move(role)) {}

  ExecutionResult PhysicalCreateUser::Execute(const CoreEngine::ExecutionContext& context) {
    auto result = ExecutionResult(context);
    result.rows.TrySetAllocator(context.GetAllocator());

    if (!this->server->CreateUser(context, this->username, this->password, this->roleName))
        result.status = Errors::RuntimeStatus(
            Errors::RuntimeError::Error,
            Messages::FAILED_TO_CREATE_USER,
            context.GetAllocator()
        );

    return result;
  }

  PhysicalGrantRole::PhysicalGrantRole(const DataTypes::Guid& sessionId, DataTypes::String& username, DataTypes::String& roleName)
    : ExecutionNode(sessionId), username(std::move(username)), roleName(std::move(roleName)) {}

  ExecutionResult PhysicalGrantRole::Execute(const CoreEngine::ExecutionContext& context) {
    auto result = ExecutionResult(context);
    result.rows.TrySetAllocator(context.GetAllocator());

    const auto* role = this->server->GetRole(this->roleName);

    if (role == nullptr) {
        result.status = Errors::RuntimeStatus(
            Errors::RuntimeError::Error,
            Messages::FAILED_TO_GET_ROLE(this->roleName.ToView(), context.GetAllocator())
        );
        return result;
    }

    result.status = this->server->GrantRole(context, this->sessionId, this->username, role);
    return result;
  }

  PhysicalCreateDatabase::PhysicalCreateDatabase(const DataTypes::Guid& sessionId, DataTypes::String& name) : ExecutionNode(sessionId), dbName(std::move(name)){}

  ExecutionResult PhysicalCreateDatabase::Execute(const CoreEngine::ExecutionContext& context){
    if (this->session == nullptr || this->session->user == nullptr)
      return ExecutionResult(Errors::RuntimeError::Error, Messages::FAILED_TO_RETRIEVE_USER_SESSION, context.GetAllocator());

    const auto path = DataTypes::String::Concat(context.GetAllocator(), this->dbName, Constants::DATA_FILE_EXTENSION);

    const auto result = this->catalog->InsertDbToMasterDb(
        context,
    this->dbName.ToView(),
    path.ToView(),
    false,
    this->session->user->name.ToView()
    );

    const auto databaseId = result.primaryKey.AsInt();

    const auto _ = this->catalog->InsertSchemaToMasterDb(context, databaseId, Constants::DEFAULT_SCHEMA_NAME);

    CoreEngine::CreateDatabase(databaseId, this->dbName);

    return ExecutionResult(context);
  }

  PhysicalUseDatabase::PhysicalUseDatabase(const DataTypes::Guid &sessionId, const Int databaseId)
    : sessionId(sessionId), databaseId(databaseId){}

  ExecutionResult PhysicalUseDatabase::Execute(const CoreEngine::ExecutionContext& context) {
    auto result = ExecutionResult(context);
    result.rows.TrySetAllocator(context.GetAllocator());

    if (this->server->UpdateSession(this->sessionId, this->databaseId)) {
        result.status = Errors::RuntimeStatus(
            Errors::RuntimeError::Ok,
            Messages::USE_DATABASE_SUCCESS,
            context.GetAllocator()
        );
        return result;
    }

    result.status = Errors::RuntimeStatus(
        Errors::RuntimeError::Error,
        Messages::USE_DATABASE_FAIL,
        context.GetAllocator()
    );
    return result;
  }

PhysicalSchemaCreate::PhysicalSchemaCreate(const DataTypes::Guid& sessionId, const Int databaseId, DataTypes::String& schemaName)
  : ExecutionNode(sessionId), schemaName(std::move(schemaName)) ,databaseId(databaseId) {}

  ExecutionResult PhysicalSchemaCreate::Execute(const CoreEngine::ExecutionContext& context){
    if (this->session == nullptr || this->session->user == nullptr)
      return ExecutionResult(
          Errors::RuntimeError::Error,
          Messages::FAILED_TO_RETRIEVE_USER_SESSION,
          context.GetAllocator()
        );

    const auto insertResult = this->catalog->InsertSchemaToMasterDb(
        context,
        this->databaseId,
        this->schemaName.ToView(),
        this->session->user->name.ToView()
    );
    return ExecutionResult(insertResult.code, insertResult.message);
  }

  PhysicalTableScan::PhysicalTableScan(Statements::DataSource* table, Expressions::Expression* expression)
    : table(table), expression(expression) {}

  PhysicalTableScan::~PhysicalTableScan() = default;

  ExecutionResult PhysicalTableScan::Execute(const CoreEngine::ExecutionContext& context){
    auto result = ExecutionResult(context);

    const auto* db = this->server->UseDatabase(context, this->table->databaseId);

    const auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

    tablePtr->GetConstantColumns(&result.columns);

    tablePtr->HeapScan(context, &result.rows, this->state);

    result.canFetchMore = this->state.canFetchMore;

    if (result.canFetchMore == false)
      this->state.Reset();

    return result;
  }

  void PhysicalTableScan::UpdateScanState(const DataTypes::RowIdentifier& rowId){
    this->state.lastFetchedRowId = rowId;
  }

  PhysicalIndexScan::PhysicalIndexScan(Statements::DataSource* table, const bool isClustered)
    : table(table), expression(nullptr), isClustered(isClustered) {}

    PhysicalIndexScan::PhysicalIndexScan(
        Statements::DataSource *table,
        Expressions::Expression *expression,
        const bool isClustered
    ): table(table), expression(expression), isClustered(isClustered) {}

    PhysicalIndexScan::~PhysicalIndexScan() = default;

  ExecutionResult PhysicalIndexScan::Execute(const CoreEngine::ExecutionContext& context){
    auto result = ExecutionResult(context);

    const auto* db =  this->server->UseDatabase(context, this->table->databaseId);

    auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

    tablePtr->GetConstantColumns(&result.columns);

    if (this->isClustered) {
      tablePtr->ClusteredIndexScan(context, &result.rows, this->state, this->expression);
      result.canFetchMore = this->state.canFetchMore;

      if (result.canFetchMore == false)
        this->state.Reset();

      return result;
    }

    tablePtr->NonClusteredIndexScan(context, &result.rows, 0, this->state, this->expression);

    result.canFetchMore = this->state.canFetchMore;
    if (result.canFetchMore == false)
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

  PhysicalIndexSeek::~PhysicalIndexSeek() = default;

  ExecutionResult PhysicalIndexSeek::Execute(const CoreEngine::ExecutionContext& context){
    auto result = ExecutionResult(context);

    const auto* db = Network::Server::Get().UseDatabase(context, this->table->databaseId);

    auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

    tablePtr->GetConstantColumns(&result.columns);

    //select if to use clustered or non clustered index here
    tablePtr->ClusteredIndexSeek(context, &result.rows, this->key, this->expression);

    return result;
  }

  PhysicalIndexSeekRange::PhysicalIndexSeekRange(
    Statements::DataSource* table,
    DataTypes::Indexing::Key& minKey,
    DataTypes::Indexing::Key& maxKey,
    Expressions::Expression* expression
  )
    : table(table), expression(expression), minKey(std::move(minKey)), maxKey(std::move(maxKey)) {}

  PhysicalIndexSeekRange::~PhysicalIndexSeekRange() = default;

  ExecutionResult PhysicalIndexSeekRange::Execute(const CoreEngine::ExecutionContext& context){
    auto result = ExecutionResult(context);

    const auto* db = Network::Server::Get().UseDatabase(context, this->table->databaseId);

    auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

    tablePtr->GetConstantColumns(&result.columns);

    //select if to use clustered or non clustered index here
    tablePtr->ClusteredIndexSeekRange(context, &result.rows, this->minKey, this->maxKey, this->expression);

    return result;
  }

  ExecutionResult PhysicalProject::ExecuteStatement(const CoreEngine::ExecutionContext& context) const{
    auto result = this->child->Execute(context);

    for (const auto& expression : this->resultExpressions)
      result.displayColumnNames.Push(expression->name);

    Expressions::EvaluationContext evaluationContext(
        Expressions::EvaluationContext::EvaluationContextType::SingleRow,
        context
    );

    result.results.Reserve(result.rows.Size());
    for (const auto& row: result.rows) {
      QueryResult resultRow(context.GetAllocator());

      for (const auto& expression : this->resultExpressions) {
        evaluationContext.row = &row;
        resultRow.AddColumn(expression->Evaluate(evaluationContext));
      }

      result.results.Push(std::move(resultRow));
    }

    // ranges::sort(this->columnHeaders,
    //   [](const Headers::ColumnHeader& a, const Headers::ColumnHeader& b) {
    //       return a.ordinalPosition < b.ordinalPosition;
    //   }
    // );

    return result;
  }

  ExecutionResult PhysicalProject::ExecuteConstantStatement(const CoreEngine::ExecutionContext& context)const{
    auto result = ExecutionResult(context);
    QueryResult resultRow(context.GetAllocator());

    const Expressions::EvaluationContext evaluationContext(
        Expressions::EvaluationContext::EvaluationContextType::Constant,
        context
    );

    for (const auto& expression : this->resultExpressions) {
      result.displayColumnNames.Push(expression->name);

      auto field = expression->Evaluate(evaluationContext);
      resultRow.AddColumn(field);
    }

    result.results.Push(std::move(resultRow));

    return result;
  }

  PhysicalProject:: PhysicalProject(
    ExecutionNode *child,
    std::vector<Expressions::Expression*>& resultExpressions,
    std::vector<Headers::ColumnHeader>& columnHeaders)
    : resultExpressions(std::move(resultExpressions)), columnHeaders(std::move(columnHeaders)), child(child) {}

  PhysicalProject::~PhysicalProject() = default;

  ExecutionResult PhysicalProject::Execute(const CoreEngine::ExecutionContext& context){
      return (this->child == nullptr)
        ? this->ExecuteConstantStatement(context)
        : this->ExecuteStatement(context);
  }

  void PhysicalProject::UpdateScanState(const DataTypes::RowIdentifier& rowId){
    this->child->UpdateScanState(rowId);
  }

  PhysicalFilter::PhysicalFilter(ExecutionNode *child, Expressions::Expression* filter)
        : filter(filter) , child(child) {}

  PhysicalFilter::~PhysicalFilter() = default;

  ExecutionResult PhysicalFilter::Execute(const CoreEngine::ExecutionContext& context){
    auto result = child->Execute(context);

    if(dynamic_cast<PhysicalIndexScan*>(this->child) != nullptr
      || dynamic_cast<PhysicalIndexSeekRange*>(this->child) != nullptr)
      return result;

    Expressions::EvaluationContext evaluationContext(
        Expressions::EvaluationContext::EvaluationContextType::SingleRow,
        context
    );

    DataStructures::PolymorphicArray<Pages::RowReference> filteredRows(context.GetAllocator(), result.rows.Size() / 2);
    for (auto& row : result.rows) {
      evaluationContext.row = &row;

      if (!this->filter->Evaluate(evaluationContext).AsBool())
        continue;

      filteredRows.Push(std::move(row));
    }

    result.rows = std::move(filteredRows);
    return result;
  }

  void PhysicalFilter::UpdateScanState(const DataTypes::RowIdentifier& rowId){
    this->child->UpdateScanState(rowId);
  }

  PhysicalTop::PhysicalTop(ExecutionNode* child, const BigInt top)
    : top(top), child(child){}

  PhysicalTop::~PhysicalTop() = default;

  ExecutionResult PhysicalTop::Execute(const CoreEngine::ExecutionContext& context){
    auto result = this->child->Execute(context);

    if (this->top > result.results.Size())
      return result;

    result.results.RemoveFrom(this->top);

    return result;
  }

  void PhysicalTop::UpdateScanState(const DataTypes::RowIdentifier& rowId){
    this->child->UpdateScanState(rowId);
  }

  PhysicalDistinct::PhysicalDistinct(ExecutionNode *child)
    : child(child){}

  PhysicalDistinct::~PhysicalDistinct() = default;

  ExecutionResult PhysicalDistinct::Execute(const CoreEngine::ExecutionContext& context){
    auto result = this->child->Execute(context);

    DataStructures::PolymorphicArray<QueryResult> results;

    HashSet<int64_t> computedHashes;

    for (auto& row : result.results) {
      const auto hash = row.ComputeHash();

      //if no collision occurs
      if (!computedHashes.Contains(hash)) {
        results.Push(std::move(row));
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
        results.Push(std::move(row));
    }

    result.results = std::move(results);

    return result;
  }

  void PhysicalDistinct::UpdateScanState(const DataTypes::RowIdentifier& rowId){
    this->child->UpdateScanState(rowId);
  }

  bool PhysicalInsert::SortInsertsAscending(const Value& lhs, const Value& rhs){
    return lhs.GetColumnIndex() < rhs.GetColumnIndex();
  }

    DataStructures::PolymorphicArray<Value> PhysicalInsert::ConvertExpressionsToValues(
        const CoreEngine::ExecutionContext& context,
        const Int index
    ) const{
        auto& [expressions] = this->fields.at(index);

        DataStructures::PolymorphicArray<Value> values(context.GetAllocator());
        values.Reserve(expressions.size());
        for (int i = 0; i < expressions.size(); i++) {
            const Expressions::EvaluationContext evaluationContext(
                Expressions::EvaluationContext::EvaluationContextType::Constant,
                context
            );

            auto value = expressions[i]->Evaluate(evaluationContext);
            value.SetColumnIndex(this->columnsIndices.at(index));
            values.Push(std::move(value));
        }

        std::ranges::sort(values, SortInsertsAscending);
        return values;
    }

  ExecutionResult PhysicalInsert::InsertFromChild(CoreEngine::StorageTypes::Table* tablePtr, const CoreEngine::ExecutionContext& context)const{
    bool canFetchMore = true;
    int rowCount = 0;

    while (canFetchMore) {
      auto result = this->child->Execute(context);

      result.status = tablePtr->BatchInsert(context, result.results);

      if (!result.status.IsOk())
        return result;

      canFetchMore = result.canFetchMore;
      rowCount += result.results.Size();
    }

    return ExecutionResult(
        Errors::RuntimeError::Ok,
        Messages::INSERT_ROWS_FROM_CHILD_QUERY(rowCount, context.GetAllocator())
    );
  }

    ExecutionResult PhysicalInsert::InsertFromFields(
      CoreEngine::StorageTypes::Table* tablePtr,
      const CoreEngine::ExecutionContext& context
    ) const{
        auto result = ExecutionResult(context);

        for (int i = 0;i < this->fields.size(); i++){
            const auto values = this->ConvertExpressionsToValues(context, i);

            result.status = tablePtr->InsertRow(context, values);
            if (! result.status.IsOk())
              return result;
        }

        result.status = Errors::RuntimeStatus(
            Errors::RuntimeError::Ok,
            Messages::INSERT_ROWS_FROM_FIELDS(static_cast<Int>(this->fields.size()), context.GetAllocator())
        );
        return result;
    }

    PhysicalInsert::PhysicalInsert(
        Statements::DataSource* table,
        std::vector<Statements::Inserts> &fields,
        ExecutionNode* child,
        std::vector<column_index_t>& columnsIndices
    ): table(table), fields(std::move(fields)), child(child), columnsIndices(std::move(columnsIndices)) {}

    PhysicalInsert::~PhysicalInsert() = default;

    ExecutionResult PhysicalInsert::Execute(const CoreEngine::ExecutionContext& context){
        const auto* db =  this->server->UseDatabase(context, this->table->databaseId);

        auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

        return (this->child != nullptr)
            ? this->InsertFromChild(tablePtr, context)
            : this->InsertFromFields(tablePtr, context);
    }

    PhysicalHeapDelete::PhysicalHeapDelete(Statements::DataSource *table, Expressions::Expression *expression)
    : table(table), expression(expression) {}

    PhysicalHeapDelete::~PhysicalHeapDelete() = default;

    ExecutionResult PhysicalHeapDelete::Execute(const CoreEngine::ExecutionContext& context){
        auto result = ExecutionResult(context);

        const auto* db = Network::Server::Get().UseDatabase(context, this->table->databaseId);

        const CoreEngine::StorageTypes::Table* tablePtr = db->OpenTable(this->table->ordinalPosition);

        tablePtr->HeapDelete(context, this->expression);

        return result;
    }

    PhysicalIndexScanDelete::PhysicalIndexScanDelete(Statements::DataSource *table, Expressions::Expression *expression)
    : table(table), expression(expression) {}

    PhysicalIndexScanDelete::~PhysicalIndexScanDelete() = default;

    ExecutionResult PhysicalIndexScanDelete::Execute(const CoreEngine::ExecutionContext& context){
        auto result = ExecutionResult(context);

        const auto* db =  this->server->UseDatabase(context, this->table->databaseId);

        auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

        tablePtr->ClusteredIndexScanDelete(context, this->expression, state);

        return result;
    }

    PhysicalIndexSeekDelete::PhysicalIndexSeekDelete(Statements::DataSource *table, Expressions::Expression *expression)
    : table(table), expression(expression) {}

    PhysicalIndexSeekDelete::~PhysicalIndexSeekDelete() = default;

    ExecutionResult PhysicalIndexSeekDelete::Execute(const CoreEngine::ExecutionContext& context){
        auto result = ExecutionResult(context);

        const auto* db =  this->server->UseDatabase(context, this->table->databaseId);

        auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

        tablePtr->ClusteredIndexSeekDelete(context, this->expression, this->state);

        return result;
    }

    PhysicalHeapUpdate::PhysicalHeapUpdate(
        Statements::DataSource *table,
        Expressions::Expression *expression,
        std::vector<Expressions::Expression*>& updates
    ) : table(table), updates(std::move(updates)), expression(expression) {}

    PhysicalHeapUpdate::~PhysicalHeapUpdate() = default;

    ExecutionResult PhysicalHeapUpdate::Execute(const CoreEngine::ExecutionContext& context){
        auto result = ExecutionResult(context);

        const auto* db =  this->server->UseDatabase(context, this->table->databaseId);
        auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

        result.status = tablePtr->HeapUpdate(context, this->expression, this->updates);
        return result;
    }

    PhysicalIndexScanUpdate::PhysicalIndexScanUpdate(
      Statements::DataSource *table,
      Expressions::Expression *expression,
      std::vector<Expressions::Expression*>& updates
    ): table(table), updates(std::move(updates)), expression(expression) {}

    PhysicalIndexScanUpdate::~PhysicalIndexScanUpdate() = default;

    ExecutionResult PhysicalIndexScanUpdate::Execute(const CoreEngine::ExecutionContext& context){
        auto result = ExecutionResult(context);

        const auto* db =  this->server->UseDatabase(context, this->table->databaseId);
        auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

        result.status = tablePtr->ClusteredIndexScanUpdate(context, this->expression, this->updates);
        return result;
    }

    PhysicalIndexSeekUpdate::PhysicalIndexSeekUpdate(
      Statements::DataSource *table,
      Expressions::Expression *expression,
      std::vector<Expressions::Expression*>& updates
    ): table(table), updates(std::move(updates)), expression(expression) {}

  PhysicalIndexSeekUpdate::~PhysicalIndexSeekUpdate() = default;

  ExecutionResult PhysicalIndexSeekUpdate::Execute(const CoreEngine::ExecutionContext& context){
    const auto* db = this->server->UseDatabase(context, this->table->databaseId);

    auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

    DataTypes::Indexing::Key key;

    const auto updateResult = tablePtr->ClusteredIndexScanUpdate(context, this->expression, this->updates);

    // tablePtr->ClusteredIndexSeekUpdate(this->expression, &key, &key, this->fields);

    return ExecutionResult(updateResult.code, updateResult.message);
  }

  PhysicalTableCreate::PhysicalTableCreate(
      const DataTypes::Guid& sessionId,
      Statements::DataSource*  table,
      std::vector<Statements::NewColumn*> &columns,
      const Headers::Index& primaryKey,
      DataTypes::String& constraintName
    ): ExecutionNode(sessionId), table(table), constraintName(std::move(constraintName)),
      columns(std::move(columns)), primaryKey(primaryKey) {}

  PhysicalTableCreate::~PhysicalTableCreate() = default;

  ExecutionResult PhysicalTableCreate::Execute(const CoreEngine::ExecutionContext& context){
    if (this->session == nullptr || this->session->user == nullptr)
      return ExecutionResult(
          Errors::RuntimeError::Error,
          Messages::FAILED_TO_RETRIEVE_USER_SESSION,
          context.GetAllocator()
    );

    auto* db =  this->server->UseDatabase(context, this->table->databaseId);

    const auto& tables = this->catalog->SelectTables(context.GetAllocator(), this->table->databaseId);

    const auto index = static_cast<SmallInt>(tables.empty() ? 0 : tables[tables.size() - 1].ordinalPosition + 1);

    const auto tableResult = this->catalog->InsertTableToMasterDb(
        context,
        this->table->databaseId,
        this->table->schemaId,
        this->table->name.ToView(),
        index,
        false,
        this->session->user->name.ToView()
    );

    const auto tableId = tableResult.primaryKey.AsInt(1);

    auto* tablePtr = db->CreateTable(tableId, index);

    const auto tableStatsResult = this->catalog->InsertTableStatisticsToMasterDb(
      context,
      tableId
    );

    Dictionary<int, Int> columnIdsDict;
    for (const auto* column: this->columns){
        const auto normalizedTableName = DataTypes::String::Normalize(column->type.name);
        auto* columnPtr =
            tablePtr->AddColumn(
                column->name.name.ToView(),
                ColumnTypesDictionary.Get(normalizedTableName.ToView()),
                column->type.size,
                column->index,
                column->isNullable
            );

      const auto columnResult =
          this->catalog->InsertColumnToMasterDb(
            context,
            tableId,
            column->name.name.ToView(),
            ColumnTypesDictionary.Get(normalizedTableName.ToView()),
            column->type.size,
            column->type.decimal.precision,
            column->type.decimal.scale,
            column->isNullable,
            column->index,
            false,
            this->session->user->name.ToView()
          );

      const auto columnId = columnResult.primaryKey.AsInt(1);

      const auto columnStatsResult = this->catalog->InsertColumnStatisticsToMasterDb(context, columnId);

      columnIdsDict.Add(column->index, columnId);

      if (!column->defaultValue.IsNull() || column->defaultValue.Size() != 0) {
        const auto _ = this->catalog->InsertDefaultValuesToMasterDb(
          context,
          columnId,
          column->defaultValue
        );
      }

      //insert identity columns
      if (column->identity == nullptr) continue;

      const auto _ = this->catalog->InsertIdentityColumnToMasterDb(
          context,
          tableId,
          columnId,
          column->identity->seed,
          column->identity->incrementFactor,
          column->identity->seed,
          true,
          static_cast<Int>(column->identity->cacheBlock)
        );
    }

    DataStructures::PolymorphicArray<Int> primaryKeyColumnIdsArray(context.GetAllocator(), this->primaryKey.columns.Size());
    for (const auto& column: this->primaryKey.columns) {
      if (this->constraintName.Empty()){
          this->constraintName.SetAllocator(context.GetAllocator());

          const auto& columnName = this->columns[column]->name.name;
          this->constraintName = DataTypes::String::Concat(context.GetAllocator(), "PK_", columnName, "_", columnName);

      }
      primaryKeyColumnIdsArray.Push(columnIdsDict.Get(column));
    }

    static constexpr DataTypes::StringView TABLE_CREATED_MESSAGE = "Table created successfully";
    if (primaryKeyColumnIdsArray.Empty()) {
        tablePtr->RetrieveColumnHeadersFromCatalog(context.GetAllocator());
        tablePtr->RetrieveIdentityColumnsFromCatalog(context.GetAllocator());
        return ExecutionResult(Errors::RuntimeError::Ok, TABLE_CREATED_MESSAGE, context.GetAllocator());
    }

    const auto indexResult = this->catalog->InsertIndexToMasterDb(
         context,
        tableId,
        this->constraintName.ToView(),
        true,
        false,
        this->session->user->name.ToView()
      );

    const auto indexId = indexResult.primaryKey.AsInt(1);

    const auto constraintResult = this->catalog->InsertConstraintToMasterDb(
        context,
      tableResult.primaryKey.AsInt(),
        this->constraintName.ToView(),
        Headers::ConstraintType::PrimaryKey,
        false,
        &indexId,
        this->session->user->name.ToView()
    );

    const auto constraintId = constraintResult.primaryKey.AsInt(1);

    for(int i = 0; i < primaryKeyColumnIdsArray.Size(); i++){
      auto _ = this->catalog->InsertIndexColumnToMasterDb(
        context,
        indexResult.primaryKey.AsInt(),
        primaryKeyColumnIdsArray[i],
        this->primaryKey.columns[i],
        true
      );


      _ = this->catalog->InsertConstraintColumnToMasterDb(
          context,
          constraintId,
          primaryKeyColumnIdsArray[i],
      this->primaryKey.columns[i]
        );
    }

    const auto indexStatsResult = this->catalog->InsertIndexStatisticsToMasterDb(
      context,
      tableId,
      indexId
    );


    tablePtr->RetrieveIndexesFromCatalog(context.GetAllocator());
    tablePtr->RetrieveColumnHeadersFromCatalog(context.GetAllocator());
    tablePtr->RetrieveIdentityColumnsFromCatalog(context.GetAllocator());

    return ExecutionResult(Errors::RuntimeError::Ok, TABLE_CREATED_MESSAGE, context.GetAllocator());
  }

  bool PhysicalOrderBy::CanBeSortedInMemory(const bool canFetchMore) const{
    return !canFetchMore && !this->UsesExternalStorage();
  }

  PhysicalOrderBy::PhysicalOrderBy(
      ExecutionNode *child,
      std::vector<Statements::OrderColumn*>& expressions
  )   : child(child)
   , expressions(std::move(expressions))
   , comparator(&this->expressions, nullptr)
   , priorityQueue(this->comparator){}

  PhysicalOrderBy::~PhysicalOrderBy() = default;

  ExecutionResult PhysicalOrderBy::Execute(const CoreEngine::ExecutionContext& context){
    if (!this->comparator.HasProperties())
        this->comparator.SetExecutionContext(&context);

    auto result = this->child->Execute(context);

    // Case 1: In-memory sort (no external storage needed)
    if (!result.canFetchMore && !this->UsesExternalStorage()){
        SortingFunctions::OrderBy(context, result.results, this->expressions);
        return result;
    }

    // Case 2: Build phase - collect and sort batches
    while (result.canFetchMore || (result.canFetchMore == false && this->priorityQueue.Empty())) {
        SortingFunctions::OrderBy(context, result.results, this->expressions);

        DataTypes::RowIdentifier rowId;
        this->InsertPostProjectionResultsToTemporaryDatabase(context, result, rowId);

        auto element = MergeElement(
            result.results[0],
            static_cast<int>(this->priorityQueue.Size()),
            rowId
        );

        this->priorityQueue.Add(std::move(element));

        if (!result.canFetchMore)
            break;

        result = this->child->Execute(context);
    }

    // Case 3: Merge phase - k-way merge
    result.results.Clear();

    DataStructures::PolymorphicArray<DataStructures::PolymorphicArray<QueryResult>> batches;
    batches.Resize(this->priorityQueue.Size());

    while (!this->priorityQueue.Empty()){
        auto top = this->priorityQueue.Top();
        this->priorityQueue.Remove();

        result.results.Push(std::move(top.value));

        const auto batchId = top.batchId;

        // Lazy load batch if needed
        if (batches[batchId].Empty()) {
            auto state = CoreEngine::ScanState();
            state.lastFetchedRowId = top.rowId;
            state.extentId = CoreEngine::Database::CalculateExtentId(state.lastFetchedRowId.pageId);

            auto batchResult = this->StreamFromTemporaryDatabase(context, state);
            batches[batchId] = std::move(batchResult.results);
        }

        // Remove consumed element
        batches[batchId].Remove(0);

        // Add next element from same batch if available
        if (!batches[batchId].Empty()) {
            auto nextElement = MergeElement(
                batches[batchId][0],
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
        DataTypes::String& constraintName,
        std::vector<column_index_t> &columns
    ): ExecutionNode(sessionId), table(table), constraintName(std::move(constraintName)), columns(std::move(columns)) {}

    ExecutionResult PhysicalIndexCreate::Execute(const CoreEngine::ExecutionContext& context){
        auto result = ExecutionResult(context);

        const auto* db = this->server->UseDatabase(context, this->table->databaseId);

        auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

        const auto columnsHeaders =this->catalog->SelectColumns(context.GetAllocator(), this->table->tableId);

        const auto indexResult =this->catalog->InsertIndexToMasterDb(
            context,
            this->table->tableId,
            this->constraintName.ToView(),
            false,
            false,
            this->session->user->name.ToView()
        );

        const auto indexId = indexResult.primaryKey.AsInt(1);

        const auto constraintResult =this->catalog->InsertConstraintToMasterDb(
            context,
            this->table->tableId,
            this->constraintName.ToView(),
            Headers::ConstraintType::IndexKey,
            false,
            &indexId,
            this->session->user->name.ToView()
        );

        const auto constraintId = constraintResult.primaryKey.AsInt(1);

        for (const auto& columnPos : this->columns) {
        const auto& header = columnsHeaders.at(columnPos);

        const auto indexColumnResult =
            this->catalog->InsertIndexColumnToMasterDb(
                context,
                indexId,
                header.id,
                columnPos,
                true
            );

        const auto constraintColumnResult =
        this->catalog->InsertConstraintColumnToMasterDb(
            context,
            constraintId,
            header.id,
            columnPos
            );
        }

        const auto indexStatsResult = this->catalog->InsertIndexStatisticsToMasterDb(
            context,
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