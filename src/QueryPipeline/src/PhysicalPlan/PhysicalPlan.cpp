#include "../../include/PhysicalPlan.h"

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
#include "../../../CoreEngine/include/Vectorization/Vectorization.h"
#include "BufferPool/StorageManager.h"

namespace QueryPipeline::PhysicalPlan {
    ExecutionResult::ExecutionResult(const CoreEngine::ExecutionContext& context)
        : status(context.GetAllocator()), displayColumnNames(context.GetAllocator()),
          columns(context.GetAllocator()),
          selectionVector(context.GetAllocator()->Allocate<CoreEngine::SelectionVector>()), canFetchMore(false){}

    ExecutionResult::ExecutionResult(const Errors::RuntimeError &code, const DataTypes::String& message)
        : status(code, message), selectionVector(nullptr),
          canFetchMore(false) {}

    ExecutionResult::ExecutionResult(
        const Errors::RuntimeError& code,
        const DataTypes::StringView& message,
        const ::Memory::IAllocator* allocator
    ) : status(code, DataTypes::String(message, allocator)),
        selectionVector(nullptr), canFetchMore(false) {}

    ExecutionResult::ExecutionResult(ExecutionResult&& other) noexcept{
        this->status = std::move(other.status);
        this->canFetchMore = other.canFetchMore;
        this->vectorBatch = std::move(other.vectorBatch);
        this->columns = std::move(other.columns);
        this->displayColumnNames = std::move(other.displayColumnNames);
        this->selectionVector = other.selectionVector;
    }

    ExecutionResult& ExecutionResult::operator=(ExecutionResult&& other) noexcept{
        if (this == &other) return *this;
        this->status = std::move(other.status);
        this->canFetchMore = other.canFetchMore;
        this->vectorBatch = std::move(other.vectorBatch);
        this->columns = std::move(other.columns);
        this->displayColumnNames = std::move(other.displayColumnNames);
        this->selectionVector = other.selectionVector;
        return *this;
    }

    bool ExecutionResult::IsOk() const {
        return this->status.code == Errors::RuntimeError::Ok;
    }

    PlanNode::PlanNode() {
        this->catalog = &CoreEngine::SystemCatalog::Get();
        this->server = &Network::Server::Get();
        this->session = nullptr;
        this->temporaryTableId = INVALID_TABLE_ID;
    }

    PlanNode::PlanNode(const DataTypes::Guid &currentSessionId){
        this->sessionId = currentSessionId;
        this->catalog = &CoreEngine::SystemCatalog::Get();
        this->server = &Network::Server::Get();
        this->session = this->server->GetSession(this->sessionId);
        this->temporaryTableId = INVALID_TABLE_ID;
    }

    void PlanNode::InsertToTemporaryDatabase(const DataStructures::PolymorphicArray<CoreEngine::StorageTypes::RID>& rows){

    }

  void PlanNode::InsertPostProjectionResultsToTemporaryDatabase(
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
      // result.status = table->BatchInsert(context, result.results);

      // firstRowId = result.status.rowId;
  }

  ExecutionResult PlanNode::StreamFromTemporaryDatabase(
      const CoreEngine::ExecutionContext& context,
      CoreEngine::ScanState& state
  ) const
  {
      static auto& tempDb = CoreEngine::TemporaryDatabase::Get();

      auto result = ExecutionResult(context);

      if (this->temporaryTableId == INVALID_TABLE_ID)
          return result;

      const auto* table = tempDb.OpenTable(this->temporaryTableId);

      // table->TemporaryDatabaseHeapScan(&result.rows, state, context.GetBatchSize());

      // result.results.Reserve(result.rows.Size());

      // for (const auto& row: result.rows)
      //   result.results.Push(row->Materialize(context.GetAllocator()));

      return result;
  }

  void PlanNode::UpdateScanState(const DataTypes::RowIdentifier& rowId){ }

  bool PlanNode::UsesExternalStorage() const{ return this->temporaryTableId != INVALID_TABLE_ID; }

  PhysicalCreateUser::PhysicalCreateUser(DataTypes::String& username, DataTypes::String& password, DataTypes::String& role)
      : username(std::move(username)), password(std::move(password)), roleName(std::move(role)) {}

  ExecutionResult PhysicalCreateUser::Execute(CoreEngine::ExecutionContext& context) {
      auto result = ExecutionResult(context);

      if (!this->server->CreateUser(context, this->username, this->password, this->roleName))
          result.status = Errors::RuntimeStatus(
              Errors::RuntimeError::Error,
              Messages::FAILED_TO_CREATE_USER,
              context.GetAllocator()
          );

      return result;
  }

  PhysicalGrantRole::PhysicalGrantRole(const DataTypes::Guid& sessionId, DataTypes::String& username, DataTypes::String& roleName)
      : PlanNode(sessionId), username(std::move(username)), roleName(std::move(roleName)) {}

  ExecutionResult PhysicalGrantRole::Execute(CoreEngine::ExecutionContext& context) {
      auto result = ExecutionResult(context);

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

  PhysicalCreateDatabase::PhysicalCreateDatabase(const DataTypes::Guid& sessionId, DataTypes::String& name) : PlanNode(sessionId), dbName(std::move(name)){}

  ExecutionResult PhysicalCreateDatabase::Execute(CoreEngine::ExecutionContext& context){
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

  ExecutionResult PhysicalUseDatabase::Execute(CoreEngine::ExecutionContext& context) {
      auto result = ExecutionResult(context);

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
      : PlanNode(sessionId), schemaName(std::move(schemaName)) ,databaseId(databaseId) {}

  ExecutionResult PhysicalSchemaCreate::Execute(CoreEngine::ExecutionContext& context){
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

PhysicalTableCreate::PhysicalTableCreate(
    const DataTypes::Guid& sessionId,
    Statements::DataSource*  table,
    DataStructures::PolymorphicArray<Statements::NewColumn*> &columns,
    const Headers::Index& primaryKey,
    DataTypes::String& constraintName
): PlanNode(sessionId), table(table), constraintName(std::move(constraintName)),
   columns(std::move(columns)), primaryKey(primaryKey) {}

  ExecutionResult PhysicalTableCreate::Execute(CoreEngine::ExecutionContext& context){
      if (this->session == nullptr || this->session->user == nullptr)
          return ExecutionResult(
              Errors::RuntimeError::Error,
              Messages::FAILED_TO_RETRIEVE_USER_SESSION,
              context.GetAllocator()
          );

      const auto* allocator = context.GetAllocator();

      auto* db =  this->server->UseDatabase(context, this->table->databaseId);

      const auto& tables = this->catalog->SelectTables(allocator, this->table->databaseId);

      const auto index = static_cast<SmallInt>(tables.Empty() ? 0 : tables[tables.Size() - 1].ordinalPosition + 1);

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
                  COLUMN_TYPENAMES_TO_ENUMS.Get(normalizedTableName.ToView()),
                  column->type.size,
                  column->index,
                  column->isNullable
              );

          const auto columnResult =
              this->catalog->InsertColumnToMasterDb(
                  context,
                  tableId,
                  column->name.name.ToView(),
                  COLUMN_TYPENAMES_TO_ENUMS.Get(normalizedTableName.ToView()),
                  column->type.size,
                  column->type.decimal.precision,
                  column->type.decimal.scale,
                  column->isNullable,
                  column->index,
                  false,
                  this->session->user->name.ToView()
              );

          const auto columnId = columnResult.primaryKey.AsInt(1);
          columnPtr->SetColumnId(columnId);

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

      DataStructures::PolymorphicArray<Int> primaryKeyColumnIdsArray(allocator, this->primaryKey.columns.Size());
      for (const auto& column: this->primaryKey.columns) {
          if (this->constraintName.Empty()){
              this->constraintName.SetAllocator(allocator);

              const auto& columnName = this->columns[column]->name.name;
              this->constraintName = DataTypes::String::Concat(allocator, "PK_", columnName, "_", columnName);
          }

          primaryKeyColumnIdsArray.Push(columnIdsDict.Get(column));
      }

      static constexpr DataTypes::StringView TABLE_CREATED_MESSAGE = "Table created successfully";
      if (primaryKeyColumnIdsArray.Empty()) {
          tablePtr->RetrieveColumnHeadersFromCatalog(allocator);
          tablePtr->RetrieveIdentityColumnsFromCatalog(allocator);
          return ExecutionResult(Errors::RuntimeError::Ok, TABLE_CREATED_MESSAGE, allocator);
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

      tablePtr->RetrieveIndexesFromCatalog(allocator);
      tablePtr->RetrieveColumnHeadersFromCatalog(allocator);
      tablePtr->RetrieveIdentityColumnsFromCatalog(allocator);

      return ExecutionResult(Errors::RuntimeError::Ok, TABLE_CREATED_MESSAGE, context.GetAllocator());
  }

  PhysicalIndexCreate::PhysicalIndexCreate(
      const DataTypes::Guid& sessionId,
      Statements::DataSource *table,
      DataTypes::String& constraintName,
      DataStructures::PolymorphicArray<column_index_t> &columns
  ): PlanNode(sessionId), table(table), constraintName(std::move(constraintName)), columns(std::move(columns)) {}

  ExecutionResult PhysicalIndexCreate::Execute(CoreEngine::ExecutionContext& context){
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
          const auto& header = columnsHeaders[columnPos];

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

    PhysicalTableScan::PhysicalTableScan(Statements::DataSource* table, Expressions::Expression* expression)
        : table(table), expression(expression) {}

    ExecutionResult PhysicalTableScan::Execute(CoreEngine::ExecutionContext& context){
        auto result = ExecutionResult(context);

        const auto* db = this->server->UseDatabase(context, this->table->databaseId);

        const auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

        tablePtr->GetConstantColumns(&result.columns);

        DataStructures::PolymorphicArray<CoreEngine::StorageTypes::RID> rows(context.GetAllocator());
        tablePtr->HeapScan(context, &rows, this->state);

        result.canFetchMore = this->state.canFetchMore;

        if (result.canFetchMore == false)
            this->state.Reset();

        context.AddScanHandle(rows.Data(), rows.Size());
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

  ExecutionResult PhysicalIndexScan::Execute(CoreEngine::ExecutionContext& context){
      auto result = ExecutionResult(context);

      const auto* db =  this->server->UseDatabase(context, this->table->databaseId);
      auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

      tablePtr->GetConstantColumns(&result.columns);

      DataStructures::PolymorphicArray<CoreEngine::StorageTypes::RID> rows(context.GetAllocator());
      if (this->isClustered)
          tablePtr->ClusteredIndexScan(context, &rows, this->state, this->expression);
      else
          tablePtr->NonClusteredIndexScan(context, &rows, 0, this->state, this->expression);

      result.canFetchMore = this->state.canFetchMore;
      if (result.canFetchMore == false)
          this->state.Reset();

      context.AddTable(tablePtr);
      context.AddScanHandle(rows.Data(), rows.Size());

      result.selectionVector->selectedRidsCount = rows.Size();
      result.selectionVector->isIdentity = true;
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

  ExecutionResult PhysicalIndexSeek::Execute(CoreEngine::ExecutionContext& context){
      auto result = ExecutionResult(context);

      const auto* db = Network::Server::Get().UseDatabase(context, this->table->databaseId);

      auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

      tablePtr->GetConstantColumns(&result.columns);

      //select if to use clustered or non clustered index here
      DataStructures::PolymorphicArray<CoreEngine::StorageTypes::RID> rows(context.GetAllocator());
      tablePtr->ClusteredIndexSeek(context, &rows, this->key, this->expression);

      context.AddTable(tablePtr);
      context.AddScanHandle(rows.Data(), rows.Size());

      result.selectionVector->selectedRidsCount = rows.Size();
      result.selectionVector->isIdentity = true;
      return result;
  }

  PhysicalIndexSeekRange::PhysicalIndexSeekRange(
      Statements::DataSource* table,
      DataTypes::Indexing::Key& minKey,
      DataTypes::Indexing::Key& maxKey,
      Expressions::Expression* expression
  ):    table(table), expression(expression),
        minKey(std::move(minKey)), maxKey(std::move(maxKey)) {}

  ExecutionResult PhysicalIndexSeekRange::Execute(CoreEngine::ExecutionContext& context){
      auto result = ExecutionResult(context);

      const auto* db = Network::Server::Get().UseDatabase(context, this->table->databaseId);

      auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

      tablePtr->GetConstantColumns(&result.columns);

      //select if to use clustered or non clustered index here
      DataStructures::PolymorphicArray<CoreEngine::StorageTypes::RID> rows(context.GetAllocator());
      tablePtr->ClusteredIndexSeekRange(context, &rows, this->minKey, this->maxKey, this->expression);

      context.AddTable(tablePtr);
      context.AddScanHandle(rows.Data(), rows.Size());

      result.selectionVector->selectedRidsCount = rows.Size();
      result.selectionVector->isIdentity = true;
      return result;
  }

    void PhysicalProject::ExecuteVectorizedMode(
        const ExecutionResult& result,
        const CoreEngine::ExecutionContext& context
    ) const{
        for (Int i = 0;i < this->resultExpressions.Size(); i++){
            result.vectorBatch.SetColumn(
                Expressions::EvaluateExpression(
                    this->resultExpressions[i],
                    context,
                    result.selectionVector
                ),
                i
            );
        }
    }

    void PhysicalProject::ExecuteRowMode(const ExecutionResult& result, const CoreEngine::ExecutionContext& context) const{
        const Int rowCount    = result.selectionVector->selectedRidsCount;
        const Int projectCount = this->resultExpressions.Size();

        for (Int i = 0;i < projectCount; i++){
            const auto type = Expressions::GetExpressionReturnType(this->resultExpressions[i]);
            auto* column = CoreEngine::DataVector::FlatVector(context.GetAllocator(), type, rowCount);
            result.vectorBatch.SetColumn(column, i);
        }

        auto* table = context.GetTable(0);

        // 2. Transpose: evaluate each row scalar-wise, scatter every Value into its column slot.
        Expressions::EvaluationContext evaluationContext(
            Expressions::EvaluationContext::EvaluationContextType::SingleRow,
            context.GetAllocator(),
            table
        );

        const auto& scanHandle = context.GetScanHandle(0);

        bool outNull = false;
        Pages::PageView page;
        if (result.selectionVector->isIdentity){
            for (Int i = 0; i < rowCount; i++){
                evaluationContext.row = &scanHandle.rids[i];

                if (!page.IsValid()
                    || page.PageId() != evaluationContext.row->_pageId
                ){
                    page = table->GetPage(evaluationContext.row->_pageId);
                    evaluationContext.page = &page;
                }

                for (Int j = 0; j < projectCount; j++){
                    Expressions::EvaluateExpression(
                        this->resultExpressions[j],
                        evaluationContext,
                        result.vectorBatch._columns[j]->SlotAt(i),
                        &outNull
                    );
                    result.vectorBatch._columns[j]->SetNullValue(i, outNull);
                }
            }

            return;
        }

        for (Int i = 0; i < rowCount; i++){
            evaluationContext.row = &scanHandle.rids[result.selectionVector->selectedRids[0][i]];
            for (Int j = 0; j < projectCount; j++){
                Expressions::EvaluateExpression(
                    this->resultExpressions[j],
                    evaluationContext,
                    result.vectorBatch._columns[j]->SlotAt(i),
                    &outNull
                );

                result.vectorBatch._columns[j]->SetNullValue(i, outNull);
            }
        }
    }

    ExecutionResult PhysicalProject::ExecuteStatement(CoreEngine::ExecutionContext& context) const{
        auto result = this->child->Execute(context);

        for (const auto& expression : this->resultExpressions)
            result.displayColumnNames.Push(expression->name);

        if (result.selectionVector->selectedRidsCount == 0)
            return result;

        result.vectorBatch._numberOfRows = result.selectionVector->selectedRidsCount;
        result.vectorBatch._numberOfColumns = this->resultExpressions.Size();

        result.vectorBatch.AllocateColumns(
            context.GetAllocator(),
            result.vectorBatch._numberOfColumns
        );

        if (context.GetMode() == Constants::ExecutionMode::Vectorized)
            this->ExecuteVectorizedMode(result, context);
        else
            this->ExecuteRowMode(result, context);

        return result;
    }

    ExecutionResult PhysicalProject::ExecuteConstantStatement(const CoreEngine::ExecutionContext& context)const{
        auto result = ExecutionResult(context);

        static constexpr Int ROW_COUNT = 1;
        const auto projectCount = this->resultExpressions.Size();

        result.vectorBatch._numberOfRows = ROW_COUNT;
        result.vectorBatch._numberOfColumns = projectCount;

        result.vectorBatch.AllocateColumns(
            context.GetAllocator(),
            projectCount
        );

        for (Int i = 0;i < projectCount; i++){
            const auto type = Expressions::GetExpressionReturnType(this->resultExpressions[i]);
            auto* column = CoreEngine::DataVector::FlatVector(context.GetAllocator(), type, ROW_COUNT);
            result.vectorBatch.SetColumn(column, i);
        }

        const Expressions::EvaluationContext evaluationContext(
            Expressions::EvaluationContext::EvaluationContextType::Constant,
            context
        );

        auto outNull = false;
        for (Int j = 0; j < projectCount; j++){
            Expressions::EvaluateExpression(
                this->resultExpressions[j],
                evaluationContext,
          result.vectorBatch._columns[j]->SlotAt(0),
                &outNull
            );

            result.vectorBatch._columns[j]->SetNullValue(0, outNull);
        }

        result.canFetchMore = false;
        return result;
    }

    PhysicalProject:: PhysicalProject(
        PlanNode *child,
        DataStructures::PolymorphicArray<Expressions::Expression*>& resultExpressions,
        DataStructures::PolymorphicArray<Headers::ColumnHeader>& columnHeaders
    ): resultExpressions(std::move(resultExpressions)), columnHeaders(std::move(columnHeaders)), child(child) {}

    ExecutionResult PhysicalProject::Execute(CoreEngine::ExecutionContext& context){
        return (this->child == nullptr)
                   ? this->ExecuteConstantStatement(context)
                   : this->ExecuteStatement(context);
    }

    void PhysicalProject::UpdateScanState(const DataTypes::RowIdentifier& rowId){
        this->child->UpdateScanState(rowId);
    }

    void PhysicalFilter::ExecuteVectorizedMode(
        const ExecutionResult& result,
        const CoreEngine::ExecutionContext& context
    ) const{

    }

    void PhysicalFilter::ExecuteRowMode(
        const ExecutionResult& result,
        const CoreEngine::ExecutionContext& context
    ) const{
        Expressions::EvaluationContext evaluationContext(
            Expressions::EvaluationContext::EvaluationContextType::SingleRow,
            context
        );

        const auto& firstHandle = context.GetScanHandle(0);
        const auto* table = context.GetTable(0);

        Pages::PageView page;
        for (Int i = 0; i < firstHandle.size; i++){
            evaluationContext.row = &firstHandle.rids[i];
            if (!page.IsValid()
                || page.PageId() != evaluationContext.row->_pageId
            ){
                page = table->GetPage(evaluationContext.row->_pageId);
                evaluationContext.page = &page;
            }

            if (Expressions::RowModeFilter(this->filter, evaluationContext))
                result.selectionVector->selectedRids[0][result.selectionVector->selectedRidsCount++] = i;
        }
    }

    PhysicalFilter::PhysicalFilter(PlanNode *child, Expressions::Expression* filter)
        : filter(filter) , child(child) {}

    ExecutionResult PhysicalFilter::Execute(CoreEngine::ExecutionContext& context){
        auto result = this->child->Execute(context);

        const auto& firstHandle = context.GetScanHandle(0);
        if (firstHandle.size == 0)
            return result;

        result.selectionVector->AllocateRids(
            context.GetAllocator(),
            0,
            firstHandle.size
        );
        result.selectionVector->selectedRidsCount = 0;

        if (context.GetMode() == Constants::ExecutionMode::Vectorized)
            this->ExecuteVectorizedMode(result, context);
        else
            this->ExecuteRowMode(result, context);

        return result;
    }

    void PhysicalFilter::UpdateScanState(const DataTypes::RowIdentifier& rowId){
        this->child->UpdateScanState(rowId);
    }

    PhysicalTop::PhysicalTop(PlanNode* child, const BigInt top)
        : top(top), child(child){}

    ExecutionResult PhysicalTop::Execute(CoreEngine::ExecutionContext& context){
        auto result = this->child->Execute(context);

        if (this->top > result.vectorBatch._numberOfRows)
            return result;

        result.vectorBatch._numberOfRows = this->top;
        // result.results.RemoveFrom(this->top);
        result.canFetchMore = false;
        return result;
    }

    void PhysicalTop::UpdateScanState(const DataTypes::RowIdentifier& rowId){
        this->child->UpdateScanState(rowId);
    }

    PhysicalDistinct::PhysicalDistinct(PlanNode *child)
        : child(child){}

    ExecutionResult PhysicalDistinct::Execute(CoreEngine::ExecutionContext& context){
        auto result = this->child->Execute(context);

        DataStructures::PolymorphicArray<QueryResult> results;

        HashSet<int64_t> computedHashes;

        // for (auto& row : result.results) {
        //     const auto hash = row.ComputeHash();
        //
        //     //if no collision occurs
        //     if (!computedHashes.Contains(hash)) {
        //         results.Push(std::move(row));
        //         computedHashes.Add(hash);
        //         continue;
        //     }
        //
        //     bool isDuplicate = false;
        //     for (const auto& distinctRow : results) {
        //         if (distinctRow == row) {
        //             isDuplicate = true;
        //             break;
        //         }
        //     }
        //
        //     if (!isDuplicate)
        //         results.Push(std::move(row));
        // }
        //
        // result.results = std::move(results);

        return result;
    }

    void PhysicalDistinct::UpdateScanState(const DataTypes::RowIdentifier& rowId){
        this->child->UpdateScanState(rowId);
    }

    bool PhysicalOrderBy::CanBeSortedInMemory(const bool canFetchMore) const{
        return !canFetchMore && !this->UsesExternalStorage();
    }

    PhysicalOrderBy::PhysicalOrderBy(
        PlanNode *child,
        DataStructures::PolymorphicArray<Statements::OrderColumn*>& expressions
    )   : child(child)
          , expressions(std::move(expressions))
          , comparator(&this->expressions, nullptr)
          , priorityQueue(this->comparator){}

    ExecutionResult PhysicalOrderBy::Execute(CoreEngine::ExecutionContext& context){
        if (!this->comparator.HasProperties())
            this->comparator.SetExecutionContext(&context);

        auto result = this->child->Execute(context);

        // Case 1: In-memory sort (no external storage needed)
        // if (!result.canFetchMore && !this->UsesExternalStorage()){
        //     SortingFunctions::OrderBy(context, result.results, this->expressions);
        //     return result;
        // }

        // Case 2: Build phase - collect and sort batches
        // while (result.canFetchMore || (result.canFetchMore == false && this->priorityQueue.Empty())) {
        //     SortingFunctions::OrderBy(context, result.results, this->expressions);

        //     DataTypes::RowIdentifier rowId;
        //     this->InsertPostProjectionResultsToTemporaryDatabase(context, result, rowId);
        //
        //     auto element = MergeElement(
        //         result.results[0],
        //         static_cast<int>(this->priorityQueue.Size()),
        //         rowId
        //     );
        //
        //     this->priorityQueue.Add(std::move(element));
        //
        //     if (!result.canFetchMore)
        //         break;
        //
        //     result = this->child->Execute(context);
        // }
        //
        // // Case 3: Merge phase - k-way merge
        // result.results.Clear();
        //
        // DataStructures::PolymorphicArray<DataStructures::PolymorphicArray<QueryResult>> batches;
        // batches.Resize(this->priorityQueue.Size());
        //
        // while (!this->priorityQueue.Empty()){
        //     auto top = this->priorityQueue.Top();
        //     this->priorityQueue.Remove();
        //
        //     result.results.Push(std::move(top.value));
        //
        //     const auto batchId = top.batchId;
        //
        //     // Lazy load batch if needed
        //     if (batches[batchId].Empty()) {
        //         auto state = CoreEngine::ScanState();
        //         state.lastFetchedRowId = top.rowId;
        //         state.extentId = CoreEngine::Database::CalculateExtentId(state.lastFetchedRowId.pageId);
        //
        //         auto batchResult = this->StreamFromTemporaryDatabase(context, state);
        //         batches[batchId] = std::move(batchResult.results);
        //     }
        //
        //     // Remove consumed element
        //     batches[batchId].Remove(0);
        //
        //     // Add next element from same batch if available
        //     if (!batches[batchId].Empty()) {
        //         auto nextElement = MergeElement(
        //             batches[batchId][0],
        //             batchId,
        //             top.rowId // Update with proper next rowId if needed
        //         );
        //         this->priorityQueue.Add(std::move(nextElement));
        //     }
        // }

        return result;
    }

    void PhysicalOrderBy::UpdateScanState(const DataTypes::RowIdentifier& rowId){
        this->child->UpdateScanState(rowId);
    }

    bool PhysicalInsert::SortInsertsAscending(const Value& lhs, const Value& rhs){
        return lhs.GetColumnIndex() < rhs.GetColumnIndex();
    }

    ExecutionResult PhysicalInsert::InsertFromChild(
        CoreEngine::StorageTypes::Table* tablePtr,
        CoreEngine::ExecutionContext& context
    )const{
        Int rowCount = 0;
        while (true){
            auto result = this->child->Execute(context);

            // result.status = tablePtr->BatchInsert(context, result.results);

            if (!result.status.IsOk())
                return result;

            if (!result.canFetchMore)
                break;

            rowCount += result.vectorBatch._numberOfRows;
            context.ResetAllocator();
        }

        return ExecutionResult(
            Errors::RuntimeError::Ok,
            Messages::INSERT_ROWS_FROM_CHILD_QUERY(rowCount, context.GetAllocator())
        );
    }

    ExecutionResult PhysicalInsert::InsertFromValues(
        CoreEngine::StorageTypes::Table* tablePtr,
        const CoreEngine::ExecutionContext& context
    ) const{
        auto result = ExecutionResult(context);

        for (Int i = 0;i < this->fields.Size(); i++){
            result.status = tablePtr->InsertRow(context, this->fields[i].values, this->insertPlan);
            if (!result.status.IsOk())
                return result;
        }

        result.status = Errors::RuntimeStatus(
            Errors::RuntimeError::Ok,
            Messages::INSERT_ROWS_FROM_FIELDS(this->fields.Size(), context.GetAllocator())
        );
        return result;
    }

    PhysicalInsert::PhysicalInsert(
        Statements::DataSource* table,
        DataStructures::PolymorphicArray<Statements::Inserts> &fields,
        PlanNode* child,
        CoreEngine::StorageTypes::InsertPlan& insertPlan
    ): insertPlan(std::move(insertPlan)), fields(std::move(fields)), table(table), child(child) {}

    ExecutionResult PhysicalInsert::Execute(CoreEngine::ExecutionContext& context){
        const auto* db =  this->server->UseDatabase(context, this->table->databaseId);
        auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

        return (this->child != nullptr)
                   ? this->InsertFromChild(tablePtr, context)
                   : this->InsertFromValues(tablePtr, context);
    }

    PhysicalHeapUpdate::PhysicalHeapUpdate(
        Statements::DataSource *table,
        Expressions::Expression *expression,
        DataStructures::PolymorphicArray<Expressions::Expression*>& updates
    ) : table(table), updates(std::move(updates)), expression(expression) {}

    ExecutionResult PhysicalHeapUpdate::Execute(CoreEngine::ExecutionContext& context){
        auto result = ExecutionResult(context);

        const auto* db =  this->server->UseDatabase(context, this->table->databaseId);
        auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

        result.status = tablePtr->HeapUpdate(context, this->expression, this->updates);
        return result;
    }

    PhysicalIndexScanUpdate::PhysicalIndexScanUpdate(
        Statements::DataSource *table,
        Expressions::Expression *expression,
        DataStructures::PolymorphicArray<Expressions::Expression*>& updates
    ): table(table), updates(std::move(updates)), expression(expression) {}

    ExecutionResult PhysicalIndexScanUpdate::Execute(CoreEngine::ExecutionContext& context){
        auto result = ExecutionResult(context);

        const auto* db =  this->server->UseDatabase(context, this->table->databaseId);
        auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

        result.status = tablePtr->ClusteredIndexScanUpdate(context, this->expression, this->updates);
        return result;
    }

    PhysicalIndexSeekUpdate::PhysicalIndexSeekUpdate(
        Statements::DataSource *table,
        Expressions::Expression *expression,
        DataStructures::PolymorphicArray<Expressions::Expression*>& updates
    ): table(table), updates(std::move(updates)), expression(expression) {}

  ExecutionResult PhysicalIndexSeekUpdate::Execute(CoreEngine::ExecutionContext& context){
      const auto* db = this->server->UseDatabase(context, this->table->databaseId);

      auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

      DataTypes::Indexing::Key key;

      const auto updateResult = tablePtr->ClusteredIndexScanUpdate(context, this->expression, this->updates);

      // tablePtr->ClusteredIndexSeekUpdate(this->expression, &key, &key, this->fields);

      return ExecutionResult(updateResult.code, updateResult.message);
  }

  PhysicalHeapDelete::PhysicalHeapDelete(Statements::DataSource *table, Expressions::Expression *expression)
      : table(table), expression(expression) {}

  ExecutionResult PhysicalHeapDelete::Execute(CoreEngine::ExecutionContext& context){
      auto result = ExecutionResult(context);

      const auto* db = Network::Server::Get().UseDatabase(context, this->table->databaseId);

      const CoreEngine::StorageTypes::Table* tablePtr = db->OpenTable(this->table->ordinalPosition);

      tablePtr->HeapDelete(context, this->expression);

      return result;
  }

  PhysicalIndexScanDelete::PhysicalIndexScanDelete(Statements::DataSource *table, Expressions::Expression *expression)
      : table(table), expression(expression) {}

  ExecutionResult PhysicalIndexScanDelete::Execute(CoreEngine::ExecutionContext& context){
      auto result = ExecutionResult(context);

      const auto* db =  this->server->UseDatabase(context, this->table->databaseId);

      auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

      tablePtr->ClusteredIndexScanDelete(context, this->expression, state);

      return result;
  }

  PhysicalIndexSeekDelete::PhysicalIndexSeekDelete(Statements::DataSource *table, Expressions::Expression *expression)
      : table(table), expression(expression) {}

    ExecutionResult PhysicalIndexSeekDelete::Execute(CoreEngine::ExecutionContext& context){
        auto result = ExecutionResult(context);

        const auto* db =  this->server->UseDatabase(context, this->table->databaseId);

        auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

        tablePtr->ClusteredIndexSeekDelete(context, this->expression, this->state);

        return result;
    }

    PhysicalDeclareVariable::PhysicalDeclareVariable(const DataTypes::Guid &currentSessionId, Variable& variable, Expressions::Expression* expression)
        : PlanNode(currentSessionId), variable(std::move(variable)), expression(expression){}

    ExecutionResult PhysicalDeclareVariable::Execute(CoreEngine::ExecutionContext& context) {
        auto result = ExecutionResult(context);

        const Expressions::EvaluationContext evaluationContext(
            Expressions::EvaluationContext::EvaluationContextType::Constant,
            context
        );

        auto value = Expressions::EvaluateExpression(this->expression, evaluationContext);
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
}
