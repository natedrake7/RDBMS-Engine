#include "../../include/PhysicalPlan.h"

#include <cassert>
#include <utility>

#include "ValidationMessages.h"
#include "../../../CoreEngine/include/Database.h"
#include "../../../CoreEngine/include/SystemDatabases/SystemCatalog.h"
#include "../../../Server/include/Server.h"
#include "../../../CoreEngine/include/Algorithms/Sort/SortingFunctions.h"
#include "../../../CoreEngine/include/ScanState.h"
#include "../../../CoreEngine/include/DataStorage/Table.h"
#include "../../../Systemic/include/DataTypes/DataTypes.StaticData.h"
#include "../../../CoreEngine/include/Contexts/ExecutionContext.h"
#include "../../../CoreEngine/include/SystemDatabases/TemporaryDatabase.h"
#include "../../../CoreEngine/include/Vectorization/Vectorization.h"
#include "../../../CoreEngine/include/BufferPool/StorageManager.h"
#include "../../../CoreEngine/include/Contexts/OutputSchema.h"
#include "../../../CoreEngine/include/DataStorage/ColumnMaterializationInfo.h"
#include "Evaluators/VectorizedPushedDownFilter.h"

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
        this->dataChunk = std::move(other.dataChunk);
        this->columns = std::move(other.columns);
        this->displayColumnNames = std::move(other.displayColumnNames);
        this->selectionVector = other.selectionVector;
    }

    ExecutionResult& ExecutionResult::operator=(ExecutionResult&& other) noexcept{
        if (this == &other) return *this;
        this->status = std::move(other.status);
        this->canFetchMore = other.canFetchMore;
        this->dataChunk = std::move(other.dataChunk);
        this->columns = std::move(other.columns);
        this->displayColumnNames = std::move(other.displayColumnNames);
        this->selectionVector = other.selectionVector;
        return *this;
    }

    bool ExecutionResult::IsOk() const {
        return this->status.code == Errors::RuntimeError::Ok;
    }

    PlanNode::PlanNode()
        :   sessionId(DataTypes::Guid::Empty()), _schema(nullptr),
            session(nullptr), temporaryTableId(INVALID_TABLE_ID){}

    PlanNode::PlanNode(const DataTypes::Guid &currentSessionId)
        :   sessionId(currentSessionId), _schema(nullptr),
            session(Network::Server::Get().GetSession(this->sessionId)), temporaryTableId(INVALID_TABLE_ID){}

    PlanNode::PlanNode(const CoreEngine::OutputSchema* schema)
        :   sessionId(DataTypes::Guid::Empty()), _schema(schema),
            session(nullptr), temporaryTableId(INVALID_TABLE_ID){}

    void PlanNode::InsertToTemporaryDatabase(const DataStructures::PolymorphicArray<CoreEngine::StorageTypes::RID>& rows){

    }

  void PlanNode::InsertPostProjectionResultsToTemporaryDatabase(
      const CoreEngine::ExecutionContext& context,
      ExecutionResult& result,
      DataTypes::RowIdentifier& firstRowId
  ){
      static auto& tempDb = CoreEngine::TemporaryDatabase::Get();

      const auto* table = (this->temporaryTableId == INVALID_TABLE_ID)
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

    void PlanNode::UpdateScanState(const CoreEngine::StorageTypes::RID* rid){ }

    const CoreEngine::OutputSchema* PlanNode::GetSchema() const{
        return this->_schema;
    }

    bool PlanNode::UsesExternalStorage() const{ return this->temporaryTableId != INVALID_TABLE_ID; }

    PhysicalMaterialize::PhysicalMaterialize(
        DataStructures::PolymorphicArray<CoreEngine::StorageTypes::ColumnMaterializationInfo>& materializationInfo,
        PlanNode* child,
        const CoreEngine::OutputSchema* schema,
        const UnsignedSmallInt slotIndex
    ):  PlanNode(schema), _materializationInfo(std::move(materializationInfo)),
        child(child), _slotIndex(slotIndex){
        assert(this->_materializationInfo.Size() == this->_schema->_columns.Size());
    }

    ExecutionResult PhysicalMaterialize::Execute(CoreEngine::ExecutionContext& context){
        auto result = this->child->Execute(context);

        const auto* allocator = context.GetAllocator();

        const auto columnsSize = this->_materializationInfo.Size();
        const auto rowCount = result.selectionVector->selectedRidsCount;

        result.dataChunk.AllocateColumns(allocator, rowCount, columnsSize);
        for (Int index = 0; index < columnsSize; index++){
            const auto& info = this->_materializationInfo[index];

            auto* vector = CoreEngine::DataVector::FlatVector(allocator, info._type,  rowCount);
            // Read from the table ordinal, write to the output position: with pruning these differ.
            info._function(context, result.selectionVector, vector, this->_slotIndex, info._ordinalPosition);
            result.dataChunk.SetColumn(vector, index);
        }

        result.dataChunk._numberOfRows = rowCount;
        result.dataChunk._numberOfColumns = columnsSize;

        result.selectionVector = nullptr;
        return result;
    }

    PhysicalCreateUser::PhysicalCreateUser(DataTypes::String& username, DataTypes::String& password, DataTypes::String& role)
        : username(std::move(username)), password(std::move(password)), roleName(std::move(role)) {}

    ExecutionResult PhysicalCreateUser::Execute(CoreEngine::ExecutionContext& context) {
        auto result = ExecutionResult(context);

        if (!Network::Server::Get().CreateUser(context, this->username, this->password, this->roleName))
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

      const auto* role = Network::Server::Get().GetRole(this->roleName);
      if (role == nullptr) {
          result.status = Errors::RuntimeStatus(
              Errors::RuntimeError::Error,
              Messages::FAILED_TO_GET_ROLE(DataTypes::StringView::ViewOf(this->roleName), context.GetAllocator())
          );
          return result;
      }

      result.status = Network::Server::Get().GrantRole(context, this->sessionId, this->username, role);
      return result;
  }

  PhysicalCreateDatabase::PhysicalCreateDatabase(const DataTypes::Guid& sessionId, DataTypes::String& name) : PlanNode(sessionId), dbName(std::move(name)){}

  ExecutionResult PhysicalCreateDatabase::Execute(CoreEngine::ExecutionContext& context){
      if (this->session == nullptr || this->session->user == nullptr)
          return ExecutionResult(Errors::RuntimeError::Error, Messages::FAILED_TO_RETRIEVE_USER_SESSION, context.GetAllocator());

      const auto path = DataTypes::String::Concat(context.GetAllocator(), this->dbName, Constants::DATA_FILE_EXTENSION);

      const auto result = CoreEngine::SystemCatalog::Get().InsertDbToMasterDb(
          context,
          DataTypes::StringView::ViewOf(this->dbName),
          DataTypes::StringView::ViewOf(path),
          false,
          DataTypes::StringView::ViewOf(this->session->user->name)
      );

      const auto databaseId = result.primaryKey.AsInt<Int>();

      const auto _ = CoreEngine::SystemCatalog::Get().InsertSchemaToMasterDb(context, databaseId, Constants::DEFAULT_SCHEMA_NAME);

      CoreEngine::CreateDatabase(databaseId, this->dbName);

      return ExecutionResult(context);
  }

  PhysicalUseDatabase::PhysicalUseDatabase(const DataTypes::Guid &sessionId, const Int databaseId)
      : sessionId(sessionId), databaseId(databaseId){}

  ExecutionResult PhysicalUseDatabase::Execute(CoreEngine::ExecutionContext& context) {
      auto result = ExecutionResult(context);

      if (Network::Server::Get().UpdateSession(this->sessionId, this->databaseId)) {
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

      const auto insertResult = CoreEngine::SystemCatalog::Get().InsertSchemaToMasterDb(
          context,
          this->databaseId,
          DataTypes::StringView::ViewOf(this->schemaName),
          DataTypes::StringView::ViewOf(this->session->user->name)
      );
      return ExecutionResult(insertResult.code, insertResult.message);
  }

    PhysicalTableCreate::PhysicalTableCreate(
        const DataTypes::Guid& sessionId,
        Statements::DataSource*  table,
        DataStructures::PolymorphicArray<Statements::NewColumn*> &columns,
        const Headers::Index& primaryKey,
        DataTypes::String& constraintName
    ):  PlanNode(sessionId), table(table), constraintName(std::move(constraintName)),
        columns(std::move(columns)), primaryKey(primaryKey) {}

    ExecutionResult PhysicalTableCreate::Execute(CoreEngine::ExecutionContext& context){
        if (this->session == nullptr || this->session->user == nullptr)
            return ExecutionResult(
                Errors::RuntimeError::Error,
                Messages::FAILED_TO_RETRIEVE_USER_SESSION,
                 context.GetAllocator()
            );

        const auto* allocator = context.GetAllocator();

        auto* db =  Network::Server::Get().UseDatabase(context, this->table->_databaseId);

        const auto& tables = CoreEngine::SystemCatalog::Get().SelectTables(allocator, this->table->_databaseId);

        const auto index = static_cast<SmallInt>(tables.Empty() ? 0 : tables[tables.Size() - 1].ordinalPosition + 1);

        const auto tableResult = CoreEngine::SystemCatalog::Get().InsertTableToMasterDb(
            context,
            this->table->_databaseId,
            this->table->_schemaId,
            DataTypes::StringView::ViewOf(this->table->name),
            index,
            false,
            DataTypes::StringView::ViewOf(this->session->user->name)
        );

        const auto tableId = tableResult.primaryKey.AsInt<Int>(1);

        auto* tablePtr = db->CreateTable(tableId, index);

        const auto tableStatsResult = CoreEngine::SystemCatalog::Get().InsertTableStatisticsToMasterDb(
            context,
            tableId
        );

        Dictionary<Int, Int> columnIdsDict;
        for (const auto* column: this->columns){
            const auto normalizedTableName = DataTypes::String::Normalize(column->type.name, context.GetAllocator());
                auto* columnPtr =
                    tablePtr->AddColumn(
                          DataTypes::StringView::ViewOf(column->name.name),
                          COLUMN_TYPENAMES_TO_ENUMS.Get(DataTypes::StringView::ViewOf(normalizedTableName)),
                          column->type.size,
                          column->index,
                          column->isNullable
                    );

            const auto columnResult =
                CoreEngine::SystemCatalog::Get().InsertColumnToMasterDb(
                      context,
                      tableId,
                      DataTypes::StringView::ViewOf(column->name.name),
                      COLUMN_TYPENAMES_TO_ENUMS.Get(DataTypes::StringView::ViewOf(normalizedTableName)),
                      column->type.size,
                      column->type.decimal.precision,
                      column->type.decimal.scale,
                      column->isNullable,
                      column->index,
                      false,
                      DataTypes::StringView::ViewOf(this->session->user->name)
                );

            const auto columnId = columnResult.primaryKey.AsInt<Int>(1);
            columnPtr->SetColumnId(columnId);

            const auto columnStatsResult = CoreEngine::SystemCatalog::Get().InsertColumnStatisticsToMasterDb(context, columnId);
            columnIdsDict.Add(column->index, columnId);

            if (!column->defaultValue.IsNull() || column->defaultValue.Size() != 0) {
                const auto _ = CoreEngine::SystemCatalog::Get().InsertDefaultValuesToMasterDb(
                  context,
                  columnId,
                  column->defaultValue
                );
            }

            //insert identity columns
            if (column->identity == nullptr) continue;

            const auto _ = CoreEngine::SystemCatalog::Get().InsertIdentityColumnToMasterDb(
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
            tablePtr->CalculateInsertPayloadSize();
            return ExecutionResult(Errors::RuntimeError::Ok, TABLE_CREATED_MESSAGE, allocator);
        }

        const auto indexResult = CoreEngine::SystemCatalog::Get().InsertIndexToMasterDb(
            context,
            tableId,
            DataTypes::StringView::ViewOf(this->constraintName),
            true,
            false,
            DataTypes::StringView::ViewOf(this->session->user->name)
        );

        const auto indexId = indexResult.primaryKey.AsInt<Int>(1);

        const auto constraintResult = CoreEngine::SystemCatalog::Get().InsertConstraintToMasterDb(
            context,
            tableResult.primaryKey.AsInt<Int>(),
            DataTypes::StringView::ViewOf(this->constraintName),
            Headers::ConstraintType::PrimaryKey,
            false,
            &indexId,
            DataTypes::StringView::ViewOf(this->session->user->name)
        );

        const auto constraintId = constraintResult.primaryKey.AsInt<Int>(1);

        for(Int i = 0; i < primaryKeyColumnIdsArray.Size(); i++){
            auto _ = CoreEngine::SystemCatalog::Get().InsertIndexColumnToMasterDb(
                context,
                indexResult.primaryKey.AsInt<Int>(),
                primaryKeyColumnIdsArray[i],
                this->primaryKey.columns[i],
                true
            );


            _ = CoreEngine::SystemCatalog::Get().InsertConstraintColumnToMasterDb(
                context,
                constraintId,
                primaryKeyColumnIdsArray[i],
                this->primaryKey.columns[i]
            );
        }

        const auto indexStatsResult = CoreEngine::SystemCatalog::Get().InsertIndexStatisticsToMasterDb(
            context,
            tableId,
            indexId
        );

        tablePtr->RetrieveIndexesFromCatalog(allocator);
        tablePtr->RetrieveColumnHeadersFromCatalog(allocator);
        tablePtr->RetrieveIdentityColumnsFromCatalog(allocator);
        tablePtr->CalculateInsertPayloadSize();

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

      const auto* db = Network::Server::Get().UseDatabase(context, this->table->_databaseId);

      auto* tablePtr = db->OpenTable(this->table->_ordinalPosition);

      const auto columnsHeaders =CoreEngine::SystemCatalog::Get().SelectColumns(context.GetAllocator(), this->table->_tableId);

      const auto indexResult =CoreEngine::SystemCatalog::Get().InsertIndexToMasterDb(
          context,
          this->table->_tableId,
          DataTypes::StringView::ViewOf(this->constraintName),
          false,
          false,
          DataTypes::StringView::ViewOf(this->session->user->name)
      );

      const auto indexId = indexResult.primaryKey.AsInt<Int>(1);

      const auto constraintResult =CoreEngine::SystemCatalog::Get().InsertConstraintToMasterDb(
          context,
          this->table->_tableId,
          DataTypes::StringView::ViewOf(this->constraintName),
          Headers::ConstraintType::IndexKey,
          false,
          &indexId,
          DataTypes::StringView::ViewOf(this->session->user->name)
      );

      const auto constraintId = constraintResult.primaryKey.AsInt<Int>(1);

      for (const auto& columnPos : this->columns) {
          const auto& header = columnsHeaders[columnPos];

          const auto indexColumnResult =
              CoreEngine::SystemCatalog::Get().InsertIndexColumnToMasterDb(
                  context,
                  indexId,
                  header.id,
                  columnPos,
                  true
              );

          const auto constraintColumnResult =
              CoreEngine::SystemCatalog::Get().InsertConstraintColumnToMasterDb(
                  context,
                  constraintId,
                  header.id,
                  columnPos
              );
      }

      const auto indexStatsResult = CoreEngine::SystemCatalog::Get().InsertIndexStatisticsToMasterDb(
          context,
          this->table->_tableId,
          indexId
      );

      const auto indexPos = tablePtr->CreateNonClusteredIndex(this->columns);

      const auto pages = 1;
      tablePtr->NonClusteredIndexInsertExistingRows(indexPos, pages);

      //if there are rows in the table update the index
      //do stuff here

      return result;
  }

    PhysicalTableScan::PhysicalTableScan(
        Statements::DataSource* table,
        const CoreEngine::OutputSchema* schema,
        Expressions::Expression* expression
    ): PlanNode(schema), table(table), expression(expression) {}

    ExecutionResult PhysicalTableScan::Execute(CoreEngine::ExecutionContext& context){
        auto result = ExecutionResult(context);

        const auto* db = Network::Server::Get().UseDatabase(context, this->table->_databaseId);

        const auto* tablePtr = db->OpenTable(this->table->_ordinalPosition);

        tablePtr->GetConstantColumns(&result.columns);

        DataStructures::PolymorphicArray<CoreEngine::StorageTypes::RID> rows(context.GetAllocator());
        tablePtr->HeapScan(context, &rows, this->state);

        result.canFetchMore = this->state.canFetchMore;

        if (result.canFetchMore == false)
            this->state.Reset();

        context.SetTable(tablePtr, this->table->_slotIndex);
        context.SetScanHandle(rows.Data(), rows.Size(), this->table->_slotIndex);
        context.SetFileKey(tablePtr->GetDataFileKey(), this->table->_slotIndex);

        result.selectionVector->selectedRidsCount = rows.Size();
        result.selectionVector->isIdentity = true;
        return result;
    }

    void PhysicalTableScan::UpdateScanState(const CoreEngine::StorageTypes::RID* rid){
        this->state._lastRID = *rid;
    }

    PhysicalIndexScan::PhysicalIndexScan(
        Statements::DataSource* table,
        const CoreEngine::OutputSchema* schema,
        DataStructures::PolymorphicArray<CoreEngine::StorageTypes::FilterColumnInfo>& filterColumns,
        const bool isClustered
    ):  PlanNode(schema), filterColumns(std::move(filterColumns)),
        table(table), expression(nullptr), isClustered(isClustered) {}

    PhysicalIndexScan::PhysicalIndexScan(
        Statements::DataSource *table,
        const CoreEngine::OutputSchema* schema,
        DataStructures::PolymorphicArray<CoreEngine::StorageTypes::FilterColumnInfo>& filterColumns,
        Expressions::Expression *expression,
        const bool isClustered
    ):  PlanNode(schema), filterColumns(std::move(filterColumns)),
        table(table), expression(expression), isClustered(isClustered) {}

    ExecutionResult PhysicalIndexScan::Execute(CoreEngine::ExecutionContext& context){
        ExecutionResult result(context);

        const auto slotIndex = this->table->_slotIndex;

        const auto* db =  Network::Server::Get().UseDatabase(context, this->table->_databaseId);
        auto* tablePtr = db->OpenTable(this->table->_ordinalPosition);

        tablePtr->GetConstantColumns(&result.columns);
        context.SetFileKey(tablePtr->GetDataFileKey(), slotIndex);

        DataStructures::PolymorphicArray<CoreEngine::StorageTypes::RID> rids(context.GetAllocator(), context.GetBatchSize());

        if (this->isClustered){
            tablePtr->ClusteredIndexScan(context, &rids, this->state, this->expression, this->filterColumns, slotIndex, this->_schema->_columns.Size());
        }
        else{
            tablePtr->NonClusteredIndexScan(context, &rids, 0, this->state, this->expression);
        }

        result.canFetchMore = this->state.canFetchMore;
        if (result.canFetchMore == false)
            this->state.Reset();

        context.SetTable(tablePtr, slotIndex);
        context.SetScanHandle(rids.Data(), rids.Size(), slotIndex);

        result.selectionVector->selectedRidsCount = rids.Size();
        result.selectionVector->isIdentity = true;
        return result;
    }

    void PhysicalIndexScan::UpdateScanState(const CoreEngine::StorageTypes::RID* rid){
        this->state.pageId = rid->_pageId;
        this->state.lastFetchedKeyIndex =  rid->_index;
    }

    PhysicalIndexSeek::PhysicalIndexSeek(
        Statements::DataSource* table,
        const CoreEngine::OutputSchema* schema,
        DataTypes::Indexing::Key& key,
        DataStructures::PolymorphicArray<CoreEngine::StorageTypes::FilterColumnInfo>& filterColumns,
        Expressions::Expression* expression
    ):  PlanNode(schema), filterColumns(std::move(filterColumns)),
        table(table), expression(expression), key(std::move(key)){}

    ExecutionResult PhysicalIndexSeek::Execute(CoreEngine::ExecutionContext& context){
        auto result = ExecutionResult(context);

        const auto* db = Network::Server::Get().UseDatabase(context, this->table->_databaseId);

        auto* tablePtr = db->OpenTable(this->table->_ordinalPosition);

        tablePtr->GetConstantColumns(&result.columns);

        //select if to use clustered or non clustered index here
        DataStructures::PolymorphicArray<CoreEngine::StorageTypes::RID> rows(context.GetAllocator());
        tablePtr->ClusteredIndexSeek(context, &rows, this->key, this->expression, this->filterColumns, this->table->_slotIndex, this->_schema->_columns.Size());

        context.SetTable(tablePtr, this->table->_slotIndex);
        context.SetScanHandle(rows.Data(), rows.Size(), this->table->_slotIndex);
        context.SetFileKey(tablePtr->GetDataFileKey(), this->table->_slotIndex);

        result.selectionVector->selectedRidsCount = rows.Size();
        result.selectionVector->isIdentity = true;
        return result;
    }

    PhysicalIndexSeekRange::PhysicalIndexSeekRange(
        Statements::DataSource* table,
        const CoreEngine::OutputSchema* schema,
        DataTypes::Indexing::Key& minKey,
        DataTypes::Indexing::Key& maxKey,
        DataStructures::PolymorphicArray<CoreEngine::StorageTypes::FilterColumnInfo> filterColumns,
        Expressions::Expression* expression
    ):  PlanNode(schema), filterColumns(std::move(filterColumns)),
        table(table), expression(expression),
        minKey(std::move(minKey)), maxKey(std::move(maxKey)) {}

    ExecutionResult PhysicalIndexSeekRange::Execute(CoreEngine::ExecutionContext& context){
        auto result = ExecutionResult(context);

        const auto* db = Network::Server::Get().UseDatabase(context, this->table->_databaseId);

        auto* tablePtr = db->OpenTable(this->table->_ordinalPosition);

        tablePtr->GetConstantColumns(&result.columns);

        //select if to use clustered or non clustered index here
        DataStructures::PolymorphicArray<CoreEngine::StorageTypes::RID> rows(context.GetAllocator());
        tablePtr->ClusteredIndexSeekRange(context, &rows, this->minKey, this->maxKey, this->expression, this->filterColumns, this->table->_slotIndex, this->_schema->_columns.Size());

        context.SetTable(tablePtr, this->table->_slotIndex);
        context.SetScanHandle(rows.Data(), rows.Size(), this->table->_slotIndex);
        context.SetFileKey(tablePtr->GetDataFileKey(), this->table->_slotIndex);

        result.selectionVector->selectedRidsCount = rows.Size();
        result.selectionVector->isIdentity = true;
        return result;
    }

    PhysicalProject:: PhysicalProject(
        PlanNode *child,
        DataStructures::PolymorphicArray<Expressions::Expression*>& projections,
        const CoreEngine::OutputSchema* schema,
        const UnsignedSmallInt slotCount
    ):  PlanNode(schema), _projections(std::move(projections)),
        child(child), _slotCount(slotCount) {}

    ExecutionResult PhysicalProject::Execute(CoreEngine::ExecutionContext& context){
        const auto isConstantStatement = this->child == nullptr;

        auto result = isConstantStatement
            ? ExecutionResult(context)
            : this->child->Execute(context);

        const auto rowCount = result.dataChunk._numberOfRows + isConstantStatement;

        for (const auto* expression : this->_projections)
            result.displayColumnNames.Push(expression->name);

        if (rowCount == 0)
            return result;

        CoreEngine::DataChunk newChunk;
        newChunk.AllocateColumns(
            context.GetAllocator(),
            rowCount,
            this->_projections.Size()
        );

        for (Int index = 0;index < this->_projections.Size(); index++){
            newChunk.SetColumn(
                Expressions::EvaluateExpression(
                    this->_projections[index],
                    &context,
                    &result.dataChunk
                ),
                index
            );
        }

        result.dataChunk = std::move(newChunk);
        return result;
    }

    void PhysicalProject::UpdateScanState(const CoreEngine::StorageTypes::RID* rid){
        this->child->UpdateScanState(rid);
    }

    PhysicalFilter::PhysicalFilter(
        PlanNode *child,
        Expressions::Expression* filter,
        const UnsignedSmallInt slotCount
    ): PlanNode(child->GetSchema()), filter(filter) , child(child), _slotCount(slotCount) {}

    ExecutionResult PhysicalFilter::Execute(CoreEngine::ExecutionContext& context){
        auto result = this->child->Execute(context);

        auto* mask = Expressions::EvaluateExpression(this->filter, &context, &result.dataChunk);
        Int rowCounter = 0;

        for (Int i = 0;i < result.dataChunk._numberOfRows; i++){
            const auto index = mask->PhysicalIndex(i);
            if (!mask->GetNullValue(index) && *mask->SlotAt<bool>(index))
                result.dataChunk._selection[rowCounter++] = result.dataChunk.RowPhysicalIndex(i);
        }

        if (rowCounter == result.dataChunk._numberOfRows)
            return result;

        result.dataChunk._numberOfRows = rowCounter;
        for (Int i = 0;i < result.dataChunk._numberOfColumns; i++){
            auto* vec = result.dataChunk._columns[i];
            if (vec->IsConstant())
                continue;
            vec->ConvertToDictionary(result.dataChunk._selection);
        }

        return result;
    }

    void PhysicalFilter::UpdateScanState(const CoreEngine::StorageTypes::RID* rid){
        this->child->UpdateScanState(rid);
    }

    PhysicalTop::PhysicalTop(PlanNode* child, const BigInt top)
        : PlanNode(child->GetSchema()), top(top), child(child){}

    ExecutionResult PhysicalTop::Execute(CoreEngine::ExecutionContext& context){
        auto result = this->child->Execute(context);

        if (this->top > result.dataChunk._numberOfRows){
            this->top -= result.dataChunk._numberOfRows;
            return result;
        }

        result.dataChunk._numberOfRows = static_cast<Int>(this->top);
        this->top = 0;
        result.canFetchMore = false;
        return result;
    }

    void PhysicalTop::UpdateScanState(const CoreEngine::StorageTypes::RID* rid){
        this->child->UpdateScanState(rid);
    }

    PhysicalDistinct::PhysicalDistinct(PlanNode *child)
        : PlanNode(child->GetSchema()), child(child){}

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

    void PhysicalDistinct::UpdateScanState(const CoreEngine::StorageTypes::RID* rid){
        this->child->UpdateScanState(rid);
    }

    bool PhysicalOrderBy::CanBeSortedInMemory(const bool canFetchMore) const{
        return !canFetchMore && !this->UsesExternalStorage();
    }

    PhysicalOrderBy::PhysicalOrderBy(
        PlanNode *child,
        DataStructures::PolymorphicArray<Statements::OrderColumn*>& expressions
    )   : PlanNode(child->GetSchema())
          , child(child)
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

    void PhysicalOrderBy::UpdateScanState(const CoreEngine::StorageTypes::RID* rid){
        this->child->UpdateScanState(rid);
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

            rowCount += result.dataChunk._numberOfRows;
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
        const auto* db =  Network::Server::Get().UseDatabase(context, this->table->_databaseId);
        auto* tablePtr = db->OpenTable(this->table->_ordinalPosition);

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

        const auto* db =  Network::Server::Get().UseDatabase(context, this->table->_databaseId);
        auto* tablePtr = db->OpenTable(this->table->_ordinalPosition);

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

        const auto* db =  Network::Server::Get().UseDatabase(context, this->table->_databaseId);
        auto* tablePtr = db->OpenTable(this->table->_ordinalPosition);

        result.status = tablePtr->ClusteredIndexScanUpdate(context, this->expression, this->updates);
        return result;
    }

    PhysicalIndexSeekUpdate::PhysicalIndexSeekUpdate(
        Statements::DataSource *table,
        Expressions::Expression *expression,
        DataStructures::PolymorphicArray<Expressions::Expression*>& updates
    ): table(table), updates(std::move(updates)), expression(expression) {}

  ExecutionResult PhysicalIndexSeekUpdate::Execute(CoreEngine::ExecutionContext& context){
      const auto* db = Network::Server::Get().UseDatabase(context, this->table->_databaseId);

      auto* tablePtr = db->OpenTable(this->table->_ordinalPosition);

      DataTypes::Indexing::Key key;

      const auto updateResult = tablePtr->ClusteredIndexScanUpdate(context, this->expression, this->updates);

      // tablePtr->ClusteredIndexSeekUpdate(this->expression, &key, &key, this->fields);

      return ExecutionResult(updateResult.code, updateResult.message);
  }

  PhysicalHeapDelete::PhysicalHeapDelete(Statements::DataSource *table, Expressions::Expression *expression)
      : table(table), expression(expression) {}

  ExecutionResult PhysicalHeapDelete::Execute(CoreEngine::ExecutionContext& context){
      auto result = ExecutionResult(context);

      const auto* db = Network::Server::Get().UseDatabase(context, this->table->_databaseId);

      const CoreEngine::StorageTypes::Table* tablePtr = db->OpenTable(this->table->_ordinalPosition);

      tablePtr->HeapDelete(context, this->expression);

      return result;
  }

  PhysicalIndexScanDelete::PhysicalIndexScanDelete(Statements::DataSource *table, Expressions::Expression *expression)
      : table(table), expression(expression) {}

  ExecutionResult PhysicalIndexScanDelete::Execute(CoreEngine::ExecutionContext& context){
      auto result = ExecutionResult(context);

      const auto* db =  Network::Server::Get().UseDatabase(context, this->table->_databaseId);

      auto* tablePtr = db->OpenTable(this->table->_ordinalPosition);

      tablePtr->ClusteredIndexScanDelete(context, this->expression, state);

      return result;
  }

  PhysicalIndexSeekDelete::PhysicalIndexSeekDelete(Statements::DataSource *table, Expressions::Expression *expression)
      : table(table), expression(expression) {}

    ExecutionResult PhysicalIndexSeekDelete::Execute(CoreEngine::ExecutionContext& context){
        auto result = ExecutionResult(context);

        const auto* db =  Network::Server::Get().UseDatabase(context, this->table->_databaseId);

        auto* tablePtr = db->OpenTable(this->table->_ordinalPosition);

        tablePtr->ClusteredIndexSeekDelete(context, this->expression, this->state);

        return result;
    }

    PhysicalDeclareVariable::PhysicalDeclareVariable(
        const DataTypes::Guid &currentSessionId,
        Variable& variable,
        Expressions::Expression* expression
    ): PlanNode(currentSessionId), variable(std::move(variable)), expression(expression){}

    ExecutionResult PhysicalDeclareVariable::Execute(CoreEngine::ExecutionContext& context) {
        auto result = ExecutionResult(context);

        const Expressions::EvaluationContext evaluationContext(
            Expressions::EvaluationContext::EvaluationContextType::Constant,
            &context
        );

        auto value = Expressions::EvaluateExpression(this->expression, evaluationContext);
        this->variable.SetValue(value);

        if (!Network::Server::Get().AddOrSetVariable(this->sessionId, this->variable)){
            result.status = Errors::RuntimeStatus(
                Errors::RuntimeError::Error,
                Messages::FAILED_TO_ADD_VARIABLE,
                context.GetAllocator()
            );
            return result;
        }

        result.status = Errors::RuntimeStatus(
            Errors::RuntimeError::Ok,
            Messages::ADDED_VARIABLE(DataTypes::StringView::ViewOf(this->variable.GetName()), context.GetAllocator())
        );
        return result;
    }
}
