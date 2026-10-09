#include <CoreEngine/DatabaseConstants.h>
#include <Systemic/DataTypes/Value.h>
#include <Systemic/DataStructures/BitMap.h>
#include <CoreEngine/DataStorage/Row/Row.h>
#include <CoreEngine/DataStorage/Table.h>

#include <cassert>
#include <CoreEngine/Messages.h>
#include <CoreEngine/SystemDatabases/SystemCatalog.h>
#include <CoreEngine/SystemDatabases/CatalogSchema.h>
#include <CoreEngine/BufferPool/StorageManager.h>
#include <CoreEngine/Indexing/BTree.h>
#include <Server/Server.h>
#include <Systemic/Guards/ReaderGuard.h>
#include <Systemic/Guards/WriterGuard.h>
#include <QueryPipeline/Statements.h>
#include <CoreEngine/Database.h>
#include <CoreEngine/Contexts/ExecutionContext.h>
#include <CoreEngine/DataStorage/LargeObjects/LobWriter.h>
#include <CoreEngine/DataStorage/Row/Row.SerializationContext.h>
#include <Systemic/DataStructures/PolymorphicArray.h>
#include <CoreEngine/Logger/WriteAheadLogger.h>
#include <CoreEngine/Memory/PersistentAllocator.h>

#include <Systemic/Macros.h>
#include <CoreEngine/DataStorage/Row/Row.InsertPlan.Templates.h>

#include "CoreEngine/Managers/IdentityManager.h"

#ifdef IS_GCC
    #include <cmath>
#endif

namespace CoreEngine::StorageTypes {
    page_id_t TableHeader::GetAllocationPageId()const{
        const std::atomic_ref _allocationPageIdAtomic(this->_allocationPageId);
        return _allocationPageIdAtomic.load(std::memory_order_acquire);
    }

    void TableHeader::SetAllocationPageId(const page_id_t allocationPageId) const{
        const std::atomic_ref _allocationPageIdAtomic(this->_allocationPageId);
        _allocationPageIdAtomic.store(allocationPageId, std::memory_order_release);
    }

    page_id_t TableHeader::GetClusteredIndexPageId() const{
        const std::atomic_ref _clusteredIndexPageIdAtomic(this->_clusteredIndexPageId);
        return _clusteredIndexPageIdAtomic.load(std::memory_order_acquire);
    }

    void TableHeader::SetClusteredIndexPageId(const page_id_t indexPageId) const{
        const std::atomic_ref _clusteredIndexPageIdAtomic(this->_clusteredIndexPageId);
        _clusteredIndexPageIdAtomic.store(indexPageId, std::memory_order_release);
    }

    bool Table::VectorContainsIndex(const DataStructures::PolymorphicArray<column_index_t>& vector, const column_index_t index, int& indexPosition){
        for(Int i = 0;i < vector.Size(); i++)
            if(vector[i] == index){
                indexPosition = i;
                return true;
            }

        return false;
    }

    Value Table::GenerateIdentityValue(const ::Memory::IAllocator* allocator, const column_index_t ordinal){
        return DataTypes::VisitDataType(this->Column(ordinal)->_type, [&]<typename T>()
        {
            if constexpr(DataTypes::IsInteger<T>)
                return Value(GenerateIdentityValue<T>(allocator, ordinal));
        });
    }

    void Table::InsertExistingRowsToNonClusteredIndexByClusteredIndex(const Int indexPos, const Int pagesToAllocate){
        const auto* clusteredTree = this->GetClusteredIndexedTree();
        // clusteredTree->InsertRowsToOtherTree(indexPos, pagesToAllocate);
    }

    void Table::InsertExistingRowToNonClusteredIndexByHeap(const Int indexPos, const Int pagesToAllocate){
        if(this->IsEmpty())
            return;

        const auto dataKey = this->_db->DataFileKey();

        const auto tableMapPage = Storage::StorageManager::Get().GetPage<Pages::AllocationPageView>(
            dataKey,
            this->_physicalHeader.GetAllocationPageId()
        );

        DataStructures::PolymorphicArray<extent_id_t> tableExtentIds;
        tableMapPage.GetAllocatedExtents(&tableExtentIds, 0);

        // const auto& indexedColumns = this->nonClusteredHeaders[indexPos].columns;
        //
        // for (const auto& extentId : tableExtentIds){
        //     const page_id_t extentFirstPageId = extentId * Constants::EXTENT_SIZE;
        //
        //     const auto pageFreeSpacePage = CoreEngine::Database::GetAssociatedPfsPage(
        //         this->_db->SystemFileKey(),
        //         extentFirstPageId
        //     );
        //
        //     const page_id_t pageId = (tableMapPage.PageId() != extentFirstPageId)
        //                                  ? extentFirstPageId
        //                                  : extentFirstPageId + 1;
        //
        //     for (page_id_t extentPageId = pageId; extentPageId < extentFirstPageId + Constants::EXTENT_SIZE; extentPageId++)
        //     {
        //         if (pageFreeSpacePage.GetPageType(extentPageId) != Constants::PageType::DATA)
        //             break;
        //
        //         auto page = Storage::StorageManager::Get().GetPage<Pages::PageView>(dataKey, extentPageId);
        //
        //         if (page.IsEmpty())
        //             continue;
        //
        //         // const auto rows = page.DataRowsNoLock(this);
        //         //
        //         // DataStructures::PolymorphicArray<extent_id_t> allocatedExtents;
        //         // extent_id_t startingExtentIndex = 0;
        //         //
        //         // for (int i = 0; i < rows.size(); i++) {
        //         //   const auto& row = rows.at(i);
        //         //
        //         //   Headers::RowIdentifier rowId(extentPageId, i);
        //         //   const auto key = Database::CreateKey(indexedColumns, &row, rowId);
        //         //   this->NonClusteredIndexInsert(&row, indexPos, pagesToAllocate, rowId);
        //         // }
        //     }
        // }
    }

    void Table::RemoveColumnByClusteredIndex(const column_index_t index){
        const auto* tree = this->GetClusteredIndexedTree();
        tree->RemoveColumnFromRow(index);
    }

    void Table::RemoveColumnByHeap(const column_index_t index)const{
        // const auto& filename = this->GetFileNameView();
        //
        // const auto tableMapPage = Storage::StorageManager::Get().GetPage<Pages::AllocationPageView>(filename, this->header.allocationPageId, this);
        //
        // DataStructures::PolymorphicArray<extent_id_t> allocatedExtents;
        // tableMapPage.GetAllocatedExtents(&allocatedExtents, 0);
        //
        // for (const auto& extentId: allocatedExtents) {
        // const page_id_t extentFirstPageId = Database::CalculateExtentFirstPageId(extentId);
        //
        // const page_id_t firstDataPageId = (tableMapPage.PageId() != extentFirstPageId)
        //             ? extentFirstPageId
        //             : extentFirstPageId + 1;
        //
        // for (page_id_t pageId = firstDataPageId; pageId < extentFirstPageId + EXTENT_SIZE; pageId++)
        // {
        // auto pageFreeSpacePage = Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), pageId);
        //
        // if (pageFreeSpacePage.GetPageType(pageId) != PageType::DATA)
        // break;
        //
        // auto page = Storage::StorageManager::Get().GetPage<Pages::PageView>(filename, pageId, this);
        //
        // // for (auto& row: page.DataRowsNoLock(this))
        // //   Table::HandleRemoveColumn(page.Get(), &row, index);
        //
        // pageFreeSpacePage.SetPageMetaData(&page);
        // }
        // }
    }

    RID Table::InsertToVersionDatabase(
        const ::Memory::IAllocator* allocator,
        const Pages::RawRowReference& rowRef
    ){
        static auto& versionDatabase = VersionDatabase::Get();
        return versionDatabase.InsertRow(allocator, rowRef);
    }

    void Table::PrepareChunkSources(
        const ExecutionContext& context,
        const InsertPlan& plan,
        const ChunkInsertState& state
    ){
        const auto* allocator = context.GetAllocator();

        const Expressions::EvaluationContext evaluationContext(
            Expressions::EvaluationContext::EvaluationContextType::SingleRow,
            &context
        );

        const auto vectorSourceFunc = [&](
            const column_index_t stateIndex,
            const column_index_t chunkSlotIndex
        ){
            state._vectors[stateIndex] = state._chunk->_columns[chunkSlotIndex];
        };

        const auto defaultSourceFunc = [&](
            const column_index_t stateIndex,
            const column_index_t defaultIndex,
            const DataType type
        ){
            const auto* expression = plan._sharedDefaults[defaultIndex];
            const DataVector* vector = nullptr;

            const auto evaluateExpression = [&](const Int i){
                bool isNull = false;
                Expressions::EvaluateExpression(expression, evaluationContext, vector->SlotAt(i), &isNull);
                vector->SetNullValue(i, isNull);
            };

            if (expression->Is<Expressions::ConstantExpression>()){
                vector = DataVector::ConstantVector(allocator, false, type);
                evaluateExpression(0);
            }
            else{
                vector = DataVector::FlatVector(allocator, type, state._rowCount);
                for (Int i = 0;i < state._rowCount; i++)
                    evaluateExpression(i);
            }

            state._vectors[stateIndex] = vector;
        };

        const auto nullSourceFunc = [&](const column_index_t stateIndex, const DataType type){
            const auto* vector = DataVector::ConstantVector(allocator, true, type);
            state._vectors[stateIndex] = vector;
        };

        //TODO add identity source.
        const auto identitySourceFunc = [&](const column_index_t slotIndex, const DataType type){
            const auto* column = this->Column(slotIndex);
            assert(column->_ordinalPosition == slotIndex && "Table::PrepareChunkSources: Invalid slot index");

            auto* vector = DataVector::FlatVector(allocator, type, state._rowCount);

            auto* identityManager = this->IdentityOf(slotIndex);
            const auto base = identityManager->ReserveRange<BigInt>(allocator, state._rowCount);
            const auto increment = identityManager->GetIncrement();

            VisitStorageType(type, [&]<typename T>(){
                if constexpr (DataTypes::IsInteger<T>){
                    auto* __restrict__ data = vector->DataAs<T>();
                    for (auto i = 0; i < state._rowCount; i++)
                        data[i] = static_cast<T>(base + i * increment);
                }
                else
                    std::unreachable();
            });

            state._vectors[slotIndex] = vector;
        };

        const auto prepareChunk = [&](const Int index, const InsertColumnPlan& columnPlan){
            switch (columnPlan._source) {
            case InsertColumnSource::Vector:
                vectorSourceFunc(index, columnPlan._slot);
                break;
            case InsertColumnSource::Default:
                defaultSourceFunc(index, columnPlan._slot, columnPlan._type);
                break;
            case InsertColumnSource::Null:
                nullSourceFunc(index, columnPlan._type);
                break;
            case InsertColumnSource::Identity:
                identitySourceFunc(index, columnPlan._type);
                break;
            case InsertColumnSource::Computed:
                std::unreachable();
            }
        };

        for (auto i = 0;i < state._columnCount; i++)
            prepareChunk(i, plan._columnsPlans[i]);
    }

    Table::Table(
        const Schemas::TableSchema* schema,
        const TableHeader &physicalHeader,
        Database *database
    ):  _physicalHeader(physicalHeader), _schema(schema),
        _db(database), _clusteredTree(nullptr){
        const auto nonClusteredCount = this->_schema->_indexesCount - this->IsClustered();
        if (nonClusteredCount > 0){
            this->_nonClusteredTrees.SetAllocator(&this->_allocator);
            this->_nonClusteredTrees.Resize(nonClusteredCount);
        }

        this->_identities.SetAllocator(&this->_allocator);
        this->_identities.Resize(this->_schema->_identitiesCount);

        for (Int i = 0;i < this->_schema->_columnCount; i++){
            const auto* column = this->Column(i);

            if (column->_identity != nullptr){
                auto& manager = this->_identities[column->_identity->_slot];
                manager.Bootstrap(column->_identity, this->_schema->_id, column->_id);
            }
        }
    }

    void Table::Destroy() const{
        this->_allocator.Release();
    }

    Errors::RuntimeStatus Table::ChunkInsert(
        const ExecutionContext& executionContext,
        const DataChunk* chunk,
        const InsertPlan& plan
    ) {
        Int rowSize = 0;
        DataStructures::PolymorphicArray<char> buffer;

        const auto* allocator = executionContext.GetAllocator();

        RowSerializationContext rowContext(allocator, this->ColumnCount());
        const auto transactionId = executionContext.GetCurrentTransactionId();

        rowContext._header._createdTransactionId = transactionId;

        // for (Int i = 0;i < input.Size(); i++){
        //     Errors::RuntimeStatus status;
        //     auto payload = this->SerializeRow(
        //         status,
        //         rowContext,
        //         input[i].Data()
        //     );
        //
        //     if (!status.IsOk())
        //         return status;
        //
        //     rowSize += static_cast<Int>(payload.Size());
        //     rows.Push(std::move(payload));
        // }

        auto checkPoint = Database::LogRowBatchInsert(
            buffer,
            transactionId,
            this->_schema->_ordinalPosition
        );

        const auto pageSize = this->IsClustered()
           ? static_cast<float>(Constants::INDEX_PAGE_DEFAULT_SIZE)
           : static_cast<float>(Constants::PAGE_SIZE_WITHOUT_HEADER);

        const auto pagesNeeded = static_cast<Int>(std::ceil(static_cast<float>(rowSize) / pageSize));

        auto extentReservation = this->ReserveExtents(allocator, pagesNeeded);

        Errors::RuntimeStatus result;
        // for (auto& payload: rows){
        //     result = this->InsertRowPayload(executionContext, extentReservation, payload);
        //     if (!result.IsOk())
        //         return result;
        // }

        Database::LogCheckPoint(checkPoint);
        return result;
    }

    Errors::RuntimeStatus Table::SystemInsertRow(
        const ExecutionContext& executionContext,
        const DataStructures::PolymorphicArray<Value> &inputData
    ){
        Logging::CheckPoint checkPoint;
        Errors::RuntimeStatus status;

        const auto* allocator = executionContext.GetAllocator();

        RowSerializationContext rowContext(allocator, this->ColumnCount());
        const auto transactionId = executionContext.GetCurrentTransactionId();
        rowContext._header._createdTransactionId = transactionId;

        auto payload = this->SerializeRow(
            status,
            rowContext,
            inputData
        );

        checkPoint.transactionId = transactionId;
        Database::LogCheckPoint(checkPoint);

        if (!status.IsOk())
            return status;

        auto extentReservation = this->LazyReservation(allocator);
        status = this->InsertRowPayload(executionContext, extentReservation, payload);

        if (!status.IsOk())
            return status;

        status.message = Messages::NUMBER_OF_ROWS_AFFECTED(allocator, 1);
        return status;
    }

    Errors::RuntimeStatus Table::InsertRowPayload(
        const ExecutionContext& executionContext,
        ExtentReservation& extentReservation,
        SerializedRow& payload
    ){
        //row_id
        auto status = this->IsClustered()
          ? this->ClusteredIndexInsert(executionContext, extentReservation, payload)
          : this->HeapInsert(executionContext, extentReservation, payload);

        // const auto rowId = status.rowId;
        if (!status.IsOk())
            return status;

        //insert to NonClustered Indexes
        // for (int i = 0; i < this->nonClusteredIndexes.size(); i++) {
        //     status = this->NonClusteredIndexInsert(row, i, pagesToAllocate, rowId);
        //
        //     if (status.code != Errors::RuntimeError::Ok)
        //         return status;
        // }

        // status.rowId = rowId;
        return status;
    }

    Errors::RuntimeStatus Table::HeapInsertToNewPage(
        ExtentReservation& extentReservation,
        const SerializedRow& payload
    ) const{
        const auto systemFileKey = this->_db->SystemFileKey();

        const auto newPage = extentReservation.Next<Pages::PageView>();
        const auto pfs = Database::GetAssociatedPfsPage(systemFileKey, newPage.PageId());

        MultiThreading::WriterGuard pfsLatch(&pfs.Latch());
        MultiThreading::WriterGuard pageLock(&newPage.Latch());

        const auto _ = newPage.InsertRow(payload);
        pfs.SetPageMetaData(&newPage);

        Errors::RuntimeStatus result;
        result.rid._pageId = newPage.PageId();
        result.rid._index = 0;
        return result;
    }

    Errors::RuntimeStatus Table::HeapInsert(
        const ExecutionContext& executionContext,
        ExtentReservation& extentReservation,
        const SerializedRow& payload
    )const{
        const auto systemFileKey = this->_db->SystemFileKey();
        const auto dataKey = this->_db->DataFileKey();

        if (this->IsEmpty())
            return this->HeapInsertToNewPage(extentReservation, payload);

        const auto allocPage = Storage::StorageManager::Get().GetPage<Pages::AllocationPageView>(
            dataKey,
            this->_physicalHeader.GetAllocationPageId()
        );

        DataStructures::PolymorphicArray<extent_id_t> tableExtentIds(executionContext.GetAllocator());
        allocPage.GetAllocatedExtents(&tableExtentIds, 0);

        const auto rowCategory = Database::GetObjectSizeToCategory(payload.Size());

        Errors::RuntimeStatus result;
        for (const auto extentId : tableExtentIds){
            const auto extentFirstPageId = Database::CalculateExtentFirstPageId(extentId);

            for (page_id_t pageId = extentFirstPageId; pageId < extentFirstPageId + Constants::EXTENT_SIZE; pageId++){
                auto pfs = Database::GetAssociatedPfsPage(systemFileKey, pageId);

                MultiThreading::ReaderGuard pfsLatch(&pfs.Latch());

                if (
                    pfs.GetPageType(pageId) != Constants::PageType::DATA
                    || !pfs.IsPageAllocated(pageId)
                ) break;

                const auto pageSizeCategory = pfs.GetPageSizeCategory(pageId);
                if (rowCategory > pageSizeCategory)
                    continue;

                auto page = Storage::StorageManager::Get().GetPage<Pages::PageView>(dataKey, pageId);

                MultiThreading::WriterGuard pageLock(&page.Latch());

                if (payload.Size() > page.BytesLeft())
                    continue;

                const auto index = page.InsertRow(payload);
                pfs.SetPageMetaData(&page);

                result.rid._pageId = pageId;
                result.rid._index = index;
                return result;
            }
        }

        return this->HeapInsertToNewPage(extentReservation, payload);
    }

    void Table::HeapScan(
        const ExecutionContext& executionContext,
        DataStructures::PolymorphicArray<RID>* result,
        ScanState& state
    )const{
        if(this->IsEmpty())
            return;

        const auto dataKey = this->_db->DataFileKey();
        const auto systemFileKey = this->_db->SystemFileKey();

        const auto& snapshot = executionContext.GetSnapshot();

        const auto allocPage = Storage::StorageManager::Get().GetPage<Pages::AllocationPageView>(dataKey, this->_physicalHeader.GetAllocationPageId());

        DataStructures::PolymorphicArray<extent_id_t> tableExtentIds(executionContext.GetAllocator());
        allocPage.GetAllocatedExtents(&tableExtentIds, state._extentId);

        state.canFetchMore = false;
        RID rid;
        for (const auto extentId : tableExtentIds){
            const auto firstPageId = Database::CalculateExtentFirstPageId(extentId);
            const auto pfs = Database::GetAssociatedPfsPage(systemFileKey, firstPageId);

            MultiThreading::ReaderGuard pfsLatch(&pfs.Latch());
            for (page_id_t currentPageId = state.GetPageId(firstPageId); currentPageId < firstPageId + Constants::EXTENT_SIZE; currentPageId++){
                if (
                    pfs.GetPageType(currentPageId) != Constants::PageType::DATA
                    || !pfs.IsPageAllocated(currentPageId)
                ) break;

                auto page = Storage::StorageManager::Get().GetPage<Pages::PageView>(dataKey, currentPageId);

                MultiThreading::ReaderGuard pageLock(&page.Latch());

                if (page.IsEmpty())
                    continue;

                for (Int index = state.GetNextKeyIndex(); index < page.PageSize(); index++){
                    if (!page.RetrieveVisibleRow(snapshot, index, &rid))
                        continue;
                    result->Push(rid);
                }

                if (result->Size() >= executionContext.GetBatchSize()) {
                    state.canFetchMore = true;
                    state.Update(extentId, result->Back());
                    return;
                }
            }
        }

        state.Reset();
    }

    void Table::TemporaryDatabaseHeapScan(
        DataStructures::PolymorphicArray<RID>* result,
        ScanState& state,
        const Int batchSize
    ) const{
        auto properties = ExecutionContext();
        properties.SetBatchSize(batchSize);

        // this->HeapScan(properties, result, state);
    }

    Errors::RuntimeStatus Table::HeapUpdate(
        const ExecutionContext& executionContext,
        const Expressions::Expression *expression,
        const DataStructures::PolymorphicArray<Value> &updates
    ){
        if(this->IsEmpty())
            return Errors::RuntimeStatus();

        const auto dataKey = this->_db->DataFileKey();
        const auto systemFileKey = this->_db->SystemFileKey();

        const auto tableMapPage = Storage::StorageManager::Get().GetPage<Pages::AllocationPageView>(
            dataKey,
            this->_physicalHeader.GetAllocationPageId()
        );

        DataStructures::PolymorphicArray<extent_id_t> tableExtentIds;
        tableMapPage.GetAllocatedExtents(&tableExtentIds, 0);

        for (const auto& extentId : tableExtentIds){
            const auto extentFirstPageId = extentId * Constants::EXTENT_SIZE;

            const auto pageFreeSpacePage = Database::GetAssociatedPfsPage(systemFileKey, extentFirstPageId);

            const auto pageId = (tableMapPage.PageId() != extentFirstPageId)
                                    ? extentFirstPageId
                                    : extentFirstPageId + 1;

            Expressions::EvaluationContext evaluationContext(
                Expressions::EvaluationContext::EvaluationContextType::SingleRow,
                &executionContext
            );

            for (page_id_t extentPageId = pageId; extentPageId < extentFirstPageId + Constants::EXTENT_SIZE; extentPageId++){
                if (pageFreeSpacePage.GetPageType(extentPageId) != Constants::PageType::DATA)
                    break;

                auto page = Storage::StorageManager::Get().GetPage<Pages::PageView>(dataKey, extentPageId);

                if (page.IsEmpty())
                    continue;

                for (Int i = 0; i < page.PageSize(); i++) {
                    auto row = RID(extentPageId, i);
                    evaluationContext._rids = &row;

                    const auto value = Expressions::EvaluateExpression(expression, evaluationContext);
                    if(!value.AsBool()) continue;

                    auto result = this->UpdateRowNoLock(&page, &row, executionContext, updates);
                    if (!result.IsOk()) return result;
                }
            }
        }

        return Errors::RuntimeStatus();
    }

    Errors::RuntimeStatus Table::HeapUpdate(
        const ExecutionContext& executionContext,
        const Expressions::Expression *expression,
        const DataStructures::PolymorphicArray<Expressions::Expression*> &updates
    ){
        if(this->IsEmpty())
            return Errors::RuntimeStatus();

        const auto dataKey = this->_db->DataFileKey();
        const auto systemFileKey = this->_db->SystemFileKey();

        const auto tableMapPage = Storage::StorageManager::Get().GetPage<Pages::AllocationPageView>(
            dataKey,
            this->_physicalHeader.GetAllocationPageId()
        );

        DataStructures::PolymorphicArray<extent_id_t> tableExtentIds;
        tableMapPage.GetAllocatedExtents(&tableExtentIds, 0);

        for (const auto extentId : tableExtentIds){
            const page_id_t extentFirstPageId = extentId * Constants::EXTENT_SIZE;

            const auto pageFreeSpacePage = Database::GetAssociatedPfsPage(systemFileKey, extentFirstPageId);

            const page_id_t pageId = (tableMapPage.PageId() != extentFirstPageId)
                                         ? extentFirstPageId
                                         : extentFirstPageId + 1;

            Expressions::EvaluationContext evaluationContext(
                Expressions::EvaluationContext::EvaluationContextType::SingleRow,
                &executionContext
            );
            for (page_id_t extentPageId = pageId; extentPageId < extentFirstPageId + Constants::EXTENT_SIZE; extentPageId++){
                if (pageFreeSpacePage.GetPageType(extentPageId) != Constants::PageType::DATA)
                    break;

                auto page = Storage::StorageManager::Get().GetPage<Pages::PageView>(dataKey, extentPageId);

                for (int i = 0;i < page.PageSize(); i++){
                    auto row = RID(extentPageId, i);
                    evaluationContext._rids = &row;

                    const auto value = Expressions::EvaluateExpression(expression, evaluationContext);
                    if(!value.AsBool())
                        continue;

                    auto result = this->UpdateRowNoLock(&page, &row, executionContext, updates);
                    if (!result.IsOk())
                        return result;
                }
            }
        }

        return Errors::RuntimeStatus();
    }

    void Table::ClusteredIndexScanUpdate(
        const ExecutionContext& executionContext,
        const Expressions::Expression *expression,
        const DataStructures::PolymorphicArray<Value> &updates
    ){
        const auto* tree = this->GetClusteredIndexedTree();
        tree->ScanUpdate(executionContext, expression, updates);
    }

    Errors::RuntimeStatus Table::ClusteredIndexScanUpdate(
        const ExecutionContext& executionContext,
        const Expressions::Expression *expression,
        const DataStructures::PolymorphicArray<Expressions::Expression*>& updates
    ){
        const auto* tree = this->GetClusteredIndexedTree();
        return (expression == nullptr)
                   ? tree->ScanUpdate(executionContext, updates)
                   : tree->ScanUpdate(executionContext, expression, updates);
    }

    Errors::RuntimeStatus Table::ClusteredIndexSeekUpdate(
        const ExecutionContext& executionContext,
        const Expressions::Expression* expression,
        const DataTypes::Indexing::Key* minimumValue,
        const DataTypes::Indexing::Key* maximumValue,
        const DataStructures::PolymorphicArray<Value> &updates
    ){
        const auto* tree = this->GetClusteredIndexedTree();

        return (expression == nullptr)
                   ? tree->SeekUpdate(executionContext, minimumValue, maximumValue, updates)
                   : tree->SeekUpdate(executionContext, expression, minimumValue, maximumValue, updates);
    }

    Errors::RuntimeStatus Table::ClusteredIndexSeekUpdate(
        const ExecutionContext& executionContext,
        const DataTypes::Indexing::Key &key,
        const DataStructures::PolymorphicArray<Value> &updates
    ) {
        const auto* tree = this->GetClusteredIndexedTree();
        return tree->SeekUpdate(executionContext, key, updates);
    }

    Errors::RuntimeStatus Table::SystemClusteredIndexSeekUpdate(
        const ::Memory::IAllocator* allocator,
        const DataTypes::Indexing::Key& key,
        const DataStructures::PolymorphicArray<Value>& updates
    ){
        const auto* tree = this->GetClusteredIndexedTree();
        return tree->SystemIndexSeekUpdate(allocator, key, updates);
    }

    //Handle overflow too dynamically probably during row insert
    Errors::RuntimeStatus Table::UpdateRowNoLock(
        const Pages::PageView* page,
        const RID* rid,
        const ExecutionContext& context,
        const DataStructures::PolymorphicArray<Value>& updates
    ){
        //copy row for old transactions
        //this has the pointers of the old row to LOBS and overflow pages
        const auto* allocator = context.GetAllocator();
        const auto versionRid = Table::InsertToVersionDatabase(allocator, page->RawRowData(rid->_index));

        auto materializedRow = page->MaterializeRow(allocator, this, rid->_index);
        materializedRow.Update(updates);

        Errors::RuntimeStatus status;

        RowSerializationContext rowContext(allocator, this->ColumnCount());
        rowContext._header._createdTransactionId = context.GetCurrentTransactionId();
        rowContext._header._versionRID = versionRid;

        auto newPayload = this->SerializeRow(
            status,
            rowContext,
            materializedRow.Data()
        );

        if (!status.IsOk())
            return status;

        if (page->UpdateRow(allocator, newPayload, rid->_index))
            return status;

        ExtentReservation extentReservation = this->LazyReservation(allocator);
        //only for heap tables
        auto insertResult = this->InsertRowPayload(context, extentReservation, newPayload);
        if (!insertResult.IsOk())
            return insertResult;

        //install forward referencing ptr to older row pos
        page->SetForwardPointer(rid->_index, &insertResult.rid);
        return insertResult;
    }

    Errors::RuntimeStatus Table::UpdateRowNoLock(
        const Pages::PageView* page,
        const RID* row,
        const ExecutionContext& context,
        const DataStructures::PolymorphicArray<Expressions::Expression*>& updates
    ){
        const auto rowRawData = page->RawRowData(row->_index);

        const auto* allocator = context.GetAllocator();
        const auto versionRid = Table::InsertToVersionDatabase(allocator, rowRawData);

        auto materializedRow = page->MaterializeRow(allocator, this, row->_index);

        const Expressions::EvaluationContext evaluationContext(row, &context);
        for (const auto* updateExpr : updates) {
            auto updatedValue = Expressions::EvaluateExpression(updateExpr, evaluationContext);
            updatedValue.SetColumnIndex(updateExpr->ordinalPosition);
            materializedRow.Update(updatedValue);
        }

        Errors::RuntimeStatus status;
        //update function here (all columns will be present on the materialized row now)
        RowSerializationContext rowContext(allocator, this->ColumnCount());
        rowContext._header._createdTransactionId = context.GetCurrentTransactionId();
        rowContext._header._versionRID = versionRid;

        auto newPayload = this->SerializeRow(
            status,
            rowContext,
            materializedRow.Data()
        );

        if (!status.IsOk())
            return status;

        if (page->UpdateRow(allocator, newPayload, row->_index))
            return status;

        auto extentReservation = this->ReserveExtents(allocator, 1);

        //only for heap tables
        auto insertResult = this->InsertRowPayload(context, extentReservation, newPayload);

        if (!insertResult.IsOk())
            return insertResult;

        //install forward referencing ptr to older row pos
        // page->SetForwardPointer(row->offset, insertResult.rowId);

        return insertResult;
    }

    Errors::RuntimeStatus Table::SystemUpdateRowNoLock(
        const Pages::PageView* page,
        const RID* row,
        const ::Memory::IAllocator* allocator,
        const DataStructures::PolymorphicArray<Value>& updates
    ) const{
        //copy row for old transactions
        //this has the pointers of the old row to LOBS and overflow pages
        const auto rowRawData = page->RawRowData(row->_index);
        const auto versionRid = CoreEngine::StorageTypes::Table::InsertToVersionDatabase(allocator, rowRawData);

        auto materializedRow = page->MaterializeRow(allocator, this, row->_index);
        materializedRow.Update(updates);

        Errors::RuntimeStatus status;
        RowSerializationContext rowContext(allocator, this->ColumnCount());
        rowContext._header._createdTransactionId = FIRST_TRANSACTION_ID;
        rowContext._header._versionRID = versionRid;

        const auto newPayload = this->SerializeRow(
            status,
            rowContext,
            materializedRow.Data()
        );

        if (!status.IsOk())
            return status;

        const auto _ = page->UpdateRow(allocator, newPayload, row->_index);
        return status;
    }

    void Table::HeapDelete(
        const ExecutionContext& executionContext,
        const Expressions::Expression* expression
    ) const{
        // if (this->IsEmpty()) return;
        //
        // const auto dataKey = this->database->GetDataFileKey();
        // const auto systemFileKey = this->database->GetSystemFileKey();
        //
        // const auto tableMapPage =
        //     Storage::StorageManager::Get().GetPage<Pages::AllocationPageView>(
        //         dataKey,
        //         this->header.allocationPageId,
        //         this
        //     );
        //
        // DataStructures::PolymorphicArray<extent_id_t> tableExtentIds;
        // tableMapPage.GetAllocatedExtents(&tableExtentIds, 0);
        //
        // DataStructures::PolymorphicArray<Row*> rowsToBeInserted;
        // Expressions::EvaluationContext evaluationContext(Expressions::EvaluationContext::EvaluationContextType::SingleRow, executionContext);
        //
        // for (const auto &extentId : tableExtentIds){
        //     const auto extentFirstPageId = Database::CalculateSystemPageOffset(extentId * Constants::EXTENT_SIZE);
        //
        //     auto pageFreeSpacePage = Database::GetAssociatedPfsPage(systemFileKey, extentFirstPageId);
        //
        //     const auto pageId = (tableMapPage.PageId() != extentFirstPageId)
        //            ? extentFirstPageId
        //            : extentFirstPageId + 1;
        //
        //     for (page_id_t extentPageId = pageId; extentPageId < extentFirstPageId + Constants::EXTENT_SIZE; extentPageId++){
        //         if (pageFreeSpacePage.GetPageType(extentPageId) != Constants::PageType::DATA)
        //             break;
        //
        //         auto page = Storage::StorageManager::Get().GetPage<Pages::PageView>(dataKey, extentPageId, this);
        //
        //         // const auto rows = page.DataRowsNoLock(this);
        //         //
        //         // for (int i = 0; i < rows.size(); i++) {
        //         //   const auto& row = rows.at(i);
        //         //
        //         //   executionContext.row = &row;
        //         //
        //         //   if (expression->Evaluate(executionContext).GetBool())
        //         //     page.Delete(i);
        //         // }
        //
        //         // page.UpdateBytesLeft();
        //         // page.UpdatePageSize();
        //
        //         pageFreeSpacePage.SetPageMetaData(&page);
        //     }
        // }
    }

    MaterializedRow Table::MaterializeFromPage(const ::Memory::IAllocator* allocator, const RID* row) const{
        const auto page = Storage::StorageManager::Get().GetPage<Pages::PageView>(
            this->_db->DataFileKey(),
            row->_pageId
        );

        return page.MaterializeRow(allocator, this, row->_index);
    }

    void Table::UpdateAllocationPageId(const page_id_t allocationPageId) const{
        this->_physicalHeader.SetAllocationPageId(allocationPageId);
    }

    page_id_t Table::GetAllocationPageId() const{ return this->_physicalHeader.GetAllocationPageId(); }

    void Table::DeleteLargeObjectFromPage(
        RID* rowPtr,
        const HashSet<column_index_t>& updatedColumns
    ){
        // auto* rowHeader = row->GetHeader();
        //
        // for(const auto& block : row->GetData()){
        //   const auto& columnIndex = block->ColumnIndex();
        //
        //   if(!updatedColumns.Contains(columnIndex)
        //     || !rowHeader->largeObjectBitMap.Get(columnIndex))
        //     continue;
        //
        //   rowHeader->largeObjectBitMap.Set(columnIndex, false);
        //
        //   auto objectPointer = block->AsLargeObjectPointer();
        //
        //   auto largeObjectPage = Storage::StorageManager::Get().GetLargeDataPage(filename, objectPointer, this);
        //
        //   auto* objectPtr = largeObjectPage->DeleteObject();
        //
        //   {
        //     auto pfsPage = Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), objectPointer);
        //
        //     MultiThreading::WriterGuard lock(&pfsPage->Latch());
        //
        //     pfsPage->SetPageMetaData(largeObjectPage.Get());
        //     pfsPage->SetPageFreed(largeObjectPage->GetPageId());
        //   }
        //
        //
        //   while(objectPtr->nextPageId != 0){
        //       auto nextLargeObjectPage = Storage::StorageManager::Get().GetLargeDataPage(filename, objectPtr->nextPageId, this);
        //
        //       auto* prevObject = objectPtr;
        //       objectPtr = nextLargeObjectPage->DeleteObject();
        //
        //       {
        //         auto pfsPage = Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), objectPointer);
        //
        //         MultiThreading::WriterGuard lock(&pfsPage->Latch());
        //
        //         pfsPage->SetPageMetaData(nextLargeObjectPage.Get());
        //         pfsPage->SetPageFreed(nextLargeObjectPage->GetPageId());
        //
        //       }
        //   }
        // }
    }

    void Table::DeleteOverflowedRowsFromPage(RID* rowPtr, const HashSet<column_index_t> & updatedColumns)const{
        // auto* rowHeader = row->GetHeader();
        //
        // for(const auto& block : row->GetData()){
        //   if(!updatedColumns.Contains(block->ColumnIndex())
        //     || !rowHeader->overflowBitMap.Get(block->ColumnIndex()))
        //       continue;
        //
        //   rowHeader->overflowBitMap.Set(block->ColumnIndex(), false);
        //
        //   const auto objectPointer = block->AsOverflowPointer();
        //
        //   auto overflowPage = Storage::StorageManager::Get().GetOverflowPage(filename, objectPointer.pageId, this);
        //
        //   const auto* overflowRow = overflowPage->DeleteObject(objectPointer.index);
        //
        //   auto pfsPage = Storage::StorageManager::Get().GetPageFreeSpacePage(filename, Database::GetPfsAssociatedPage(objectPointer.pageId));
        //
        //   pfsPage->SetPageMetaData(overflowPage.Get());
        //
        //   delete overflowRow;
        // }
    }

    ExtentReservation Table::ReserveExtents(const ::Memory::IAllocator* allocator, const Int requiredPages) const{
        return this->_db->ReserveExtents(
            allocator,
            requiredPages,
            this->_schema->_ordinalPosition
        );
    }

    ExtentReservation Table::LazyReservation(const ::Memory::IAllocator* allocator) const{
        return ExtentReservation(allocator, this->_db, this->_schema->_ordinalPosition);
    }

    void Table::Truncate(){
        this->_db->TruncateTable(this->_schema->_id);
    }

    Database* Table::GetDatabase() const { return this->_db; }


    bool Table::IsEmpty() const{
        return this->_physicalHeader.GetAllocationPageId() == INVALID_PAGE_ID;
    }

    void Table::PopulateColumn(const column_index_t index, const Value &defaultValue){
        if (this->IsEmpty())
            return;

        if (this->GetType() == Constants::TableType::CLUSTERED) {
            this->PopulateColumnByClusteredIndex(index, defaultValue);
            return;
        }

        this->PopulateColumnByHeap(index, defaultValue);
    }

    void Table::PopulateColumnByClusteredIndex(const column_index_t index, const Value &defaultValue){
        const auto* tree = this->GetClusteredIndexedTree();

        tree->InsertColumnToRow(index, defaultValue);
    }
    void Table::PopulateColumnByHeap(const column_index_t index, const Value &defaultValue){

        const auto dataKey = this->_db->DataFileKey();
        const auto systemKey = this->_db->SystemFileKey();

        const auto tableMapPage = Storage::StorageManager::Get().GetPage<Pages::AllocationPageView>(dataKey, this->_physicalHeader.GetAllocationPageId());

        DataStructures::PolymorphicArray<extent_id_t> allocatedExtents;
        tableMapPage.GetAllocatedExtents(&allocatedExtents, 0);

        for (const auto& extentId: allocatedExtents) {
            const page_id_t extentFirstPageId = Database::CalculateExtentFirstPageId(extentId);

            const page_id_t firstDataPageId = (tableMapPage.PageId() != extentFirstPageId)
                                                  ? extentFirstPageId
                                                  : extentFirstPageId + 1;

            for (page_id_t pageId = firstDataPageId; pageId < extentFirstPageId + Constants::EXTENT_SIZE; pageId++)
            {
                auto pageFreeSpacePage = Database::GetAssociatedPfsPage(systemKey, pageId);

                if (pageFreeSpacePage.GetPageType(pageId) != Constants::PageType::DATA)
                    break;

                auto page = Storage::StorageManager::Get().GetPage<Pages::PageView>(dataKey, pageId);

                // for (auto& row: page.DataRowsNoLock(this))
                //   this->HandleAddColumn(page.Get(), &row, index, defaultValue);

                pageFreeSpacePage.SetPageMetaData(&page);
            }
        }
    }

    void Table::Rollback(const Snapshot& snapshot, const DataTypes::RowIdentifier& rowId) const{
        // auto page = Storage::StorageManager::Get().GetPage<Pages::PageView>(this->database->GetFileName(), rowId.pageId, this);

        // MultiThreading::WriterGuard guard(&page.Latch());

        // auto rows = page.DataRowsNoLock(this);
        //
        // auto& row = rows.at(rowId.indexId);
        //
        // const auto& versionHeader = row.GetVersionHeader();
        //
        // if (versionHeader.olderVersionPointer.pageId == INVALID_PAGE_ID){
        //   //if insert remove indexes too
        //   //row did not exist previously mark it as invisible and delete it later
        //   row.SetDeletedTransactionId(0);
        //   return;
        // }
        //
        // static auto& versionDatabase = VersionDatabase::Get();
        //
        // const auto& versionRow = versionDatabase.RetrieveRow(snapshot, versionHeader.olderVersionPointer, this);
        //
        // const auto& rowData = row.GetData();
        // const auto& prevRowData = versionRow.GetData();
        //
        // for (Int i = 0;i < rowData.size(); i++){
        //   const auto& data = rowData[i];
        //
        //   auto& prevData = prevRowData[i];
        //
        //   data->SetData(prevData->GetRawData(), prevData->GetSize());
        // }
        //
        // const auto& prevRowVersionHeader = versionRow.GetVersionHeader();
        //
        // row.SetOlderVersionPointer(prevRowVersionHeader.olderVersionPointer.pageId, prevRowVersionHeader.olderVersionPointer.offset);
        // row.SetCurrentTransactionId(prevRowVersionHeader.createdTransactionId);
        // row.SetDeletedTransactionId(prevRowVersionHeader.deletedTransactionId);

        // page.UpdateBytesLeft();
    }
}
