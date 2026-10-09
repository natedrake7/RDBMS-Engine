#include <CoreEngine/DatabaseConstants.h>
#include <CoreEngine/DataStorage/Table.h>
#include <CoreEngine/Database.h>

#include <Systemic/Guards/ReaderGuard.h>
#include <CoreEngine/BufferPool/StorageManager.h>
#include <CoreEngine/Contexts/ExecutionContext.h>
#include <CoreEngine/Evaluators/Expression.h>
#include <CoreEngine/Evaluators/VectorizedPushedDownFilter.h>
#include <CoreEngine/Memory/PersistentAllocator.h>

namespace CoreEngine::StorageTypes {
    Pages::IndexPageView Table::GetIndexFromDisk(const page_id_t indexPageId) const{
        return Storage::StorageManager::Get().GetPage<Pages::IndexPageView>(
            this->_db->DataFileKey(),
            indexPageId
        );
    }

    Errors::RuntimeStatus Table::ClusteredIndexInsert(
        const ExecutionContext& executionContext,
        ExtentReservation& extentReservation,
        SerializedRow& payload
    ){
        auto* tree = this->GetClusteredIndexedTree();

        auto* index = &this->_schema->_indexes[this->_schema->_clusteredIndexOrdinalPosition];
        std::span keyColumns(index->_keyColumns, index->_keyCount);

        auto key = this->CreateKey(
            executionContext,
            keyColumns,
            payload
        );

        auto tuple = Pages::IndexInsertTuple(key, &payload);
        auto status = tree->InsertRow(
            executionContext,
            tuple,
            extentReservation
        );

        if (!status.IsOk())
            return status;

        //since we already have the key we use that
        status.primaryKey = std::move(tuple.key);
        return status;
    }

    // Errors::RuntimeStatus Table::NonClusteredIndexInsert(
    //     const StorageTypes::Row* row,
    //     const Int nonClusteredIndexId,
    //     const Int pagesToAllocate,
    //     const DataTypes::RowIdentifier& data
    // ){
    //
    //     // const auto& indexedColumns = this->header.nonClusteredIndexes.at(nonClusteredIndexId).columns;
    //     //
    //     // auto* tree = this->GetNonClusteredIndexTree(nonClusteredIndexId);
    //     //
    //     // const auto key = Database::CreateKey(indexedColumns, row, data);
    //     //
    //     // int indexPosition = 0;
    //     // Errors::RuntimeStatus status;
    //     //
    //     // auto node = tree->InsertRow(key, pagesToAllocate, indexPosition, status);
    //     //
    //     // if (status.code != Errors::RuntimeError::Ok)
    //     //     return status;
    //     //
    //     // auto* keys = node->GetKeysUnsafe();
    //     //
    //     // keys->insert(keys->begin() + indexPosition, new DataTypes::Indexing::Key(key));
    //     //
    //     // auto* rows = node->NonClusteredDataNoLock();
    //     //
    //     // rows->insert(rows->begin() + indexPosition, data);
    //     //
    //     // node->UpdatePageSize();
    //     // node->UpdateBytesLeft();
    //     //
    //     // auto pageFreeSpacePage =  Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), node->GetPageId());
    //     // pageFreeSpacePage->SetPageMetaData(node.Get());
    //
    //     // return status;
    // }

    Errors::RuntimeStatus Table::NonClusteredIndexInsertExistingRows(const Int indexPos, const Int pagesToAllocate){
        if (this->GetType() == Constants::TableType::CLUSTERED) {
            this->InsertExistingRowsToNonClusteredIndexByClusteredIndex(indexPos, pagesToAllocate);
            return Errors::RuntimeStatus();
        }

        this->InsertExistingRowToNonClusteredIndexByHeap(indexPos, pagesToAllocate);
        return Errors::RuntimeStatus();
    }

    Storage::FileKey Table::GetSystemFileKey() const{ return this->_db->SystemFileKey(); }

    Storage::FileKey Table::GetDataFileKey() const{ return this->_db->DataFileKey(); }

    void Table::ClusteredIndexSeekRange(
        const ExecutionContext& executionContext,
        DataStructures::PolymorphicArray<RID>* selectedRows,
        const DataTypes::Indexing::Key& minKey,
        const DataTypes::Indexing::Key& maxKey,
        const Expressions::Expression* expression,
        const DataStructures::PolymorphicArray<FilterColumnInfo>& filterColumns,
        const UnsignedSmallInt slotIndex,
        const UnsignedSmallInt schemaWidth
    ){
        const auto* tree = this->GetClusteredIndexedTree();

        if (expression != nullptr) {
            VectorizedPushedDownFilter filter(expression, &filterColumns, &executionContext, slotIndex, schemaWidth);
            tree->SeekRange(executionContext, minKey, maxKey, selectedRows, filter);
            return;
        }

        tree->SeekRange(executionContext, minKey, maxKey, selectedRows);
    }

    void Table::ClusteredIndexSeek(
        const ExecutionContext& executionContext,
        DataStructures::PolymorphicArray<RID>* selectedRows,
        const DataTypes::Indexing::Key &key,
        const Expressions::Expression* expression,
        const DataStructures::PolymorphicArray<FilterColumnInfo>& filterColumns,
        const UnsignedSmallInt slotIndex,
        const UnsignedSmallInt schemaWidth
    ){
        const auto* tree = this->GetClusteredIndexedTree();

        if (expression != nullptr) {
            VectorizedPushedDownFilter filter(expression, &filterColumns, &executionContext, slotIndex, schemaWidth);
            tree->Seek(executionContext, key, selectedRows, filter);
            return;
        }

        tree->Seek(executionContext, key, selectedRows);
    }

    void Table::SystemClusteredIndexSeek(
        const ::Memory::IAllocator* allocator,
        DataStructures::PolymorphicArray<RID>* selectedRows,
        const DataTypes::Indexing::Key& key,
        const Expressions::Expression* expression
    ){
        const auto* tree = this->GetClusteredIndexedTree();

        if (expression != nullptr){
            tree->SystemSeek(allocator, key, selectedRows, expression);
            return;
        }

        tree->SystemSeek(key, selectedRows);
    }

    void Table::ClusteredIndexScan(
        const ExecutionContext& executionContext,
        DataStructures::PolymorphicArray<RID> *selectedRows,
        IndexState& state,
        const Expressions::Expression* expression,
        const DataStructures::PolymorphicArray<FilterColumnInfo>& filterColumns,
        const UnsignedSmallInt slotIndex,
        const UnsignedSmallInt schemaWidth
    ){
        if (this->IsEmpty())
            return;

        const auto* tree = this->GetClusteredIndexedTree();

        if(expression != nullptr){
            VectorizedPushedDownFilter filter(expression, &filterColumns, &executionContext, slotIndex, schemaWidth);
            tree->Scan(executionContext, selectedRows, state, filter);
            return;
        }

        tree->Scan(executionContext, selectedRows, state);
    }

    void Table::ClusteredIndexScan(
        const ExecutionContext& executionContext,
        DataStructures::PolymorphicArray<RID> *selectedRows,
        const Expressions::Expression *expression,
        const DataStructures::PolymorphicArray<FilterColumnInfo>& filterColumns,
        const Storage::FileKey* fileKeys
    ){
        if (this->IsEmpty())
            return;

        const auto* tree = this->GetClusteredIndexedTree();

        if(expression != nullptr){
            tree->Scan(executionContext, selectedRows, expression);
            return;
        }

        tree->Scan(executionContext, selectedRows);
    }

    void Table::SystemClusteredIndexScan(
        const ::Memory::IAllocator* allocator,
        DataStructures::PolymorphicArray<RID>* selectedRows,
        const Expressions::Expression* expression
    ){
        if (this->_physicalHeader.GetAllocationPageId() == INVALID_PAGE_ID)
            return;

        const auto* tree = this->GetClusteredIndexedTree();

        if(expression != nullptr){
            tree->SystemScan(allocator, selectedRows, expression);
            return;
        }

        tree->SystemScan(selectedRows);
    }

    void Table::NonClusteredIndexScan(
        const ExecutionContext& executionContext,
        DataStructures::PolymorphicArray<RID> *selectedRows,
        const Int indexPos,
        IndexState& state,
        const Expressions::Expression *expression
    ){
        const auto* tree = this->GetNonClusteredIndexTree(indexPos);

        DataStructures::PolymorphicArray<DataTypes::RowIdentifier> rowIds(executionContext.GetAllocator());
        tree->Scan(&rowIds, state, executionContext.GetBatchSize());

        Expressions::EvaluationContext evaluationContext(
            Expressions::EvaluationContext::EvaluationContextType::SingleRow,
            &executionContext
        );

        const auto fileKey = this->_db->DataFileKey();
        if (expression != nullptr) {

            for (const auto& rowId : rowIds) {
                const auto page = Storage::StorageManager::Get().GetPage<Pages::PageView>(
                    fileKey,
                    rowId.pageId
                );

                MultiThreading::ReaderGuard lock(&page.Latch());

                // auto pageRow = page->GetRow(this, rowId.indexId);
                //
                // const auto& row = pageRow.GetVisibleVersionForTransaction(properties.snapshot);

                // context.row = &row;
                // if (row.IsInvalid() || !expression->Evaluate(context).AsBool())
                //     continue;
                //
                // selectedRows->push_back(row);
            }

            return;
        }

        for (const auto& rowId : rowIds) {
            const auto page = Storage::StorageManager::Get().GetPage<Pages::PageView>(
                fileKey,
                rowId.pageId
            );

            MultiThreading::ReaderGuard lock(&page.Latch());

            // auto pageRow = page->GetRow(this, rowId.indexId);
            //
            // auto row = pageRow.GetVisibleVersionForTransaction(properties.snapshot);
            //
            // if (row.IsInvalid())
            //     continue;

            // selectedRows->push_back(row);
        }
    }

    void Table::ClusteredIndexScanDelete(
        const ExecutionContext& executionContext,
        const Expressions::Expression *expression,
        IndexState& state
    ){
        auto* tree = this->GetClusteredIndexedTree();

        std::vector<RID> results;
        // tree->IndexScan(properties, &results, state);

        if(results.empty())
            return;

        Expressions::EvaluationContext evaluationContext(
            Expressions::EvaluationContext::EvaluationContextType::SingleRow,
            &executionContext
        );

        for(const auto& row : results){

            // evaluationContext.row = &row;
            if(Expressions::RowModeFilter(expression, evaluationContext))
            {
                // const auto& key = Database::CreateKey(this->header.clusteredIndex.columns, &row);
                // tree->Remove(key);
            }
        }
    }

    void Table::ClusteredIndexSeekDelete(
        const ExecutionContext& executionContext,
        const Expressions::Expression *expression,
        IndexState &state
    ){

    }

    Int Table::CreateNonClusteredIndex(const DataStructures::PolymorphicArray<column_index_t>& columnIndices){
        // const Catalog::Index index(columnIndices.Data(), columnIndices.Size());
        // this->nonClusteredHeaders.Push(index);
        // return this->nonClusteredHeaders.Size() - 1;
    }

    page_id_t Table::GetClusteredIndexPageId() const { return this->_physicalHeader.GetClusteredIndexPageId(); }

    void Table::SetClusteredIndexPageId(const page_id_t pageId) const{
        this->_physicalHeader.SetClusteredIndexPageId(pageId);
    }

    page_id_t Table::GetNonClusteredIndexPageId(const Int indexPosition) const{
        return INVALID_PAGE_ID;
        // return this->_header.nonClusteredIndexPageIds[indexPosition];
    }

    void Table::SetNonClusteredIndexPageId(const page_id_t indexPageId, const Int indexPosition){
        // this->_header.nonClusteredIndexPageIds[indexPosition] = indexPageId;
    }

    Indexing::BTree* Table::GetClusteredIndexedTree(){
        if(this->_clusteredTree != nullptr)
            return this->_clusteredTree;

        this->_clusteredTree = this->_allocator.Allocate<Indexing::BTree>(
            this,
            this->_physicalHeader.GetClusteredIndexPageId(),
            Constants::TreeType::Clustered
        );

        if (this->_physicalHeader.GetClusteredIndexPageId() == INVALID_PAGE_ID)
            return this->_clusteredTree;

        this->_clusteredTree->SetTreeType(Constants::TreeType::Clustered);
        return this->_clusteredTree;
    }

      Indexing::BTree* Table::GetNonClusteredIndexTree(const Int nonClusteredIndexId){
          //
          // if(this->_nonClusteredTrees.Empty())
          //     this->_nonClusteredTrees.Resize(numOfIndexes);

          // if (this->header.nonClusteredIndexPageIds.Size() < numOfIndexes)
          //     this->header.nonClusteredIndexPageIds.Resize(numOfIndexes);

          auto* nonClusteredTree = this->_nonClusteredTrees[nonClusteredIndexId];

          // if (nonClusteredTree == nullptr){
          //     const auto indexPageId = this->_header.nonClusteredIndexPageIds[nonClusteredIndexId];
          //
          //     nonClusteredTree = this->_allocator.Allocate<Indexing::BTree>(
          //         this,
          //         indexPageId,
          //         Constants::TreeType::NonClustered,
          //         nonClusteredIndexId
          //     );
          //
          //     if (indexPageId == INVALID_PAGE_ID)
          //         return nonClusteredTree;
          //
          //     this->_clusteredTree->SetTreeType(Constants::TreeType::NonClustered);
          // }

          return nonClusteredTree;
      }

    DataTypes::Indexing::Key Table::CreateKey(
        const ExecutionContext& executionContext,
        const std::span<const Schemas::IndexKeyColumn>& columnKeys,
        const SerializedRow& payload
    ) const{
        DataStructures::PolymorphicArray<Value> values(executionContext.GetAllocator());
        for (const auto [ordinal, sort] : columnKeys){
            const auto value = payload.MaterializeColumn(
                executionContext,
                this->Column(ordinal)
            );
            values.Push(value);
        }

        return DataTypes::Indexing::Key(executionContext.GetAllocator(), values);
    }

    key_size_t Table::CalculateIndexKeySize(const Int indexPos) const {
        key_size_t keySize = 0;
        const auto* index = &this->_schema->_indexes[indexPos];
        for (Int i = 0;i < index->_keyCount; i++){
            const auto ordinal = index->_keyColumns[i]._ordinalPosition;
            keySize += this->_schema->_columns[ordinal]._recordSize;
        }

        return keySize;
    }
}
