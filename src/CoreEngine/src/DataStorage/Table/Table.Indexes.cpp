#include "../../../include/DatabaseConstants.h"
#include "../../../include/DataStorage/Table.h"
#include "../../../include/DataStorage/Column.h"
#include "../../../include/Database.h"

#include "../../../../Systemic/include/Guards/ReaderGuard.h"
#include "../../../include/BufferPool/StorageManager.h"
#include "Contexts/ExecutionContext.h"
#include "Managers/GlobalMemoryManager.h"
#include "Memory/PersistentAllocator.h"

namespace CoreEngine::StorageTypes {
    void Table::GetClusteredIndexFromDisk() const{
        // auto root = Table::GetIndexFromDisk(this->header.clusteredIndexPageId);
        this->clusteredIndexedTree->SetTreeType(Constants::TreeType::Clustered);
    }

    void Table::GetNonClusteredIndexFromDisk(const Int indexId) const{
        // auto root =  Table::GetIndexFromDisk(this->header.nonClusteredIndexPageIds[indexId]);
        this->nonClusteredIndexedTrees[indexId]->SetTreeType(Constants::TreeType::NonClustered);
    }

    Pages::IndexPageView Table::GetIndexFromDisk(const page_id_t indexPageId) const{
        const auto filename = this->database->GetFileName();
        const auto dataKey = this->database->GetDataFileKey();

        return Storage::StorageManager::Get().GetIndexPage(dataKey, filename, indexPageId, this);
    }

    Errors::RuntimeStatus Table::ClusteredIndexInsert(
        const ExecutionContext& executionContext,
        InsertPayload& payload,
        const Int pagesToAllocate
    ){
        auto* tree = this->GetClusteredIndexedTree();
        // auto key = Database::CreateKey(this->GetClusteredIndex(), row);

        auto key = this->CreateKey(
            executionContext,
            this->GetClusteredIndex(),
            payload
        );

        Int indexPosition = 0;
        auto tuple = Pages::IndexInsertTuple(key, &payload);
        auto status = tree->InsertRow(
            executionContext,
            tuple,
            pagesToAllocate,
            indexPosition
        );

        if (!status.IsOk()) return status;

        //TODO
        // rowId->indexId = indexPosition;
        // rowId->pageId = node->GetPageId();

        status.primaryKey = std::move(tuple.key);
        return status;
    }

    Errors::RuntimeStatus Table::NonClusteredIndexInsert(
        const StorageTypes::Row* row,
        const Int nonClusteredIndexId,
        const Int pagesToAllocate,
        const DataTypes::RowIdentifier& data
    ){

        // const auto& indexedColumns = this->header.nonClusteredIndexes.at(nonClusteredIndexId).columns;
        //
        // auto* tree = this->GetNonClusteredIndexTree(nonClusteredIndexId);
        //
        // const auto key = Database::CreateKey(indexedColumns, row, data);
        //
        // int indexPosition = 0;
        // Errors::RuntimeStatus status;
        //
        // auto node = tree->InsertRow(key, pagesToAllocate, indexPosition, status);
        //
        // if (status.code != Errors::RuntimeError::Ok)
        //     return status;
        //
        // auto* keys = node->GetKeysUnsafe();
        //
        // keys->insert(keys->begin() + indexPosition, new DataTypes::Indexing::Key(key));
        //
        // auto* rows = node->NonClusteredDataNoLock();
        //
        // rows->insert(rows->begin() + indexPosition, data);
        //
        // node->UpdatePageSize();
        // node->UpdateBytesLeft();
        //
        // auto pageFreeSpacePage =  Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), node->GetPageId());
        // pageFreeSpacePage->SetPageMetaData(node.Get());

        // return status;
    }

    Errors::RuntimeStatus Table::NonClusteredIndexInsertExistingRows(const Int indexPos, const Int pagesToAllocate){
        if (this->GetType() == Constants::TableType::CLUSTERED) {
            this->InsertExistingRowsToNonClusteredIndexByClusteredIndex(indexPos, pagesToAllocate);
            return {};
        }

        this->InsertExistingRowToNonClusteredIndexByHeap(indexPos, pagesToAllocate);
        return {};
    }

    Storage::FileKey Table::GetSystemFileKey() const{ return this->database->GetSystemFileKey(); }

    Storage::FileKey Table::GetDataFileKey() const{ return this->database->GetDataFileKey(); }

    const Headers::Index& Table::GetNonClusteredIndexes(const Int indexPos) const { return this->nonClusteredIndexes[indexPos]; }

    const DataStructures::StaticArray<column_index_t, 10>& Table::GetClusteredIndex() const { return this->clusteredIndexHeader.columns; }

    void Table::ClusteredIndexSeekRange(
        const ExecutionContext& executionContext,
        DataStructures::PolymorphicArray<Pages::RowReference>* selectedRows,
        const DataTypes::Indexing::Key& minKey,
        const DataTypes::Indexing::Key& maxKey,
        const Expressions::Expression* expression
    ){
        const auto* tree = this->GetClusteredIndexedTree();

        if (expression != nullptr) {
            tree->IndexSeekRange(executionContext, minKey, maxKey, selectedRows, expression);
            return;
        }

        tree->IndexSeekRange(executionContext, minKey, maxKey, selectedRows);
    }

    void Table::ClusteredIndexSeek(
        const ExecutionContext& executionContext,
        DataStructures::PolymorphicArray<Pages::RowReference>* selectedRows,
        const DataTypes::Indexing::Key &key,
        const Expressions::Expression* expression
    ){
        const auto* tree = this->GetClusteredIndexedTree();

        if (expression != nullptr) {
            tree->IndexSeek(executionContext, key, selectedRows, expression);
            return;
        }

        tree->IndexSeek(executionContext, key, selectedRows);
    }

    void Table::SystemClusteredIndexSeek(
        const ::Memory::IAllocator* allocator,
        DataStructures::PolymorphicArray<Pages::RowReference>* selectedRows,
        const DataTypes::Indexing::Key& key,
        const Expressions::Expression* expression
    ){
        const auto* tree = this->GetClusteredIndexedTree();

        if (expression != nullptr){
            tree->SystemIndexSeek(allocator, key, selectedRows, expression);
            return;
        }

        tree->SystemIndexSeek(allocator, key, selectedRows);
    }

    void Table::ClusteredIndexScan(
        const ExecutionContext& executionContext,
        DataStructures::PolymorphicArray<Pages::RowReference> *selectedRows,
        IndexState& state,
        const Expressions::Expression* expression
    ){
        if (this->header.allocationPageId == INVALID_PAGE_ID)
            return;

        const auto* tree = this->GetClusteredIndexedTree();

        if(expression != nullptr){
            tree->IndexScan(executionContext, selectedRows, state, expression);
            return;
        }

        tree->IndexScan(executionContext, selectedRows, state);
    }

    void Table::ClusteredIndexScan(
        const ExecutionContext& executionContext,
        DataStructures::PolymorphicArray<Pages::RowReference> *selectedRows,
        const Expressions::Expression *expression
    ){
        if (this->header.allocationPageId == INVALID_PAGE_ID)
            return;

        const auto* tree = this->GetClusteredIndexedTree();

        if(expression != nullptr){
            tree->IndexScan(executionContext, selectedRows, expression);
            return;
        }

        tree->IndexScan(executionContext, selectedRows);
    }

    void Table::SystemClusteredIndexScan(
        const ::Memory::IAllocator* allocator,
        DataStructures::PolymorphicArray<Pages::RowReference>* selectedRows,
        const Expressions::Expression* expression
    ){
        if (this->header.allocationPageId == INVALID_PAGE_ID)
            return;

        const auto* tree = this->GetClusteredIndexedTree();

        if(expression != nullptr){
            tree->SystemIndexScan(allocator, selectedRows, expression);
            return;
        }

        tree->SystemIndexScan(allocator, selectedRows);
    }

    void Table::NonClusteredIndexScan(
        const ExecutionContext& executionContext,
        DataStructures::PolymorphicArray<Pages::RowReference> *selectedRows,
        const Int indexPos,
        IndexState& state,
        const Expressions::Expression *expression
    ){
        const auto* tree = this->GetNonClusteredIndexTree(indexPos);

        DataStructures::PolymorphicArray<DataTypes::RowIdentifier> rowIds(executionContext.GetAllocator());
        tree->IndexScan(&rowIds, state, executionContext.GetBatchSize());

        Expressions::EvaluationContext evaluationContext(
            Expressions::EvaluationContext::EvaluationContextType::SingleRow,
            executionContext
        );

        const auto fileKey = this->database->GetDataFileKey();
        const auto filename = this->database->GetFileName();
        if (expression != nullptr) {

            for (const auto& rowId : rowIds) {
                const auto page = Storage::StorageManager::Get().GetPage(
                    fileKey,
                    filename,
                    rowId.pageId,
                    this
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
            const auto page = Storage::StorageManager::Get().GetPage(
                fileKey,
                filename,
                rowId.pageId,
                this
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

        std::vector<Pages::RowReference> results;
        // tree->IndexScan(properties, &results, state);

        if(results.empty())
            return;

        Expressions::EvaluationContext evaluationContext(
            Expressions::EvaluationContext::EvaluationContextType::SingleRow,
            executionContext
        );

        for(const auto& row : results){

            evaluationContext.row = &row;
            const auto value = expression->Evaluate(evaluationContext);
            if(value.AsBool())
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
        const Headers::Index index(columnIndices.Data(), columnIndices.Size());
        this->nonClusteredIndexes.Push(index);
        return this->nonClusteredIndexes.Size() - 1;
    }

    page_id_t Table::GetClusteredIndexPageId() const { return this->header.clusteredIndexPageId; }

    void Table::SetClusteredIndexPageId(const page_id_t indexPageId) {
        this->header.clusteredIndexPageId = indexPageId;
    }

    page_id_t Table::GetNonClusteredIndexPageId(const Int indexPosition) const{
        return this->header.nonClusteredIndexPageIds[indexPosition];
    }

    void Table::SetNonClusteredIndexPageId(const page_id_t indexPageId, const Int indexPosition){
        this->header.nonClusteredIndexPageIds[indexPosition] = indexPageId;
    }

    Indexing::BTree* Table::GetClusteredIndexedTree(){
        if(this->clusteredIndexedTree != nullptr)
            return this->clusteredIndexedTree;

        this->clusteredIndexedTree = this->_allocator.Allocate<Indexing::BTree>(
            this,
            this->header.clusteredIndexPageId,
            Constants::TreeType::Clustered
        );

        if (this->header.clusteredIndexPageId == INVALID_PAGE_ID)
            return this->clusteredIndexedTree;

        this->GetClusteredIndexFromDisk();
        return this->clusteredIndexedTree;
    }

      Indexing::BTree* Table::GetNonClusteredIndexTree(const Int nonClusteredIndexId){
          const auto numOfIndexes = this->nonClusteredIndexes.Size();

          if(this->nonClusteredIndexedTrees.Empty())
              this->nonClusteredIndexedTrees.Resize(numOfIndexes);

          // if (this->header.nonClusteredIndexPageIds.Size() < numOfIndexes)
          //     this->header.nonClusteredIndexPageIds.Resize(numOfIndexes);

          auto* nonClusteredTree = this->nonClusteredIndexedTrees[nonClusteredIndexId];

          if (nonClusteredTree == nullptr){
              const auto indexPageId = this->header.nonClusteredIndexPageIds[nonClusteredIndexId];

              nonClusteredTree = this->_allocator.Allocate<Indexing::BTree>(
                  this,
                  indexPageId,
                  Constants::TreeType::NonClustered,
                  nonClusteredIndexId
              );

              if (indexPageId == INVALID_PAGE_ID)
                  return nonClusteredTree;

              this->GetNonClusteredIndexFromDisk(nonClusteredIndexId);
          }

          return nonClusteredTree;
      }

    bool Table::HasNonClusteredIndexes() const { return !this->nonClusteredIndexes.Empty(); }

    DataTypes::Indexing::Key Table::CreateKey(
        const ExecutionContext& executionContext,
        const DataStructures::StaticArray<column_index_t, 10>& indexedColumns,
        const InsertPayload& payload
    ) const{
        auto key = DataTypes::Indexing::Key(executionContext.GetAllocator());

        for (const auto columnId : indexedColumns){
            auto value = payload.MaterializeColumn(
                executionContext,
                this->_columns[columnId],
                this->_columns.Size()
            );
            key.InsertKey(DataTypes::Indexing::Key(value));
        }

        return key;
    }

    key_size_t Table::CalculateIndexKeySize(const Int indexPos) const {
        HashSet<column_index_t> clusteredColumns;

        if (indexPos != -1)
            return this->CalculateNonClusteredIndexKeySize(indexPos);

        key_size_t keySize = 0;
        for(const auto& column : this->clusteredIndexHeader.columns)
            clusteredColumns.Add(column);

        for (const auto &column : this->_columns)
            if(clusteredColumns.Contains(column->OrdinalPosition()))
                keySize += column->Size();

        return keySize;
    }

    key_size_t Table::CalculateNonClusteredIndexKeySize(const Int indexPos) const{
        HashSet<column_index_t> clusteredColumns;

        key_size_t keySize = 0;
        for(const auto& column : this->nonClusteredIndexes[indexPos].columns)
            clusteredColumns.Add(column);

        for (const auto &column : this->_columns)
            if(clusteredColumns.Contains(column->OrdinalPosition()))
                keySize += column->Size();

        return keySize;
    }
}
