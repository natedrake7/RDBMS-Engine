#include "../../../include/DatabaseConstants.h"
#include "../../../include/DataStorage/Table.h"
#include "../../../include/DataStorage/Column.h"
#include "../../../include/Database.h"

#include "../../../include/Pages/IndexPage.h"
#include "../../../../Systemic/include/Guards/ReaderGuard.h"
#include "../../../include/BufferPool/StorageManager.h"

namespace DatabaseEngine::StorageTypes {
    void Table::GetClusteredIndexFromDisk() const{
        auto root = Table::GetIndexFromDisk(this->header.clusteredIndexPageId);

        this->clusteredIndexedTree->SetTreeType(TreeType::Clustered);
    }

    void Table::GetNonClusteredIndexFromDisk(const Int indexId) const{
        auto root =  Table::GetIndexFromDisk(this->header.nonClusteredIndexPageIds[indexId]);

        this->nonClusteredIndexedTrees[indexId]->SetTreeType(TreeType::NonClustered);
    }

    Pages::PageGuard<Pages::IndexPage> Table::GetIndexFromDisk(const page_id_t indexPageId) const{
        const auto& filename = this->database->GetFileName();

        return Storage::StorageManager::Get().GetIndexPage(filename, indexPageId, this);
    }

    Errors::RuntimeStatus Table::ClusteredIndexInsert(InsertPayload& payload, const Int pagesToAllocate){
        auto* tree = this->GetClusteredIndexedTree();
        // auto key = Database::CreateKey(this->GetClusteredIndex(), row);

        auto key = this->CreateKey(this->GetClusteredIndex(), payload);

        int indexPosition = 0;

        auto tuple = Pages::IndexInsertTuple(key, &payload);
        auto status = tree->InsertRow(tuple, pagesToAllocate, indexPosition);

        if (status.code != Errors::RuntimeError::Ok)
            return status;

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
        if (this->GetType() == TableType::CLUSTERED) {
            this->InsertExistingRowsToNonClusteredIndexByClusteredIndex(indexPos, pagesToAllocate);
            return {};
        }

        this->InsertExistingRowToNonClusteredIndexByHeap(indexPos, pagesToAllocate);
        return {};
    }

    const Headers::Index& Table::GetNonClusteredIndexes(const Int indexPos) const { return this->header.nonClusteredIndexes.at(indexPos); }

    const std::vector<column_index_t> & Table::GetClusteredIndex() const { return this->header.clusteredIndex.columns; }

    void Table::ClusteredIndexSeekRange(
        const ExecutionProperties& properties,
        std::vector<Pages::RowReference> *selectedRows,
        const DataTypes::Indexing::Key& minKey,
        const DataTypes::Indexing::Key& maxKey,
        const Expressions::Expression* expression
    ){
        const auto* tree = this->GetClusteredIndexedTree();

        if (expression != nullptr) {
            tree->IndexSeekRange(properties, minKey, maxKey, selectedRows, expression);
            return;
        }

        tree->IndexSeekRange(properties, minKey, maxKey, selectedRows);
    }

    void Table::ClusteredIndexSeek(
        const ExecutionProperties &properties,
        std::vector<Pages::RowReference> *selectedRows,
        const DataTypes::Indexing::Key &key,
        const Expressions::Expression* expression
    ){
        const auto* tree = this->GetClusteredIndexedTree();

        if (expression != nullptr) {
            tree->IndexSeek(properties, key, selectedRows, expression);
            return;
        }

        tree->IndexSeek(properties, key, selectedRows);
    }

    void Table::ClusteredIndexScan(
        const ExecutionProperties& properties,
        std::vector<Pages::RowReference> *selectedRows,
        IndexState& state,
        const Expressions::Expression* expression
    ){
        if (this->header.indexAllocationMapPageId == INVALID_PAGE_ID)
            return;

        const auto* tree = this->GetClusteredIndexedTree();

        if(expression != nullptr){
            tree->IndexScan(properties, selectedRows, state, expression);
            return;
        }

        tree->IndexScan(properties, selectedRows, state);
    }

    void Table::ClusteredIndexScan(
        const ExecutionProperties& properties,
        std::vector<Pages::RowReference> *selectedRows,
        const Expressions::Expression *expression
    ){
        if (this->header.indexAllocationMapPageId == INVALID_PAGE_ID)
            return;

        const auto* tree = this->GetClusteredIndexedTree();

        if(expression != nullptr){
            tree->IndexScan(properties, selectedRows, expression);
            return;
        }

        tree->IndexScan(properties, selectedRows);
    }

    void Table::NonClusteredIndexScan(
        const ExecutionProperties& properties,
        std::vector<Pages::RowReference> *selectedRows,
        const Int indexPos,
        IndexState& state,
        const Expressions::Expression *expression
    ){
        const auto* tree = this->GetNonClusteredIndexTree(indexPos);

        std::vector<DataTypes::RowIdentifier> rowIds;
        tree->IndexScan(&rowIds, state, properties.batchSize);

        Expressions::EvaluationContext context(Expressions::EvaluationContext::EvaluationContextType::SingleRow, properties.variables);

        if (expression != nullptr) {

            for (const auto& rowId : rowIds) {
                const auto page = Storage::StorageManager::Get().GetPage(this->GetFileName(), rowId.pageId, this);

                MultiThreading::ReaderGuard lock(&page->Latch());

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
            const auto page = Storage::StorageManager::Get().GetPage(this->GetFileName(), rowId.pageId, this);

            MultiThreading::ReaderGuard lock(&page->Latch());

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
        const ExecutionProperties& properties,
        const Expressions::Expression *expression,
        IndexState& state
    ){
        auto* tree = this->GetClusteredIndexedTree();

        std::vector<Pages::RowReference> results;
        // tree->IndexScan(properties, &results, state);

        if(results.empty())
            return;

        Expressions::EvaluationContext context(Expressions::EvaluationContext::EvaluationContextType::SingleRow, properties.variables);

        for(const auto& row : results){

            context.row = &row;
            const auto value = expression->Evaluate(context);
            if(value.AsBool())
            {
                // const auto& key = Database::CreateKey(this->header.clusteredIndex.columns, &row);
                // tree->Remove(key);
            }
        }
    }

    void Table::ClusteredIndexSeekDelete(
        const ExecutionProperties& properties,
        const Expressions::Expression *expression,
        IndexState &state
    ){

    }

    int Table::CreateNonClusteredIndex(vector<column_index_t> &columnIndices){
        Headers::Index index;
        index.columns = std::move(columnIndices);

        this->header.nonClusteredIndexes.push_back(std::move(index));

        return static_cast<int>(this->header.nonClusteredIndexes.size() - 1);
    }

    page_id_t Table::GetClusteredIndexPageId() const { return this->header.clusteredIndexPageId; }

    void Table::SetClusteredIndexPageId(const page_id_t indexPageId) {
        this->header.clusteredIndexPageId = indexPageId;
    }

    page_id_t Table::GetNonClusteredIndexPageId(const Int indexPosition) const{
        return this->header.nonClusteredIndexPageIds.at(indexPosition);
    }

    void Table::SetNonClusteredIndexPageId(const page_id_t indexPageId, const Int indexPosition){
        this->header.nonClusteredIndexPageIds.at(indexPosition) = indexPageId;
    }

    Indexing::BTree* Table::GetClusteredIndexedTree(){
        if(this->clusteredIndexedTree != nullptr)
            return this->clusteredIndexedTree;

        this->clusteredIndexedTree = new Indexing::BTree(this, this->header.clusteredIndexPageId, TreeType::Clustered);

        if (this->header.clusteredIndexPageId == INVALID_PAGE_ID)
            return this->clusteredIndexedTree;

        this->GetClusteredIndexFromDisk();

        return this->clusteredIndexedTree;
    }

      Indexing::BTree * Table::GetNonClusteredIndexTree(const Int nonClusteredIndexId){
          const auto numOfIndexes = this->header.nonClusteredIndexes.size();

          if(this->nonClusteredIndexedTrees.empty())
              this->nonClusteredIndexedTrees.resize(numOfIndexes);

          if (this->header.nonClusteredIndexPageIds.size() < numOfIndexes)
              this->header.nonClusteredIndexPageIds.resize(numOfIndexes, INVALID_PAGE_ID);

          Indexing::BTree*& nonClusteredTree = this->nonClusteredIndexedTrees.at(nonClusteredIndexId);

          if (nonClusteredTree == nullptr)
          {
              const auto& indexPageId = this->header.nonClusteredIndexPageIds.at(nonClusteredIndexId);

              nonClusteredTree = new Indexing::BTree(this, indexPageId, TreeType::NonClustered, nonClusteredIndexId);

              if (indexPageId == INVALID_PAGE_ID)
                  return nonClusteredTree;

              this->GetNonClusteredIndexFromDisk(nonClusteredIndexId);
          }

          return nonClusteredTree;
      }

    bool Table::HasNonClusteredIndexes() const { return !this->header.nonClusteredIndexes.empty(); }

    DataTypes::Indexing::Key Table::CreateKey(
        const std::vector<column_index_t>& indexedColumns,
        const InsertPayload& payload
    ) const{
        auto key = DataTypes::Indexing::Key();
        for (const auto columnId : indexedColumns){
            auto value = payload.MaterializeColumn(
                this->columns.at(columnId),
                static_cast<Int>(this->columns.size())
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
        for(const auto& column : this->header.clusteredIndex.columns)
            clusteredColumns.Add(column);

        for (const auto &column : this->columns)
            if(clusteredColumns.Contains(column->OrdinalPosition()))
                keySize += column->Size();

        return keySize;
    }

    key_size_t Table::CalculateNonClusteredIndexKeySize(const Int indexPos) const{
        HashSet<column_index_t> clusteredColumns;

        key_size_t keySize = 0;
        for(const auto& column : this->header.nonClusteredIndexes.at(indexPos).columns)
            clusteredColumns.Add(column);

        for (const auto &column : this->columns)
            if(clusteredColumns.Contains(column->OrdinalPosition()))
                keySize += column->Size();

        return keySize;
    }
}
