#include "../../../include/DatabaseConstants.h"
#include "../../../../Systemic/include/DataTypes/Value.h"
#include "../../../../Systemic/include/DataStructures/BitMap.h"
#include "../../../include/DataStorage/Column.h"
#include "../../../include/DataStorage/Row.h"
#include "../../../include/DataStorage/Table.h"

#include <cassert>

#include "../../../include/SystemDatabases/CatalogSchema.h"
#include "../../../include/SystemDatabases/SystemCatalog.h"
#include "../../../include/BufferPool/StorageManager.h"
#include "../../../include/BTree.h"
#include "../../../../Server/include/Server.h"
#include "../../../../Systemic/include/Guards/ReaderGuard.h"
#include "../../../../Systemic/include/Guards/WriterGuard.h"
#include "../../../../QueryPipeline/include/Statements.h"
#include "../../../include/Database.h"
#include "Logger/WriteAheadLogger.h"

#include "Memory/Allocator.h"

namespace DatabaseEngine::StorageTypes {
      TableHeader::TableHeader() {
        this->indexAllocationMapPageId = INVALID_PAGE_ID;
        this->tableId = 0;
        this->numberOfColumns = 0;
        this->clusteredIndexPageId = INVALID_PAGE_ID;
        this->ordinalPosition = 0;
      }

      TableHeader::~TableHeader() = default;

      TableHeader &TableHeader::operator=(const TableHeader &tableHeader) {
        if (this == &tableHeader)
          return *this;

        this->numberOfColumns = tableHeader.numberOfColumns;
        this->tableId = tableHeader.tableId;


        this->indexAllocationMapPageId = tableHeader.indexAllocationMapPageId;
        this->clusteredIndexPageId = tableHeader.clusteredIndexPageId;
        this->nonClusteredIndexPageIds = tableHeader.nonClusteredIndexPageIds;

        this->clusteredIndex = tableHeader.clusteredIndex;

        for(const auto& nonClusteredIndex: tableHeader.nonClusteredIndexes)
          this->nonClusteredIndexes.push_back(nonClusteredIndex);

        return *this;
      }

      bool Table::VectorContainsIndex(const std::vector<column_index_t>& vector, const column_index_t index, int& indexPosition){
        for(int i = 0;i < vector.size(); i++)
          if(vector[i] == index)
          {
            indexPosition = i;
            return true;
          }

        return false;
      }

      void Table::PopulateClusteredIndexCache(const Headers::Index& index){
        for (const auto& columnIndex: index.columns) {
          const auto* column = this->columns[columnIndex];
          this->clusteredIndexColumnsCache.Add(column->GetColumnId());
        }
      }

      bool Table::IsColumnAutoComputedPrimaryKey(const Column *column) const{
        return this->clusteredIndexColumnsCache.Contains(column->GetColumnId())
          && this->clusteredIndexColumnsCache.Size() == 1;
      }

      void Table::InsertExistingRowsToNonClusteredIndexByClusteredIndex(const Int indexPos, const Int pagesToAllocate){

        const auto* clusteredTree = this->GetClusteredIndexedTree();

        clusteredTree->InsertRowsToOtherTree(indexPos, pagesToAllocate);
    }

      void Table::InsertExistingRowToNonClusteredIndexByHeap(const Int indexPos, const Int pagesToAllocate){
        if(this->header.indexAllocationMapPageId == INVALID_PAGE_ID)
          return;

        const auto& filename = this->database->GetFileName();

        const auto tableMapPage = Storage::StorageManager::Get().GetAllocationPage(filename, this->header.indexAllocationMapPageId, this);

        std::vector<extent_id_t> tableExtentIds;
        tableMapPage.GetAllocatedExtents(&tableExtentIds, 0);

        const auto& indexedColumns = this->header.nonClusteredIndexes.at(indexPos).columns;

        for (const auto& extentId : tableExtentIds){
          const page_id_t extentFirstPageId = DatabaseEngine::Database::CalculateSystemPageOffset(extentId * EXTENT_SIZE);

          const auto pageFreeSpacePage = DatabaseEngine::Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), extentFirstPageId);

          const page_id_t pageId = (tableMapPage.PageId() != extentFirstPageId)
                                      ? extentFirstPageId
                                      : extentFirstPageId + 1;

          for (page_id_t extentPageId = pageId; extentPageId < extentFirstPageId + EXTENT_SIZE; extentPageId++)
          {
            if (pageFreeSpacePage.GetPageType(extentPageId) != PageType::DATA)
              break;

            auto page = Storage::StorageManager::Get().GetPage(filename, extentPageId, this);

            if (page.PageSize() == 0)
              continue;

            // const auto rows = page.DataRowsNoLock(this);
            //
            // std::vector<extent_id_t> allocatedExtents;
            // extent_id_t startingExtentIndex = 0;
            //
            // for (int i = 0; i < rows.size(); i++) {
            //   const auto& row = rows.at(i);
            //
            //   Headers::RowIdentifier rowId(extentPageId, i);
            //   const auto key = Database::CreateKey(indexedColumns, &row, rowId);
            //   this->NonClusteredIndexInsert(&row, indexPos, pagesToAllocate, rowId);
            // }
          }
        }
    }

      void Table::RemoveColumnByClusteredIndex(const column_index_t index){

    const auto* tree = this->GetClusteredIndexedTree();

    tree->RemoveColumnFromRow(index);
  }

     void Table::RemoveColumnByHeap(const column_index_t index)const{
    const auto& filename = this->GetFileName();

    const auto tableMapPage = Storage::StorageManager::Get().GetAllocationPage(filename, this->header.indexAllocationMapPageId, this);

    std::vector<extent_id_t> allocatedExtents;
    tableMapPage.GetAllocatedExtents(&allocatedExtents, 0);

    for (const auto& extentId: allocatedExtents) {
      const page_id_t extentFirstPageId = Database::CalculateExtentFirstPageId(extentId);

      const page_id_t firstDataPageId = (tableMapPage.PageId() != extentFirstPageId)
                                            ? extentFirstPageId
                                            : extentFirstPageId + 1;

      for (page_id_t pageId = firstDataPageId; pageId < extentFirstPageId + EXTENT_SIZE; pageId++)
      {
        auto pageFreeSpacePage = Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), pageId);

        if (pageFreeSpacePage.GetPageType(pageId) != PageType::DATA)
          break;

        auto page = Storage::StorageManager::Get().GetPage(filename, pageId, this);

        // for (auto& row: page.DataRowsNoLock(this))
        //   Table::HandleRemoveColumn(page.Get(), &row, index);

        pageFreeSpacePage.SetPageMetaData(&page);
      }
    }
  }

     void Table::InsertToVersionDatabase(const Pages::RowReference& rowPtr, const transaction_id_t transactionId) const{
        static auto& versionDatabase = VersionDatabase::Get();

        RowVersionPointer oldVersionPointer;
        versionDatabase.InsertRow(rowPtr, oldVersionPointer, this);
        // row->SetOlderVersionPointer(oldVersionPointer.pageId, oldVersionPointer.offset);
        // row->SetCurrentTransactionId(transactionId);
     }

     Table::Table(
        const table_id_t tableId,
        const Int ordinalPosition,
        const std::vector<Column*> &columns,
        Database *database,
        const Headers::Index* clusteredIndex,
        const std::vector<Headers::Index> *nonClusteredIndexes
      ){
//        this->schema = schema;
        this->columns = columns;
        this->database = database;
        this->header.numberOfColumns = columns.size();
        this->header.tableId = tableId;
        this->header.ordinalPosition = ordinalPosition;

        this->clusteredIndexedTree = nullptr;

        if(clusteredIndex) {
          this->header.clusteredIndex = *clusteredIndex;
          this->PopulateClusteredIndexCache(this->header.clusteredIndex);
        }

        if(nonClusteredIndexes)
          this->header.nonClusteredIndexes = *nonClusteredIndexes;
      }

      Table::Table(const Headers::TableHeader& masterDbHeader, const TableHeader &tableHeader, DatabaseEngine::Database *database)
      {
        this->header = tableHeader;
        this->header.tableId = masterDbHeader.id;
        this->header.ordinalPosition = masterDbHeader.ordinalPosition;
        this->database = database;
        this->clusteredIndexedTree = nullptr;

        this->PopulateClusteredIndexCache(this->header.clusteredIndex);
      }

      Table::Table(const std::string& tableName, const TableHeader &tableHeader, DatabaseEngine::Database *database)
      {
        this->header = tableHeader;
        this->database = database;
        this->clusteredIndexedTree = nullptr;

        this->PopulateClusteredIndexCache(this->header.clusteredIndex);
      }

      Table::Table(
        const Headers::sysTable &systemHeader,
        const TableHeader &tableHeader,
        const Headers::Index& primaryKey,
        DatabaseEngine::Database *database,
        const Int ordinalPosition){

        this->header = tableHeader;
        this->header.clusteredIndex = primaryKey;
        this->database = database;
        this->header.tableId = systemHeader.id;
        this->header.ordinalPosition = ordinalPosition;
        this->clusteredIndexedTree = nullptr;

        for (int i = 0;i < systemHeader.columns.size(); i++)
          this->AddColumn(new Column(systemHeader.columns[i], i,  this));

        this->PopulateClusteredIndexCache(this->header.clusteredIndex);
      }

      Table::~Table(){
        delete this->clusteredIndexedTree;

        for (const auto & nonClusteredIndexedTree : this->nonClusteredIndexedTrees) {
          this->header.nonClusteredIndexPageIds.push_back(nonClusteredIndexedTree->GetFirstIndexPageId());
            delete nonClusteredIndexedTree;
        }

        auto headerPage = Storage::StorageManager::Get().GetHeaderPage(this->database->GetSystemFilename());
        headerPage.SetTableHeader(this->header.ordinalPosition, this->header);

        for (const auto &column : columns)
            delete column;
      }

    Errors::RuntimeStatus Table::BatchInsert(
        const ExecutionProperties &properties,
        std::vector<QueryResult> &input
      ) {
        std::vector<InsertPayload> rows;
        rows.reserve(input.size());

        Int pagesNeeded = 0;

        std::vector<char> buffer;

        Int allocationSize = 0;
        for (const auto& row : input)
          allocationSize += row.GetByteSize();

        Memory::Allocator allocator(allocationSize, Memory::AllocationType::Persistent);

        for (auto& insertedRow : input) {
            Errors::RuntimeStatus status;
            auto payload = this->CreateInsertPayload(
                status,
                properties.snapshot.transactionId,
                insertedRow.Data()
            );

          if (!status.IsOk())
            return status;

            pagesNeeded += static_cast<Int>(payload.Size());
            rows.push_back(std::move(payload));
        }

        auto checkPoint = Database::LogRowBatchInsert(
            buffer,
            properties.snapshot.transactionId,
            this->header.ordinalPosition
        );

        if (this->IsClustered())
          pagesNeeded /= INDEX_PAGE_DEFAULT_SIZE;
        else
          pagesNeeded /= PAGE_SIZE_WITHOUT_HEADER;

        if (pagesNeeded == 0)
          pagesNeeded = 1;

        Errors::RuntimeStatus result;
        for (auto& payload: rows){
            result = this->InsertRow(payload, pagesNeeded);
            if (!result.IsOk())
                return result;
        }

        Database::LogCheckPoint(checkPoint);
        return result;
      }

  Errors::RuntimeStatus Table::InsertRow(
        const ExecutionProperties& properties,
        std::vector<Value> &inputData
    ){
        Logging::CheckPoint checkPoint;

        Errors::RuntimeStatus status;
        auto payload = this->CreateInsertPayload(status, properties.snapshot.transactionId, inputData);

        checkPoint.transactionId = properties.snapshot.transactionId;
        Logging::WriteAheadLogger::Get().LogCheckPoint(checkPoint);

        if (!status.IsOk())
            return status;

        status =  this->InsertRow(payload, 1);

        if (!status.IsOk())
          return status;

        Database::LogCheckPoint(checkPoint);

        status.message = "Rows affected: 1";

        return status;
    }

    // Errors::RuntimeStatus Table::InsertRow(
    //   const ExecutionProperties& properties,
    //   const vector<Expressions::Expression *> &inputData,
    //   const std::vector<column_index_t> &columnIndices
    // ){
    //     Logging::CheckPoint checkPoint;
    //
    //     auto* row = new Row(*this);
    //     auto result = this->CreateRow(
    //       row,
    //       properties.snapshot.transactionId,
    //       inputData,
    //       columnIndices,
    //       &checkPoint
    //     );
    //
    //     if (result.code != Errors::RuntimeError::Ok)
    //       return result;
    //
    //     result = this->InsertRow(row, 1);
    //
    //     if (result.code != Errors::RuntimeError::Ok)
    //       return result;
    //
    //     Database::LogCheckPoint(checkPoint);
    //
    //     result.message = "Rows affected: 1";
    //
    //     return result;
    //
    // }

    Errors::RuntimeStatus Table::InsertRow(InsertPayload& payload, const Int pagesToAllocate){
        // this->InsertLargeObjectToPage(row);

        //row_id
        auto status = this->IsClustered()
            ? this->ClusteredIndexInsert(payload, pagesToAllocate)
            : this->HeapInsert(payload, pagesToAllocate);

        const auto rowId = status.rowId;
        if (status.code != Errors::RuntimeError::Ok)
            return status;

        //insert to NonClustered Indexes
        // for (int i = 0; i < this->header.nonClusteredIndexes.size(); i++) {
        //     status = this->NonClusteredIndexInsert(row, i, pagesToAllocate, rowId);
        //
        //     if (status.code != Errors::RuntimeError::Ok)
        //         return status;
        // }

        status.rowId = rowId;
        return status;
      }

    void Table::DeleteLargeObjectFromPage(
        Pages::RowReference& rowPtr,
        const HashSet<column_index_t>& updatedColumns
    ){
      const auto& filename = this->database->GetFileName();

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

    void Table::DeleteOverflowedRowsFromPage(Pages::RowReference& rowPtr, const HashSet<column_index_t> & updatedColumns)const{
      const auto& filename = this->database->GetFileName();

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

    std::string Table::GetFileName() const{ return this->database->GetFileName(); }

    column_number_t Table::GetNumberOfColumns() const { return this->columns.size(); }

    const TableHeader &Table::GetHeader() const { return this->header; }

    const std::vector<Column *> &Table::GetColumns() const { return this->columns; }

    std::vector<const Column *> Table::GetConstantColumns() const {
        std::vector<const Column*> constColumns;

        for (const auto* column : this->columns)
          constColumns.push_back(column);

        return constColumns;
      }

    void Table::HeapScan(
      const ExecutionProperties& properties,
      std::vector<Pages::RowReference> *result,
      ScanState& state
    )const
    {
        if(this->header.indexAllocationMapPageId == INVALID_PAGE_ID)
            return;

        const auto& filename = this->database->GetFileName();

        const auto tableMapPage = Storage::StorageManager::Get().GetAllocationPage(filename, this->header.indexAllocationMapPageId, this);

        std::vector<extent_id_t> tableExtentIds;
        tableMapPage.GetAllocatedExtents(&tableExtentIds, state.extentId);

        state.canFetchMore = false;
        for (const auto& extentId : tableExtentIds){
          const page_id_t extentFirstPageId = Database::CalculateSystemPageOffset(extentId * EXTENT_SIZE);

          const auto pageFreeSpacePage = Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), extentFirstPageId);

          const auto extentStartingPageId = (tableMapPage.PageId() != extentFirstPageId)
                                      ? extentFirstPageId
                                      : extentFirstPageId + 1;

          MultiThreading::ReaderGuard pfsLatch(&pageFreeSpacePage.Latch());

          for (page_id_t extentPageId = state.GetPageId(extentStartingPageId); extentPageId < extentFirstPageId + EXTENT_SIZE; extentPageId++){
            if (pageFreeSpacePage.GetPageType(extentPageId) != PageType::DATA)
              break;

            auto page = Storage::StorageManager::Get().GetPage(filename, extentPageId, this);

            MultiThreading::ReaderGuard lock(&page.Latch());

            if (page.PageSize() == 0)
              continue;

            //update state to know where to start
            state.extentId = extentId;
            state.lastFetchedRowId.pageId = extentPageId;

            for (int i = state.GetNextKeyIndex(); i < page.PageSize(); i++) {
              auto rowPtr = page.PeekRow(i, 0);

              result->push_back(std::move(rowPtr));

              state.lastFetchedRowId.indexId = i;

              if (result->size() == properties.batchSize) {
                state.canFetchMore = true;
                return;
              }

              //if can fetch more in current batch reset index
              state.lastFetchedRowId.indexId = INVALID_INDEX_ID;
            }
          }
        }
    }

    void Table::TemporaryDatabaseHeapScan(
      std::vector<Pages::RowReference>* result,
      ScanState& state,
      const Int batchSize
    ) const{
        auto properties = ExecutionProperties();
        properties.batchSize = batchSize;

        this->HeapScan(properties, result, state);
    }

    void Table::HeapDelete(
      const ExecutionProperties& properties,
      const Expressions::Expression* expression
    ) const
    {
        if (this->header.indexAllocationMapPageId == INVALID_PAGE_ID)
          return;

        const auto& filename = this->database->GetFileName();

        const auto tableMapPage =
            Storage::StorageManager::Get().GetAllocationPage(
              filename,
              this->header.indexAllocationMapPageId,
              this
            );

        std::vector<extent_id_t> tableExtentIds;
        tableMapPage.GetAllocatedExtents(&tableExtentIds, 0);

        std::vector<Row*> rowsToBeInserted;
        Expressions::EvaluationContext context(Expressions::EvaluationContext::EvaluationContextType::SingleRow, properties.variables);

        for (const auto &extentId : tableExtentIds)
        {
          const auto extentFirstPageId = Database::CalculateSystemPageOffset(extentId * EXTENT_SIZE);

          auto pageFreeSpacePage = Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), extentFirstPageId);

          const auto pageId = (tableMapPage.PageId() != extentFirstPageId)
                                       ? extentFirstPageId
                                       : extentFirstPageId + 1;

          for (page_id_t extentPageId = pageId; extentPageId < extentFirstPageId + EXTENT_SIZE; extentPageId++)
          {
            if (pageFreeSpacePage.GetPageType(extentPageId) != PageType::DATA)
              break;

            auto page = Storage::StorageManager::Get().GetPage(filename, extentPageId, this);

            // const auto rows = page.DataRowsNoLock(this);
            //
            // for (int i = 0; i < rows.size(); i++) {
            //   const auto& row = rows.at(i);
            //
            //   context.row = &row;
            //
            //   if (expression->Evaluate(context).GetBool())
            //     page.Delete(i);
            // }

            // page.UpdateBytesLeft();
            // page.UpdatePageSize();

            pageFreeSpacePage.SetPageMetaData(&page);
          }
        }
    }

    Errors::RuntimeStatus Table::HeapInsert(const InsertPayload& payload, const Int pagesToAllocate)const{
        const auto& filename = this->database->GetFileName();

        Errors::RuntimeStatus status;
        // while(payload.Size() > Constants::PAGE_SIZE_WITHOUT_HEADER)
        // this->HandleRowOverflow(row);

      if (this->header.indexAllocationMapPageId == INVALID_PAGE_ID){
          const auto newPage = this->database->CreateDataPage(this->header.ordinalPosition, pagesToAllocate);

          MultiThreading::WriterGuard pageLock(&newPage.Latch());

          status.rowId.indexId = newPage.InsertRow(payload);
          status.rowId.pageId = newPage.PageId();

          return status;
      }

      const auto tableMapPage = Storage::StorageManager::Get().GetAllocationPage(filename, this->header.indexAllocationMapPageId, this);

      std::vector<extent_id_t> tableExtentIds;
      tableMapPage.GetAllocatedExtents(&tableExtentIds, 0);

      const auto rowCategory = Database::GetObjectSizeToCategory(payload.Size());

      for (const auto &extentId : tableExtentIds)
      {
          const page_id_t extentFirstPageId = Database::CalculateExtentFirstPageId(extentId);

          const page_id_t firstDataPageId = (tableMapPage.PageId() != extentFirstPageId)
                                                ? extentFirstPageId
                                                : extentFirstPageId + 1;

          for (page_id_t pageId = firstDataPageId; pageId < extentFirstPageId + EXTENT_SIZE; pageId++)
          {

              auto pageFreeSpacePage = Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), pageId);

              MultiThreading::ReaderGuard pfsLatch(&pageFreeSpacePage.Latch());

              if (pageFreeSpacePage.GetPageType(pageId) != PageType::DATA)
                  break;

              const auto pageSizeCategory = pageFreeSpacePage.GetPageSizeCategory(pageId);

              // find potential candidate
              if (rowCategory <= pageSizeCategory)
              {
                  auto page = Storage::StorageManager::Get().GetPage(filename, pageId, this);

                  MultiThreading::WriterGuard pageLock(&page.Latch());

                  if (payload.Size() > page.BytesLeft())
                      continue;

                  status.rowId.indexId = page.InsertRow(payload);
                  pageFreeSpacePage.SetPageMetaData(&page);

                  status.rowId.pageId = pageId;
                  return status;
              }
          }
      }

      const auto newPage = this->database->CreateDataPage(this->header.ordinalPosition, pagesToAllocate);

      MultiThreading::WriterGuard pageLock(&newPage.Latch());

      status.rowId.indexId = newPage.InsertRow(payload);
      status.rowId.pageId = newPage.PageId();

      return {};
    }

    Errors::RuntimeStatus Table::HeapUpdate(
        const ExecutionProperties& properties,
        const Expressions::Expression *expression,
        const std::vector<Value> &updates
    ){
        if(this->header.indexAllocationMapPageId == INVALID_PAGE_ID)
            return {};

        const auto& filename = this->database->GetFileName();

        const auto tableMapPage = Storage::StorageManager::Get().GetAllocationPage(filename, this->header.indexAllocationMapPageId, this);

        std::vector<extent_id_t> tableExtentIds;
        tableMapPage.GetAllocatedExtents(&tableExtentIds, 0);

        for (const auto& extentId : tableExtentIds){
            const auto extentFirstPageId = Database::CalculateSystemPageOffset(extentId * EXTENT_SIZE);

            const auto pageFreeSpacePage = Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), extentFirstPageId);

            const auto pageId = (tableMapPage.PageId() != extentFirstPageId)
                                      ? extentFirstPageId
                                      : extentFirstPageId + 1;

            Expressions::EvaluationContext context(Expressions::EvaluationContext::EvaluationContextType::SingleRow, properties.variables);

            for (page_id_t extentPageId = pageId; extentPageId < extentFirstPageId + EXTENT_SIZE; extentPageId++){
                if (pageFreeSpacePage.GetPageType(extentPageId) != PageType::DATA)
                  break;

                auto page = Storage::StorageManager::Get().GetPage(filename, extentPageId, this);

                if (page.PageSize() == 0)
                  continue;


                std::vector<extent_id_t> allocatedExtents;
                for (int i = 0; i < page.PageSize(); i++) {
                    auto row = page.PeekRow(i, 0);

                    context.row = &row;
                    const auto value = expression->Evaluate(context);
                    if(!value.AsBool())
                        continue;

                    auto result = this->UpdateRowNoLock(&page, row, properties, updates);

                    if (result.code != Errors::RuntimeError::Ok)
                        return result;
                }
            }
        }

        return {};
    }

    Errors::RuntimeStatus Table::HeapUpdate(
        const ExecutionProperties& properties,
        const Expressions::Expression *expression,
        const std::vector<Expressions::Expression*> &updates
    ){
        if(this->header.indexAllocationMapPageId == INVALID_PAGE_ID)
            return {};

        const auto& filename = this->database->GetFileName();

        const auto tableMapPage = Storage::StorageManager::Get().GetAllocationPage(filename, this->header.indexAllocationMapPageId, this);

        std::vector<extent_id_t> tableExtentIds;
        tableMapPage.GetAllocatedExtents(&tableExtentIds, 0);

        for (const auto& extentId : tableExtentIds){

            const page_id_t extentFirstPageId = Database::CalculateSystemPageOffset(extentId * EXTENT_SIZE);

            const auto pageFreeSpacePage = Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), extentFirstPageId);

            const page_id_t pageId = (tableMapPage.PageId() != extentFirstPageId)
                                      ? extentFirstPageId
                                      : extentFirstPageId + 1;

            Expressions::EvaluationContext context(Expressions::EvaluationContext::EvaluationContextType::SingleRow, properties.variables);
            for (page_id_t extentPageId = pageId; extentPageId < extentFirstPageId + EXTENT_SIZE; extentPageId++){
                if (pageFreeSpacePage.GetPageType(extentPageId) != PageType::DATA)
                  break;

                auto page = Storage::StorageManager::Get().GetPage(filename, extentPageId, this);

                for (int i = 0;i < page.PageSize(); i++){
                    auto rowPtr = page.PeekRow(i, 0);
                    context.row = &rowPtr;

                    const auto value = expression->Evaluate(context);
                    if(!value.AsBool())
                        continue;

                    auto result = this->UpdateRowNoLock(&page, rowPtr, properties, updates);
                    if (!result.IsOk())
                        return result;
                }
            }
        }

        return {};
    }

    void Table::ClusteredIndexScanUpdate(
      const ExecutionProperties& properties,
      const Expressions::Expression *expression,
      const std::vector<Value> &updates
    ){
      const auto* tree = this->GetClusteredIndexedTree();
      tree->IndexScanUpdate(properties, expression, updates);
    }

    Errors::RuntimeStatus Table::ClusteredIndexScanUpdate(
      const ExecutionProperties& properties,
      const Expressions::Expression *expression,
      const std::vector<Expressions::Expression*>& updates
    ){
        const auto* tree = this->GetClusteredIndexedTree();

        return (expression == nullptr)
          ? tree->IndexScanUpdate(properties, updates)
          : tree->IndexScanUpdate(properties, expression, updates);
    }

    Errors::RuntimeStatus Table::ClusteredIndexSeekUpdate(
        const ExecutionProperties& properties,
        const Expressions::Expression* expression,
        const DataTypes::Indexing::Key* minimumValue,
        const DataTypes::Indexing::Key* maximumValue,
        const std::vector<Value> &updates
    ){
        const auto* tree = this->GetClusteredIndexedTree();

        return (expression == nullptr)
            ? tree->IndexSeekUpdate(properties, minimumValue, maximumValue, updates)
            : tree->IndexSeekUpdate(properties, expression, minimumValue, maximumValue, updates);
    }

    Errors::RuntimeStatus Table::ClusteredIndexSeekUpdate(
      const ExecutionProperties &properties,
      const DataTypes::Indexing::Key &key,
      const std::vector<Value> &updates
    ) {
        const auto* tree = this->GetClusteredIndexedTree();
        return tree->IndexSeekUpdate(properties, key, updates);
      }

    void Table::Truncate()
    {
        this->database->TruncateTable(this->header.tableId);
    }

    void Table::UpdateIndexAllocationMapPageId(const page_id_t indexAllocationMapPageId){
      this->header.indexAllocationMapPageId = indexAllocationMapPageId;
    }

    page_id_t Table::GetIndexAllocationMapPageId() const{ return this->header.indexAllocationMapPageId; }

    void Table::AddColumn(Column *column) { this->columns.push_back(column); }

    table_id_t Table::GetTableId() const { return this->header.tableId; }

    TableType Table::GetType() const{
        return !this->header.clusteredIndex.columns.empty()
                    ? TableType::CLUSTERED
                    : TableType::HEAP;
    }

    bool Table::IsClustered() const{
        return !this->header.clusteredIndex.columns.empty();
    }

    row_size_t Table::GetMaximumRowSize() const
    {
        row_size_t maximumRowSize = 0;

        for (const auto &column : this->columns)
            maximumRowSize += column->isColumnLOB()
                ? Constants::LARGE_OBJECT_POINTER_SIZE
                : column->Size();

        return maximumRowSize;
    }

    row_size_t Table::ReduceMaximumRowSize() const {
      row_size_t maximumRowSize = 0;

      row_size_t largestVariableLengthColumnSize = 0;

      Column* largestColumn = nullptr;

      HashSet<column_index_t> clusteredColumns;

      for(const auto& column : this->header.clusteredIndex.columns)
        clusteredColumns.Add(column);

      for (auto &column : this->columns) {
        const auto& columnSize = column->Size();

        if (column->isColumnOverflowed())
          maximumRowSize += Constants::OVERFLOW_POINTER_SIZE;
        else if (column->isColumnLOB())
          maximumRowSize += Constants::LARGE_OBJECT_POINTER_SIZE;
        else
          maximumRowSize += columnSize;

        if(columnSize <= largestVariableLengthColumnSize
            || column->isColumnOverflowed()
            || columnSize >= LARGE_DATA_OBJECT_SIZE
            || clusteredColumns.Contains(column->OrdinalPosition()))
          continue;

        largestVariableLengthColumnSize = columnSize;
        largestColumn = column;
      }

      maximumRowSize -= largestVariableLengthColumnSize;
      maximumRowSize += Constants::OVERFLOW_POINTER_SIZE;

      if (largestColumn != nullptr)
        largestColumn->SetIsOverflowed(true);

      return maximumRowSize;
    }

    std::vector<DataType> Table::GetColumnTypeByTreeId(const uint8_t& treeId) const{
          std::vector<DataType> columnDatatypes;

          if(treeId == 0){
            for(const auto& columnIndex: this->header.clusteredIndex.columns)
                columnDatatypes.emplace_back(this->columns[columnIndex]->Type());

            return columnDatatypes;
          }

//          for(const auto& columnIndex: this->header.nonClusteredColumnIndexes[treeId - 1])
//              columns.emplace_back(this->columns[columnIndex]->GetColumnType());

          return columnDatatypes;
      }

    int Table::HandleRowOverflow(Pages::RowReference& rowPtr) const{
      // auto largestBlock = row->FindLargestVariableLengthColumn();

      // if(largestBlock.IsNull())
      //   return -1;
      //
      // auto overflowPage = this->database->GetLastOverflowPage(this->header.ordinalPosition, largestBlock->Size());
      //
      // int indexPos = 0;
      // overflowPage->InsertObject(largestBlock.Data(), largestBlock.Size(), indexPos);
      //
      // row->SetOverflowBitMapValue(largestBlock.GetColumnIndex(), true);
      //
      // auto pfsPage = Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), overflowPage->GetPageId());
      //
      // pfsPage->SetPageMetaData(overflowPage.Get());
      //
      // const Pages::OverflowPointer ptr(overflowPage->GetPageId(), indexPos);
      // largestBlock->SetData(&ptr, Constants::OVERFLOW_POINTER_SIZE);

      // return largestBlock.Size();
    }

      int Table::HandleRowOverflow(Pages::RowReference& rowPtr, const Column *column)const{
        // auto& data = row->GetData();
        //
        // if(data.size() < column->OrdinalPosition())
        //   return -1;
        //
        // auto* largestBlock = row->GetData().at(column->OrdinalPosition());
        //
        // auto overflowPage = this->database->GetLastOverflowPage(this->header.ordinalPosition, largestBlock->Size());
        //
        // int indexPos = 0;
        // overflowPage->InsertObject(largestBlock->Data(), largestBlock->Size(), indexPos);
        //
        // row->SetOverflowBitMapValue(largestBlock->ColumnIndex(), true);
        //
        // const auto pfsPageId = DatabaseEngine::Database::GetPfsAssociatedPage(overflowPage->GetPageId());
        //
        // auto pfsPage = Storage::StorageManager::Get().GetPageFreeSpacePage(this->database->GetFileName(), pfsPageId);
        //
        // pfsPage->SetPageMetaData(overflowPage.Get());
        //
        // const Pages::OverflowPointer ptr(overflowPage->GetPageId(), indexPos);
        // largestBlock->SetData(&ptr, Constants::OVERFLOW_POINTER_SIZE);
        //
        // return largestBlock->Size();
    }

    //Handle overflow too dynamically probably during row insert
    Errors::RuntimeStatus Table::UpdateRowNoLock(
        Pages::PageView* page,
        const Pages::RowReference& rowPtr,
        const ExecutionProperties& properties,
        const std::vector<Value>& updates
    ){
        // this->DeleteLargeObjectFromPage(row, updatedColumns);
        // this->DeleteOverflowedRowsFromPage(row, updatedColumns);

        //copy row for old transactions
        //this has the pointers of the old row to LOBS and overflow pages
        // this->InsertToVersionDatabase(row, properties.snapshot.transactionId);

        auto materializedRow = rowPtr.Materialize();
        materializedRow.Update(updates);

        Errors::RuntimeStatus status;
        auto newPayload = this->CreateUpdatePayload(status, properties.snapshot.transactionId, materializedRow.Data());

        if (!status.IsOk())
            return status;

          if (page->UpdateRow(newPayload, rowPtr))
              return status;

          //only for heap tables
          auto insertResult = this->InsertRow(newPayload, 1);

          if (!insertResult.IsOk())
              return insertResult;

          //install forward referencing ptr to older row pos
          page->SetForwardPointer(rowPtr.indexPosition, insertResult.rowId);
          return insertResult;
    }

    Errors::RuntimeStatus Table::UpdateRowNoLock(
        Pages::PageView* page,
        const Pages::RowReference& rowPtr,
        const ExecutionProperties& properties,
        const std::vector<Expressions::Expression*>& updates
    ){
        auto materializedRow = rowPtr.Materialize();

        const Expressions::EvaluationContext context(&rowPtr, properties.variables);
        for (const auto& updateExpr : updates) {
            auto updatedValue = updateExpr->Evaluate(context);
            updatedValue.SetColumnIndex(updateExpr->columnIndex);
            materializedRow.Update(updatedValue);
        }

        Errors::RuntimeStatus status;
        //update function here (all columns will be present on the materialized row now)
        auto newPayload = this->CreateUpdatePayload(status, properties.snapshot.transactionId, materializedRow.Data());

        if (!status.IsOk())
            return status;

        if (page->UpdateRow(newPayload, rowPtr))
            return status;

        //only for heap tables
        auto insertResult = this->InsertRow(newPayload, 1);

        if (!insertResult.IsOk())
            return insertResult;

        //install forward referencing ptr to older row pos
        page->SetForwardPointer(rowPtr.indexPosition, insertResult.rowId);

        return insertResult;
    }

    void Table::RetrieveDefaultValuesFromCatalog() const{
        for(const auto& column: this->columns) {
          const auto systemHeader = SystemCatalog::Get().SelectDefaultValueByColumnId(column->GetColumnId());

          if (systemHeader.columnId == INVALID_COLUMN_ID)
            continue;

          column->SetDefaultValue(systemHeader);
        }
    }

    void Table::RetrieveColumnHeadersFromCatalog()const{
      const auto headers = SystemCatalog::Get().SelectColumns(this->header.tableId);

      assert(headers.size() == this->columns.size());

      if (headers.empty())
        return;

      for (int i = 0;i < this->columns.size(); i++) {
        auto& column = columns[i];

        column->SetColumnId(headers[i].id);
      }
    }

    void Table::UpdateCatalogIdentityColumns() const{
        static auto& catalog = SystemCatalog::Get();

        const auto headers = catalog.SelectIdentityColumnsByTableId(this->header.tableId);

        if(headers.empty())
          return;

        for(const auto& column: this->columns){
          for (const auto& identity: headers) {
            if(column->GetColumnId() != identity.columnId)
              continue;

            column->SetIdentityManagerIds(this->header.tableId);
            break;
          }
        }
    }

    void Table::RetrieveIdentityColumnsFromCatalog()const{
      static auto& catalog = SystemCatalog::Get();

      const auto identityHeaders = catalog.SelectIdentityColumnsByTableId(this->header.tableId);

      if(identityHeaders.empty())
        return;

      for(const auto& column: this->columns){
        for (const auto& identity: identityHeaders) {

          if(column->GetColumnId() != identity.columnId)
            continue;

          column->SetIdentity(identity);
          break;
        }
      }
    }

    void Table::RetrieveIdentityColumnById(const int32_t &columnId)const{
        static auto& catalog = SystemCatalog::Get();

        const auto identityHeaders = catalog.SelectIdentityColumnsByTableId(this->header.tableId);

        if (identityHeaders.empty())
          return;

        for(const auto& column: this->columns){

          if (columnId != column->GetColumnId())
            continue;

          for (const auto& identity: identityHeaders) {
            if(column->GetColumnId() != identity.columnId)
              continue;

            column->SetIdentity(identity);
            break;
          }
        }
    }

  void Table::RetrieveIndexesFromCatalog(){
        Dictionary<int32_t, Column*> columnsDict;

        for (auto& column: this->columns)
          columnsDict.Add(column->GetColumnId(), column);

        const auto indexes = SystemCatalog::Get().SelectIndexes(this->header.tableId);

        for (const auto& index: indexes) {
          const auto indexedColumns = SystemCatalog::Get().SelectIndexColumnsByIndexId(index.id);

          std::vector<column_index_t> indexColumnsIndices;
          for (const auto& indexedColumn : indexedColumns)
            indexColumnsIndices.emplace_back(columnsDict.Get(indexedColumn.columnId)->OrdinalPosition());


          if (index.isClustered) {
            this->header.clusteredIndex.columns = std::move(indexColumnsIndices);
            continue;
          }

          Headers::Index tableIndex(indexColumnsIndices);

          this->header.nonClusteredIndexes.push_back(std::move(tableIndex));
        }
    }

  void Table::UpdateSystemCatalog() const{
      for (const auto& column: this->columns)
        column->UpdateMetadata();
    }

  void Table::UpdateColumnName(const column_index_t index, const std::string &name)const{
      auto* column = this->columns.at(index);

      column->SetColumnName(name);
  }
void Table::PopulateColumn(const column_index_t index, const Value &defaultValue){
      if (this->header.indexAllocationMapPageId == INVALID_PAGE_ID)
        return;

      if (this->GetType() == TableType::CLUSTERED) {
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
    const auto& filename = this->GetFileName();

    const auto tableMapPage = Storage::StorageManager::Get().GetAllocationPage(filename, this->header.indexAllocationMapPageId, this);

    std::vector<extent_id_t> allocatedExtents;
    tableMapPage.GetAllocatedExtents(&allocatedExtents, 0);

    for (const auto& extentId: allocatedExtents) {
      const page_id_t extentFirstPageId = Database::CalculateExtentFirstPageId(extentId);

      const page_id_t firstDataPageId = (tableMapPage.PageId() != extentFirstPageId)
                                            ? extentFirstPageId
                                            : extentFirstPageId + 1;

      for (page_id_t pageId = firstDataPageId; pageId < extentFirstPageId + EXTENT_SIZE; pageId++)
      {
          auto pageFreeSpacePage = Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), pageId);

          if (pageFreeSpacePage.GetPageType(pageId) != PageType::DATA)
            break;

          auto page = Storage::StorageManager::Get().GetPage(filename, pageId, this);

          // for (auto& row: page.DataRowsNoLock(this))
          //   this->HandleAddColumn(page.Get(), &row, index, defaultValue);

          pageFreeSpacePage.SetPageMetaData(&page);
        }
    }
  }

    //TODO add heap insert if row still cant remain in page if heap
    void Table::HandleAddColumn(
        const Pages::PageView* page,
        const Pages::RowReference& rowPtr,
        const column_index_t index,
        const Value &defaultValue
    ){
        auto materializedRow = rowPtr.Materialize();

        materializedRow.AddColumn(defaultValue, index);

        Errors::RuntimeStatus status;
        const auto payload = this->CreateInsertPayload(status, 0, materializedRow.Data());

        page->UpdateRow(payload, rowPtr);

        // this->InsertLargeObjectToPage(row);

        // if(isHeap && (PAGE_SIZE - PageHeader::GetPageHeaderSize() - row->GetTotalRowSize()) > 0){
        //   vector<extent_id_t> allocatedExtents;
        //   extent_id_t startingExtentIndex = 0;
        //
        //   this->InsertRInsertRow(row, allocatedExtents, startingExtentIndex);
        //
        //   return;
        // }

        // while(page.BytesLeft() - diff < 0){
        //     const int result = this->HandleRowOverflow(row);
        //
        //     if(result == -1)
        //     break;
        //
        //     diff -= result;
        // }
    }

  void Table::HandleRemoveColumn(Pages::PageView* page, QueryResult& row, const column_index_t index){
        // auto& data = row->GetData();

        // data.erase(data.begin() + index);

        // page.UpdateBytesLeft();
  }

    void Table::RemoveColumn(const column_index_t index){
        //add also last updated at deleted at etc...
        const auto* removedColumn = this->columns.at(index);

        const auto& server = SystemCatalog::Get();

        //schema adjustments in master db change this as well
        std::vector updates = {
            Value(true, static_cast<column_index_t>(SysColumns::IsDeleted)),
            Value(DataTypes::DateTime::Now(), static_cast<column_index_t>(SysColumns::LastModifiedAt)),
            Value(DataTypes::DateTime::Now(), static_cast<column_index_t>(SysColumns::DeletedAt)),
        };

        auto result = server.UpdateColumnById(removedColumn->GetColumnId(), updates);

        this->HandleRemoveColumn(removedColumn->OrdinalPosition());
        this->columns.erase(this->columns.begin() + index);

        for (int i = index; i < this->columns.size(); i++) {
            const auto& column = this->columns[i];

            column->SetOrdinalPosition(i);

            updates = {
                Value(i, static_cast<column_index_t>(DatabaseEngine::SysColumns::OrdinalPosition))
            };

            //adjust in master db
            result = server.UpdateColumnById(column->GetColumnId(), updates);
        }

        //adjust rows by heap or clustered
        delete removedColumn;
    }

  void Table::HandleRemoveColumn(const column_index_t index){
    if (this->header.indexAllocationMapPageId == INVALID_PAGE_ID)
      return;

    if (this->GetType() == TableType::CLUSTERED) {
      this->RemoveColumnByClusteredIndex(index);
      return;
    }

    this->RemoveColumnByHeap(index);
  }

  void Table::Rollback(const Snapshot& snapshot, const DataTypes::RowIdentifier& rowId) const{
        auto page = Storage::StorageManager::Get().GetPage(this->database->GetFileName(), rowId.pageId, this);

        MultiThreading::WriterGuard guard(&page.Latch());

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

  Database* Table::GetDatabase() const { return this->database; }

}
