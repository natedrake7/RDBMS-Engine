#include "../../../include/DatabaseConstants.h"
#include "../../../../Systemic/include/DataTypes/Value.h"
#include "../../../../Systemic/include/DataStructures/BitMap.h"
#include "../../../include/DataStorage//Block.h"
#include "../../../include/DataStorage/Column.h"
#include "../../../include/DataStorage/Row.h"
#include "../../../include/DataStorage/Table.h"

#include <assert.h>

#include "../../../include/SystemDatabases/CatalogSchema.h"
#include "../../../include/SystemDatabases/SystemCatalog.h"
#include "../../../include/Pages/Page.h"
#include "../../../include/Pages/HeaderPage.h"
#include "../../../include/Pages/LargeObjectPage.h"
#include "../../../include/Pages/IndexAllocationMapPage.h"
#include "../../../include/Pages/PageFreeSpacePage.h"
#include "../../../include/Pages/IndexPage.h"
#include "../../../include/BufferPool/StorageManager.h"
#include "../../../include/BTree.h"
#include "../../../../Server/include/Server.h"
#include "../../../../Systemic/include/Guards/ReaderGuard.h"
#include "../../../../Systemic/include/Guards/WriterGuard.h"
#include "../../../../QueryPipeline/include/Statements.h"
#include "../../../include/Database.h"

#include "Memory/Allocator.h"

namespace DatabaseEngine::StorageTypes {
      TableHeader::TableHeader() 
      {
        this->indexAllocationMapPageId = INVALID_PAGE_ID;
        this->tableId = 0;
        this->numberOfColumns = 0;
        this->clusteredIndexPageId = INVALID_PAGE_ID;
        this->ordinalPosition = 0;
      }

      TableHeader::~TableHeader() = default;

      TableHeader &TableHeader::operator=(const TableHeader &tableHeader) 
      {
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

      void Table::PopulateAutoComputedColumns(Row*& row)const{
        int64_t outValue = 0;

        for (auto* column: this->columns) {
          const auto result = Table::PopulateColumnIdentity(row, column, outValue);

          if (result == true)
            continue;

          Table::PopulateDefaultValues(row, column);
        }
    }

      bool Table::VectorContainsIndex(const vector<column_index_t>& vector, const column_index_t& index, int& indexPosition)
      {
        for(int i = 0;i < vector.size(); i++)
            if(vector[i] == index)
            {
              indexPosition = i;
              return true;
            }

        return false;
      }

      Errors::RuntimeStatus Table::BatchCreateRow(
        Row*& rowPtr,
        const transaction_id_t &transactionId,
        const vector<Value> &inputData,
        const std::vector<column_index_t> &columnIndices,
        std::vector<char>& buffer,
        page_offset_t& bufferOffset
      ) const{

        rowPtr = new Row(*this);

        this->PopulateAutoComputedColumns(rowPtr);

        for (int i = 0;i < inputData.size(); i++) {
          const auto& input = inputData[i];

          const auto& columnIndex = columnIndices[i];

          const auto& column = this->columns.at(columnIndex);

          //ignore auto-computed columns even if specified
          if (column->HasIdentity())
            continue;

          auto *block = new Block(column);

          if (input.IsNull())
          {
            rowPtr->SetNullBitMapValue(columnIndex, true);
            // Table::InsertNullValues(block, rowPtr, columnIndex);
            continue;
          }

          auto insertResult = block->SetData(input);

          if (insertResult.code != Errors::RuntimeError::Ok) {
            delete block;
            return insertResult;
          }

          rowPtr->InsertColumnData(block, columnIndex);
        }

        buffer.resize(buffer.size() + rowPtr->TotalSize());

        rowPtr->Serialize(&buffer, bufferOffset);
        rowPtr->SetCurrentTransactionId(transactionId);

        return {};
      }

      Errors::RuntimeStatus Table::CreateRow(
        Row*& row,
        const transaction_id_t& transactionId,
        const std::vector<Value>& inputData,
        Logging::CheckPoint* checkPoint
        )const
      {
        row = new Row(*this);
        this->PopulateAutoComputedColumns(row);

        for(const auto& input : inputData){

          const auto& associatedColumnIndex = input.GetColumnIndex();

          const auto& column = this->columns.at(associatedColumnIndex);

          //ignore auto-computed columns even if specified
          if (column->HasIdentity())
            continue;

          auto *block = new Block(column);

          if (input.IsNull())
          {
            Table::InsertNullValues(block, row, associatedColumnIndex);
            continue;
          }

          auto insertResult = block->SetData(input);

          if (insertResult.code != Errors::RuntimeError::Ok) {
            delete block;
            return insertResult;
          }

          row->InsertColumnData(block, associatedColumnIndex);
        }

        row->SetCurrentTransactionId(transactionId);

        *checkPoint = Database::LogRowInsert(row, transactionId, this->header.ordinalPosition);

        return {};
      }

      Errors::RuntimeStatus Table::CreateRow(
        Row*& row,
        const transaction_id_t &transactionId,
        const std::vector<Value> &inputData,
        const std::vector<column_index_t> &columnIndices,
        Logging::CheckPoint *checkPoint
      ) const{
        row = new Row(*this);

        this->PopulateAutoComputedColumns(row);

        for (int i = 0;i < inputData.size(); i++) {
          const auto& input = inputData[i];

          const auto& associatedColumnIndex = columnIndices.at(i);

          const auto& column = this->columns.at(associatedColumnIndex);

          //ignore auto-computed columns even if specified
          if (column->GetIdentity().columnId != INVALID_COLUMN_ID)
            continue;

          auto *block = new Block(column);

          if (input.IsNull())
          {
            Table::InsertNullValues(block, row, associatedColumnIndex);
            continue;
          }

          auto result = block->SetData(input);

          if (result.code != Errors::RuntimeError::Ok) {
            delete block;
            return result;
          }

          row->InsertColumnData(block, associatedColumnIndex);
        }

        row->SetCurrentTransactionId(transactionId);

        *checkPoint = Database::LogRowInsert(row, transactionId, this->header.ordinalPosition);

        return {};
      }

      Errors::RuntimeStatus Table::CreateRow(
        Row*& row,
        const transaction_id_t &transactionId,
        const std::vector<Expressions::Expression *> &inputData,
        const std::vector<column_index_t> &columnIndices,
        Logging::CheckPoint *checkPoint
      ) const{
        Errors::RuntimeStatus result;

        row = new Row(*this);

        this->PopulateAutoComputedColumns(row);
        result.message = "Row created successfully";

        for (int i = 0;i < inputData.size(); i++) {
          const auto& input = inputData[i]->Evaluate({});

          const auto& associatedColumnIndex = columnIndices.at(i);

          const auto& column = this->columns.at(associatedColumnIndex);

          //ignore auto-computed columns even if specified
          if (column->HasIdentity())
            continue;

          auto *block = new Block(column);

          if (input.IsNull())
          {
            Table::InsertNullValues(block, row, associatedColumnIndex);
            continue;
          }

          auto insertResult = block->SetData(input);

          if (insertResult.code != Errors::RuntimeError::Ok) {
            delete block;
            return insertResult;
          }

          row->InsertColumnData(block, associatedColumnIndex);
        }

        row->SetCurrentTransactionId(transactionId);

        *checkPoint = Database::LogRowInsert(row, transactionId, this->header.ordinalPosition);

        return result;
      }

      void Table::InsertRowToPage(
        Pages::PageGuard<Pages::PageFreeSpacePage>& pageFreeSpacePage,
        Pages::PageGuard<>& page,
         Row*& row,
        const int & indexPosition
      )const{
        while (row->TotalSize() > page->GetBytesLeft())
          this->HandleRowOverflow(row);

        page->InsertRow(row, indexPosition);
        pageFreeSpacePage->SetPageMetaData(page.Get());
    }

    // void Table::InsertRowToClusteredPage(
    //   Pages::PageGuard<Pages::PageFreeSpacePage>& pageFreeSpacePage,
    //   Pages::IndexPage* page,
    //   const DataTypes::Indexing::Key &key,
    //   Row* row,
    //   const int & indexPosition
    // )const{
    //   for(const auto& column: this->columns){
    //     if(!column->isColumnOverflowed())
    //       continue;
    //
    //     this->HandleRowOverflow(row, column);
    //   }
    //
    //   page->InsertTuple(key, row, indexPosition);
    //   pageFreeSpacePage->SetPageMetaData(page);
    // }

    bool Table::PopulateColumnIdentity( Row*& row, Column*& column, int64_t& outValue) {
        if (!column->GenerateIdentityValue(outValue))
          return false;

        const auto& columnSize = column->GetColumnSize();

        auto* block = new Block(&outValue, columnSize ,column);

        row->InsertColumnData(block, column->GetColumnIndex());

        return true;
      }

    void Table::PopulateDefaultValues( Row*& row, Column*& column) {
        const auto& defaultValue = column->GetDefaultValue();

        if (defaultValue.columnId == INVALID_COLUMN_ID)
          return;

        auto* block = new Block(defaultValue.value.data(), defaultValue.value.size(), column);

        row->InsertColumnData(block, column->GetColumnIndex());
      }

      void Table::InsertExistingRowsToNonClusteredIndexByClusteredIndex(const int32_t &indexPos, const int& pagesToAllocate){

        const auto* clusteredTree = this->GetClusteredIndexedTree();

        clusteredTree->InsertRowsToOtherTree(indexPos, pagesToAllocate);
    }

      void Table::InsertExistingRowToNonClusteredIndexByHeap(const int& indexPos, const int& pagesToAllocate){
        if(this->header.indexAllocationMapPageId == INVALID_PAGE_ID)
          return;

        const auto& filename = this->database->GetFileName();

        const auto tableMapPage = Storage::StorageManager::Get().GetIndexAllocationMapPage(filename, this->header.indexAllocationMapPageId, this);

        vector<extent_id_t> tableExtentIds;
        tableMapPage->GetAllocatedExtents(&tableExtentIds, 0);

        const auto& indexedColumns = this->header.nonClusteredIndexes.at(indexPos).columns;

        for (const auto& extentId : tableExtentIds){
          const page_id_t extentFirstPageId = DatabaseEngine::Database::CalculateSystemPageOffset(extentId * EXTENT_SIZE);

          const auto pageFreeSpacePage = DatabaseEngine::Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), extentFirstPageId);

          const page_id_t pageId = (tableMapPage->GetPageId() != extentFirstPageId)
                                      ? extentFirstPageId
                                      : extentFirstPageId + 1;

          for (page_id_t extentPageId = pageId; extentPageId < extentFirstPageId + EXTENT_SIZE; extentPageId++)
          {
            if (pageFreeSpacePage->GetPageType(extentPageId) != PageType::DATA)
              break;

            auto page = Storage::StorageManager::Get().GetPage(filename, extentPageId, this);

            if (page->GetPageSize() == 0)
              continue;

            // const auto rows = page->DataRowsNoLock(this);
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

      void Table::RemoveColumnByClusteredIndex(const column_index_t &index){

    const auto* tree = this->GetClusteredIndexedTree();

    tree->RemoveColumnFromRow(index);
  }

     void Table::RemoveColumnByHeap(const column_index_t &index)const{
    const auto& filename = this->GetFileName();

    const auto tableMapPage = Storage::StorageManager::Get().GetIndexAllocationMapPage(filename, this->header.indexAllocationMapPageId, this);

    std::vector<extent_id_t> allocatedExtents;
    tableMapPage->GetAllocatedExtents(&allocatedExtents, 0);

    for (const auto& extentId: allocatedExtents) {
      const page_id_t extentFirstPageId = Database::CalculateExtentFirstPageId(extentId);

      const page_id_t firstDataPageId = (tableMapPage->GetPageId() != extentFirstPageId)
                                            ? extentFirstPageId
                                            : extentFirstPageId + 1;

      for (page_id_t pageId = firstDataPageId; pageId < extentFirstPageId + EXTENT_SIZE; pageId++)
      {
        auto pageFreeSpacePage = Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), pageId);

        if (pageFreeSpacePage->GetPageType(pageId) != PageType::DATA)
          break;

        auto page = Storage::StorageManager::Get().GetPage(filename, pageId, this);

        // for (auto& row: page->DataRowsNoLock(this))
        //   Table::HandleRemoveColumn(page.Get(), &row, index);

        pageFreeSpacePage->SetPageMetaData(page.Get());
      }
    }
  }

     void Table::InsertToVersionDatabase(Row*& row, const transaction_id_t& transactionId) const{
        static auto& versionDatabase = VersionDatabase::Get();

        Pages::RowVersionPointer oldVersionPointer;
        versionDatabase.InsertRow(row, oldVersionPointer, this);
        row->SetOlderVersionPointer(oldVersionPointer.pageId, oldVersionPointer.offset);
        row->SetCurrentTransactionId(transactionId);
     }

     Table::Table(
        const table_id_t &tableId,
        const int& ordinalPosition,
        const vector<Column *> &columns,
        Database *database,
        const Headers::Index* clusteredIndex,
        const vector<Headers::Index> *nonClusteredIndexes
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
        const int& ordinalPosition){

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

        headerPage->SetTableHeader(this);

        for (const auto &column : columns)
            delete column;
      }

    Errors::RuntimeStatus Table::BatchInsert(
        const ExecutionProperties &properties,
        const std::vector<QueryResult> &input,
        const std::vector<column_index_t> &columnIndices
      ) {
        std::vector<Row*> rows;
        rows.reserve(input.size());

        Int pagesNeeded = 0;

        std::vector<char> buffer;
        page_offset_t pos = 0;

        auto allocationSize = 0;
        for (const auto& row : input)
          allocationSize += row.GetByteSize();

        Memory::Allocator allocator(allocationSize, Memory::AllocationType::Persistent);

        for (const auto& insertedRow : input) {
          auto row = new Row(*this);
          //TODO implement better to avoid multiple loggings
          auto status = this->BatchCreateRow(
            row,
            properties.snapshot.transactionId,
            insertedRow.GetData(),
            columnIndices,
            buffer,
            pos
          );

          if (status.code != Errors::RuntimeError::Ok)
            return status;

          pagesNeeded += static_cast<Int>(row->TotalSize());
          rows.push_back(row);
        }

        auto checkPoint = Database::LogRowBatchInsert(buffer, properties.snapshot.transactionId, this->header.ordinalPosition);

        if (this->IsClustered())
          pagesNeeded /= INDEX_PAGE_DEFAULT_SIZE;
        else
          pagesNeeded /= PAGE_SIZE_WITHOUT_HEADER;

        if (pagesNeeded == 0)
          pagesNeeded = 1;

        Headers::RowIdentifier rowId;
        for (int i = 0;i < rows.size(); i++){
          auto& row = rows[i];

          auto result = this->InsertRow(row, pagesNeeded);

          if (result.code != Errors::RuntimeError::Ok)
            return result;

          if (i == 0)
            rowId = result.rowId;
        }

        Database::LogCheckPoint(checkPoint);

        Errors::RuntimeStatus status;
        status.rowId = rowId;
        return status;
      }

    Errors::RuntimeStatus Table::InsertRow(const ExecutionProperties& properties, const std::vector<Value> &inputData){
        Logging::CheckPoint checkPoint;
        auto* row = new Row(*this);

        auto result = this->CreateRow(
          row,
          properties.snapshot.transactionId,
          inputData,
          &checkPoint
        );

        if (result.code != Errors::RuntimeError::Ok)
          return result;

        result = this->InsertRow(row, 1);

        if (result.code != Errors::RuntimeError::Ok)
          return result;

        Database::LogCheckPoint(checkPoint);

        result.message = "Rows affected: 1";
        return result;
    }

  Errors::RuntimeStatus Table::InsertRow(
      const ExecutionProperties& properties,
      const vector<Value> &inputData,
      const std::vector<column_index_t> &columnIndices
    ){
        Logging::CheckPoint checkPoint;

        auto* row = new Row(*this);

        auto result = this->CreateRow(
          row,
          properties.snapshot.transactionId,
          inputData,
          columnIndices,
          &checkPoint
        );

        if (result.code != Errors::RuntimeError::Ok)
          return result;

        result =  this->InsertRow(row, 1);

        if (result.code != Errors::RuntimeError::Ok)
          return result;

        Database::LogCheckPoint(checkPoint);

        result.message = "Rows affected: 1";

        return result;
    }

    Errors::RuntimeStatus Table::InsertRow(
      const ExecutionProperties& properties,
      const vector<Expressions::Expression *> &inputData,
      const std::vector<column_index_t> &columnIndices
    ){
        Logging::CheckPoint checkPoint;

        auto* row = new Row(*this);
        auto result = this->CreateRow(
          row,
          properties.snapshot.transactionId,
          inputData,
          columnIndices,
          &checkPoint
        );

        if (result.code != Errors::RuntimeError::Ok)
          return result;

        result = this->InsertRow(row, 1);

        if (result.code != Errors::RuntimeError::Ok)
          return result;

        Database::LogCheckPoint(checkPoint);

        result.message = "Rows affected: 1";

        return result;

    }

    Errors::RuntimeStatus Table::InsertRow(Row*& row, const int& pagesToAllocate){
        this->InsertLargeObjectToPage(row);

        //row_id
        auto status = this->IsClustered()
            ? this->ClusteredIndexInsert(row, pagesToAllocate)
            : this->HeapInsert(row, pagesToAllocate);

        const auto rowId = status.rowId;
        if (status.code != Errors::RuntimeError::Ok)
            return status;

        //insert to NonClustered Indexes
        for (int i = 0; i < this->header.nonClusteredIndexes.size(); i++) {
            status = this->NonClusteredIndexInsert(row, i, pagesToAllocate, rowId);

            if (status.code != Errors::RuntimeError::Ok)
                return status;
        }

        status.rowId = rowId;
        return status;
      }

    void Table::DeleteLargeObjectFromPage( Row*& row, const HashSet<column_index_t>& updatedColumns)const{
      const auto& filename = this->database->GetFileName();

      const RowHeader* rowHeader = row->GetHeader();

      for(const auto& block : row->GetData()){
        const auto& columnIndex = block->GetColumnIndex();

        if(!updatedColumns.Contains(columnIndex)
          || !rowHeader->largeObjectBitMap->Get(columnIndex))
          continue;

        rowHeader->largeObjectBitMap->Set(columnIndex, false);

        auto objectPointer = block->GetLargeObjectPointer();

        auto largeObjectPage = Storage::StorageManager::Get().GetLargeDataPage(filename, objectPointer.pageId, this);

        auto* objectPtr = largeObjectPage->DeleteObject();

        {
          auto pfsPage = Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), objectPointer.pageId);

          MultiThreading::WriterGuard lock(&pfsPage->Latch());

          pfsPage->SetPageMetaData(largeObjectPage.Get());
          pfsPage->SetPageFreed(largeObjectPage->GetPageId());
        }


        while(objectPtr->nextPageId != 0){
            auto nextLargeObjectPage = Storage::StorageManager::Get().GetLargeDataPage(filename, objectPtr->nextPageId, this);

            auto* prevObject = objectPtr;
            objectPtr = nextLargeObjectPage->DeleteObject();

            {
              auto pfsPage = Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), objectPointer.pageId);

              MultiThreading::WriterGuard lock(&pfsPage->Latch());

              pfsPage->SetPageMetaData(nextLargeObjectPage.Get());
              pfsPage->SetPageFreed(nextLargeObjectPage->GetPageId());

            }

        }
      }
    }

    void Table::DeleteOverflowedRowsFromPage( Row*& row, const HashSet<column_index_t> & updatedColumns)const{
      const auto& filename = this->database->GetFileName();

      const RowHeader* rowHeader = row->GetHeader();

      for(const auto& block : row->GetData()){
        if(!updatedColumns.Contains(block->GetColumnIndex())
          || !rowHeader->overflowBitMap->Get(block->GetColumnIndex()))
            continue;

        rowHeader->overflowBitMap->Set(block->GetColumnIndex(), false);

        auto objectPointer = block->GetOverflowPointer();

        auto overflowPage = Storage::StorageManager::Get().GetOverflowPage(filename, objectPointer.pageId, this);

        const auto* overflowRow = overflowPage->DeleteObject(objectPointer.index);

        auto pfsPage = Storage::StorageManager::Get().GetPageFreeSpacePage(filename, Database::GetPfsAssociatedPage(objectPointer.pageId));

        pfsPage->SetPageMetaData(overflowPage.Get());

        delete overflowRow;
      }
    }

    string Table::GetSchema(){ return {}; }

    string Table::GetFileName() const{ return this->database->GetFileName(); }

    column_number_t Table::GetNumberOfColumns() const { return this->columns.size(); }

    const TableHeader &Table::GetHeader() const { return this->header; }

    const vector<Column *> &Table::GetColumns() const { return this->columns; }

    std::vector<const Column *> Table::GetConstantColumns() const {
        std::vector<const Column*> constColumns;

        for (const auto* column : this->columns)
          constColumns.push_back(column);

        return constColumns;
      }

    void Table::HeapScan(
      const ExecutionProperties& properties,
      std::vector<Row> *result,
      ScanState& state
    )const
    {
        if(this->header.indexAllocationMapPageId == INVALID_PAGE_ID)
            return;

        const auto& filename = this->database->GetFileName();

        const auto tableMapPage = Storage::StorageManager::Get().GetIndexAllocationMapPage(filename, this->header.indexAllocationMapPageId, this);

        vector<extent_id_t> tableExtentIds;
        tableMapPage->GetAllocatedExtents(&tableExtentIds, state.extentId);

        state.canFetchMore = false;
        for (const auto& extentId : tableExtentIds){
          const page_id_t extentFirstPageId = Database::CalculateSystemPageOffset(extentId * EXTENT_SIZE);

          const auto pageFreeSpacePage = Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), extentFirstPageId);

          const auto extentStartingPageId = (tableMapPage->GetPageId() != extentFirstPageId)
                                      ? extentFirstPageId
                                      : extentFirstPageId + 1;

          MultiThreading::ReaderGuard pfsLatch(&pageFreeSpacePage->Latch());

          for (page_id_t extentPageId = state.GetPageId(extentStartingPageId); extentPageId < extentFirstPageId + EXTENT_SIZE; extentPageId++){
            if (pageFreeSpacePage->GetPageType(extentPageId) != PageType::DATA)
              break;

            auto page = Storage::StorageManager::Get().GetPage(filename, extentPageId, this);

            MultiThreading::ReaderGuard lock(&page->Latch());

            if (page->GetPageSize() == 0)
              continue;

            //update state to know where to start
            state.extentId = extentId;
            state.lastFetchedRowId.pageId = extentPageId;

            // auto pageRows = page->DataRowsNoLock(this);
            //
            // for (int i = state.GetNextKeyIndex(); i < page->GetPageSize(); i++) {
            //   auto row = page->GetRow(this, i);
            //
            //   auto visibleRow = row.GetVisibleVersionForTransaction(properties.snapshot);
            //
            //   if (visibleRow.IsInvalid())
            //     continue;
            //
            //   result->push_back(std::move(visibleRow));
            //
            //   state.lastFetchedRowId.indexId = i;
            //
            //   if (result->size() == properties.batchSize) {
            //     state.canFetchMore = true;
            //     return;
            //   }
            //
            //   //if can fetch more in current batch reset index
            //   state.lastFetchedRowId.indexId = INVALID_INDEX_ID;
            // }
          }
        }
    }

    void Table::TemporaryDatabaseHeapScan(
      std::vector<Row>* result,
      ScanState& state,
      const int& batchSize
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
            Storage::StorageManager::Get().GetIndexAllocationMapPage(
              filename,
              this->header.indexAllocationMapPageId,
              this
            );

        vector<extent_id_t> tableExtentIds;
        tableMapPage->GetAllocatedExtents(&tableExtentIds, 0);

        vector<Row*> rowsToBeInserted;
        Expressions::EvaluationContext context(Expressions::EvaluationContext::EvaluationContextType::SingleRow, properties.variables);

        for (const auto &extentId : tableExtentIds)
        {
          const page_id_t extentFirstPageId = Database::CalculateSystemPageOffset(extentId * EXTENT_SIZE);

          auto pageFreeSpacePage = Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), extentFirstPageId);

          const page_id_t pageId = (tableMapPage->GetPageId() != extentFirstPageId)
                                       ? extentFirstPageId
                                       : extentFirstPageId + 1;

          for (page_id_t extentPageId = pageId; extentPageId < extentFirstPageId + EXTENT_SIZE; extentPageId++)
          {
            if (pageFreeSpacePage->GetPageType(extentPageId) != PageType::DATA)
              break;

            auto page = Storage::StorageManager::Get().GetPage(filename, extentPageId, this);

            // const auto rows = page->DataRowsNoLock(this);
            //
            // for (int i = 0; i < rows.size(); i++) {
            //   const auto& row = rows.at(i);
            //
            //   context.row = &row;
            //
            //   if (expression->Evaluate(context).GetBool())
            //     page->Delete(i);
            // }

            page->UpdateBytesLeft();
            page->UpdatePageSize();

            pageFreeSpacePage->SetPageMetaData(page.Get());
          }
        }
    }

    void Table::ClusteredIndexScanDelete(
      const ExecutionProperties& properties,
        const Expressions::Expression *expression,
        IndexState& state
    ){
        auto* tree = this->GetClusteredIndexedTree();

        std::vector<Row> results;
        tree->IndexScan(properties, &results, state);

        if(results.empty())
          return;

        Expressions::EvaluationContext context(Expressions::EvaluationContext::EvaluationContextType::SingleRow, properties.variables);

        for(const auto& row : results){

          context.row = &row;
          const auto value = expression->Evaluate(context);
          if(value.GetBool())
          {
            const auto& key = Database::CreateKey(this->header.clusteredIndex.columns, &row);
            tree->Remove(key);
          }
        }
   }

    void Table::ClusteredIndexSeekDelete(
    const ExecutionProperties& properties,
    const Expressions::Expression *expression,
    IndexState &state
  ){

  }

    Errors::RuntimeStatus Table::HeapInsert(Row*& row, const int& pagesToAllocate)const{
      const auto& filename = this->database->GetFileName();

      while(row->TotalSize() > Constants::PAGE_SIZE_WITHOUT_HEADER)
        this->HandleRowOverflow(row);

        Errors::RuntimeStatus status;

      if (this->header.indexAllocationMapPageId == INVALID_PAGE_ID)
      {
          auto newPage = this->database->CreateDataPage(this->header.ordinalPosition, pagesToAllocate);

          MultiThreading::WriterGuard pageLock(&newPage->Latch());

          status.rowId.indexId = newPage->InsertRow(row);
          status.rowId.pageId = newPage->GetPageId();

          return status;
      }

      const auto tableMapPage = Storage::StorageManager::Get().GetIndexAllocationMapPage(filename, this->header.indexAllocationMapPageId, this);

      std::vector<extent_id_t> tableExtentIds;
      tableMapPage->GetAllocatedExtents(&tableExtentIds, 0);

      const auto rowCategory = Database::GetObjectSizeToCategory(row->TotalSize());

      for (const auto &extentId : tableExtentIds)
      {
          const page_id_t extentFirstPageId = Database::CalculateExtentFirstPageId(extentId);

          const page_id_t firstDataPageId = (tableMapPage->GetPageId() != extentFirstPageId)
                                                ? extentFirstPageId
                                                : extentFirstPageId + 1;

          for (page_id_t pageId = firstDataPageId; pageId < extentFirstPageId + EXTENT_SIZE; pageId++)
          {

              auto pageFreeSpacePage = Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), pageId);

              MultiThreading::ReaderGuard pfsLatch(&pageFreeSpacePage->Latch());

              if (pageFreeSpacePage->GetPageType(pageId) != PageType::DATA)
                  break;

              const auto pageSizeCategory = pageFreeSpacePage->GetPageSizeCategory(pageId);

              // find potential candidate
              if (rowCategory <= pageSizeCategory)
              {
                  auto page = Storage::StorageManager::Get().GetPage(filename, pageId, this);

                  MultiThreading::WriterGuard pageLock(&page->Latch());

                  if (row->TotalSize() > page->GetBytesLeft())
                      continue;

                  status.rowId.indexId = page->InsertRow(row);
                  pageFreeSpacePage->SetPageMetaData(page.Get());

                  status.rowId.pageId = pageId;
                  return status;
              }
          }
      }

      auto newPage = this->database->CreateDataPage(this->header.ordinalPosition, pagesToAllocate);

      MultiThreading::WriterGuard pageLock(&newPage->Latch());

      status.rowId.indexId = newPage->InsertRow(row);
      status.rowId.pageId = newPage->GetPageId();

      return status;
    }

  Errors::RuntimeStatus Table::ClusteredIndexInsert(Row*& row, const int& pagesToAllocate){
      auto* tree = this->GetClusteredIndexedTree();
      auto key = Database::CreateKey(this->GetClusteredIndex(), row);

      int indexPosition = 0;

      auto tuple = Pages::LeafNodeTuple(*row, key);
      auto status = tree->InsertRow(tuple, pagesToAllocate, indexPosition);

      if (status.code != Errors::RuntimeError::Ok)
         return status;

      // auto pageFreeSpacePage =  Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), node->GetPageId());
      //
      // MultiThreading::WriterGuard pfsPageLock(&pageFreeSpacePage->Latch());
      // MultiThreading::WriterGuard pageLock(&node->Latch());

      // should never fail
      // this->InsertRowToClusteredPage(pageFreeSpacePage, node.Get(), key, row, indexPosition);

      // auto* keys = node->GetKeysUnsafe();

      // keys->insert(keys->begin() + indexPosition, new DataTypes::Indexing::Key(key));

      // node->UpdateBytesLeft();
      // node->UpdatePageSize();

        //TODO
      // rowId->indexId = indexPosition;
      // rowId->pageId = node->GetPageId();

      status.primaryKey = std::move(tuple.key);
      return status;
     }

    Errors::RuntimeStatus Table::NonClusteredIndexInsert(
      const StorageTypes::Row* row,
      const int & nonClusteredIndexId,
      const int& pagesToAllocate,
      const Headers::RowIdentifier & data
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

    Errors::RuntimeStatus Table::NonClusteredIndexInsertExistingRows(const int &indexPos, const int& pagesToAllocate){
        if (this->GetType() == TableType::CLUSTERED) {
          this->InsertExistingRowsToNonClusteredIndexByClusteredIndex(indexPos, pagesToAllocate);
          return {};
        }

        this->InsertExistingRowToNonClusteredIndexByHeap(indexPos, pagesToAllocate);
        return {};
    }

    int Table::CreateNonClusteredIndex(vector<column_index_t> &columnIndices){
        Headers::Index index;
        index.columns = std::move(columnIndices);

        this->header.nonClusteredIndexes.push_back(std::move(index));

        return static_cast<int>(this->header.nonClusteredIndexes.size() - 1);
    }

    Errors::RuntimeStatus Table::HeapUpdate(
    const ExecutionProperties& properties,
    const Expressions::Expression *expression,
    const vector<Value> & updates
  ){
        if(this->header.indexAllocationMapPageId == INVALID_PAGE_ID)
          return {};

        const auto& filename = this->database->GetFileName();

        const auto tableMapPage = Storage::StorageManager::Get().GetIndexAllocationMapPage(filename, this->header.indexAllocationMapPageId, this);

        vector<extent_id_t> tableExtentIds;
        tableMapPage->GetAllocatedExtents(&tableExtentIds, 0);

        for (const auto& extentId : tableExtentIds){
          const page_id_t extentFirstPageId = DatabaseEngine::Database::CalculateSystemPageOffset(extentId * EXTENT_SIZE);

          const auto pageFreeSpacePage = DatabaseEngine::Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), extentFirstPageId);

          const page_id_t pageId = (tableMapPage->GetPageId() != extentFirstPageId)
                                      ? extentFirstPageId
                                      : extentFirstPageId + 1;

          Expressions::EvaluationContext context(Expressions::EvaluationContext::EvaluationContextType::SingleRow, properties.variables);

          for (page_id_t extentPageId = pageId; extentPageId < extentFirstPageId + EXTENT_SIZE; extentPageId++)
          {
            if (pageFreeSpacePage->GetPageType(extentPageId) != PageType::DATA)
              break;

            auto page = Storage::StorageManager::Get().GetPage(filename, extentPageId, this);

            if (page->GetPageSize() == 0)
              continue;

            // auto rows = page->DataRowsNoLock(this);
            //
            // std::vector<extent_id_t> allocatedExtents;
            //
            // for(auto& row : rows){
            //
            //   context.row = &row;
            //   const auto value = expression->Evaluate(context);
            //   if(!value.GetBool())
            //       continue;
            //
            //     const auto result = this->HandleRowUpdate(page.Get(), &row, properties, updates);
            //
            //     if (result.code != Errors::RuntimeError::Ok)
            //       return result;
            // }
          }
        }

        return {};
    }

    Errors::RuntimeStatus Table::HeapUpdate(
      const ExecutionProperties& properties,
      const Expressions::Expression *expression,
      const vector<QueryPipeline::Statements::UpdateColumn *> &updates
    ){
        if(this->header.indexAllocationMapPageId == INVALID_PAGE_ID)
          return {};

        const auto& filename = this->database->GetFileName();

        const auto tableMapPage = Storage::StorageManager::Get().GetIndexAllocationMapPage(filename, this->header.indexAllocationMapPageId, this);

        vector<extent_id_t> tableExtentIds;
        tableMapPage->GetAllocatedExtents(&tableExtentIds, 0);

        HashSet<column_index_t> updatedColumns;
        for(const auto& update : updates)
              updatedColumns.Add(update->name.index);

        for (const auto& extentId : tableExtentIds){
          const page_id_t extentFirstPageId = DatabaseEngine::Database::CalculateSystemPageOffset(extentId * EXTENT_SIZE);

          const auto pageFreeSpacePage = DatabaseEngine::Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), extentFirstPageId);

          const page_id_t pageId = (tableMapPage->GetPageId() != extentFirstPageId)
                                      ? extentFirstPageId
                                      : extentFirstPageId + 1;

          Expressions::EvaluationContext context(Expressions::EvaluationContext::EvaluationContextType::SingleRow, properties.variables);
          for (page_id_t extentPageId = pageId; extentPageId < extentFirstPageId + EXTENT_SIZE; extentPageId++)
          {
            if (pageFreeSpacePage->GetPageType(extentPageId) != PageType::DATA)
              break;

            auto page = Storage::StorageManager::Get().GetPage(filename, extentPageId, this);

            if (page->GetPageSize() == 0)
              continue;

            // auto rows = page->DataRowsNoLock(this);
            //
            // std::vector<extent_id_t> allocatedExtents;
            // extent_id_t startingExtentIndex = 0;
            //
            // for(auto& row : rows){
            //   context.row = &row;
            //   const auto value = expression->Evaluate(context);
            //   if(!value.GetBool())
            //       continue;
            //
            //     const auto result = this->HandleRowUpdate(page.Get(), &row, properties, updates, updatedColumns);
            //
            //     if (result.code != Errors::RuntimeError::Ok)
            //       return result;
            // }
          }
        }

        return {};
    }

    void Table::ClusteredIndexScanUpdate(
      const ExecutionProperties& properties,
      const Expressions::Expression *expression,
      const std::vector<Value> & updates
    ){
      const auto* tree = this->GetClusteredIndexedTree();
      tree->IndexScanUpdate(properties, expression, updates);
    }

    Errors::RuntimeStatus Table::ClusteredIndexScanUpdate(
      const ExecutionProperties& properties,
      const Expressions::Expression *expression,
      const std::vector<QueryPipeline::Statements::UpdateColumn *> &updates
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
        const vector<Value> & updates
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

  void Table::UpdateIndexAllocationMapPageId(const page_id_t &indexAllocationMapPageId)
    {
        this->header.indexAllocationMapPageId = indexAllocationMapPageId;
    }

    page_id_t Table::GetIndexAllocationMapPageId() const{ return this->header.indexAllocationMapPageId; }

    bool Table::IsColumnNullable(const column_index_t &columnIndex) const
    {
        return this->columns.at(columnIndex)->IsColumnNullable();
    }

    void Table::AddColumn(Column *column) { this->columns.push_back(column); }

    const table_id_t &Table::GetTableId() const { return this->header.tableId; }

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
                ? sizeof(Pages::DataObjectPointer)
                : column->GetColumnSize();

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
        const auto& columnSize = column->GetColumnSize();

        if (column->isColumnOverflowed())
          maximumRowSize += sizeof(Pages::OverflowPointer);
        else if (column->isColumnLOB())
          maximumRowSize += sizeof(Pages::DataObjectPointer);
        else
          maximumRowSize += columnSize;

        if(columnSize <= largestVariableLengthColumnSize
            || column->isColumnOverflowed()
            || columnSize >= LARGE_DATA_OBJECT_SIZE
            || clusteredColumns.Contains(column->GetColumnIndex()))
          continue;

        largestVariableLengthColumnSize = columnSize;
        largestColumn = column;
      }

      maximumRowSize -= largestVariableLengthColumnSize;
      maximumRowSize += sizeof(Pages::OverflowPointer);

      if (largestColumn != nullptr)
        largestColumn->SetIsOverflowed(true);

      return maximumRowSize;
    }

    vector<DataType> Table::GetColumnTypeByTreeId(const uint8_t& treeId) const{
          std::vector<DataType> columnDatatypes;

          if(treeId == 0){
            for(const auto& columnIndex: this->header.clusteredIndex.columns)
                columnDatatypes.emplace_back(this->columns[columnIndex]->GetColumnType());

            return columnDatatypes;
          }

//          for(const auto& columnIndex: this->header.nonClusteredColumnIndexes[treeId - 1])
//              columns.emplace_back(this->columns[columnIndex]->GetColumnType());

          return columnDatatypes;
      }

    int Table::HandleRowOverflow(const Row* row)const{
      auto* largestBlock = row->FindLargestVariableLengthColumn();

      if(largestBlock == nullptr)
        return -1;

      auto overflowPage = this->database->GetLastOverflowPage(this->header.ordinalPosition, largestBlock->GetSize());

      int indexPos = 0;
      overflowPage->InsertObject(largestBlock->GetRawData(), largestBlock->GetSize(), indexPos);

      row->SetOverflowBitMapValue(largestBlock->GetColumnIndex(), true);

      auto pfsPage = Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), overflowPage->GetPageId());

      pfsPage->SetPageMetaData(overflowPage.Get());

      const Pages::OverflowPointer ptr(overflowPage->GetPageId(), indexPos);
      largestBlock->SetData(&ptr, sizeof(Pages::OverflowPointer));

      return largestBlock->GetSize();
    }

      int Table::HandleRowOverflow(Row*& row, const Column *column)const{
        auto& data = row->GetData();

        if(data.size() < column->GetColumnIndex())
          return -1;

        auto* largestBlock = row->GetData().at(column->GetColumnIndex());

        auto overflowPage = this->database->GetLastOverflowPage(this->header.ordinalPosition, largestBlock->GetSize());

        int indexPos = 0;
        overflowPage->InsertObject(largestBlock->GetRawData(), largestBlock->GetSize(), indexPos);

        row->SetOverflowBitMapValue(largestBlock->GetColumnIndex(), true);

        const auto pfsPageId = DatabaseEngine::Database::GetPfsAssociatedPage(overflowPage->GetPageId());

        auto pfsPage = Storage::StorageManager::Get().GetPageFreeSpacePage(this->database->GetFileName(), pfsPageId);

        pfsPage->SetPageMetaData(overflowPage.Get());

        const Pages::OverflowPointer ptr(overflowPage->GetPageId(), indexPos);
        largestBlock->SetData(&ptr, sizeof(Pages::OverflowPointer));

        return largestBlock->GetSize();
    }

    //create differrent one to handle clustered updates
    Errors::RuntimeStatus Table::UpdateRowNoLock(
      Pages::Page *page,
      Row* row,
      const ExecutionProperties& properties,
      const std::vector<Value> &updates,
      const int& indexPosition,
      const bool &isHeap
    ){
        // this->DeleteLargeObjectFromPage(row, updatedColumns);
        // this->DeleteOverflowedRowsFromPage(row, updatedColumns);

        //copy row for old transactions
        //this has the pointers of the old row to LOBS and overflow pages

        // this->InsertToVersionDatabase(row, properties.snapshot.transactionId);

        int diff = 0;
        auto result = row->Update(updates, diff);

        if (result.code != Errors::RuntimeError::Ok)
          return result;

        // if(page->GetBytesLeft() - diff > 0){
        //   page->UpdateBytesLeft();
        //   return result;
        // }

        // this->InsertLargeObjectToPage(row);

        // if(isHeap && (Constants::PAGE_SIZE_WITHOUT_HEADER - row->TotalSize()) > 0)
        //   return this->InsertRow(row, 1);
        //
        // while(page->GetBytesLeft() - diff < 0){
        //   const auto overflowResult = this->HandleRowOverflow(row);
        //   if(overflowResult == -1)
        //     break;
        //
        //   diff -= overflowResult;
        // }

        page->UpdateRow(row, indexPosition);

        return {};
    }

    Errors::RuntimeStatus Table::UpdateRowNoLock(
      Pages::Page *page,
      Row* row,
      const ExecutionProperties& properties,
      const std::vector<QueryPipeline::Statements::UpdateColumn *> &updates,
      const HashSet<column_index_t> &updatedColumns,
      const int& indexPosition,
      const bool &isHeap
    ){
        // this->DeleteLargeObjectFromPage(row, updatedColumns);
        // this->DeleteOverflowedRowsFromPage(row, updatedColumns);

        // this->InsertToVersionDatabase(row, properties.snapshot.transactionId);

        int diff = 0;
        auto result = row->Update(updates, diff);

        // if(page->GetBytesLeft() - diff > 0){
        //   page->UpdateBytesLeft();
        //   return result;
        // }

        // this->InsertLargeObjectToPage(row);

        // if(isHeap && (Constants::PAGE_SIZE_WITHOUT_HEADER - row->TotalSize()) > 0)
        //   return this->InsertRow(row, 1);
        //
        // while(page->GetBytesLeft() - diff < 0){
        //   const auto overflowResult = this->HandleRowOverflow(row);
        //   if(overflowResult == -1)
        //     break;
        //
        //   diff -= overflowResult;
        // }

        page->UpdateRow(row, indexPosition);

        return {};
  }

    void Table::GetDefaultValuesHeaders() const{
        for(const auto& column: this->columns) {
          const auto systemHeader = SystemCatalog::Get().SelectDefaultValueByColumnId(column->GetColumnId());

          if (systemHeader.columnId == INVALID_COLUMN_ID)
            continue;

          column->SetDefaultValue(systemHeader);
        }
    }

    void Table::GetColumnsHeaders()const{
      const auto headers = SystemCatalog::Get().SelectColumns(this->header.tableId);

      assert(headers.size() == this->columns.size());

      if (headers.empty())
        return;

      for (int i = 0;i < this->columns.size(); i++) {
        auto& column = columns[i];

        column->SetColumnId(headers[i].id);
      }
    }

    void Table::UpdateIdentityManagersIds() const{
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

    void Table::GetIdentityColumns()const{
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

    void Table::GetIdentityColumnById(const int32_t &columnId)const{
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

  void Table::GetIndexes(){
        Dictionary<int32_t, Column*> columnsDict;

        for (auto& column: this->columns)
          columnsDict.Add(column->GetColumnId(), column);

        const auto indexes = SystemCatalog::Get().SelectIndexes(this->header.tableId);

        for (const auto& index: indexes) {
          const auto indexedColumns = SystemCatalog::Get().SelectIndexColumnsByIndexId(index.id);

          std::vector<column_index_t> indexColumnsIndices;
          for (const auto& indexedColumn : indexedColumns)
            indexColumnsIndices.emplace_back(columnsDict.Get(indexedColumn.columnId)->GetColumnIndex());


          if (index.isClustered) {
            this->header.clusteredIndex.columns = std::move(indexColumnsIndices);
            continue;
          }

          Headers::Index tableIndex(indexColumnsIndices);

          this->header.nonClusteredIndexes.push_back(std::move(tableIndex));
        }
    }

  void Table::UpdateMasterDatabase() const{
      for (const auto& column: this->columns)
        column->UpdateMetadata();
    }

  void Table::UpdateColumnName(const column_index_t &index, const std::string &name)const{
      auto* column = this->columns.at(index);

      column->SetColumnName(name);
  }
void Table::PopulateColumn(const column_index_t &index, const Value &defaultValue){
      if (this->header.indexAllocationMapPageId == INVALID_PAGE_ID)
        return;

      if (this->GetType() == TableType::CLUSTERED) {
        this->PopulateColumnByClusteredIndex(index, defaultValue);
        return;
      }

      this->PopulateColumnByHeap(index, defaultValue);
  }

  void Table::PopulateColumnByClusteredIndex(const column_index_t &index, const Value &defaultValue){
        const auto* tree = this->GetClusteredIndexedTree();

        tree->InsertColumnToRow(index, defaultValue);
  }

  void Table::PopulateColumnByHeap(const column_index_t &index, const Value &defaultValue){
    const auto& filename = this->GetFileName();

    const auto tableMapPage = Storage::StorageManager::Get().GetIndexAllocationMapPage(filename, this->header.indexAllocationMapPageId, this);

    std::vector<extent_id_t> allocatedExtents;
    tableMapPage->GetAllocatedExtents(&allocatedExtents, 0);

    for (const auto& extentId: allocatedExtents) {
      const page_id_t extentFirstPageId = Database::CalculateExtentFirstPageId(extentId);

      const page_id_t firstDataPageId = (tableMapPage->GetPageId() != extentFirstPageId)
                                            ? extentFirstPageId
                                            : extentFirstPageId + 1;

      for (page_id_t pageId = firstDataPageId; pageId < extentFirstPageId + EXTENT_SIZE; pageId++)
      {
          auto pageFreeSpacePage = Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), pageId);

          if (pageFreeSpacePage->GetPageType(pageId) != PageType::DATA)
            break;

          auto page = Storage::StorageManager::Get().GetPage(filename, pageId, this);

          // for (auto& row: page->DataRowsNoLock(this))
          //   this->HandleAddColumn(page.Get(), &row, index, defaultValue);

          pageFreeSpacePage->SetPageMetaData(page.Get());
        }
    }
  }

  //TODO add heap insert if row still cant remain in page if heap
  void Table::HandleAddColumn(Pages::Page* page,  Row* row, const column_index_t& index, const Value &defaultValue){
        const auto& column = this->columns.at(index);

        auto* block = new Block(defaultValue.GetRawData(), defaultValue.GetSize(), column);

        int diff = row->InsertNewColumn(block);

        if(page->GetBytesLeft() - diff > 0){
          page->UpdateBytesLeft();
          return;
        }

        this->InsertLargeObjectToPage(row);

        // if(isHeap && (PAGE_SIZE - PageHeader::GetPageHeaderSize() - row->GetTotalRowSize()) > 0){
        //   vector<extent_id_t> allocatedExtents;
        //   extent_id_t startingExtentIndex = 0;
        //
        //   this->InsertRInsertRow(row, allocatedExtents, startingExtentIndex);
        //
        //   return;
        // }

        while(page->GetBytesLeft() - diff < 0){
          const int result = this->HandleRowOverflow(row);

          if(result == -1)
            break;

          diff -= result;
        }
  }

  void Table::HandleRemoveColumn(Pages::Page* page, Row* row, const column_index_t &index){
        auto& data = row->GetData();

        data.erase(data.begin() + index);

        page->UpdateBytesLeft();
  }

  void Table::RemoveColumn(const column_index_t &index){
    //add also last updated at deleted at etc...
    const auto* removedColumn = this->columns.at(index);

    const auto& server = SystemCatalog::Get();

    //schema adjustments in master db change this as well
    const std::vector<Value> removedColumnUpdates = {
      Value(true, static_cast<column_index_t>(DatabaseEngine::SysColumns::IsDeleted)),
      Value(DataTypes::DateTime::Now(), static_cast<column_index_t>(DatabaseEngine::SysColumns::LastModifiedAt)),
      Value(DataTypes::DateTime::Now(), static_cast<column_index_t>(DatabaseEngine::SysColumns::DeletedAt)),
    };

    auto result = server.UpdateColumnById(removedColumn->GetColumnId(), removedColumnUpdates);

    this->HandleRemoveColumn(removedColumn->GetColumnIndex());
    this->columns.erase(this->columns.begin() + index);

    for (int i = index; i < this->columns.size(); i++) {
      const auto& column = this->columns[i];

      column->SetColumnIndex(i);

      const vector<Value> updates = {
        Value(i, static_cast<column_index_t>(DatabaseEngine::SysColumns::OrdinalPosition))
      };

      //adjust in master db
      result = server.UpdateColumnById(column->GetColumnId(), updates);
    }

    //adjust rows by heap or clustered
    delete removedColumn;
  }

  void Table::HandleRemoveColumn(const column_index_t &index){
    if (this->header.indexAllocationMapPageId == INVALID_PAGE_ID)
      return;

    if (this->GetType() == TableType::CLUSTERED) {
      this->RemoveColumnByClusteredIndex(index);
      return;
    }

    this->RemoveColumnByHeap(index);
  }

  void Table::Rollback(const Snapshot& snapshot, const Headers::RowIdentifier& rowId) const{
        auto page = Storage::StorageManager::Get().GetPage(this->database->GetFileName(), rowId.pageId, this);

        MultiThreading::WriterGuard guard(&page->Latch());

        // auto rows = page->DataRowsNoLock(this);
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

        page->UpdateBytesLeft();
  }
}
