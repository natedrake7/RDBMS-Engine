#include "Table.h"
#include "../../AdditionalLibraries/AdditionalDataTypes/Field/Field.h"
#include "../../AdditionalLibraries/BitMap/BitMap.h"
#include "../Block/Block.h"
#include "../Column/Column.h"
#include "../Constants.h"
#include "../Database.h"
#include "../../QueryPipeline/Statements/Statements.h"
#include "../Pages/LargeObject/LargeDataPage.h"
#include "../Pages/IndexMapAllocation/IndexAllocationMapPage.h"
#include "../Pages/Header/HeaderPage.h"
#include "../Pages/PageFreeSpace/PageFreeSpacePage.h"
#include "../Storage/StorageManager/StorageManager.h"
#include "../Pages/Page.h"
#include "../Row/Row.h"
#include "../B+Tree/BPlusTree.h"
#include "../Pages/IndexPage/IndexPage.h"

#include <iostream>
#include <stdexcept>
#include <unordered_set>


using namespace Pages;
using namespace ByteMaps;
using namespace Indexing;
using namespace Storage;
using namespace Constants;

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
        this->nonClusteredIndexesIds = tableHeader.nonClusteredIndexesIds;

        this->clusteredIndex = tableHeader.clusteredIndex;

        for(const auto& nonClusteredIndex: tableHeader.nonClusteredIndexes)
            this->nonClusteredIndexes.push_back(nonClusteredIndex);

        return *this;
      }

      Table::Table(
        const string &tableName,
        const std::string& schema,
        const table_id_t &tableId,
        const vector<Column *> &columns,
        DatabaseEngine::Database *database,
        Headers::Index* clusteredIndex,
        vector<Headers::Index> *nonClusteredIndexes)
      {
//        this->schema = schema;
        this->columns = columns;
        this->database = database;
        this->header.numberOfColumns = columns.size();
        this->header.tableId = tableId;
        this->header.ordinalPosition = tableId;

        this->clusteredIndexedTree = nullptr;

        if(clusteredIndex)
          this->header.clusteredIndex = *clusteredIndex;

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
      }

      Table::Table(const std::string& tableName, const TableHeader &tableHeader, DatabaseEngine::Database *database)
      {
        this->header = tableHeader;
        this->database = database;
        this->clusteredIndexedTree = nullptr;
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
      }

      Table::~Table()
      {
        delete this->clusteredIndexedTree;

        for (const auto & nonClusteredIndexedTree : this->nonClusteredIndexedTrees) {
          this->header.nonClusteredIndexPageIds.push_back(nonClusteredIndexedTree->GetFirstIndexPageId());
            delete nonClusteredIndexedTree;
        }

        HeaderPage* headerPage = StorageManager::Get().GetHeaderPage(this->database->GetSystemFilename());

        headerPage->SetTableHeader(this);

        for (const auto &column : columns)
            delete column;
      }

      vector<ColumnType> Table::GetColumnTypeByTreeId(const uint8_t& treeId) const
      {
          vector<ColumnType> columns;

          if(treeId == 0)
          {
            for(const auto& columnIndex: this->header.clusteredIndex.columns)
                columns.emplace_back(this->columns[columnIndex]->GetColumnType());

            return columns;
          }

//          for(const auto& columnIndex: this->header.nonClusteredColumnIndexes[treeId - 1])
//              columns.emplace_back(this->columns[columnIndex]->GetColumnType());

          return columns;
      }

      AdditionalDataTypes::ResultStatus Table::InsertRows(const vector<vector<Field>> &inputData) 
      {
        uint32_t rowsInserted = 0;
        extent_id_t startingExtentIndex = 0;
        vector<extent_id_t> extents;

        int64_t primaryKeyVal = 0;
        for (const auto &rowData : inputData) 
        {
            Row* row = this->CreateRow(rowData, &primaryKeyVal);

            const auto result = this->InsertRow(row, extents, startingExtentIndex);

            if (result.code != AdditionalDataTypes::ResultCode::Ok)
              return result;
          
            rowsInserted++;

            if (rowsInserted % 1000 == 0)
                cout << rowsInserted << endl;
        }

        AdditionalDataTypes::ResultStatus status;
        status.message = "Rows affected: " + to_string(rowsInserted);

        return status;
      }

    AdditionalDataTypes::ResultStatus Table::InsertRow(const vector<Field> &inputData){
        extent_id_t startingExtentIndex = 0;
        vector<extent_id_t> extents;

        int64_t primaryKeyVal = 0;

        Row* row = this->CreateRow(inputData, &primaryKeyVal);

        auto result =  this->InsertRow(row, extents, startingExtentIndex);

        if (result.code != AdditionalDataTypes::ResultCode::Ok)
          return result;



        result.message = "Rows affected: 1";
        result.primaryKeyVal = primaryKeyVal;
        
        return result;
    }

      AdditionalDataTypes::ResultStatus Table::InsertRow(Row* row, vector<extent_id_t> &allocatedExtents, extent_id_t &startingExtentIndex)
      {
        this->InsertLargeObjectToPage(row);

        page_id_t rowPageId;
        int rowIndexPosition;

        AdditionalDataTypes::ResultStatus status;

        if (this->GetTableType() == TableType::CLUSTERED) {
            status = this->ClusteredIndexInsert(row, &rowPageId, &rowIndexPosition);

            if (status.code != AdditionalDataTypes::ResultCode::Ok)
                return status;
        }
        else
            this->HeapInsert(allocatedExtents, startingExtentIndex, row, &rowPageId, &rowIndexPosition);

        //insert to Non Clustered Indexes
        if(!this->HasNonClusteredIndexes())
           return status;

        const BPlusTreeNonClusteredData nonClusteredData(rowPageId, rowIndexPosition);

        const auto& nonClusteredIndexes = this->GetNonClusteredIndexes();

        for (int i = 0; i < nonClusteredIndexes.size(); i++) {
            status = this->NonClusteredIndexInsert(row, i, nonClusteredIndexes[i], nonClusteredData);

            if (status.code != AdditionalDataTypes::ResultCode::Ok)
                return status;
        }

        return status;
      }

      Row* Table::CreateRow(const vector<Field>& inputData, int64_t* primaryKeyVal)
      {
        auto *row = new Row(*this);

        *primaryKeyVal = this->PopulateAutoComputedColumns(row);

        for(const auto& input : inputData){

          const auto& associatedColumnIndex = input.GetColumnIndex();

          const auto& column = this->columns.at(associatedColumnIndex);

          auto *block = new Block(column);

          const ColumnType columnType = column->GetColumnType();

          if (columnType > Constants::ColumnType::ColumnTypeCount)
            throw invalid_argument("Table::InsertRow: Unsupported Column Type");

          if (input.GetIsNull())
          {
            Table::CheckAndInsertNullValues(block, row, associatedColumnIndex);
            continue;
          }

          block->SetData(input.GetRawData(), input.GetSize());

          row->InsertColumnData(block, associatedColumnIndex);
        }

        return row;
      }

      column_number_t Table::GetNumberOfColumns() const 
      {
        return this->columns.size();
      }

      const TableHeader &Table::GetTableHeader() const { return this->header; }

      const vector<Column *> &Table::GetColumns() const { return this->columns; }

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

      void Table::SelectForJoin(vector<Row> &selectedRows, const vector<column_index_t>& selectedColumnIndices, const vector<Block> *conditions, const size_t &count)
      {
        const size_t rowsToSelect =  (count == -1) 
                                    ? numeric_limits<size_t>::max() 
                                    : count;

          const auto tableType = this->GetTableType();

          const auto& clusteredIndexes = this->GetClusteredIndex();
          const auto& nonClusteredIndexes = this->GetNonClusteredIndexes();

          Key minimumValue;
          Key maximumValue;

          bool useClusteredIndex = false;
          bool useNonClusteredIndex = false;
          bool useHeap = false;

          bool clusteredIndexSeek = false;
          bool nonClusteredIndexSeek = false;

          if(conditions != nullptr)
          {
            for(const auto& block: *conditions)
            {
                const auto& columnIndex = block.GetColumnIndex();

                const ColumnType columnType = columns[columnIndex]->GetColumnType();
      
                if (columnType > Constants::ColumnType::ColumnTypeCount)
                  throw invalid_argument("Table::Select: Unsupported Column Type");

                int indexPosition = 0;
                if(Table::VectorContainsIndex(clusteredIndexes, columnIndex, indexPosition))
                {
                  useClusteredIndex = true;
                  
                  //figure out how to perform index seek and index scan
                  clusteredIndexSeek = clusteredIndexes[0] == columnIndex;

                  if(!clusteredIndexSeek)
                  {
                    minimumValue.indexKeyPosition = indexPosition;
                    minimumValue.currentSearchKeyPosition = minimumValue.subKeys.size();
                  
                    maximumValue.indexKeyPosition = indexPosition;
                    maximumValue.currentSearchKeyPosition = maximumValue.subKeys.size();
                  }
                }

                int nonClusteredIndexPosition = 0;

                for(int i = 0;i < nonClusteredIndexes.size(); i++)
                {
                  if(Table::VectorContainsIndex(nonClusteredIndexes[i], columnIndex, indexPosition) && !clusteredIndexSeek)
                  {
                      useNonClusteredIndex = true;
                      nonClusteredIndexPosition = i;

                      nonClusteredIndexSeek = nonClusteredIndexes[i][0] == columnIndex;

                      //prioritize clustered index seek over nonclustered index seek or scan
                      if(!nonClusteredIndexSeek)
                      {
                        minimumValue.indexKeyPosition = indexPosition;
                        minimumValue.currentSearchKeyPosition = minimumValue.subKeys.size();
                      
                        maximumValue.indexKeyPosition = indexPosition;
                        maximumValue.currentSearchKeyPosition = maximumValue.subKeys.size();
                      }
                  }
                }

                useHeap = !useNonClusteredIndex && !useClusteredIndex;

                minimumValue.InsertKey(Key(block.GetBlockData(), block.GetBlockSize(), columnType));
                maximumValue.InsertKey(Key(block.GetBlockData(), block.GetBlockSize(), columnType));
            }
          }
        //handle more complex queries like prefer index seek over index scan
//        if(useClusteredIndex)
//        {
//            this->SelectRowsFromClusteredIndex(
//              &selectedRows,
//              rowsToSelect,
//              conditions != nullptr ? &minimumValue : nullptr,
//              conditions != nullptr ? &maximumValue : nullptr,
//              clusteredIndexSeek,
//              selectedColumnIndices
//            );
//            return;
//        }
//        else if (useNonClusteredIndex)
//        {
//            this->SelectRowsFromNonClusteredIndex(&selectedRows, rowsToSelect, nullptr, selectedColumnIndices);
//            return;
//        }
//
//        this->HeapScan(&selectedRows, rowsToSelect);
      }

    void Table::HeapDelete(const Expressions::Expression* expression) const
    {
        if (this->header.indexAllocationMapPageId == INVALID_PAGE_ID)
          return;

        const auto& filename = this->database->GetFileName();

        const auto extentId = Database::CalculateExtentIdByPageId(this->header.indexAllocationMapPageId);

        const IndexAllocationMapPage *tableMapPage = StorageManager::Get().GetIndexAllocationMapPage(filename, this->header.indexAllocationMapPageId, extentId, this);

        vector<extent_id_t> tableExtentIds;
        tableMapPage->GetAllocatedExtents(&tableExtentIds, 0);

        vector<Row*> rowsToBeInserted;

        for (const auto &extentId : tableExtentIds)
        {
          const page_id_t extentFirstPageId = Database::CalculateSystemPageOffset(extentId * EXTENT_SIZE);

          auto* pageFreeSpacePage = Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), extentFirstPageId);

          const page_id_t pageId = (tableMapPage->GetPageId() != extentFirstPageId)
                                       ? extentFirstPageId
                                       : extentFirstPageId + 1;

          for (page_id_t extentPageId = pageId; extentPageId < extentFirstPageId + EXTENT_SIZE; extentPageId++)
          {
            if (pageFreeSpacePage->GetPageType(extentPageId) != PageType::DATA)
              break;

            Page *page = StorageManager::Get().GetPage(filename, extentPageId, extentId, this);

            page->Delete(expression);

            page->UpdateBytesLeft();
            page->UpdatePageSize();

            pageFreeSpacePage->SetPageMetaData(page);
          }
        }
    }

    void Table::ClusteredIndexScanDelete(const Expressions::Expression *expression){
        auto* tree = this->GetClusteredIndexedTree();

        vector<Row> results;
        tree->IndexScan(&results);

        if(results.empty())
          return;

        for(const auto& row : results){
          if(row.Evaluate(expression))
          {
            const auto& key = Database::CreateKey(this->header.clusteredIndex.columns, &row);
            tree->Remove(key);
          }
        }
  }

  void Table::ClusteredIndexSeekDelete(const Expressions::Expression *expression){
  }

    void Table::Truncate()
    {
        this->database->TruncateTable(this->header.tableId);
    }

    void Table::UpdateIndexAllocationMapPageId(const page_id_t &indexAllocationMapPageId) 
    {
        this->header.indexAllocationMapPageId = indexAllocationMapPageId;
    }

    bool Table::IsColumnNullable(const column_index_t &columnIndex) const 
    {
        return this->columns.at(columnIndex)->IsColumnNullable();
    }

    void Table::AddColumn(Column *column) { this->columns.push_back(column); }

    string Table::GetSchema(){ return {}; }

    const table_id_t &Table::GetTableId() const { return this->header.tableId; }

    TableType Table::GetTableType() const 
    {
        return !this->header.clusteredIndex.columns.empty()
                    ? TableType::CLUSTERED
                    : TableType::HEAP;
    }

    row_size_t Table::GetMaximumRowSize() const 
    {
        row_size_t maximumRowSize = 0;

        for (const auto &column : this->columns)
            maximumRowSize += column->GetColumnSize();

        return maximumRowSize;
    }

    key_size_t Table::CalculateIndexKeySize() const {
      HashSet<column_index_t> clusteredColumns;

      key_size_t keySize = 0;
      for(const auto& column : this->header.clusteredIndex.columns)
        clusteredColumns.Add(column);

      for (const auto &column : this->columns)
        if(clusteredColumns.Contains(column->GetColumnIndex()))
          keySize += column->GetColumnSize();

        return keySize;
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

        maximumRowSize += column->isColumnOverflowed() ? sizeof(OverflowPointer) : columnSize;

        if(columnSize <= largestVariableLengthColumnSize
            || column->isColumnOverflowed()
            || columnSize >= LARGE_DATA_OBJECT_SIZE
            || clusteredColumns.Contains(column->GetColumnIndex()))
          continue;

        largestVariableLengthColumnSize = columnSize;
        largestColumn = column;
      }

      maximumRowSize -= largestVariableLengthColumnSize;
      maximumRowSize += sizeof(OverflowPointer);
      largestColumn->SetIsOverflowed(true);

      return maximumRowSize;
    }



    void Table::ClusteredIndexSeek(vector<Row> *selectedRows, const Indexing::Key *minimumValue, const Indexing::Key *maximumValue){
        auto* tree = this->GetClusteredIndexedTree();

        tree->IndexSeek(*minimumValue, *maximumValue, selectedRows);
    }

    void Table::ClusteredIndexScan(vector<Row> *selectedRows, Expressions::Expression* expression){

        auto* tree = this->GetClusteredIndexedTree();

        if(expression != nullptr){
          tree->IndexScan(selectedRows, expression);
          return;
        }

        tree->IndexScan(selectedRows);
    }

    void Table::HeapScan(vector<Row> *selectedRows, const size_t &rowsToSelect)const
    {
        if(this->header.indexAllocationMapPageId == INVALID_PAGE_ID)
            return;

        const auto& filename = this->database->GetFileName();

        const auto extentId = Database::CalculateExtentIdByPageId(this->header.indexAllocationMapPageId);

        const IndexAllocationMapPage *tableMapPage = StorageManager::Get().GetIndexAllocationMapPage(filename, this->header.indexAllocationMapPageId, extentId, this);

        vector<extent_id_t> tableExtentIds;
        tableMapPage->GetAllocatedExtents(&tableExtentIds, 0);

        for (const auto& extentId : tableExtentIds){
          const page_id_t extentFirstPageId = DatabaseEngine::Database::CalculateSystemPageOffset(extentId * EXTENT_SIZE);

          const auto* pageFreeSpacePage = DatabaseEngine::Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), extentFirstPageId);

          const page_id_t pageId = (tableMapPage->GetPageId() != extentFirstPageId)
                                      ? extentFirstPageId
                                      : extentFirstPageId + 1;
          
          for (page_id_t extentPageId = pageId; extentPageId < extentFirstPageId + EXTENT_SIZE; extentPageId++)
          {
            if (pageFreeSpacePage->GetPageType(extentPageId) != PageType::DATA)
              break;

            const Page *page = StorageManager::Get().GetPage(filename, extentPageId, extentId, this);

            if (page->GetPageSize() == 0)
              continue;

            page->GetRows(selectedRows, *this, rowsToSelect);
          }
        }
    }

    AdditionalDataTypes::ResultStatus Table::ClusteredIndexInsert(Row *row, page_id_t *rowPageId, int *rowIndex){

       BPlusTree* tree = this->GetClusteredIndexedTree();

       const auto key = Database::CreateKey(this->GetClusteredIndex(), row);

       int indexPosition = 0;

       AdditionalDataTypes::ResultStatus status;

       auto *node = tree->FindAppropriateNodeForInsert(key, &indexPosition, status);

       if (status.code != AdditionalDataTypes::ResultCode::Ok)
           return status;

       *rowIndex = indexPosition;

       PageFreeSpacePage *pageFreeSpacePage =  Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), node->GetPageId());

       // should never fail
       this->InsertRowToClusteredPage(pageFreeSpacePage, node, row, indexPosition);

       auto* keys = node->GetKeysUnsafe();

       keys->insert(keys->begin() + indexPosition, new Key(key));

       node->UpdateBytesLeft();

       *rowPageId = node->GetPageId();

       // this->SplitNodeFromIndexPage(tableId, node);
       return {};
     }

    AdditionalDataTypes::ResultStatus Table::HeapInsert(vector<extent_id_t> & allocatedExtents, extent_id_t & lastExtentIndex, Row *row, page_id_t *rowPageId, int *rowIndex){
      const auto& filename = this->database->GetFileName();

      while(row->GetTotalRowSize() > PAGE_SIZE - PageHeader::GetPageHeaderSize())
        this->HandleRowOverflow(row);

      if (this->header.indexAllocationMapPageId == INVALID_PAGE_ID)
      {
          Page *newPage = this->database->CreateDataPage(this->header.ordinalPosition);

          newPage->InsertRow(row, rowIndex);

          *rowPageId = newPage->GetPageId();

          return {};
      }

      const auto extentId = Database::CalculateExtentIdByPageId(this->header.indexAllocationMapPageId);

      const IndexAllocationMapPage *tableMapPage = StorageManager::Get().GetIndexAllocationMapPage(filename, this->header.indexAllocationMapPageId, extentId, this);

      tableMapPage->GetAllocatedExtents(&allocatedExtents, lastExtentIndex);
      lastExtentIndex = allocatedExtents.size() - 1;

      const Constants::byte rowCategory = Database::GetObjectSizeToCategory(row->GetTotalRowSize());

      for (const auto &extentId : allocatedExtents)
      {
          const page_id_t extentFirstPageId = Database::CalculateSystemPageOffsetByExtentId(extentId);

          const page_id_t firstDataPageId = (tableMapPage->GetPageId() != extentFirstPageId)
                                                ? extentFirstPageId
                                                : extentFirstPageId + 1;

          for (page_id_t pageId = firstDataPageId; pageId < extentFirstPageId + EXTENT_SIZE; pageId++)
          {

              auto* pageFreeSpacePage = Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), pageId);

              if (pageFreeSpacePage->GetPageType(pageId) != PageType::DATA)
                  break;

              const Constants::byte pageSizeCategory = pageFreeSpacePage->GetPageSizeCategory(pageId);

              // find potential candidate
              if (rowCategory <= pageSizeCategory)
              {
                  Page *page = StorageManager::Get().GetPage(filename, pageId, extentId, this);

                  if (row->GetTotalRowSize() > page->GetBytesLeft())
                      continue;

                  page->InsertRow(row, rowIndex);
                  pageFreeSpacePage->SetPageMetaData(page);

                  *rowPageId = pageId;
                  return {};
              }
          }
      }

      Page *newPage = this->database->CreateDataPage(this->header.ordinalPosition);
      newPage->InsertRow(row, rowIndex);
      *rowPageId = newPage->GetPageId();

      return {};
    }

    void Table::HeapUpdate(const Expressions::Expression *expression, const vector<Field> & updates){
        if(this->header.indexAllocationMapPageId == INVALID_PAGE_ID)
          return;

        const auto& filename = this->database->GetFileName();

        const auto extentId = Database::CalculateExtentIdByPageId(this->header.indexAllocationMapPageId);

        const IndexAllocationMapPage *tableMapPage = StorageManager::Get().GetIndexAllocationMapPage(filename, this->header.indexAllocationMapPageId, extentId, this);

        vector<extent_id_t> tableExtentIds;
        tableMapPage->GetAllocatedExtents(&tableExtentIds, 0);

        HashSet<column_index_t> updatedColumns;
        for(const auto& update : updates)
              updatedColumns.Add(update.GetColumnIndex());

        for (const auto& extentId : tableExtentIds){
          const page_id_t extentFirstPageId = DatabaseEngine::Database::CalculateSystemPageOffset(extentId * EXTENT_SIZE);

          const PageFreeSpacePage *pageFreeSpacePage = DatabaseEngine::Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), extentFirstPageId);

          const page_id_t pageId = (tableMapPage->GetPageId() != extentFirstPageId)
                                      ? extentFirstPageId
                                      : extentFirstPageId + 1;

          for (page_id_t extentPageId = pageId; extentPageId < extentFirstPageId + EXTENT_SIZE; extentPageId++)
          {
            if (pageFreeSpacePage->GetPageType(extentPageId) != PageType::DATA)
              break;

            Page *page = StorageManager::Get().GetPage(filename, extentPageId, extentId, this);

            if (page->GetPageSize() == 0)
              continue;

            auto* rows = page->GetDataRowsUnsafe();

            std::vector<extent_id_t> allocatedExtents;
            extent_id_t startingExtentIndex = 0;

            for(auto* row : *rows){
                if(!row->Evaluate(expression))
                  continue;

                this->HandleRowUpdate(page, row, updates, updatedColumns);
            }
          }
        }
    }

    void Table::DeleteLargeObjectFromPage(Row *row, const HashSet<column_index_t>& updatedColumns){
      const auto& filename = this->database->GetFileName();

      RowHeader* rowHeader = row->GetHeader();

      for(const auto& block : row->GetData()){
        if(!updatedColumns.Contains(block->GetColumnIndex())
          || !rowHeader->largeObjectBitMap->Get(block->GetColumnIndex()))
          continue;

        rowHeader->largeObjectBitMap->Set(block->GetColumnIndex(), false);

        auto objectPointer = block->GeObjectPointer();

        auto largeObjectExtentId = Database::CalculateExtentIdByPageId(objectPointer.pageId);

        auto* largeObjectPage = StorageManager::Get().GetLargeDataPage(filename, objectPointer.pageId, largeObjectExtentId, this);

        auto* objectPtr = largeObjectPage->DeleteObject();

        auto* pfsPage = Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), objectPointer.pageId);

        pfsPage->SetPageMetaData(largeObjectPage);
        pfsPage->SetPageFreed(largeObjectPage->GetPageId());

        while(objectPtr->nextPageId != 0){
            largeObjectExtentId = Database::CalculateExtentIdByPageId(objectPtr->nextPageId);

            auto* nextLargeObjectPage = StorageManager::Get().GetLargeDataPage(filename, objectPtr->nextPageId, largeObjectExtentId, this);

            DataObject* prevObject = objectPtr;
            objectPtr = nextLargeObjectPage->DeleteObject();

            pfsPage = Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), objectPointer.pageId);

            pfsPage->SetPageMetaData(nextLargeObjectPage);
            pfsPage->SetPageFreed(nextLargeObjectPage->GetPageId());

        }
      }
    }

    void Table::ClusteredIndexScanUpdate(Expressions::Expression *expression, const vector<Field> & updates){
      auto* tree = this->GetClusteredIndexedTree();

      tree->IndexScanUpdate(expression, updates);
    }

    void Table::ClusteredIndexSeekUpdate(
        Expressions::Expression* expression,
        const Indexing::Key *minimumValue,
        const Indexing::Key *maximumValue,
        const vector<Field> & updates){
      auto* tree = this->GetClusteredIndexedTree();

      tree->IndexSeekUpdate(expression, minimumValue, maximumValue, updates);
    }
    string Table::GetFileName() const{ return this->database->GetFileName(); }

    int Table::HandleRowOverflow(Row *row){
      auto* largestBlock = row->FindLargestVariableLengthColumn();

      if(largestBlock == nullptr)
        return -1;

      auto* overflowPage = this->database->GetLastOverflowPage(this->header.ordinalPosition, largestBlock->GetBlockSize());

      int indexPos = 0;
      overflowPage->InsertObject(largestBlock->GetBlockData(), largestBlock->GetBlockSize(), indexPos);

      row->SetOverflowBitMapValue(largestBlock->GetColumnIndex(), true);

      auto* pfsPage = DatabaseEngine::Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), overflowPage->GetPageId());

      pfsPage->SetPageMetaData(overflowPage);

      OverflowPointer ptr(overflowPage->GetPageId(), indexPos);
      largestBlock->SetData(&ptr, sizeof(OverflowPointer));

      return largestBlock->GetBlockSize();
    }

    void Table::DeleteOverflowedRowsFromPage(Row *row, const HashSet<column_index_t> & updatedColumns){
      const auto& filename = this->database->GetFileName();

      RowHeader* rowHeader = row->GetHeader();

      for(const auto& block : row->GetData()){
        if(!updatedColumns.Contains(block->GetColumnIndex())
          || !rowHeader->overflowBitMap->Get(block->GetColumnIndex()))
            continue;

        rowHeader->overflowBitMap->Set(block->GetColumnIndex(), false);

        auto objectPointer = block->GetOverflowPointer();

        auto overflowExtentId = Database::CalculateExtentIdByPageId(objectPointer.pageId);

        auto* overflowPage = StorageManager::Get().GetOverflowPage(filename, objectPointer.pageId, overflowExtentId, this);

        auto* overflowRow = overflowPage->DeleteObject(objectPointer.index);

        auto* pfsPage = StorageManager::Get().GetPageFreeSpacePage(filename, Database::GetPfsAssociatedPage(objectPointer.pageId));

        pfsPage->SetPageMetaData(overflowPage);

        delete overflowRow;
      }
    }

    //create differrent one to handle clustered updates
    void Table::HandleRowUpdate(Pages::Page *page, Row *row, const std::vector<Field> &updates, const HashSet<column_index_t>& updatedColumns, const bool &isHeap){
        this->DeleteLargeObjectFromPage(row, updatedColumns);
        this->DeleteOverflowedRowsFromPage(row, updatedColumns);

        int diff = row->Update(updates);

        if(page->GetBytesLeft() - diff > 0){
          page->UpdateBytesLeft();
          return;
        }

        this->InsertLargeObjectToPage(row);

        if(isHeap && (PAGE_SIZE - PageHeader::GetPageHeaderSize() - row->GetRowSize()) > 0){
          vector<extent_id_t> allocatedExtents;
          extent_id_t startingExtentIndex = 0;

          this->InsertRow(row, allocatedExtents, startingExtentIndex);

          return;
        }

        if(!isHeap)


        while(page->GetBytesLeft() - diff < 0){
          const int result = this->HandleRowOverflow(row);
          if(result == -1)
            break;

          diff -= result;
        }
    }

    void Table::InsertRowToPage(Pages::PageFreeSpacePage *pageFreeSpacePage, Pages::Page *page, Row *row, const int & indexPosition){

      while (row->GetTotalRowSize() > page->GetBytesLeft())
        this->HandleRowOverflow(row);

      page->InsertRow(row, indexPosition);
      pageFreeSpacePage->SetPageMetaData(page);
    }

    void Table::InsertRowToClusteredPage(Pages::PageFreeSpacePage *pageFreeSpacePage, Pages::Page *page, Row *row, const int & indexPosition){
      for(auto& column: this->columns){
        if(!column->isColumnOverflowed())
          continue;

        this->HandleRowOverflow(row, column);
      }

      page->InsertRow(row, indexPosition);
      pageFreeSpacePage->SetPageMetaData(page);
    }


    AdditionalDataTypes::ResultStatus Table::NonClusteredIndexInsert(const StorageTypes::Row *row, const int & nonClusteredIndexId, const vector<column_index_t> & indexedColumns, const BPlusTreeNonClusteredData & data){

      BPlusTree* tree = this->GetNonClusteredIndexTree(nonClusteredIndexId);
      const auto key = Database::CreateKey(indexedColumns, row);

      int indexPosition = 0;
      AdditionalDataTypes::ResultStatus status;

      // Node *node = tree->FindAppropriateNodeForInsert(key, &indexPosition, status);

      // if (status.code != AdditionalDataTypes::ResultCode::Ok)
      //     return status;

      // node->keys.insert(node->keys.begin() + indexPosition, key);
      // node->nonClusteredData.insert(node->nonClusteredData.begin() + indexPosition, data);
      // node->prevNodeSize = node->currentNodeSize;
      // node->currentNodeSize = node->GetNodeSize();

      // this->SplitNodeFromIndexPage(tableId, node, nonClusteredIndexId);

      return status;
    }

    int Table::HandleRowOverflow(Row *row, Column *column){

        auto& data = row->GetData();

        if(data.size() < column->GetColumnIndex())
          return -1;

        auto* largestBlock = row->GetData().at(column->GetColumnIndex());

        auto* overflowPage = this->database->GetLastOverflowPage(this->header.ordinalPosition, largestBlock->GetBlockSize());

        int indexPos = 0;
        overflowPage->InsertObject(largestBlock->GetBlockData(), largestBlock->GetBlockSize(), indexPos);

        row->SetOverflowBitMapValue(largestBlock->GetColumnIndex(), true);

        const auto pfsPageId = DatabaseEngine::Database::GetPfsAssociatedPage(overflowPage->GetPageId());

        auto* pfsPage = StorageManager::Get().GetPageFreeSpacePage(this->database->GetFileName(), pfsPageId);

        pfsPage->SetPageMetaData(overflowPage);

        OverflowPointer ptr(overflowPage->GetPageId(), indexPos);
        largestBlock->SetData(&ptr, sizeof(OverflowPointer));

        row->UpdateRowSize();

        return largestBlock->GetBlockSize();
    }

    int64_t Table::PopulateAutoComputedColumns(Row *row){
      int64_t primaryKeyValue = 0;

      if(this->header.clusteredIndex.seed != -1){
        const auto& columnIndex = this->header.clusteredIndex.columns[0];

        const auto& column = this->columns.at(columnIndex);

        const auto& columnSize = column->GetColumnSize();

        auto* block = new Block(&this->header.clusteredIndex.lastValue, columnSize ,column);

        primaryKeyValue = this->header.clusteredIndex.lastValue;

        this->header.clusteredIndex.lastValue += this->header.clusteredIndex.incrementFactor;

        row->InsertColumnData(block, columnIndex);
      }

      for(auto& nonClusteredIndexes: this->header.nonClusteredIndexes){
        if(nonClusteredIndexes.seed == -1)
          continue;

        const auto& columnIndex = nonClusteredIndexes.columns[0];

        auto* block = row->GetData()[columnIndex];

        const auto& columnSize = this->columns.at(columnIndex)->GetColumnSize();

        block->SetData(&nonClusteredIndexes.lastValue, columnSize);

        nonClusteredIndexes.lastValue += nonClusteredIndexes.incrementFactor;
      }

      return primaryKeyValue;
    }

    void Table::GetIdentityColumns(){
      const auto identityHeaders = Server::ServerInstance::Get().SelectIdentityColumnsByTableId(this->header.tableId);

      if(identityHeaders.empty())
        return;

      for(const auto& column: this->columns){
          if(column->GetColumnId() == identityHeaders.begin()->columnId){
            this->header.clusteredIndex.columns.emplace_back(column->GetColumnIndex());
            break;
          }
      }

      this->header.clusteredIndex.seed = identityHeaders.begin()->seedValue;
      this->header.clusteredIndex.incrementFactor = identityHeaders.begin()->increment;
      this->header.clusteredIndex.lastValue = identityHeaders.begin()->lastValue;
      this->header.clusteredIndex.cacheBlock = identityHeaders.begin()->cacheBlock;
    }

    void Table::UpdateMasterDatabase() const{
      Server::ServerInstance::Get().UpdateIdentityByTableId(this->header.tableId, static_cast<int32_t>(this->header.clusteredIndex.lastValue));
    }

}

 // namespace DatabaseEngine::StorageTypes