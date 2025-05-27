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
        this->indexAllocationMapPageId = 0;
        this->tableId = 0;
        this->maxRowSize = 0;
        this->numberOfColumns = 0;
        this->clusteredIndexPageId = 0;
        // this->columnsNullBitMap = nullptr;
        // this->clusteredIndexesBitMap = nullptr;
      }

      TableHeader::~TableHeader() = default;

      TableHeader &TableHeader::operator=(const TableHeader &tableHeader) 
      {
        if (this == &tableHeader)
          return *this;

        this->indexAllocationMapPageId = tableHeader.indexAllocationMapPageId;
        this->maxRowSize = tableHeader.maxRowSize;
        this->numberOfColumns = tableHeader.numberOfColumns;
        this->tableId = tableHeader.tableId;
        this->clusteredIndexPageId = tableHeader.clusteredIndexPageId;
        this->nonClusteredIndexPageIds = tableHeader.nonClusteredIndexPageIds;
        this->nonClusteredIndexesIds = tableHeader.nonClusteredIndexesIds;

        // this->columnsNullBitMap = new BitMap(*tableHeader.columnsNullBitMap);
        this->clusteredColumnIndexes = tableHeader.clusteredColumnIndexes;

          for(const auto& nonClusteredIndexes: tableHeader.nonClusteredColumnIndexes)
              this->nonClusteredColumnIndexes.push_back(nonClusteredIndexes);

        return *this;
      }

      Table::Table(const string &tableName, const std::string& schema, const table_id_t &tableId, const vector<Column *> &columns,  DatabaseEngine::Database *database, const vector<column_index_t> *clusteredKeyIndexes, const vector<vector<column_index_t>> *nonClusteredIndexes)
      {
        this->name = tableName;
        this->schema = schema;
        this->columns = columns;
        this->database = database;
        this->header.numberOfColumns = columns.size();
        // this->header.columnsNullBitMap = new BitMap(this->header.numberOfColumns);
        this->header.tableId = tableId;

        this->clusteredIndexedTree = nullptr;

        this->SetTableIndexesToHeader(clusteredKeyIndexes, nonClusteredIndexes);

        for (const auto &column : columns) 
        {
          // this->header.columnsNullBitMap->Set(counter, column->GetAllowNulls());
          this->header.maxRowSize += column->GetColumnSize();
        }
      }

      Table::Table(const Headers::TableHeader& masterDbHeader, const TableHeader &tableHeader, DatabaseEngine::Database *database)
      {
        this->header = tableHeader;
        this->header.tableId = masterDbHeader.id;
        this->database = database;
        this->name = masterDbHeader.name;
        this->schema = masterDbHeader.schemaName;
        this->clusteredIndexedTree = nullptr;
      }

      Table::Table(const std::string& tableName, const TableHeader &tableHeader, DatabaseEngine::Database *database)
      {
        this->header = tableHeader;
        this->database = database;
        this->name = tableName;
        this->schema = "dbo";
        this->clusteredIndexedTree = nullptr;
      }

      Table::Table(const Headers::sysTable &systemHeader, const TableHeader &tableHeader, DatabaseEngine::Database *database){
        this->header = tableHeader;
        this->database = database;
        this->name = systemHeader.name;
        this->schema = "dbo";
        this->clusteredIndexedTree = nullptr;
      }

      Table::~Table()
      {
        delete this->clusteredIndexedTree;

        for (const auto & nonClusteredIndexedTree : this->nonClusteredIndexedTrees) {
          this->header.nonClusteredIndexPageIds.push_back(nonClusteredIndexedTree->GetFirstIndexPageId());
            delete nonClusteredIndexedTree;
        }

        HeaderPage* headerPage = StorageManager::Get().GetHeaderPage(this->database->GetFileName());

        headerPage->SetTableHeader(this);

        for (const auto &column : columns)
            delete column;
      }

      void Table::SetTableIndexesToHeader(const vector<column_index_t> *clusteredKeyIndexes, const vector<vector<column_index_t>> *nonClusteredIndexes) 
      {
        if (clusteredKeyIndexes != nullptr && !clusteredKeyIndexes->empty())
        {

            this->header.clusteredColumnIndexes = *clusteredKeyIndexes;
        
            this->clusteredIndexedTree = new BPlusTree(this, this->header.clusteredIndexPageId, TreeType::Clustered);
        }

        if (nonClusteredIndexes != nullptr && !nonClusteredIndexes->empty())
        {
            for (int i = 0; i < nonClusteredIndexes->size(); i++)
            {
                this->header.nonClusteredColumnIndexes.push_back(nonClusteredIndexes->at(i));
                this->header.nonClusteredIndexesIds.emplace_back(i + 1);
            }

            this->header.nonClusteredIndexPageIds.resize(nonClusteredIndexes->size(), 0);
        }
      }

      vector<ColumnType> Table::GetColumnTypeByTreeId(const uint8_t& treeId) const
      {
          vector<ColumnType> columns;

          if(treeId == 0)
          {
            for(const auto& columnIndex: this->header.clusteredColumnIndexes)
                columns.emplace_back(this->columns[columnIndex]->GetColumnType());

            return columns;
          }

          for(const auto& columnIndex: this->header.nonClusteredColumnIndexes[treeId - 1])
              columns.emplace_back(this->columns[columnIndex]->GetColumnType());

          return columns;
      }

      AdditionalDataTypes::ResultStatus Table::InsertRows(const vector<vector<Field>> &inputData) 
      {
        uint32_t rowsInserted = 0;
        extent_id_t startingExtentIndex = 0;
        vector<extent_id_t> extents;
        for (const auto &rowData : inputData) 
        {
            const auto result = this->InsertRow(rowData, extents, startingExtentIndex);

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

        auto result =  this->InsertRow(inputData, extents, startingExtentIndex);

        if (result.code != AdditionalDataTypes::ResultCode::Ok)
          return result;

        result.message = "Rows affected: 1";
        
        return result;
    }

      AdditionalDataTypes::ResultStatus Table::InsertRow(const vector<Field> &inputData, vector<extent_id_t> &allocatedExtents, extent_id_t &startingExtentIndex) 
      {
        Row* row = this->CreateRow(inputData);

        this->InsertLargeObjectToPage(row);
        auto result =  this->database->InsertRowToPage(this->header.tableId, allocatedExtents, startingExtentIndex, row);

        if (result.code != AdditionalDataTypes::ResultCode::Ok)
          return result;

        result.message = "Rows affected: 1";
        
        return result;
      }

      Row* Table::CreateRow(const vector<Field>& inputData)const
      {
        auto *row = new Row(*this);
        for (const auto & i : inputData) 
        {
          const column_index_t &associatedColumnIndex = i.GetColumnIndex();

          auto *block = new Block(columns[associatedColumnIndex]);

          const ColumnType columnType = columns[associatedColumnIndex]->GetColumnType();

          if (columnType > Constants::ColumnType::ColumnTypeCount)
            throw invalid_argument("Table::InsertRow: Unsupported Column Type");

          if (i.GetIsNull()) 
          {
            Table::CheckAndInsertNullValues(block, row, associatedColumnIndex);
            continue;
          }

          block->SetData(i.GetRawData(), i.GetSize());

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
          else if(!this->header.clusteredColumnIndexes.empty())
            useClusteredIndex = true;
          else if(!this->header.nonClusteredColumnIndexes.empty())
            useNonClusteredIndex = true;

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

      void Table::Update(const vector<Field> &updates, const vector<Field> *conditions) const 
      {
         vector<Block *> updateBlocks;
         for (const auto &field : updates) 
         {
           const auto &associatedColumnIndex = field.GetColumnIndex();
    
           const auto &columnType = this->columns[associatedColumnIndex]->GetColumnType();

           Block *block = new Block(this->columns[associatedColumnIndex]);

           updateBlocks.push_back(block);
         }

         this->database->UpdateTableRows(this->header.tableId, updateBlocks, conditions);

         for (const auto &block : updateBlocks)
           delete block;
      }

    void Table::HeapDelete(const Expressions::Expression* expression) const
    {
        const auto& filename = this->database->GetFileName();

        const IndexAllocationMapPage *tableMapPage = StorageManager::Get().GetIndexAllocationMapPage(filename, this->header.indexAllocationMapPageId);

        vector<extent_id_t> tableExtentIds;
        tableMapPage->GetAllocatedExtents(&tableExtentIds, 0);

        vector<Row*> rowsToBeInserted;

        for (const auto &extentId : tableExtentIds)
        {
          const page_id_t extentFirstPageId = Database::CalculateSystemPageOffset(extentId * EXTENT_SIZE);

          const page_id_t pfsPageId = Database::GetPfsAssociatedPage(extentFirstPageId);

          PageFreeSpacePage *pageFreeSpacePage = StorageManager::Get().GetPageFreeSpacePage(filename, pfsPageId);

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
            const auto& key = Database::CreateKey(this->header.clusteredColumnIndexes, &row);
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

    string &Table::GetTableName() { return this->name; }

    string & Table::GetSchema(){ return this->schema; }

    row_size_t &Table::GetMaxRowSize() { return this->header.maxRowSize; }

    const table_id_t &Table::GetTableId() const { return this->header.tableId; }

    TableType Table::GetTableType() const 
    {
        return !this->header.clusteredColumnIndexes.empty()
                    ? TableType::CLUSTERED
                    : TableType::HEAP;
    }

    row_size_t Table::GetMaximumRowSize() const 
    {
        row_size_t maximumRowSize = 0;
    
        for (const auto &column : this->columns)
            maximumRowSize += (column->isColumnLOB()) ? sizeof(DataObjectPointer)
                                                    : column->GetColumnSize();

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
        if(this->header.indexAllocationMapPageId == 0)
            return;

        const auto& filename = this->database->GetFileName();

        const IndexAllocationMapPage *tableMapPage = StorageManager::Get().GetIndexAllocationMapPage(filename, this->header.indexAllocationMapPageId);

        vector<extent_id_t> tableExtentIds;
        tableMapPage->GetAllocatedExtents(&tableExtentIds, 0);

        for (const auto& extentId : tableExtentIds){
          const page_id_t extentFirstPageId = DatabaseEngine::Database::CalculateSystemPageOffset(extentId * EXTENT_SIZE);

          const page_id_t pfsPageId = DatabaseEngine::Database::GetPfsAssociatedPage(extentFirstPageId);

          const PageFreeSpacePage *pageFreeSpacePage = StorageManager::Get().GetPageFreeSpacePage(filename, pfsPageId);

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

    void Table::HeapUpdate(const Expressions::Expression *expression, const vector<Field> & updates){
        if(this->header.indexAllocationMapPageId == 0)
          return;

        const auto& filename = this->database->GetFileName();

        const IndexAllocationMapPage *tableMapPage = StorageManager::Get().GetIndexAllocationMapPage(filename, this->header.indexAllocationMapPageId);

        vector<extent_id_t> tableExtentIds;
        tableMapPage->GetAllocatedExtents(&tableExtentIds, 0);

        HashSet<column_index_t> updatedColumns;
        for(const auto& update : updates)
              updatedColumns.Add(update.GetColumnIndex());

        for (const auto& extentId : tableExtentIds){
          const page_id_t extentFirstPageId = DatabaseEngine::Database::CalculateSystemPageOffset(extentId * EXTENT_SIZE);

          const page_id_t pfsPageId = DatabaseEngine::Database::GetPfsAssociatedPage(extentFirstPageId);

          const PageFreeSpacePage *pageFreeSpacePage = StorageManager::Get().GetPageFreeSpacePage(filename, pfsPageId);

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

            for(int i = 0; i < rows->size(); i++){
                auto* row = (*rows)[i];

                if(row->Evaluate(expression)){
                  const auto diff = row->Update(updates);

                  if(page->GetBytesLeft() - diff > 0){
                    page->UpdateBytesLeft();
                    continue;
                  }

                  Table::DeleteLargeObjectFromPage(row, updatedColumns, this);

                  //TODO add Forwarding Ptr to reduce index updates
                  rows->erase(rows->begin() + i);

                  this->InsertLargeObjectToPage(row);
                  auto result = this->database->InsertRowToPage(this->header.tableId, allocatedExtents, startingExtentIndex, row);
                }
            }
          }
        }
    }

    unordered_set<column_index_t> Table::GetClusteredIndexesMap() const
    {
        unordered_set<column_index_t> hashSet = {};
          
        for(const auto& clusteredColumnIndex: this->header.clusteredColumnIndexes)
            hashSet.insert(clusteredColumnIndex);

        return hashSet;
    }

    void Table::DeleteLargeObjectFromPage(Row *row, const HashSet<column_index_t>& updatedColumns, const Table* table){
      const auto& filename = table->GetFileName();

      RowHeader* rowHeader = row->GetHeader();

      for(const auto& block : row->GetData()){
        if(!updatedColumns.Contains(block->GetColumnIndex())
          || !rowHeader->largeObjectBitMap->Get(block->GetColumnIndex()))
          continue;

        auto objectPointer = block->GeObjectPointer();

        auto largeObjectExtentId = Database::CalculateExtentIdByPageId(objectPointer.pageId);

        auto* largeObjectPage = StorageManager::Get().GetLargeDataPage(filename, objectPointer.pageId, largeObjectExtentId, table);

        auto* objectPtr = largeObjectPage->DeleteObject();

        auto* pfsPage = StorageManager::Get().GetPageFreeSpacePage(filename, Database::GetPfsAssociatedPage(objectPointer.pageId));

        pfsPage->SetPageMetaData(largeObjectPage);
        pfsPage->SetPageFreed(largeObjectPage->GetPageId());

        while(objectPtr->nextPageId != 0){
            largeObjectExtentId = Database::CalculateExtentIdByPageId(objectPtr->nextPageId);

            largeObjectPage = StorageManager::Get().GetLargeDataPage(filename, objectPtr->nextPageId, largeObjectExtentId, table);

            DataObject* prevObject = objectPtr;
            objectPtr = largeObjectPage->DeleteObject();

            pfsPage = StorageManager::Get().GetPageFreeSpacePage(filename, Database::GetPfsAssociatedPage(objectPointer.pageId));

            pfsPage->SetPageMetaData(largeObjectPage);
            pfsPage->SetPageFreed(largeObjectPage->GetPageId());

            delete prevObject;
        }
      }
    }

    void Table::ClusteredIndexScanUpdate(const Expressions::Expression *expression, const vector<Field> & updates){



    }

    string Table::GetFileName() const{ return this->database->GetFileName(); }
} // namespace DatabaseEngine::StorageTypes