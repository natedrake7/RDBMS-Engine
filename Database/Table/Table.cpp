﻿#include "Table.h"
#include "../../AdditionalLibraries/AdditionalDataTypes/Field/Field.h"
#include "../../AdditionalLibraries/BitMap/BitMap.h"
#include "../Block/Block.h"
#include "../Column/Column.h"
#include "../Constants.h"
#include "../Database.h"
#include "../Pages/LargeObject/LargeDataPage.h"
#include "../Pages/IndexMapAllocation/IndexAllocationMapPage.h"
#include "../Pages/Header/HeaderPage.h"
#include "../Pages/PageFreeSpace/PageFreeSpacePage.h"
#include "../Storage/StorageManager/StorageManager.h"
#include "../Pages/Page.h"
#include "../Row/Row.h"
#include "../B+Tree/BPlusTree.h"
#include <stdexcept>
#include <unordered_set>


using namespace Pages;
using namespace ByteMaps;
using namespace Indexing;
using namespace Storage;

namespace DatabaseEngine::StorageTypes {
      TableHeader::TableHeader() 
      {
        this->indexAllocationMapPageId = 0;
        this->tableId = 0;
        this->maxRowSize = 0;
        this->numberOfColumns = 0;
        this->tableNameSize = 0;
        this->clusteredIndexPageId = 0;
        this->columnsNullBitMap = nullptr;
        // this->clusteredIndexesBitMap = nullptr;
      }

      TableHeader::~TableHeader() 
      {
        delete this->columnsNullBitMap;
        // delete this->clusteredIndexesBitMap;

        // for(const auto& nonClusteredIndexMap: this->nonClusteredIndexesBitMap)
            // delete nonClusteredIndexMap;
      }

      TableHeader &TableHeader::operator=(const TableHeader &tableHeader) 
      {
        if (this == &tableHeader)
          return *this;

        this->indexAllocationMapPageId = tableHeader.indexAllocationMapPageId;
        this->maxRowSize = tableHeader.maxRowSize;
        this->numberOfColumns = tableHeader.numberOfColumns;
        this->tableNameSize = tableHeader.tableNameSize;
        this->tableName = tableHeader.tableName;
        this->tableId = tableHeader.tableId;
        this->clusteredIndexPageId = tableHeader.clusteredIndexPageId;
        this->nonClusteredIndexPageIds = tableHeader.nonClusteredIndexPageIds;
        this->nonClusteredIndexesIds = tableHeader.nonClusteredIndexesIds;

        this->columnsNullBitMap = new BitMap(*tableHeader.columnsNullBitMap);
        this->clusteredColumnIndexes = tableHeader.clusteredColumnIndexes;

          for(const auto& nonClusteredIndexes: tableHeader.nonClusteredColumnIndexes)
              this->nonClusteredColumnIndexes.push_back(nonClusteredIndexes);

        return *this;
      }

      TableFullHeader::TableFullHeader() = default;

      TableFullHeader::TableFullHeader(const TableFullHeader &tableHeader) 
      {
        this->tableHeader = tableHeader.tableHeader;
        this->columnsHeaders = tableHeader.columnsHeaders;
      }

      Table::Table(const string &tableName, const table_id_t &tableId, const vector<Column *> &columns,  DatabaseEngine::Database *database, const vector<column_index_t> *clusteredKeyIndexes, const vector<vector<column_index_t>> *nonClusteredIndexes) 
      {
        this->columns = columns;
        this->database = database;
        this->header.tableName = tableName;
        this->header.numberOfColumns = columns.size();
        this->header.columnsNullBitMap = new BitMap(this->header.numberOfColumns);
        this->header.tableId = tableId;

        this->clusteredIndexedTree = nullptr;

        this->SetTableIndexesToHeader(clusteredKeyIndexes, nonClusteredIndexes);

        uint16_t counter = 0;
        for (const auto &column : columns) 
        {
          this->header.columnsNullBitMap->Set(counter, column->GetAllowNulls());

          this->header.maxRowSize += column->GetColumnSize();
          column->SetColumnIndex(counter);

          counter++;
        }
      }

      Table::Table(const TableHeader &tableHeader, DatabaseEngine::Database *database) 
      {
        this->header = tableHeader;
        this->database = database;

        this->clusteredIndexedTree = nullptr;
      }

      Table::~Table() 
      {
        delete this->clusteredIndexedTree;

        for (int i = 0; i < this->nonClusteredIndexedTrees.size(); i++)
            delete this->nonClusteredIndexedTrees[i];

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

      void Table::InsertRows(const vector<vector<Field>> &inputData) 
      {
        uint32_t rowsInserted = 0;
        extent_id_t startingExtentIndex = 0;
        vector<extent_id_t> extents;
        for (const auto &rowData : inputData) 
        {
            this->InsertRow(rowData, extents, startingExtentIndex);

            rowsInserted++;

            if (rowsInserted % 1000 == 0)
                cout << rowsInserted << endl;
        }

        cout << "Rows affected: " << rowsInserted << endl;
      }

      void Table::InsertRow(const vector<Field> &inputData, vector<extent_id_t> &allocatedExtents, extent_id_t &startingExtentIndex) 
      {
        Row* row = this->CreateRow(inputData);

        this->InsertLargeObjectToPage(row);
        this->database->InsertRowToPage(this->header.tableId, allocatedExtents, startingExtentIndex, row);
      }

      Row* Table::CreateRow(const vector<Field>& inputData)
      {
        Row *row = new Row(*this);
        for (const auto & i : inputData) 
        {
          const column_index_t &associatedColumnIndex = i.GetColumnIndex();

          Block *block = new Block(columns[associatedColumnIndex]);

          const ColumnType columnType = columns[associatedColumnIndex]->GetColumnType();

          if (columnType > ColumnType::ColumnTypeCount)
            throw invalid_argument("Table::InsertRow: Unsupported Column Type");

          if (i.GetIsNull()) 
          {
            this->CheckAndInsertNullValues(block, row, associatedColumnIndex);
            continue;
          }

          this->setBlockDataByDataTypeArray[static_cast<int>(columnType)]( block, i);

          const auto &columnIndex = columns[associatedColumnIndex]->GetColumnIndex();
      
          row->InsertColumnData(block, columnIndex);
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

      void Table::Select(vector<Row> &selectedRows, const vector<column_index_t>& selectedColumnIndices, const vector<Field> *conditions, const size_t &count) 
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
          for(const auto& condition: *conditions)
          {
              const auto& columnIndex = condition.GetColumnIndex();
              const auto& data = condition.GetData();

              Block *block = new Block(columns[columnIndex]);

              const ColumnType columnType = columns[columnIndex]->GetColumnType();
    
              if (columnType > ColumnType::ColumnTypeCount)
                throw invalid_argument("Table::Select: Unsupported Column Type");
    
              this->setBlockDataByDataTypeArray[static_cast<int>(columnType)]( block, condition);

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

              minimumValue.InsertKey(Key(block->GetBlockData(), block->GetBlockSize(), columnType));
              maximumValue.InsertKey(Key(block->GetBlockData(), block->GetBlockSize(), columnType));
          }

        //handle more complex queries like prefer index seek over index scan
        if(useClusteredIndex)
        {
            this->SelectRowsFromClusteredIndex(&selectedRows, rowsToSelect, minimumValue, maximumValue, clusteredIndexSeek, selectedColumnIndices);
            return;
        }
        else if (useNonClusteredIndex)
        {
            this->SelectRowsFromNonClusteredIndex(&selectedRows, rowsToSelect, conditions, selectedColumnIndices);
            return;
        }
      
        this->SelectRowsFromHeap(&selectedRows, rowsToSelect, conditions);
      }

      void Table::Update(const vector<Field> &updates, const vector<Field> *conditions) const 
      {
         vector<Block *> updateBlocks;
         for (const auto &field : updates) 
         {
           const auto &associatedColumnIndex = field.GetColumnIndex();
    
           const auto &columnType = this->columns[associatedColumnIndex]->GetColumnType();

           Block *block = new Block(this->columns[associatedColumnIndex]);

           this->setBlockDataByDataTypeArray[static_cast<int>(columnType)](block, field);

           updateBlocks.push_back(block);
         }

         this->database->UpdateTableRows(this->header.tableId, updateBlocks, conditions);

         for (const auto &block : updateBlocks)
           delete block;
      }

    void Table::Delete(const vector<Field>* conditions) const
    {
        //build conditions
        this->database->DeleteTableRows(this->header.tableId, conditions);

        //update indexes as well
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
        return this->header.columnsNullBitMap->Get(columnIndex);
    }

    void Table::AddColumn(Column *column) { this->columns.push_back(column); }

    string &Table::GetTableName() { return this->header.tableName; }

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

    void Table::SelectRowsFromClusteredIndex(vector<Row> *selectedRows, const size_t &rowsToSelect, const Key& minimumValue, const Key& maximumValue, const bool indexSeek, const vector<column_index_t>& selectedColumnIndices)
    {
        vector<QueryData> results;
        const int32_t minKey = 0;
        const int32_t maxKey = 100;

        const BPlusTree* tree = this->GetClusteredIndexedTree();

        if(indexSeek)
          tree->RangeQuery(minimumValue, maximumValue, results);
        else
          tree->IndexScan(minimumValue, maximumValue, results);

        if(results.empty())
            return;

        extent_id_t pageExtentId = Database::CalculateExtentIdByPageId(results[0].pageId);
        const Page *page = StorageManager::Get().GetPage(results[0].pageId, pageExtentId, this);

        for (const auto &result : results)
        {
            // get new page else use current one
            if (result.pageId != 0 && result.pageId != page->GetPageId())
            {
                pageExtentId = Database::CalculateExtentIdByPageId(result.pageId);
                page = StorageManager::Get().GetPage(result.pageId, pageExtentId, this);
            }

            page->GetRowByIndex(selectedRows, *this, result.indexPosition, selectedColumnIndices);
        }
    }

    void Table::SelectRowsFromNonClusteredIndex(vector<Row>* selectedRows, const size_t & rowsToSelect, const vector<Field>* conditions, const vector<column_index_t>& selectedColumnIndices)
    {
        //find which index to use

        vector<vector<column_index_t>> indexes;
        this->GetNonClusteredIndexedColumnKeys(&indexes);

        vector<BPlusTreeNonClusteredData> results;
        const int32_t minKey = 90000;
        const int32_t maxKey = 90500;

        Key minimumValue;
        minimumValue.InsertKey(Key(&minKey, sizeof(minKey), ColumnType::Int));
        minimumValue.InsertKey(Key(&minKey, sizeof(minKey), ColumnType::Int));

        Key maximumValue;
        maximumValue.InsertKey(Key(&maxKey, sizeof(maxKey), ColumnType::Int));
        maximumValue.InsertKey(Key(&maxKey, sizeof(maxKey), ColumnType::Int));

        const BPlusTree* tree = this->GetNonClusteredIndexTree(0);

        tree->RangeQuery(minimumValue, maximumValue, results);

        if(results.empty())
            return;

        extent_id_t pageExtentId = Database::CalculateExtentIdByPageId(results[0].pageId);
        const Page *page = StorageManager::Get().GetPage(results[0].pageId, pageExtentId, this);

        for (const auto &result : results)
        {
            // get new page else use current one
            if (result.pageId != 0 && result.pageId != page->GetPageId())
            {
                pageExtentId = Database::CalculateExtentIdByPageId(result.pageId);
                page = StorageManager::Get().GetPage(result.pageId, pageExtentId, this);
            }

            page->GetRowByIndex(selectedRows, *this, result.index, selectedColumnIndices);
        }
    }

    void Table::SelectRowsFromHeap(vector<Row> *selectedRows, const size_t &rowsToSelect, const vector<Field> *conditions)
    {
        if(this->header.indexAllocationMapPageId == 0)
            return;

        const IndexAllocationMapPage *tableMapPage = StorageManager::Get().GetIndexAllocationMapPage(this->header.indexAllocationMapPageId);

        vector<extent_id_t> tableExtentIds;
        tableMapPage->GetAllocatedExtents(&tableExtentIds);

        vector<thread> workerThreads;
        for (const auto &extentId : tableExtentIds)
        {
            workerThreads.emplace_back([this, selectedRows, conditions, rowsToSelect, extentId, tableMapPage]
                                        { ThreadSelect(tableMapPage, extentId, rowsToSelect, conditions, selectedRows); });
        }

        for (auto &workerThread : workerThreads)
            workerThread.join();
    }

    void Table::ThreadSelect(const Pages::IndexAllocationMapPage *tableMapPage, const extent_id_t &extentId, const size_t &rowsToSelect, const vector<Field> *conditions, vector<Row> *selectedRows)
    {
        const page_id_t extentFirstPageId = Database::CalculateSystemPageOffset(extentId * EXTENT_SIZE);

        const page_id_t pfsPageId = Database::GetPfsAssociatedPage(extentFirstPageId);

        const PageFreeSpacePage *pageFreeSpacePage = StorageManager::Get().GetPageFreeSpacePage(pfsPageId);

        const page_id_t pageId = (tableMapPage->GetPageId() != extentFirstPageId)
                                    ? extentFirstPageId
                                    : extentFirstPageId + 1;

        for (page_id_t extentPageId = pageId; extentPageId < extentFirstPageId + EXTENT_SIZE; extentPageId++)
        {
            if (pageFreeSpacePage->GetPageType(extentPageId) != PageType::DATA)
                break;

            const Page *page = StorageManager::Get().GetPage(extentPageId, extentId, this);

            if (page->GetPageSize() == 0)
                continue;

            this->pageSelectMutex.lock();

            page->GetRows(selectedRows, *this, rowsToSelect, conditions);

            this->pageSelectMutex.unlock();

            if (selectedRows->size() >= rowsToSelect)
                return;
        }
    }

    unordered_set<column_index_t> Table::GetClusteredIndexesMap() const
    {
        unordered_set<column_index_t> hashSet = {};
          
        for(const auto& clusteredColumnIndex: this->header.clusteredColumnIndexes)
            hashSet.insert(clusteredColumnIndex);

        return hashSet;
    }

} // namespace DatabaseEngine::StorageTypes