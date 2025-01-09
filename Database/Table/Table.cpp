#include "Table.h"
#include "Table.h"
#include "../../AdditionalLibraries/AdditionalObjects/Field/Field.h"
#include "../../AdditionalLibraries/AdditionalObjects/RowCondition/RowCondition.h"
#include "../../AdditionalLibraries/BitMap/BitMap.h"
#include "../../AdditionalLibraries/B+Tree/BPlusTree.h"
#include "../Block/Block.h"
#include "../Column/Column.h"
#include "../Constants.h"
#include "../Database.h"
#include "../Pages/LargeObject/LargeDataPage.h"
#include "../Pages/IndexPage/IndexPage.h"
#include "../Pages/IndexMapAllocation/IndexAllocationMapPage.h"
#include "../Pages/Header/HeaderPage.h"
#include "../Pages/PageFreeSpace/PageFreeSpacePage.h"
#include "../Storage/StorageManager/StorageManager.h"
#include "../Pages/Page.h"
#include "../Row/Row.h"
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
      }

      TableHeader::~TableHeader() 
      {
        delete this->columnsNullBitMap;
        delete this->clusteredIndexesBitMap;

        for(const auto& nonClusteredIndexMap: this->nonClusteredIndexesBitMap)
            delete nonClusteredIndexMap;
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

        this->columnsNullBitMap = new BitMap(*tableHeader.columnsNullBitMap);
        this->clusteredIndexesBitMap = new BitMap(*tableHeader.clusteredIndexesBitMap);

        for(const auto& nonClusteredIndexBitMap: tableHeader.nonClusteredIndexesBitMap)
            this->nonClusteredIndexesBitMap.push_back(new BitMap(*nonClusteredIndexBitMap));

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
        this->header.clusteredIndexesBitMap = new BitMap(this->header.numberOfColumns);
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
        if(this->clusteredIndexedTree != nullptr && this->clusteredIndexedTree->IsTreeDirty())
          this->WriteIndexesToDisk();

        delete this->clusteredIndexedTree;

        HeaderPage* headerPage = StorageManager::Get().GetHeaderPage(this->database->GetFileName());

        headerPage->SetTableHeader(this);

        for (const auto &column : columns)
          delete column;
      }

      void Table::SetTableIndexesToHeader(const vector<column_index_t> *clusteredKeyIndexes, const vector<vector<column_index_t>> *nonClusteredIndexes) 
      {
        if (clusteredKeyIndexes != nullptr && !clusteredKeyIndexes->empty())
        {
            for (const auto &clusteredIndex : *clusteredKeyIndexes)
              this->header.clusteredIndexesBitMap->Set(clusteredIndex, true);
        
            this->clusteredIndexedTree = new BPlusTree(this);
        }

        if (nonClusteredIndexes != nullptr && !nonClusteredIndexes->empty())
        {
            for (int i = 0; i < nonClusteredIndexes->size(); i++)
            {
                 this->header.nonClusteredIndexesBitMap.push_back(new BitMap(this->header.numberOfColumns));

                for (const auto &nonClusteredIndex : (*nonClusteredIndexes)[i])
                    this->header.nonClusteredIndexesBitMap[i]->Set(nonClusteredIndex, true);
            }
        }
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
        for (size_t i = 0; i < inputData.size(); ++i) 
        {
          const column_index_t &associatedColumnIndex = inputData[i].GetColumnIndex();

          Block *block = new Block(columns[associatedColumnIndex]);

          const ColumnType columnType = columns[associatedColumnIndex]->GetColumnType();

          if (columnType > ColumnType::ColumnTypeCount)
            throw invalid_argument("Table::InsertRow: Unsupported Column Type");

          if (inputData[i].GetIsNull()) 
          {
            this->CheckAndInsertNullValues(block, row, associatedColumnIndex);
            continue;
          }

          this->setBlockDataByDataTypeArray[static_cast<int>(columnType)]( block, inputData[i]);

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

      void Table::Select(vector<Row> &selectedRows, const vector<Field> *conditions, const size_t &count) 
      {
        const size_t rowsToSelect =  (count == -1) 
                                  ? numeric_limits<size_t>::max() 
                                  : count;

        const auto tableType = this->GetTableType();

        bool conditionsHaveClusteredIndex = false;

        if(conditions != nullptr)
        {
          const auto clusteredColumnsHashSet = this->GetClusteredIndexesMap();

          for(int i = 0;i < conditions->size(); i++)
          {
            if((*conditions)[i].GetConditionType() == ConditionType::ConditionNone)
              throw invalid_argument("Table::Select: Invalid Condition specified");

            if(i > 0 && ((*conditions)[i].GetOperatorType() == Constants::OperatorNone))
              throw invalid_argument("Table::Select: Invalid Operator specified");

            if(clusteredColumnsHashSet.find((*conditions)[i].GetColumnIndex()) != clusteredColumnsHashSet.end())
              conditionsHaveClusteredIndex = true;
          }
        }

        if (this->GetTableType() == TableType::HEAP)
        {
            this->SelectRowsFromHeap(&selectedRows, rowsToSelect, conditions);
            return;
        }

        //check if tables has non clustered index and it is in the query

        this->SelectRowsFromClusteredIndex(&selectedRows, rowsToSelect, conditions);
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
        return this->header.clusteredIndexesBitMap->HasAtLeastOneEntry()
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

    void Table::SelectRowsFromClusteredIndex(vector<Row> *selectedRows, const size_t &rowsToSelect, const vector<Field> *conditions)
    {
        vector<QueryData> results;
        const int32_t minKey = 10;
        const int32_t maxKey = 252;

        if(this->clusteredIndexedTree == nullptr)
        {
            this->clusteredIndexedTree = new BPlusTree(this);

            this->GetClusteredIndexFromDisk();
        }

        this->clusteredIndexedTree->RangeQuery(Key(&minKey, sizeof(minKey), KeyType::Int), Key(&maxKey, sizeof(maxKey), KeyType::Int), results);

        const Page *page = (!results.empty())
                            ? StorageManager::Get().GetPage(results[0].treeData.pageId, results[0].treeData.extentId, this)
                            : nullptr;

        for (const auto &result : results)
        {
            // get new page else use current one
            if (result.treeData.pageId != 0 && result.treeData.pageId != page->GetPageId())
                page = StorageManager::Get().GetPage(result.treeData.pageId, result.treeData.extentId, this);

            selectedRows->push_back(page->GetRowByIndex(*this, result.indexPosition));
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
        for(const auto& column: this->columns)
        {
            const column_index_t& columnIndex = column->GetColumnIndex();
            if(this->header.clusteredIndexesBitMap->Get(columnIndex))
            hashSet.insert(columnIndex);
        }

        return hashSet;
    }

} // namespace DatabaseEngine::StorageTypes