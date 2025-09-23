#include "Table.h"
#include "../../AdditionalLibraries/DataTypes/Value/Value.h"
#include "../../AdditionalLibraries/BitMap/BitMap.h"
#include "../Block/Block.h"
#include "../Column/Column.h"
#include "../Constants.h"
#include "../Database.h"
#include "../../QueryPipeline/Statements/Statements.h"
#include "../../Server/MasterDbColumns.h"
#include "../Pages/LargeObject/LargeDataPage.h"
#include "../Pages/IndexMapAllocation/IndexAllocationMapPage.h"
#include "../Pages/Header/HeaderPage.h"
#include "../Pages/PageFreeSpace/PageFreeSpacePage.h"
#include "../Storage/StorageManager/StorageManager.h"
#include "../Pages/Page.h"
#include "../Row/Row.h"
#include "../B+Tree/BPlusTree.h"
#include "../Pages/IndexPage/IndexPage.h"
#include "../../Server/Server.h"

#include <iostream>
#include <stdexcept>


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

        this->clusteredIndex = tableHeader.clusteredIndex;

        for(const auto& nonClusteredIndex: tableHeader.nonClusteredIndexes)
            this->nonClusteredIndexes.push_back(nonClusteredIndex);

        return *this;
      }

      Table::Table(
        const table_id_t &tableId,
        const int& ordinalPosition,
        const vector<Column *> &columns,
        DatabaseEngine::Database *database,
        const Headers::Index* clusteredIndex,
        const vector<Headers::Index> *nonClusteredIndexes)
      {
//        this->schema = schema;
        this->columns = columns;
        this->database = database;
        this->header.numberOfColumns = columns.size();
        this->header.tableId = tableId;
        this->header.ordinalPosition = ordinalPosition;

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

      vector<DataType> Table::GetColumnTypeByTreeId(const uint8_t& treeId) const
      {
          vector<DataType> columns;

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

      AdditionalDataTypes::ResultStatus Table::InsertRows(const Constants::transaction_id_t& transactionId, const vector<vector<Value>> &inputData)
      {
        uint32_t rowsInserted = 0;
        extent_id_t startingExtentIndex = 0;
        vector<extent_id_t> extents;

        int64_t primaryKeyVal = 0;
        Logging::CheckPoint checkPoint;
        for (const auto &rowData : inputData)
        {
            auto* row = this->CreateRow(transactionId, rowData, &primaryKeyVal, &checkPoint);

            const auto result = this->InsertRow(row, extents, startingExtentIndex);

            if (result.code != AdditionalDataTypes::ResultCode::Ok)
              return result;


            rowsInserted++;

            if (rowsInserted % 1000 == 0)
                cout << rowsInserted << endl;
        }

        this->database->LogCheckPoint(checkPoint);

        AdditionalDataTypes::ResultStatus status;
        status.message = "Rows affected: " + to_string(rowsInserted);

        return status;
      }

    AdditionalDataTypes::ResultStatus Table::InsertRow(const Constants::transaction_id_t& transactionId, const vector<Value> &inputData){
        extent_id_t startingExtentIndex = 0;
        vector<extent_id_t> extents;

        int64_t primaryKeyVal = 0;
        Logging::CheckPoint checkPoint;

        Row* row = this->CreateRow(transactionId, inputData, &primaryKeyVal, &checkPoint);

        auto result =  this->InsertRow(row, extents, startingExtentIndex);

        if (result.code != AdditionalDataTypes::ResultCode::Ok)
          return result;

        this->database->LogCheckPoint(checkPoint);

        result.message = "Rows affected: 1";
        result.primaryKeyVal = primaryKeyVal;
        
        return result;
    }

    AdditionalDataTypes::ResultStatus Table::InsertRow(
      const Constants::transaction_id_t &transactionId,
      const vector<Value> &inputData,
      const std::vector<Constants::column_index_t> &columnIndices
    ){
        extent_id_t startingExtentIndex = 0;
        vector<extent_id_t> extents;

        int64_t primaryKeyVal = 0;
        Logging::CheckPoint checkPoint;

        auto* row = this->CreateRow(transactionId, inputData, columnIndices, &primaryKeyVal, &checkPoint);

        auto result =  this->InsertRow(row, extents, startingExtentIndex);

        if (result.code != AdditionalDataTypes::ResultCode::Ok)
          return result;

        this->database->LogCheckPoint(checkPoint);

        result.message = "Rows affected: 1";
        result.primaryKeyVal = primaryKeyVal;

        return result;
    }

    AdditionalDataTypes::ResultStatus Table::InsertRow(
      const Constants::transaction_id_t &transactionId,
      const vector<Expressions::Expression *> &inputData,
      const std::vector<Constants::column_index_t> &columnIndices
    ){
        extent_id_t startingExtentIndex = 0;
        vector<extent_id_t> extents;

        int64_t primaryKeyVal = 0;
        Logging::CheckPoint checkPoint;

        auto* row = this->CreateRow(transactionId, inputData, columnIndices, &primaryKeyVal, &checkPoint);

        auto result =  this->InsertRow(row, extents, startingExtentIndex);

        if (result.code != AdditionalDataTypes::ResultCode::Ok)
          return result;

        this->database->LogCheckPoint(checkPoint);

        result.message = "Rows affected: 1";
        result.primaryKeyVal = primaryKeyVal;

        return result;

    }

      AdditionalDataTypes::ResultStatus Table::InsertRow(Row* row, vector<extent_id_t> &allocatedExtents, extent_id_t &startingExtentIndex)
      {
        this->InsertLargeObjectToPage(row);

        //row_id
        Headers::RowIdentifier rowId;

        auto status = (this->GetTableType() == TableType::CLUSTERED) ? this->ClusteredIndexInsert(row, &rowId) : this->HeapInsert(allocatedExtents, startingExtentIndex, row, &rowId);

        if (status.code != AdditionalDataTypes::ResultCode::Ok)
            return status;

        //insert to Non Clustered Indexes
        if(!this->HasNonClusteredIndexes())
           return status;

        for (int i = 0; i < this->header.nonClusteredIndexes.size(); i++) {
            status = this->NonClusteredIndexInsert(row, i, rowId);

            if (status.code != AdditionalDataTypes::ResultCode::Ok)
                return status;
        }

        return status;
      }

      Row* Table::CreateRow(
        const Constants::transaction_id_t& transactionId,
        const vector<Value>& inputData,
        int64_t* primaryKeyVal,
        Logging::CheckPoint* checkPoint)const
      {
        auto *row = new Row(*this);

        *primaryKeyVal = this->PopulateAutoComputedColumns(row);

        //TODO
        //handle default values if no value is selected

        for(const auto& input : inputData){

          const auto& associatedColumnIndex = input.GetColumnIndex();

          const auto& column = this->columns.at(associatedColumnIndex);

          //ignore auto-computed columns even if specified
          if (column->GetIdentity().columnId != -1)
            continue;

          auto *block = new Block(column);

          if (column->GetColumnType() >= Constants::DataType::Invalid)
            throw invalid_argument("Table::InsertRow: Unsupported Column Type");

          if (input.GetIsNull())
          {
            Table::CheckAndInsertNullValues(block, row, associatedColumnIndex);
            continue;
          }

          block->SetData(input);

          row->InsertColumnData(block, associatedColumnIndex);
        }

        *checkPoint = this->database->LogRowInsert(row, transactionId, this->header.ordinalPosition);

        return row;
      }

      Row * Table::CreateRow(
        const Constants::transaction_id_t &transactionId,
        const std::vector<Value> &inputData,
        const std::vector<Constants::column_index_t> &columnIndices,
        int64_t *primaryKeyVal,
        Logging::CheckPoint *checkPoint) const{
        auto *row = new Row(*this);

        *primaryKeyVal = this->PopulateAutoComputedColumns(row);

        //TODO
        //handle default values if no value is selected

        for (int i = 0;i < inputData.size(); i++) {
          const auto& input = inputData[i];

          const auto& associatedColumnIndex = columnIndices.at(i);

          const auto& column = this->columns.at(associatedColumnIndex);

          //ignore auto-computed columns even if specified
          if (column->GetIdentity().columnId != -1)
            continue;

          auto *block = new Block(column);

          if (column->GetColumnType() >= Constants::DataType::Invalid)
            throw invalid_argument("Table::InsertRow: Unsupported Column Type");

          if (input.GetIsNull())
          {
            Table::CheckAndInsertNullValues(block, row, associatedColumnIndex);
            continue;
          }

          block->SetData(input);

          row->InsertColumnData(block, associatedColumnIndex);
        }

        *checkPoint = this->database->LogRowInsert(row, transactionId, this->header.ordinalPosition);

        return row;
      }

      Row * Table::CreateRow(
        const Constants::transaction_id_t &transactionId,
        const std::vector<Expressions::Expression *> &inputData,
        const std::vector<Constants::column_index_t> &columnIndices,
        int64_t *primaryKeyVal,
        Logging::CheckPoint *checkPoint
      ) const{
        auto *row = new Row(*this);

        *primaryKeyVal = this->PopulateAutoComputedColumns(row);

        //TODO
        //handle default values if no value is selected

        for (int i = 0;i < inputData.size(); i++) {
          const auto& input = inputData[i]->Evaluate(nullptr);

          const auto& associatedColumnIndex = columnIndices.at(i);

          const auto& column = this->columns.at(associatedColumnIndex);

          //ignore auto-computed columns even if specified
          if (column->GetIdentity().columnId != -1)
            continue;

          auto *block = new Block(column);

          if (column->GetColumnType() >= Constants::DataType::Invalid)
            throw invalid_argument("Table::InsertRow: Unsupported Column Type");

          if (input.GetIsNull())
          {
            Table::CheckAndInsertNullValues(block, row, associatedColumnIndex);
            continue;
          }

          block->SetData(input);

          row->InsertColumnData(block, associatedColumnIndex);
        }

        *checkPoint = this->database->LogRowInsert(row, transactionId, this->header.ordinalPosition);

        return row;
      }

      column_number_t Table::GetNumberOfColumns() const { return this->columns.size(); }

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
//         const size_t rowsToSelect =  (count == -1)
//                                     ? numeric_limits<size_t>::max()
//                                     : count;
//
//           const auto tableType = this->GetTableType();
//
//           const auto& clusteredIndexes = this->GetClusteredIndex();
//           const auto& nonClusteredIndexes = this->GetNonClusteredIndexes();
//
//           Key minimumValue;
//           Key maximumValue;
//
//           bool useClusteredIndex = false;
//           bool useNonClusteredIndex = false;
//           bool useHeap = false;
//
//           bool clusteredIndexSeek = false;
//           bool nonClusteredIndexSeek = false;
//
//           if(conditions != nullptr)
//           {
//             for(const auto& block: *conditions)
//             {
//                 const auto& columnIndex = block.GetColumnIndex();
//
//                 const ColumnType columnType = columns[columnIndex]->GetColumnType();
//
//                 if (columnType > Constants::ColumnType::ColumnTypeCount)
//                   throw invalid_argument("Table::Select: Unsupported Column Type");
//
//                 int indexPosition = 0;
//                 if(Table::VectorContainsIndex(clusteredIndexes, columnIndex, indexPosition))
//                 {
//                   useClusteredIndex = true;
//
//                   //figure out how to perform index seek and index scan
//                   clusteredIndexSeek = clusteredIndexes[0] == columnIndex;
//
//                   if(!clusteredIndexSeek)
//                   {
//                     minimumValue.indexKeyPosition = indexPosition;
//                     minimumValue.currentSearchKeyPosition = minimumValue.subKeys.size();
//
//                     maximumValue.indexKeyPosition = indexPosition;
//                     maximumValue.currentSearchKeyPosition = maximumValue.subKeys.size();
//                   }
//                 }
//
//                 int nonClusteredIndexPosition = 0;
//
//                 for(int i = 0;i < nonClusteredIndexes.size(); i++)
//                 {
//                   if(Table::VectorContainsIndex(nonClusteredIndexes[i], columnIndex, indexPosition) && !clusteredIndexSeek)
//                   {
//                       useNonClusteredIndex = true;
//                       nonClusteredIndexPosition = i;
//
//                       nonClusteredIndexSeek = nonClusteredIndexes[i][0] == columnIndex;
//
//                       //prioritize clustered index seek over nonclustered index seek or scan
//                       if(!nonClusteredIndexSeek)
//                       {
//                         minimumValue.indexKeyPosition = indexPosition;
//                         minimumValue.currentSearchKeyPosition = minimumValue.subKeys.size();
//
//                         maximumValue.indexKeyPosition = indexPosition;
//                         maximumValue.currentSearchKeyPosition = maximumValue.subKeys.size();
//                       }
//                   }
//                 }
//
//                 useHeap = !useNonClusteredIndex && !useClusteredIndex;
//
//                 minimumValue.InsertKey(Key(block.GetBlockData(), block.GetBlockSize(), columnType));
//                 maximumValue.InsertKey(Key(block.GetBlockData(), block.GetBlockSize(), columnType));
//             }
//           }
//         //handle more complex queries like prefer index seek over index scan
// //        if(useClusteredIndex)
// //        {
// //            this->SelectRowsFromClusteredIndex(
// //              &selectedRows,
// //              rowsToSelect,
// //              conditions != nullptr ? &minimumValue : nullptr,
// //              conditions != nullptr ? &maximumValue : nullptr,
// //              clusteredIndexSeek,
// //              selectedColumnIndices
// //            );
// //            return;
// //        }
// //        else if (useNonClusteredIndex)
// //        {
// //            this->SelectRowsFromNonClusteredIndex(&selectedRows, rowsToSelect, nullptr, selectedColumnIndices);
// //            return;
// //        }
// //
// //        this->HeapScan(&selectedRows, rowsToSelect);
      }

    void Table::HeapDelete(const Expressions::Expression* expression) const
    {
        if (this->header.indexAllocationMapPageId == INVALID_PAGE_ID)
          return;

        const auto& filename = this->database->GetFileName();

        const auto indexAllocationExtentId = Database::CalculateExtentIdByPageId(this->header.indexAllocationMapPageId);

        const IndexAllocationMapPage *tableMapPage =
            StorageManager::Get().GetIndexAllocationMapPage(
              filename,
              this->header.indexAllocationMapPageId,
              indexAllocationExtentId,
              this);

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

    void Table::ClusteredIndexScanDelete(
      const Expressions::Expression *expression,
      QueryPipeline::PhysicalPlan::IndexState& state,
      const int& batchSize){
        auto* tree = this->GetClusteredIndexedTree();

        vector<Row> results;
        tree->IndexScan(&results, state, batchSize);

        if(results.empty())
          return;

        for(const auto& row : results){

          const auto value = expression->Evaluate(&row);
          if(value.GetBool())
          {
            const auto& key = Database::CreateKey(this->header.clusteredIndex.columns, &row);
            tree->Remove(key);
          }
        }
   }

  void Table::ClusteredIndexSeekDelete(
    const Expressions::Expression *expression,
    QueryPipeline::PhysicalPlan::IndexState &state,
    const int &batchSize){

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

    void Table::HeapScan(vector<Row> *selectedRows, QueryPipeline::PhysicalPlan::TableScanState& state, const size_t &rowsToSelect)const
    {
        if(this->header.indexAllocationMapPageId == INVALID_PAGE_ID)
            return;

        const auto& filename = this->database->GetFileName();

        const auto indexAllocationPageExtentId = Database::CalculateExtentIdByPageId(this->header.indexAllocationMapPageId);

        const IndexAllocationMapPage *tableMapPage = StorageManager::Get().GetIndexAllocationMapPage(filename, this->header.indexAllocationMapPageId, indexAllocationPageExtentId, this);

        vector<extent_id_t> tableExtentIds;
        tableMapPage->GetAllocatedExtents(&tableExtentIds, state.extentId);

        for (const auto& extentId : tableExtentIds){
          const page_id_t extentFirstPageId = DatabaseEngine::Database::CalculateSystemPageOffset(extentId * EXTENT_SIZE);

          const auto* pageFreeSpacePage = DatabaseEngine::Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), extentFirstPageId);

          const auto extentStartingPageId = (tableMapPage->GetPageId() != extentFirstPageId)
                                      ? extentFirstPageId
                                      : extentFirstPageId + 1;

          const auto pageId = Table::GetPageIdByState(extentStartingPageId, state);

          for (page_id_t extentPageId = pageId; extentPageId < extentFirstPageId + EXTENT_SIZE; extentPageId++)
          {
            if (pageFreeSpacePage->GetPageType(extentPageId) != PageType::DATA)
              break;

            const Page *page = StorageManager::Get().GetPage(filename, extentPageId, extentId, this);

            if (page->GetPageSize() == 0)
              continue;

            //update state to know where to start
            state.extentId = extentId;
            state.lastFetchedRowId.pageId = extentPageId;
            state.lastFetchedRowId.indexId = page->GetRows(selectedRows, *this, rowsToSelect, state.lastFetchedRowId.indexId == -1 ? 0 : state.lastFetchedRowId.indexId + 1);

            if (selectedRows->size() == rowsToSelect)
              return;
          }
        }
    }

    AdditionalDataTypes::ResultStatus Table::ClusteredIndexInsert(Row *row, Headers::RowIdentifier* rowId){

       BPlusTree* tree = this->GetClusteredIndexedTree();

       const auto key = Database::CreateKey(this->GetClusteredIndex(), row);

       int indexPosition = 0;

       AdditionalDataTypes::ResultStatus status;

       auto *node = tree->FindAppropriateNodeForInsert(key, &indexPosition, status);

       if (status.code != AdditionalDataTypes::ResultCode::Ok)
           return status;

       PageFreeSpacePage *pageFreeSpacePage =  Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), node->GetPageId());

       // should never fail
       this->InsertRowToClusteredPage(pageFreeSpacePage, node, row, indexPosition);

       auto* keys = node->GetKeysUnsafe();

       keys->insert(keys->begin() + indexPosition, new Key(key));

       node->UpdateBytesLeft();

       rowId->indexId = indexPosition;
       rowId->pageId = node->GetPageId();

       // this->SplitNodeFromIndexPage(tableId, node);
       return {};
     }

    AdditionalDataTypes::ResultStatus Table::HeapInsert(vector<extent_id_t> & allocatedExtents, extent_id_t & lastExtentIndex, Row *row, Headers::RowIdentifier* rowId)const{
      const auto& filename = this->database->GetFileName();

      while(row->GetTotalRowSize() > PAGE_SIZE - PageHeader::GetPageHeaderSize())
        this->HandleRowOverflow(row);

      if (this->header.indexAllocationMapPageId == INVALID_PAGE_ID)
      {
          Page *newPage = this->database->CreateDataPage(this->header.ordinalPosition);

          newPage->InsertRow(row, &rowId->indexId);
          rowId->pageId = newPage->GetPageId();

          return {};
      }

      const auto indexPageExtentId = Database::CalculateExtentIdByPageId(this->header.indexAllocationMapPageId);

      const IndexAllocationMapPage *tableMapPage = StorageManager::Get().GetIndexAllocationMapPage(filename, this->header.indexAllocationMapPageId, indexPageExtentId, this);

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

                  page->InsertRow(row, &rowId->indexId);
                  pageFreeSpacePage->SetPageMetaData(page);

                  rowId->pageId = pageId;
                  return {};
              }
          }
      }

      Page *newPage = this->database->CreateDataPage(this->header.ordinalPosition);
      newPage->InsertRow(row, rowId->indexId);
      rowId->pageId = newPage->GetPageId();

      return {};
    }

    int Table::CreateNonClusteredIndex(vector<Constants::column_index_t> &columnIndices){
        Headers::Index index;
        index.columns = std::move(columnIndices);

        this->header.nonClusteredIndexes.push_back(std::move(index));

        return static_cast<int>(this->header.nonClusteredIndexes.size() - 1);
    }

  void Table::HeapUpdate(const Expressions::Expression *expression, const vector<Value> & updates){
        if(this->header.indexAllocationMapPageId == INVALID_PAGE_ID)
          return;

        const auto& filename = this->database->GetFileName();

        const auto indexAllocationExtentId = Database::CalculateExtentIdByPageId(this->header.indexAllocationMapPageId);

        const IndexAllocationMapPage *tableMapPage = StorageManager::Get().GetIndexAllocationMapPage(filename, this->header.indexAllocationMapPageId, indexAllocationExtentId, this);

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

            const auto* rows = page->GetDataRowsUnsafe();

            std::vector<extent_id_t> allocatedExtents;
            extent_id_t startingExtentIndex = 0;

            for(auto* row : *rows){
              const auto value = expression->Evaluate(row);
              if(!value.GetBool())
                  continue;

                this->HandleRowUpdate(page, row, updates, updatedColumns);
            }
          }
        }
    }

    void Table::HeapUpdate(const Expressions::Expression *expression, const vector<QueryPipeline::Statements::UpdateColumn *> &updates){
                if(this->header.indexAllocationMapPageId == INVALID_PAGE_ID)
          return;

        const auto& filename = this->database->GetFileName();

        const auto indexAllocationExtentId = Database::CalculateExtentIdByPageId(this->header.indexAllocationMapPageId);

        const IndexAllocationMapPage *tableMapPage = StorageManager::Get().GetIndexAllocationMapPage(filename, this->header.indexAllocationMapPageId, indexAllocationExtentId, this);

        vector<extent_id_t> tableExtentIds;
        tableMapPage->GetAllocatedExtents(&tableExtentIds, 0);

        HashSet<column_index_t> updatedColumns;
        for(const auto& update : updates)
              updatedColumns.Add(update->name.index);

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

            const auto* rows = page->GetDataRowsUnsafe();

            std::vector<extent_id_t> allocatedExtents;
            extent_id_t startingExtentIndex = 0;

            for(auto* row : *rows){
              const auto value = expression->Evaluate(row);
              if(!value.GetBool())
                  continue;

                this->HandleRowUpdate(page, row, updates, updatedColumns);
            }
          }
        }
    }

    void Table::DeleteLargeObjectFromPage(Row *row, const HashSet<column_index_t>& updatedColumns)const{
      const auto& filename = this->database->GetFileName();

      const RowHeader* rowHeader = row->GetHeader();

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

    void Table::ClusteredIndexScanUpdate(const Expressions::Expression *expression, const vector<Value> & updates){
      auto* tree = this->GetClusteredIndexedTree();

      tree->IndexScanUpdate(expression, updates);
    }

    void Table::ClusteredIndexScanUpdate(
      const Expressions::Expression *expression,
      const vector<QueryPipeline::Statements::UpdateColumn *> &updates){
        auto* tree = this->GetClusteredIndexedTree();

        if (expression == nullptr) {
          tree->IndexScanUpdate(updates);
          return;
        }

        tree->IndexScanUpdate(expression, updates);
    }

    void Table::ClusteredIndexSeekUpdate(
        Expressions::Expression* expression,
        const Indexing::Key *minimumValue,
        const Indexing::Key *maximumValue,
        const vector<Value> & updates){
      auto* tree = this->GetClusteredIndexedTree();

      tree->IndexSeekUpdate(expression, minimumValue, maximumValue, updates);
    }
    string Table::GetFileName() const{ return this->database->GetFileName(); }

    int Table::HandleRowOverflow(const Row *row)const{
      auto* largestBlock = row->FindLargestVariableLengthColumn();

      if(largestBlock == nullptr)
        return -1;

      auto* overflowPage = this->database->GetLastOverflowPage(this->header.ordinalPosition, largestBlock->GetBlockSize());

      int indexPos = 0;
      overflowPage->InsertObject(largestBlock->GetBlockData(), largestBlock->GetBlockSize(), indexPos);

      row->SetOverflowBitMapValue(largestBlock->GetColumnIndex(), true);

      auto* pfsPage = DatabaseEngine::Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), overflowPage->GetPageId());

      pfsPage->SetPageMetaData(overflowPage);

      const OverflowPointer ptr(overflowPage->GetPageId(), indexPos);
      largestBlock->SetData(&ptr, sizeof(OverflowPointer));

      return largestBlock->GetBlockSize();
    }

    void Table::DeleteOverflowedRowsFromPage(Row *row, const HashSet<column_index_t> & updatedColumns)const{
      const auto& filename = this->database->GetFileName();

      const RowHeader* rowHeader = row->GetHeader();

      for(const auto& block : row->GetData()){
        if(!updatedColumns.Contains(block->GetColumnIndex())
          || !rowHeader->overflowBitMap->Get(block->GetColumnIndex()))
            continue;

        rowHeader->overflowBitMap->Set(block->GetColumnIndex(), false);

        auto objectPointer = block->GetOverflowPointer();

        auto overflowExtentId = Database::CalculateExtentIdByPageId(objectPointer.pageId);

        auto* overflowPage = StorageManager::Get().GetOverflowPage(filename, objectPointer.pageId, overflowExtentId, this);

        const auto* overflowRow = overflowPage->DeleteObject(objectPointer.index);

        auto* pfsPage = StorageManager::Get().GetPageFreeSpacePage(filename, Database::GetPfsAssociatedPage(objectPointer.pageId));

        pfsPage->SetPageMetaData(overflowPage);

        delete overflowRow;
      }
    }

    //create differrent one to handle clustered updates
    void Table::HandleRowUpdate(
      Pages::Page *page,
      Row *row, const
      std::vector<Value> &updates,
      const HashSet<column_index_t>& updatedColumns,
      const bool &isHeap){
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

        while(page->GetBytesLeft() - diff < 0){
          const int result = this->HandleRowOverflow(row);
          if(result == -1)
            break;

          diff -= result;
        }
    }

  void Table::HandleRowUpdate(
    Pages::Page *page,
    Row *row,
    const std::vector<QueryPipeline::Statements::UpdateColumn *> &updates,
    const HashSet<column_index_t> &updatedColumns,
    const bool &isHeap){
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

        while(page->GetBytesLeft() - diff < 0){
          const int result = this->HandleRowOverflow(row);
          if(result == -1)
            break;

          diff -= result;
        }
  }

    void Table::InsertRowToPage(Pages::PageFreeSpacePage *pageFreeSpacePage, Pages::Page *page, Row *row, const int & indexPosition)const{

      while (row->GetTotalRowSize() > page->GetBytesLeft())
        this->HandleRowOverflow(row);

      page->InsertRow(row, indexPosition);
      pageFreeSpacePage->SetPageMetaData(page);
    }

    void Table::InsertRowToClusteredPage(Pages::PageFreeSpacePage *pageFreeSpacePage, Pages::Page *page, Row *row, const int & indexPosition)const{
      for(const auto& column: this->columns){
        if(!column->isColumnOverflowed())
          continue;

        this->HandleRowOverflow(row, column);
      }

      page->InsertRow(row, indexPosition);
      pageFreeSpacePage->SetPageMetaData(page);
    }

    void Table::UpdateColumnIdentity(const int32_t& columnId, const int32_t& lastValue)const{
        Server::ServerInstance::Get().UpdateIdentityByColumnId(this->header.tableId, columnId, lastValue);
    }

    void Table::InsertExistingRowsToNonClusteredIndexByClusteredIndex(const int32_t &indexPos){

        auto* clusteredTree = this->GetClusteredIndexedTree();

        clusteredTree->InsertRowsToOtherTree(indexPos);
    }

    void Table::InsertExistingRowToNonClusteredIndexByHeap(const int& indexPos){
        if(this->header.indexAllocationMapPageId == INVALID_PAGE_ID)
          return;

        const auto& filename = this->database->GetFileName();

        const auto indexAllocationExtentId = Database::CalculateExtentIdByPageId(this->header.indexAllocationMapPageId);

        const IndexAllocationMapPage *tableMapPage = StorageManager::Get().GetIndexAllocationMapPage(filename, this->header.indexAllocationMapPageId, indexAllocationExtentId, this);

        vector<extent_id_t> tableExtentIds;
        tableMapPage->GetAllocatedExtents(&tableExtentIds, 0);

        const auto& indexedColumns = this->header.nonClusteredIndexes.at(indexPos).columns;

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

            const auto* rows = page->GetDataRowsUnsafe();

            std::vector<extent_id_t> allocatedExtents;
            extent_id_t startingExtentIndex = 0;

            for (int i = 0; i < rows->size(); i++) {
              const auto* row = rows->at(i);

              Headers::RowIdentifier rowId(extentPageId, i);
              const auto key = Database::CreateKey(indexedColumns, row, rowId);

              this->NonClusteredIndexInsert(row, indexPos, rowId);
            }
          }
        }
    }

    AdditionalDataTypes::ResultStatus Table::NonClusteredIndexInsert(
      const StorageTypes::Row *row,
      const int & nonClusteredIndexId,
      const Headers::RowIdentifier & data){

      const auto& indexedColumns = this->header.nonClusteredIndexes.at(nonClusteredIndexId).columns;

      BPlusTree* tree = this->GetNonClusteredIndexTree(nonClusteredIndexId);

      const auto key = Database::CreateKey(indexedColumns, row, data);

      int indexPosition = 0;
      AdditionalDataTypes::ResultStatus status;

      auto *node = tree->FindAppropriateNodeForInsert(key, &indexPosition, status);

      if (status.code != AdditionalDataTypes::ResultCode::Ok)
          return status;

      auto* keys = node->GetKeysUnsafe();

      keys->insert(keys->begin() + indexPosition, new Key(key));

      auto* rows = node->GetNonClusteredDataUnsafe();

      rows->insert(rows->begin() + indexPosition, new Headers::RowIdentifier(data));

      node->UpdatePageSize();
      node->UpdateBytesLeft();

      PageFreeSpacePage *pageFreeSpacePage =  Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), node->GetPageId());
      pageFreeSpacePage->SetPageMetaData(node);

      return status;
    }

    AdditionalDataTypes::ResultStatus Table::NonClusteredIndexInsertExistingRows(const int &indexPos){
        const auto tableType = this->GetTableType();

        if (tableType == TableType::CLUSTERED) {
          this->InsertExistingRowsToNonClusteredIndexByClusteredIndex(indexPos);
          return {};
        }

        this->InsertExistingRowToNonClusteredIndexByHeap(indexPos);

        return {};
    }

    int Table::HandleRowOverflow(Row *row, const Column *column)const{

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

        const OverflowPointer ptr(overflowPage->GetPageId(), indexPos);
        largestBlock->SetData(&ptr, sizeof(OverflowPointer));

        row->UpdateRowSize();

        return largestBlock->GetBlockSize();
    }

    int64_t Table::PopulateAutoComputedColumns(Row *row)const{
        int64_t primaryKeyValue = 0;

        for (auto* column: this->columns) {
          const auto pkVal = this->PopulateColumnIdentity(row, column);

          if (pkVal > 0) {
            primaryKeyValue = pkVal;
            continue;
          }

          Table::PopulateDefaultValues(row, column);
        }

      return primaryKeyValue;
    }

    int64_t Table::PopulateColumnIdentity(Row *row, Column*& column) const{
        int64_t primaryKeyValue = 0;

        auto& identity = column->GetIdentity();

        if (identity.columnId == -1)
          return primaryKeyValue;

        const auto& columnSize = column->GetColumnSize();

        auto* block = new Block(&identity.lastValue, columnSize ,column);

        primaryKeyValue = identity.lastValue;

        identity.lastValue += identity.increment;

        row->InsertColumnData(block, column->GetColumnIndex());

        if (column->GetIdentityStartingValue() + identity.cacheBlock < primaryKeyValue )
          this->UpdateColumnIdentity(column->GetColumnId(), primaryKeyValue);

        return primaryKeyValue;
      }

      void Table::PopulateDefaultValues(Row *row, Column*& column) {
        const auto& defaultValue = column->GetDefaultValue();

        if (defaultValue.columnId == -1)
          return;

        auto* block = new Block(defaultValue.value.data(), defaultValue.value.size(), column);

        row->InsertColumnData(block, column->GetColumnIndex());
      }

    void Table::GetColumnsHeaders()const{
      const auto columnsHeaders = Server::ServerInstance::Get().SelectColumns(this->header.tableId);

      if (columnsHeaders.empty())
        return;

      for (int i = 0;i < this->columns.size(); i++) {
        auto& column = columns[i];
        column->SetColumnId(columnsHeaders[i].id);
      }
    }

    void Table::GetIdentityColumns()const{
      const auto identityHeaders = Server::ServerInstance::Get().SelectIdentityColumnsByTableId(this->header.tableId);

      if(identityHeaders.empty())
        return;

      for(const auto& column: this->columns){
        for (const auto& identity: identityHeaders) {

          if(column->GetColumnId() != identity.columnId)
            continue;

          column->SetIdentity(identity);
          column->SetIdentityStartingValue(identity.lastValue);

          // this->header.clusteredIndex.columns.emplace_back(column->GetColumnIndex());
          break;
        }
      }
    }

    void Table::GetIdentityColumnById(const int32_t &columnId)const{
        const auto identityHeaders = Server::ServerInstance::Get().SelectIdentityColumnsByTableId(this->header.tableId);

        if (identityHeaders.empty())
          return;

        for(const auto& column: this->columns){

          if (columnId != column->GetColumnId())
            continue;

          for (const auto& identity: identityHeaders) {
            if(column->GetColumnId() != identity.columnId)
              continue;

            column->SetIdentity(identity);
            column->SetIdentityStartingValue(identity.lastValue);

            // this->header.clusteredIndex.columns.emplace_back(column->GetColumnIndex());
            break;
          }
        }
    }

    void Table::GetDefaultValuesHeaders() const{
        for(const auto& column: this->columns) {
          const auto header = Server::ServerInstance::Get().SelectDefaultValueByColumnId(column->GetColumnId());

          if (header.columnId == -1)
            continue;

          column->SetDefaultValue(header);
        }
    }

    void Table::GetIndexes(){
        Dictionary<int32_t, Column*> columnsDict;

        for (auto& column: this->columns)
          columnsDict.Add(column->GetColumnId(), column);

        const auto indexes = Server::ServerInstance::Get().SelectIndexes(this->header.tableId);

        for (const auto& index: indexes) {
          const auto indexedColumns = Server::ServerInstance::Get().SelectIndexColumnsByIndexId(index.id);

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
      for (const auto& column: this->columns) {
        const auto& identity = column->GetIdentity();

        if (identity.columnId == -1)
          continue;

        this->UpdateColumnIdentity(column->GetColumnId(), identity.lastValue);
      }
    }

  void Table::UpdateColumnName(const Constants::column_index_t &index, const std::string &name)const{
      auto* column = this->columns.at(index);

      column->SetColumnName(name);
  }

  void Table::PopulateColumn(const Constants::column_index_t &index, const Value &defaultValue){
      if (this->header.indexAllocationMapPageId == INVALID_PAGE_ID)
        return;

      if (this->GetTableType() == TableType::CLUSTERED) {
        this->PopulateColumnByClusteredIndex(index, defaultValue);
        return;
      }

      this->PopulateColumnByHeap(index, defaultValue);
  }

  void Table::PopulateColumnByClusteredIndex(const Constants::column_index_t &index, const Value &defaultValue){
        auto* tree = this->GetClusteredIndexedTree();

        tree->InsertColumnToRow(index, defaultValue);
  }
//TODO add heap insert if row still cant remain in page if heap
  void Table::HandleAddColumn(Pages::Page* page, Row *row, const Constants::column_index_t& index, const Value &defaultValue){
        const auto& column = this->columns.at(index);

        auto* block = new Block(defaultValue.GetRawData(), defaultValue.GetSize(), column);

        int diff = row->InsertNewColumn(block);

        if(page->GetBytesLeft() - diff > 0){
          page->UpdateBytesLeft();
          return;
        }

        this->InsertLargeObjectToPage(row);

        // if(isHeap && (PAGE_SIZE - PageHeader::GetPageHeaderSize() - row->GetRowSize()) > 0){
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

  void Table::HandleRemoveColumn(Pages::Page *page, Row *row, const Constants::column_index_t &index){
        auto& data = row->GetData();

        data.erase(data.begin() + index);

        page->UpdateBytesLeft();
  }

  void Table::PopulateColumnByHeap(const Constants::column_index_t &index, const Value &defaultValue){
    const auto& filename = this->GetFileName();

    const auto tableMapExtentId = Database::CalculateExtentIdByPageId(this->header.indexAllocationMapPageId);

    const auto* tableMapPage = StorageManager::Get().GetIndexAllocationMapPage(filename, this->header.indexAllocationMapPageId, tableMapExtentId, this);

    std::vector<extent_id_t> allocatedExtents;
    tableMapPage->GetAllocatedExtents(&allocatedExtents, 0);

    for (const auto& extentId: allocatedExtents) {
      const page_id_t extentFirstPageId = Database::CalculateSystemPageOffsetByExtentId(extentId);

      const page_id_t firstDataPageId = (tableMapPage->GetPageId() != extentFirstPageId)
                                            ? extentFirstPageId
                                            : extentFirstPageId + 1;

      for (page_id_t pageId = firstDataPageId; pageId < extentFirstPageId + EXTENT_SIZE; pageId++)
      {
          auto* pageFreeSpacePage = Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), pageId);

          if (pageFreeSpacePage->GetPageType(pageId) != PageType::DATA)
            break;

          auto* page = StorageManager::Get().GetPage(filename, pageId, extentId, this);

          for (auto* row: *page->GetDataRowsUnsafe())
            this->HandleAddColumn(page, row, index, defaultValue);

          pageFreeSpacePage->SetPageMetaData(page);
        }
    }
  }

  void Table::RemoveColumn(const Constants::column_index_t &index){

    //add also last updated at deleted at etc...
    const auto* removedColumn = this->columns.at(index);

    //schema adjustments in master db change this as well
    const std::vector<Value> removedColumnUpdates = {
      Value(true, static_cast<column_index_t>(Server::SysColumns::IsDeleted)),
      Value(DataTypes::DateTime::Now(), static_cast<column_index_t>(Server::SysColumns::LastModifiedAt)),
      Value(DataTypes::DateTime::Now(), static_cast<column_index_t>(Server::SysColumns::DeletedAt)),
    };

    Server::ServerInstance::Get().UpdateColumnById(removedColumn->GetColumnId(), removedColumnUpdates);

    this->HandleRemoveColumn(removedColumn->GetColumnIndex());
    this->columns.erase(this->columns.begin() + index);

    for (int i = index; i < this->columns.size(); i++) {
      const auto& column = this->columns[i];

      column->SetColumnIndex(i);

      const vector<Value> updates = {
        Value(i, static_cast<column_index_t>(Server::SysColumns::OrdinalPosition))
      };

      //adjust in master db
      Server::ServerInstance::Get().UpdateColumnById(column->GetColumnId(), updates);
    }

    //adjust rows by heap or clustered
    delete removedColumn;
  }

  void Table::HandleRemoveColumn(const Constants::column_index_t &index){
    if (this->header.indexAllocationMapPageId == INVALID_PAGE_ID)
      return;

    if (this->GetTableType() == TableType::CLUSTERED) {
      this->RemoveColumnByClusteredIndex(index);
      return;
    }

    this->RemoveColumnByHeap(index);
  }

  void Table::RemoveColumnByClusteredIndex(const column_index_t &index){

    auto* tree = this->GetClusteredIndexedTree();

    tree->RemoveColumnFromRow(index);
  }

  void Table::RemoveColumnByHeap(const column_index_t &index)const{
    const auto& filename = this->GetFileName();

    const auto tableMapExtentId = Database::CalculateExtentIdByPageId(this->header.indexAllocationMapPageId);

    const auto* tableMapPage = StorageManager::Get().GetIndexAllocationMapPage(filename, this->header.indexAllocationMapPageId, tableMapExtentId, this);

    std::vector<extent_id_t> allocatedExtents;
    tableMapPage->GetAllocatedExtents(&allocatedExtents, 0);

    for (const auto& extentId: allocatedExtents) {
      const page_id_t extentFirstPageId = Database::CalculateSystemPageOffsetByExtentId(extentId);

      const page_id_t firstDataPageId = (tableMapPage->GetPageId() != extentFirstPageId)
                                            ? extentFirstPageId
                                            : extentFirstPageId + 1;

      for (page_id_t pageId = firstDataPageId; pageId < extentFirstPageId + EXTENT_SIZE; pageId++)
      {
        auto* pageFreeSpacePage = Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), pageId);

        if (pageFreeSpacePage->GetPageType(pageId) != PageType::DATA)
          break;

        auto* page = StorageManager::Get().GetPage(filename, pageId, extentId, this);

        for (auto* row: *page->GetDataRowsUnsafe())
          Table::HandleRemoveColumn(page, row, index);

        pageFreeSpacePage->SetPageMetaData(page);
      }
    }
  }

  page_id_t Table::GetPageIdByState(const page_id_t &extentFirstPageId, const QueryPipeline::PhysicalPlan::TableScanState &state){
        return state.lastFetchedRowId.pageId == INVALID_PAGE_ID ? extentFirstPageId : state.lastFetchedRowId.pageId;
  }
}

 // namespace DatabaseEngine::StorageTypes