#include "Table.h"
#include "../../AdditionalLibraries/AdditionalObjects/DateTime/DateTime.h"
#include "../../AdditionalLibraries/AdditionalObjects/Decimal/Decimal.h"
#include "../../AdditionalLibraries/AdditionalObjects/Field/Field.h"
#include "../../AdditionalLibraries/AdditionalObjects/RowCondition/RowCondition.h"
#include "../../AdditionalLibraries/BitMap/BitMap.h"
#include "../../AdditionalLibraries/SafeConverter/SafeConverter.h"
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


using namespace Pages;
using namespace DataTypes;
using namespace ByteMaps;
using namespace Indexing;
using namespace Storage;

namespace DatabaseEngine::StorageTypes {
TableHeader::TableHeader() {
  this->indexAllocationMapPageId = 0;
  this->tableId = 0;
  this->maxRowSize = 0;
  this->numberOfColumns = 0;
  this->tableNameSize = 0;
  this->clusteredIndexPageId = 0;
  this->columnsNullBitMap = nullptr;
}

TableHeader::~TableHeader() {
  delete this->columnsNullBitMap;
  delete this->clusteredIndexesBitMap;
  delete this->nonClusteredIndexesBitMap;
}

TableHeader &TableHeader::operator=(const TableHeader &tableHeader) {
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
  this->clusteredIndexesBitMap =
      new BitMap(*tableHeader.clusteredIndexesBitMap);
  this->nonClusteredIndexesBitMap =
      new BitMap(*tableHeader.nonClusteredIndexesBitMap);

  return *this;
}

TableFullHeader::TableFullHeader() = default;

TableFullHeader::TableFullHeader(const TableFullHeader &tableHeader) {
  this->tableHeader = tableHeader.tableHeader;
  this->columnsHeaders = tableHeader.columnsHeaders;
}

Table::Table(const string &tableName, const table_id_t &tableId,
             const vector<Column *> &columns,
             DatabaseEngine::Database *database,
             const vector<column_index_t> *clusteredKeyIndexes,
             const vector<column_index_t> *nonClusteredIndexes) {
  this->columns = columns;
  this->database = database;
  this->header.tableName = tableName;
  this->header.numberOfColumns = columns.size();
  this->header.columnsNullBitMap = new BitMap(this->header.numberOfColumns);
  this->header.clusteredIndexesBitMap = new BitMap(this->header.numberOfColumns);
  this->header.nonClusteredIndexesBitMap = new BitMap(this->header.numberOfColumns);
  this->header.tableId = tableId;

  this->clusteredIndexedTree = nullptr;

  this->SetTableIndexesToHeader(clusteredKeyIndexes, nonClusteredIndexes);


  uint16_t counter = 0;
  for (const auto &column : columns) {
    this->header.columnsNullBitMap->Set(counter, column->GetAllowNulls());

    this->header.maxRowSize += column->GetColumnSize();
    column->SetColumnIndex(counter);

    counter++;
  }
}

Table::Table(const TableHeader &tableHeader,
             DatabaseEngine::Database *database) {
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

void Table::SetTableIndexesToHeader(const vector<column_index_t> *clusteredKeyIndexes, const vector<column_index_t> *nonClusteredIndexes) 
{
  if (clusteredKeyIndexes != nullptr && !clusteredKeyIndexes->empty())
  {
      for (const auto &clusteredIndex : *clusteredKeyIndexes)
        this->header.clusteredIndexesBitMap->Set(clusteredIndex, true);
      
      this->clusteredIndexedTree = new BPlusTree(this);
  }


  if (nonClusteredIndexes != nullptr && !nonClusteredIndexes->empty())
    for (const auto &nonClusteredIndex : *nonClusteredIndexes)
      this->header.nonClusteredIndexesBitMap->Set(nonClusteredIndex, true);
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

  this->InsertLargeObjectToPage(row);
  this->database->InsertRowToPage(*this, allocatedExtents, startingExtentIndex, row);
}

void Table::InsertLargeObjectToPage(Row *row) 
{
  const vector<column_index_t> largeBlockIndexes = row->GetLargeBlocks();

  if (largeBlockIndexes.empty())
    return;

  RowHeader *rowHeader = row->GetHeader();

  const auto &rowData = row->GetData();

  for (const auto &largeBlockIndex : largeBlockIndexes) 
  {
    rowHeader->largeObjectBitMap->Set(largeBlockIndex, true);

    page_offset_t offset = 0;

    block_size_t remainingBlockSize = rowData[largeBlockIndex]->GetBlockSize();

    RecursiveInsertToLargePage(row, offset, largeBlockIndex, remainingBlockSize,
                               true, nullptr);
  }
}

void Table::RecursiveInsertToLargePage(Row *&row, page_offset_t &offset,
                                       const column_index_t &columnIndex,
                                       block_size_t &remainingBlockSize,
                                       const bool &isFirstRecursion,
                                       DataObject **previousDataObject) {
  LargeDataPage *largeDataPage = this->GetOrCreateLargeDataPage();

  const auto &pageSize = largeDataPage->GetBytesLeft();

  const auto &data = row->GetData()[columnIndex]->GetBlockData();

  large_page_index_t objectIndex;

  if (remainingBlockSize + OBJECT_METADATA_SIZE_T < pageSize) 
  {

    largeDataPage->InsertObject(data + offset, remainingBlockSize,
                                &objectIndex);

    this->database->SetPageMetaDataToPfs(largeDataPage);

    Table::InsertLargeDataObjectPointerToRow(row, isFirstRecursion, objectIndex,
                                             largeDataPage->GetPageId(),
                                             columnIndex);

    if (previousDataObject != nullptr) 
    {
      (*previousDataObject)->nextPageId = largeDataPage->GetPageId();
      (*previousDataObject)->nextObjectIndex = objectIndex;
    }

    return;
  }

  // blockSize < pageSize
  const auto bytesToBeInserted = pageSize - OBJECT_METADATA_SIZE_T;

  remainingBlockSize -= bytesToBeInserted;

  DataObject *dataObject = largeDataPage->InsertObject(
      data + offset, bytesToBeInserted, &objectIndex);

  this->database->SetPageMetaDataToPfs(largeDataPage);

  if (previousDataObject != nullptr) 
  {
    (*previousDataObject)->nextPageId = largeDataPage->GetPageId();
    (*previousDataObject)->nextObjectIndex = objectIndex;
  }

  offset += bytesToBeInserted;

  this->RecursiveInsertToLargePage(row, offset, columnIndex, remainingBlockSize,
                                   false, &dataObject);

  Table::InsertLargeDataObjectPointerToRow(row, isFirstRecursion, objectIndex,
                                           largeDataPage->GetPageId(),
                                           columnIndex);
}

LargeDataPage *Table::GetOrCreateLargeDataPage() const 
{
  LargeDataPage *largeDataPage = this->database->GetTableLastLargeDataPage(this->header.tableId, OBJECT_METADATA_SIZE_T + 1);

  return (largeDataPage == nullptr)
             ? this->database->CreateLargeDataPage(this->header.tableId)
             : largeDataPage;
}

void Table::LinkLargePageDataObjectChunks(DataObject *dataObject, const page_id_t &lastLargePageId, const large_page_index_t &objectIndex) 
{
  if (dataObject != nullptr) 
  {
    dataObject->nextPageId = lastLargePageId;
    dataObject->nextObjectIndex = objectIndex;
  }
}

void Table::InsertLargeDataObjectPointerToRow(Row *row, const bool &isFirstRecursion, const large_page_index_t &objectIndex, const page_id_t &lastLargePageId, const column_index_t &largeBlockIndex) const 
{
  if (!isFirstRecursion)
    return;

  const DataObjectPointer objectPointer(objectIndex, lastLargePageId);

  Block *block = new Block(&objectPointer, sizeof(DataObjectPointer),
                           this->columns[largeBlockIndex]);

  row->UpdateColumnData(block);
}

column_number_t Table::GetNumberOfColumns() const 
{
  return this->columns.size();
}

const TableHeader &Table::GetTableHeader() const { return this->header; }

const vector<Column *> &Table::GetColumns() const { return this->columns; }

LargeDataPage *Table::GetLargeDataPage(const page_id_t &pageId) const 
{
  return this->database->GetLargeDataPage(pageId, this->header.tableId);
}

void Table::Select(vector<Row> &selectedRows, const vector<RowCondition *> *conditions, const size_t &count) const 
{
  const size_t rowsToSelect =  (count == -1) 
                            ? numeric_limits<size_t>::max() 
                            : count;

  this->database->SelectTableRows(this->header.tableId, &selectedRows,
                                  rowsToSelect, conditions);
}

void Table::Update(const vector<Field> &updates, const vector<RowCondition *> *conditions) const 
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

  this->database->UpdateTableRows(this->header.tableId, updateBlocks,
                                  conditions);

  for (const auto &block : updateBlocks)
    delete block;
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

row_size_t Table::GetMaximumRowSize() const {
  row_size_t maximumRowSize = 0;
  
  for (const auto &column : this->columns)
    maximumRowSize += (column->isColumnLOB()) ? sizeof(DataObjectPointer)
                                              : column->GetColumnSize();

  return maximumRowSize;
}

void Table::GetIndexedColumnKeys(vector<column_index_t> *vector) const {
  for (bit_map_pos_t i = 0; i < this->header.clusteredIndexesBitMap->GetSize();
       i++)
    if (this->header.clusteredIndexesBitMap->Get(i))
      vector->push_back(i);

}
void Table::SetIndexPageId(const page_id_t &indexPageId) {
  this->header.clusteredIndexPageId = indexPageId;
}

void Table::SetTinyIntData(Block *&block, const Field &inputData) {
  const int8_t convertedTinyInt =
      SafeConverter<int8_t>::SafeStoi(inputData.GetData());
  block->SetData(&convertedTinyInt, sizeof(int8_t));
}

void Table::SetSmallIntData(Block *&block, const Field &inputData) {
  const int16_t convertedSmallInt =
      SafeConverter<int16_t>::SafeStoi(inputData.GetData());
  block->SetData(&convertedSmallInt, sizeof(int16_t));
}

void Table::SetIntData(Block *&block, const Field &inputData) {
  const int32_t convertedInt =
      SafeConverter<int32_t>::SafeStoi(inputData.GetData());
  block->SetData(&convertedInt, sizeof(int32_t));
}

void Table::SetBigIntData(Block *&block, const Field &inputData) {
  const int64_t convertedBigInt =
      SafeConverter<int64_t>::SafeStoi(inputData.GetData());
  block->SetData(&convertedBigInt, sizeof(int64_t));
}

void Table::SetStringData(Block *&block, const Field &inputData) {
  const string &data = inputData.GetData();
  block->SetData(data.c_str(), data.size());
}

void Table::SetBoolData(Block *&block, const Field &inputData) {
  const string &data = inputData.GetData();

  bool value = false;

  if (data == "1")
    value = true;
  else if (data == "0")
    value = false;
  else
    throw invalid_argument("InsertRow: Invalid Boolean Value specified!");

  block->SetData(&value, sizeof(bool));
}

void Table::SetDateTimeData(Block *&block, const Field &inputData) 
{
  const string &data = inputData.GetData();

  const time_t unixMilliseconds = DateTime::ToUnixTimeStamp(data);

  block->SetData(&unixMilliseconds, sizeof(time_t));
}

void Table::SetDecimalData(Block *&block, const Field &inputData) 
{
  const string &data = inputData.GetData();

  const Decimal decimalValue(data);

  block->SetData(decimalValue.GetRawData(), decimalValue.GetRawDataSize());
}

void Table::CheckAndInsertNullValues(Block *&block, Row *&row, const column_index_t &associatedColumnIndex) 
{
  if (!columns[associatedColumnIndex]->GetAllowNulls())
    throw invalid_argument("Column " + columns[associatedColumnIndex]->GetColumnName() + " does not allow NULLs. Insert Fails.");

  block->SetData(nullptr, 0);

  row->SetNullBitMapValue(associatedColumnIndex, true);

  const auto &columnIndex = columns[associatedColumnIndex]->GetColumnIndex();
  row->InsertColumnData(block, columnIndex);
}

BPlusTree* Table::GetClusteredIndexedTree() 
{
  if(this->clusteredIndexedTree == nullptr)
  {
      this->clusteredIndexedTree = new BPlusTree(this);

      this->GetClusteredIndexFromDisk();
  }

  return this->clusteredIndexedTree; 
}

void Table::WriteIndexesToDisk()
{
    IndexPage* indexPage = nullptr;

    if (this->header.clusteredIndexPageId == 0)
    {
        indexPage = this->database->CreateIndexPage(this->header.tableId);
        this->header.clusteredIndexPageId = indexPage->GetPageId();
    }
    else
        indexPage = StorageManager::Get().GetIndexPage(this->header.clusteredIndexPageId);

    Node* root = this->clusteredIndexedTree->GetRoot();

    page_offset_t offSet = 0;

    this->WriteNodeToPage(root, indexPage, offSet);
}

void Table::WriteNodeToPage(Node* node, IndexPage*& indexPage, page_offset_t &offSet)
{
    const page_size_t nodeSize = node->GetNodeSize();

    const page_id_t indexPageId = indexPage->GetPageId();

    const page_id_t freeSpacePageId = Database::GetPfsAssociatedPage(indexPageId);

    PageFreeSpacePage* pageFreeSpacePage = StorageManager::Get().GetPageFreeSpacePage(freeSpacePageId);

    if(indexPage->GetBytesLeft() < nodeSize)
    {
        IndexAllocationMapPage* indexAllocationMapPage = StorageManager::Get().GetIndexAllocationMapPage(this->header.indexAllocationMapPageId);

        vector<extent_id_t> allocatedExtents;
        indexAllocationMapPage->GetAllocatedExtents(&allocatedExtents);

        bool newPageFound = false;
        for(const auto& extentId: allocatedExtents)
        {
            const page_id_t firstExtentPageId = Database::CalculateSystemPageOffsetByExtentId(extentId);

            for(page_id_t nextIndexPageId = firstExtentPageId; nextIndexPageId < firstExtentPageId + EXTENT_SIZE; nextIndexPageId++)
            {
                if(pageFreeSpacePage->GetPageType(nextIndexPageId) != PageType::INDEX)
                    break;

                //page is free
                if(pageFreeSpacePage->GetPageSizeCategory(nextIndexPageId) == 0)
                    continue;

                IndexPage* nextIndexPage = StorageManager::Get().GetIndexPage(nextIndexPageId);

                if(nextIndexPage->GetBytesLeft() < nodeSize)
                  continue;

                indexPage->SetNextPageId(nextIndexPageId);
                indexPage = nextIndexPage;

                newPageFound = true;

                break;
            }
        }

        if(!newPageFound)
        {
            IndexPage* prevIndexPage = indexPage;
            indexPage = this->database->CreateIndexPage(this->header.tableId);

            prevIndexPage->SetNextPageId(indexPage->GetPageId());
        }

        offSet = 0;
    }

    indexPage->WriteTreeDataToPage(node, offSet);
    pageFreeSpacePage->SetPageMetaData(indexPage);

    for (auto &child : node->children)
        this->WriteNodeToPage(child, indexPage, offSet);
}

void Table::GetClusteredIndexFromDisk()
{
    IndexPage* indexPage = nullptr;

    Node* root = this->clusteredIndexedTree->GetRoot();

    indexPage = StorageManager::Get().GetIndexPage(this->header.clusteredIndexPageId);

    int currentNodeIndex = 0;
    page_offset_t offSet = 0;

    Node* prevLeafNode = nullptr;

    root = this->GetNodeFromDisk(indexPage, currentNodeIndex, offSet, prevLeafNode);

    this->clusteredIndexedTree->SetRoot(root);
}

Node* Table::GetNodeFromDisk(IndexPage*& indexPage, int& currentNodeIndex, page_offset_t& offSet, Node*& prevLeafNode)
{
    //next page must be loaded
    if(currentNodeIndex == indexPage->GetPageSize() && indexPage->GetNextPageId() != 0)
    {
        indexPage = StorageManager::Get().GetIndexPage(indexPage->GetNextPageId());

        currentNodeIndex = 0;
        offSet = 0;
    }

    const auto& treeData = indexPage->GetTreeData();

    bool isLeaf;
    memcpy(&isLeaf, treeData + offSet, sizeof(bool));
    offSet += sizeof(bool);

    Node *node = new Node(isLeaf);

    uint16_t numOfKeys;
    memcpy(&numOfKeys, treeData + offSet, sizeof(uint16_t));
    offSet += sizeof(uint16_t);

    for (int i = 0; i < numOfKeys; i++)
    {
        key_size_t keySize;
        memcpy(&keySize, treeData + offSet, sizeof(key_size_t));
        offSet += sizeof(key_size_t);

        vector<object_t> keyValue(keySize);
        memcpy(keyValue.data(), treeData + offSet, keySize);
        offSet += keySize;

        node->keys.emplace_back(keyValue.data(), keySize, KeyType::Int);
    }

    currentNodeIndex++;

    if (node->isLeaf)
    {
        memcpy(&node->data, treeData + offSet, sizeof(BPlusTreeData));
        offSet += sizeof(BPlusTreeData);

        if(prevLeafNode != nullptr)
            prevLeafNode->next = node;

        prevLeafNode = node;
    }
    else
    {
      uint16_t numberOfChildren;
      memcpy(&numberOfChildren, treeData + offSet, sizeof(uint16_t));
      offSet += sizeof(uint16_t);

      node->children.resize(numberOfChildren);

      
      for (int i = 0; i < numberOfChildren; i++)
          node->children[i] = this->GetNodeFromDisk(indexPage, currentNodeIndex, offSet, prevLeafNode);
    }

    
    //figure out how to connect leaves betwwen them
    return node;
}

} // namespace DatabaseEngine::StorageTypes