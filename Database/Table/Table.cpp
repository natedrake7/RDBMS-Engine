﻿#include "Table.h"

#include "../../AdditionalLibraries/AdditionalObjects/Field/Field.h"
#include "../Pages/Page.h"

TableHeader::TableHeader()
{
    this->indexAllocationMapPageId = 0;
    this->tableId = 0;
    this->maxRowSize = 0;
    this->numberOfColumns = 0;
    this->tableNameSize = 0;
    this->columnsNullBitMap = nullptr;
}

TableHeader::~TableHeader()
{
    delete this->columnsNullBitMap;
}

TableHeader& TableHeader::operator=(const TableHeader &tableHeader)
{
    if (this == &tableHeader)
        return *this;
    
    this->indexAllocationMapPageId = tableHeader.indexAllocationMapPageId;
    this->maxRowSize = tableHeader.maxRowSize;
    this->numberOfColumns = tableHeader.numberOfColumns;
    this->tableNameSize = tableHeader.tableNameSize;
    this->tableName = tableHeader.tableName;
    this->tableId = tableHeader.tableId;
    this->columnsNullBitMap = new BitMap(*tableHeader.columnsNullBitMap);
    
    return *this;
}

TableFullHeader::TableFullHeader() = default;

TableFullHeader::TableFullHeader(const TableFullHeader& tableHeader)
{
    this->tableHeader = tableHeader.tableHeader;
    this->columnsHeaders = tableHeader.columnsHeaders;
}

Table::Table(const string& tableName,const table_id_t& tableId, const vector<Column*>& columns, Database* database)
{
    this->columns = columns;
    this->database = database;
    this->header.tableName = tableName;
    this->header.numberOfColumns = columns.size();
    this->header.columnsNullBitMap = new BitMap(this->header.numberOfColumns);
    this->header.tableId = tableId;

    uint16_t counter = 0;
    for(const auto& column : columns)
    {
        this->header.columnsNullBitMap->Set(counter, column->GetAllowNulls());
        
        this->header.maxRowSize += column->GetColumnSize();
        column->SetColumnIndex(counter);
        
        counter++;
    }
}

Table::Table(const TableHeader &tableHeader, Database *database)
{
    this->header = tableHeader;
    this->database = database;
}

Table::~Table()
{
    for(const auto& column : columns)
        delete column;
}

void Table::InsertRows(const vector<vector<Field>> &inputData)
{
    uint32_t rowsInserted = 0;
    extent_id_t startingExtentIndex = 0;
    vector<extent_id_t> extents;
    for (const auto& rowData: inputData)
    {
        this->InsertRow(rowData, extents, startingExtentIndex);

        rowsInserted++;
    
        if(rowsInserted % 1000 == 0)
            cout << rowsInserted << endl;
    }



    cout << "Rows affected: "<< rowsInserted << endl;
}

void Table::InsertRow(const vector<Field>& inputData, vector<extent_id_t>& allocatedExtents, extent_id_t& startingExtentIndex)
{

    Row* row = new Row(*this);
    for(size_t i = 0;i < inputData.size(); ++i)
    {
        const column_index_t& associatedColumnIndex = inputData[i].GetColumnIndex();
        Block* block = new Block(columns[associatedColumnIndex]);
        const ColumnType columnType = columns[associatedColumnIndex]->GetColumnType();

        if(inputData[i].GetIsNull())
        {
            if(!columns[associatedColumnIndex]->GetAllowNulls())
                throw invalid_argument("Column " + columns[associatedColumnIndex]->GetColumnName() + " does not allow NULLs. Insert Fails.");
            
            block->SetData(nullptr, 0);

            row->SetNullBitMapValue(i, true);

            const auto& columnIndex = columns[associatedColumnIndex]->GetColumnIndex();
            row->InsertColumnData(block, columnIndex);

            continue;
        }

        Table::SetBlockDataByColumnType(block, columnType, inputData[i]);


        const auto& columnIndex = columns[associatedColumnIndex]->GetColumnIndex();
        row->InsertColumnData(block, columnIndex);
    }

    if(this->header.indexAllocationMapPageId > 0)
    {
        this->InsertLargeObjectToPage(row);

        if (this->database->InsertRowToPage(*this, allocatedExtents, startingExtentIndex, row))
            return;
    }
    
    Page* newPage = this->database->CreateDataPage(this->header.tableId);

    this->InsertLargeObjectToPage(row);

    newPage->InsertRow(row);
}

void Table::SetBlockDataByColumnType(Block *&block, const ColumnType &columnType, const Field& inputData)
{
        switch (columnType)
        {
            case ColumnType::TinyInt: 
            {
                const int8_t convertedTinyInt = SafeConverter<int8_t>::SafeStoi(inputData.GetData());
                block->SetData(&convertedTinyInt, sizeof(int8_t));
                break;
            }
            case ColumnType::SmallInt: 
            {
                const int16_t convertedSmallInt = SafeConverter<int16_t>::SafeStoi(inputData.GetData());
                block->SetData(&convertedSmallInt, sizeof(int16_t));
                break;
            }
            case ColumnType::Int: 
            {
                const int32_t convertedInt = SafeConverter<int32_t>::SafeStoi(inputData.GetData());
                block->SetData(&convertedInt, sizeof(int32_t));
                break;
            }
            case ColumnType::BigInt: 
            {
                const int64_t convertedBigInt = SafeConverter<int64_t>::SafeStoi(inputData.GetData());
                block->SetData(&convertedBigInt, sizeof(int64_t));
                break;
            }
            case ColumnType::String: 
            {
                const string& data = inputData.GetData();
                block->SetData(data.c_str(), data.size());
                break;
            }
            case ColumnType::Bool:
            {
                const string& data = inputData.GetData();

                bool value = false;
                
                if(data == "1")
                    value = true;
                else if(data == "0")
                    value = false;
                else
                    throw invalid_argument("InsertRow: Invalid Boolean Value specified!");

                block->SetData(&value, sizeof(bool));
                break; 
            }
            case ColumnType::Decimal:
            case ColumnType::UnicodeString:
                // Handle other cases if needed
                break;
            default:
                throw invalid_argument("Unsupported column type");
        }
}

void Table::InsertLargeObjectToPage(Row* row)
{
    const vector<column_index_t> largeBlockIndexes = row->GetLargeBlocks();

    if(largeBlockIndexes.empty())
        return;

    RowHeader* rowHeader = row->GetHeader();

    const auto& rowData = row->GetData();

    for (const auto& largeBlockIndex : largeBlockIndexes)
    {
        rowHeader->largeObjectBitMap->Set(largeBlockIndex, true);

        page_offset_t offset = 0;

        block_size_t remainingBlockSize = rowData[largeBlockIndex]->GetBlockSize();

        RecursiveInsertToLargePage(row, offset, largeBlockIndex, remainingBlockSize, true, nullptr);
    }
    
}

void Table::RecursiveInsertToLargePage(Row*& row
                                    , page_offset_t& offset
                                    , const column_index_t& columnIndex
                                    , block_size_t& remainingBlockSize
                                    , const bool& isFirstRecursion
                                    , DataObject** previousDataObject)
{
    LargeDataPage* largeDataPage = this->GetOrCreateLargeDataPage();

    const auto& pageSize = largeDataPage->GetBytesLeft();

    const auto& data = row->GetData()[columnIndex]->GetBlockData();

    large_page_index_t objectIndex;

    if (remainingBlockSize + OBJECT_METADATA_SIZE_T < pageSize)
    {

        largeDataPage->InsertObject(data + offset, remainingBlockSize, &objectIndex);

        this->database->SetPageMetaDataToPfs(largeDataPage);

        Table::InsertLargeDataObjectPointerToRow(row, isFirstRecursion, objectIndex, largeDataPage->GetPageId(), columnIndex);

        if (previousDataObject != nullptr)
        {
            (*previousDataObject)->nextPageId = largeDataPage->GetPageId();
            (*previousDataObject)->nextObjectIndex = objectIndex;
        }

        return;
    }

    //blockSize < pageSize
    const auto bytesToBeInserted = pageSize - OBJECT_METADATA_SIZE_T;

    remainingBlockSize -= bytesToBeInserted;

    DataObject* dataObject = largeDataPage->InsertObject(data + offset, bytesToBeInserted, &objectIndex);

    this->database->SetPageMetaDataToPfs(largeDataPage);

    if (previousDataObject != nullptr)
    {
        (*previousDataObject)->nextPageId = largeDataPage->GetPageId();
        (*previousDataObject)->nextObjectIndex = objectIndex;
    }

    offset += bytesToBeInserted;

    this->RecursiveInsertToLargePage(row, offset, columnIndex, remainingBlockSize, false, &dataObject);

    Table::InsertLargeDataObjectPointerToRow(row, isFirstRecursion, objectIndex, largeDataPage->GetPageId(), columnIndex);
}

LargeDataPage* Table::GetOrCreateLargeDataPage()
{
    LargeDataPage* largeDataPage = this->database->GetTableLastLargeDataPage(this->header.tableId, OBJECT_METADATA_SIZE_T + 1);

    return ( largeDataPage == nullptr )
            ? this->database->CreateLargeDataPage(this->header.tableId)
            : largeDataPage;
}

void Table::LinkLargePageDataObjectChunks(DataObject* dataObject, const page_id_t& lastLargePageId, const large_page_index_t& objectIndex)
{
    if (dataObject != nullptr)
    {
        dataObject->nextPageId = lastLargePageId;
        dataObject->nextObjectIndex = objectIndex;
    }
}

void Table::InsertLargeDataObjectPointerToRow(Row* row
        , const bool& isFirstRecursion
        , const large_page_index_t& objectIndex
        , const page_id_t& lastLargePageId
        , const column_index_t& largeBlockIndex)
{
    if (!isFirstRecursion)
        return;

    const DataObjectPointer objectPointer(objectIndex, lastLargePageId);
    
    Block* block = new Block(&objectPointer, sizeof(DataObjectPointer), this->columns[largeBlockIndex]);
    
    row->UpdateColumnData(block);
}

column_number_t Table::GetNumberOfColumns() const { return this->columns.size();}

const TableHeader& Table::GetTableHeader() const { return this->header; }

const vector<Column*>& Table::GetColumns() const { return this->columns; }

LargeDataPage* Table::GetLargeDataPage(const page_id_t &pageId) const
{
    return this->database->GetLargeDataPage(pageId, this->header.tableId);
}

void Table::Select(vector<Row>& selectedRows, const vector<RowCondition*>* conditions, const size_t& count) const
{
   const size_t rowsToSelect = (count == -1)
                        ? numeric_limits<size_t>::max()
                        : count;

    this->database->SelectTableRows(this->header.tableId, selectedRows, rowsToSelect, conditions);
}

void Table::Update(const vector<Field>& updates, const vector<RowCondition*>* conditions)
{
    vector<Block*> updateBlocks;
    for(const auto& field : updates)
    {
        const auto& associatedColumnIndex = field.GetColumnIndex();
        const auto& columnType = this->columns[associatedColumnIndex]->GetColumnType();
        
        Block* block = new Block(this->columns[associatedColumnIndex]);

        Table::SetBlockDataByColumnType(block, columnType, field);
        updateBlocks.push_back(block);
    }
    
    this->database->UpdateTableRows(this->header.tableId, updateBlocks, conditions);

    for(const auto& block : updateBlocks)
        delete block;
}

void Table::UpdateIndexAllocationMapPageId(const page_id_t &indexAllocationMapPageId) { this->header.indexAllocationMapPageId = indexAllocationMapPageId; }

bool Table::IsColumnNullable(const column_index_t &columnIndex) const { return this->header.columnsNullBitMap->Get(columnIndex); }

void Table::AddColumn(Column *column) { this->columns.push_back(column); }

string& Table::GetTableName(){ return this->header.tableName; }

row_size_t& Table::GetMaxRowSize(){ return this->header.maxRowSize; }

const table_id_t& Table::GetTableId() const { return this->header.tableId; }