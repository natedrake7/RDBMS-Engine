﻿#include "Table.h"

#include "../Page/Page.h"

TableFullMetaData::TableFullMetaData()
{
    this->tableMetaData.firstPageId = 0;
    this->tableMetaData.lastPageId = 0;
    this->tableMetaData.maxRowSize = 0;
    this->tableMetaData.numberOfColumns = 0;
    this->tableMetaData.tableNameSize = 0;
}

TableFullMetaData::TableFullMetaData(const TableFullMetaData& tableMetaData)
{
    this->tableMetaData = tableMetaData.tableMetaData;
    this->columnsMetaData = tableMetaData.columnsMetaData;
}

Table::Table(const string& tableName, const vector<Column*>& columns, Database* database)
{
    this->columns = columns;
    this->database = database;
    this->metadata.tableName = tableName;
    this->metadata.maxRowSize = 0;
    this->metadata.firstPageId = 0;
    this->metadata.lastPageId = 0;
    this->metadata.numberOfColumns = columns.size();

    uint16_t counter = 0;
    for(const auto& column : columns)
    {
        this->metadata.maxRowSize += column->GetColumnSize();
        column->SetColumnIndex(counter);
        counter++;
    }
}

Table::Table(const TableMetaData &tableMetaData, const vector<Column *> &columns, Database *database)
{
    this->metadata = tableMetaData;
    this->columns = columns;
    this->database = database;
}

Table::~Table()
{
    //save table metadata to file
    for(const auto& column : columns)
        delete column;


    // for(const auto& row : this->rows)
    //     delete row;
}

void Table::InsertRow(const vector<string>& inputData)
{

    Row* row = new Row(*this);
    for(size_t i = 0;i < inputData.size(); ++i)
    {
        Block* block = new Block(columns[i]);
        const ColumnType columnType = columns[i]->GetColumnType();

        if (columnType == ColumnType::TinyInt)
        {
            int8_t convertedTinyInt = SafeConverter<int8_t>::SafeStoi(inputData[i]);
            block->SetData(&convertedTinyInt, sizeof(int8_t));
        }
        else if (columnType == ColumnType::SmallInt)
        {
            int16_t convertedSmallInt = SafeConverter<int16_t>::SafeStoi(inputData[i]);
            block->SetData(&convertedSmallInt, sizeof(int16_t));
        }
        else if (columnType == ColumnType::Int)
        {
            //store each int value in 4 bits eg 04 -> 1 byte , 40 -> 8 bit
            int32_t convertedInt = SafeConverter<int32_t>::SafeStoi(inputData[i]);
            block->SetData(&convertedInt, sizeof(int32_t));
        }
        else if (columnType == ColumnType::BigInt)
        {
            int64_t convertedBigInt = SafeConverter<int64_t>::SafeStoi(inputData[i]);
            block->SetData(&convertedBigInt, sizeof(int64_t));
        }
        else if (columnType == ColumnType::String)
            block->SetData(inputData[i].c_str(), inputData[i].size() + 1);
        else
            throw invalid_argument("Unsupported column type");

        const auto& columnIndex = columns[i]->GetColumnIndex();
        row->InsertColumnData(block, columnIndex);
    }

    Page* lastPage = nullptr;
    if(this->metadata.lastPageId > EXTENT_SIZE - 1)
    {
        lastPage = this->database->GetPage(this->metadata.lastPageId, *this);

        auto largeBlockIndexes = row->GetLargeBlocks();

        this->InsertLargeObjectToPage(row, 0, largeBlockIndexes);

        const uint32_t& rowSize = row->GetRowSize();

        if(lastPage->GetBytesLeft() >= rowSize + this->GetNumberOfColumns() * sizeof(uint16_t))
        {
            lastPage->InsertRow(row, *this);
            return;
        }
    }

    Page* newPage = this->database->CreatePage();

    const int newPageId = newPage->GetPageId();

    if(lastPage != nullptr)
        lastPage->SetNextPageId(newPageId);

    if(this->metadata.firstPageId <= 0)
        this->metadata.firstPageId = newPageId;

    this->metadata.lastPageId = newPageId;

    this->InsertLargeObjectToPage(row, 0, row->GetLargeBlocks());

    newPage->InsertRow(row, *this);

    // this->rows.push_back(row);
    // cout<<"Row Affected: 1"<<'\n';
}

void Table::InsertLargeObjectToPage(Row* row, uint16_t offset, const vector<uint16_t>& largeBlocksIndexes)
{
    if(largeBlocksIndexes.empty())
        return;

    const auto& rowData = row->GetData();

    const auto& lastLargePageId = this->database->GetLastLargeDataPageId();


    LargeDataPage* largeDataPage = nullptr;

    for(const auto& largeBlockIndex : largeBlocksIndexes)
    {
        offset = 0;
        uint16_t blockSize = rowData[largeBlockIndex]->GetBlockSize();

        DataObject* dataObject = nullptr;

        while(true)
        {
            if(lastLargePageId > 0)
                largeDataPage = this->database->GetLargeDataPage(lastLargePageId, *this);

            if (largeDataPage == nullptr)
                largeDataPage = this->database->CreateLargeDataPage();

            if (largeDataPage->GetBytesLeft() == 0
                || largeDataPage->GetBytesLeft() < 2 * sizeof(uint16_t) + 1)
            {
                largeDataPage->SetNextPageId(this->database->GetLastLargeDataPageId() + 1);

                largeDataPage = this->database->CreateLargeDataPage();
            }

            const auto& data = rowData[largeBlockIndex]->GetBlockData();

            blockSize -= offset;

            const auto& availableBytesInPage = largeDataPage->GetBytesLeft();

            const auto& dataSize = blockSize + 2 * sizeof(uint16_t);

            uint16_t objectOffset;

            if (availableBytesInPage >= dataSize)
            {
                largeDataPage->InsertObject(data + offset, blockSize, &objectOffset);

                if (dataObject != nullptr)
                {
                    dataObject->nextPageId = largeDataPage->GetPageId();
                    dataObject->nextPageOffset = objectOffset;
                }

                if (offset == 0)
                {
                    DataObjectPointer objectPointer(blockSize, objectOffset, largeDataPage->GetPageId());
                    rowData[largeBlockIndex]->SetData(&objectPointer, sizeof(DataObjectPointer), true);
                }

                break;
            }

            const auto bytesAllocated = availableBytesInPage - 2 * sizeof(uint32_t);

            dataObject = largeDataPage->InsertObject(data + offset, bytesAllocated, &objectOffset);

            if (offset == 0)
            {
                DataObjectPointer objectPointer(availableBytesInPage, objectOffset, largeDataPage->GetPageId());

                rowData[largeBlockIndex]->SetData(&objectPointer, sizeof(DataObjectPointer), true);

                row->UpdateRowSize();
            }

            offset += bytesAllocated;
        }
    }

}

vector<Row> Table::GetRowByBlock(const Block& block, const vector<Column*>& selectedColumns) const
{
    vector<Row> selectedRows;
    const uint16_t& blockIndex = block.GetColumnIndex();
    const ColumnType& columnType = this->columns[blockIndex]->GetColumnType();
    const auto searchBlockData = block.GetBlockData();

    for(const auto& row : this->rows)
    {
        const auto& rowData = row->GetData();
        if(memcmp(rowData[blockIndex]->GetBlockData(), searchBlockData, rowData[blockIndex]->GetBlockSize()) == 0)
        {
            vector<Block*> selectedBlocks;
            if(selectedColumns.empty())
                selectedBlocks = rowData;
            else
                for(const auto& column : selectedColumns)
                    selectedBlocks.push_back(rowData[column->GetColumnIndex()]);

            selectedRows.emplace_back(*this, selectedBlocks);
        }
    }

    return selectedRows;
}

void Table::PrintTable(size_t maxNumberOfItems) const
{
    if(maxNumberOfItems == -1)
        maxNumberOfItems = this->rows.size();

    for(size_t i = 0; i < this->columns.size(); ++i)
    {
        cout<< this->columns[i]->GetColumnName();

        if(i != this->columns.size() - 1)
            cout<<" || ";
        else
            cout<<"\n";
    }

    for(size_t i = 0; i < maxNumberOfItems; ++i)
        this->rows[i]->PrintRow();
}

uint16_t Table::GetNumberOfColumns() const { return this->columns.size();}

const TableMetaData& Table::GetTableMetadata() const { return this->metadata; }

const vector<Column*>& Table::GetColumns() const { return this->columns; }

vector<Row> Table::SelectRows(const size_t& count) const
{
    vector<Row> selectedRows;
    int pageId = this->metadata.firstPageId;

   const size_t rowsToSelect = (count == -1)
                        ? numeric_limits<size_t>::max()
                        : count;

    while(pageId > 0)
    {
        Page* page = this->database->GetPage(pageId, *this);
        const vector<Row> pageRows = page->GetRows(*this);

        if(selectedRows.size() + pageRows.size() > rowsToSelect)
        {
            selectedRows.insert(selectedRows.end(), pageRows.begin(), pageRows.begin() + ( rowsToSelect - selectedRows.size()));
            break;
        }

        selectedRows.insert(selectedRows.end(), pageRows.begin(), pageRows.end());
        pageId = page->GetNextPageId();
    }

    return selectedRows;
}

string& Table::GetTableName(){ return this->metadata.tableName; }

size_t& Table::GetMaxRowSize(){ return this->metadata.maxRowSize; }
