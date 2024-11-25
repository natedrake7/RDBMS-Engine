#include "Table.h"

#include "../../AdditionalLibraries/AdditionalObjects/Field/Field.h"
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

void Table::InsertRows(const vector<vector<Field>> &inputData)
{
    uint32_t rowsInserted = 0;
    for (const auto& rowData: inputData)
    {
        this->InsertRow(rowData);
        rowsInserted++;
    }

    cout<<"Rows affected: "<<rowsInserted<<endl;
}

void Table::InsertRow(const vector<Field>& inputData)
{

    Row* row = new Row(*this);
    for(size_t i = 0;i < inputData.size(); ++i)
    {
        Block* block = new Block(columns[i]);
        const ColumnType columnType = columns[i]->GetColumnType();

        if(inputData[i].GetIsNull())
        {
            if(!columns[i]->GetAllowNulls())
                throw invalid_argument("Column " + columns[i]->GetColumnName() + " does not allow NULLs. Insert Fails.");
            
            block->SetData(nullptr, 0);

            row->SetNullBitMapValue(i, true);
        }
        else if (columnType == ColumnType::TinyInt)
        {
            int8_t convertedTinyInt = SafeConverter<int8_t>::SafeStoi(inputData[i].GetData());
            block->SetData(&convertedTinyInt, sizeof(int8_t));
        }
        else if (columnType == ColumnType::SmallInt)
        {
            int16_t convertedSmallInt = SafeConverter<int16_t>::SafeStoi(inputData[i].GetData());
            block->SetData(&convertedSmallInt, sizeof(int16_t));
        }
        else if (columnType == ColumnType::Int)
        {
            //store each int value in 4 bits eg 04 -> 1 byte , 40 -> 8 bit
            int32_t convertedInt = SafeConverter<int32_t>::SafeStoi(inputData[i].GetData());
            block->SetData(&convertedInt, sizeof(int32_t));
        }
        else if (columnType == ColumnType::BigInt)
        {
            int64_t convertedBigInt = SafeConverter<int64_t>::SafeStoi(inputData[i].GetData());
            block->SetData(&convertedBigInt, sizeof(int64_t));
        }
        else if (columnType == ColumnType::String)
            block->SetData(inputData[i].GetData().c_str(), inputData[i].GetData().size() + 1);
        else
            throw invalid_argument("Unsupported column type");

        const auto& columnIndex = columns[i]->GetColumnIndex();
        row->InsertColumnData(block, columnIndex);
    }

    Page* lastPage = nullptr;
    if(this->metadata.lastPageId > EXTENT_SIZE - 1)
    {
        lastPage = this->database->GetPage(this->metadata.lastPageId, *this);

        this->InsertLargeObjectToPage(row, 0,  row->GetLargeBlocks());

        const row_size_t& rowSize = row->GetRowSize();

        if(lastPage->GetBytesLeft() >= rowSize + this->GetNumberOfColumns() * sizeof(uint16_t))
        {
            lastPage->InsertRow(row, *this);
            return;
        }
    }

    Page* newPage = this->database->CreatePage();

    const page_id_t newPageId = newPage->GetPageId();

    if(lastPage != nullptr)
        lastPage->SetNextPageId(newPageId);

    if(this->metadata.firstPageId <= 0)
        this->metadata.firstPageId = newPageId;

    this->metadata.lastPageId = newPageId;

    this->InsertLargeObjectToPage(row, 0, row->GetLargeBlocks());

    newPage->InsertRow(row, *this);
}

void Table::InsertLargeObjectToPage(Row* row, page_offset_t offset, const vector<column_index_t>& largeBlocksIndexes)
{
    if(largeBlocksIndexes.empty())
        return;

    RowMetaData* rowMetaData = row->GetMetaData();
    
    const auto& rowData = row->GetData();

    for(const auto& largeBlockIndex : largeBlocksIndexes)
    {
        rowMetaData->largeObjectBitMap->Set(largeBlockIndex, true);

        DataObject* dataObject = nullptr;

        offset = 0;

        block_size_t blockSize = rowData[largeBlockIndex]->GetBlockSize();

        const auto& data = rowData[largeBlockIndex]->GetBlockData();

        while(true)
        {
            LargeDataPage* largeDataPage = this->GetOrCreateLargeDataPage();

            blockSize -= offset;

            const auto& availableBytesInPage = largeDataPage->GetBytesLeft();

            const auto& dataSize = blockSize + OBJECT_METADATA_SIZE_T;

            large_page_index_t objectIndex;

            if (availableBytesInPage >= dataSize)
            {
                largeDataPage->InsertObject(data + offset, blockSize, &objectIndex);

                LinkLargePageDataObjectChunks(dataObject, largeDataPage->GetPageId(), objectIndex);

                InsertLargeDataObjectPointerToRow(row, offset, objectIndex, largeDataPage->GetPageId(), largeBlockIndex);

                break;
            }

            const auto bytesAllocated = availableBytesInPage - OBJECT_METADATA_SIZE_T;//add page Id maybe;

            DataObject* prevDataObject = nullptr;

            if (dataObject != nullptr)
                prevDataObject = dataObject;

            dataObject = largeDataPage->InsertObject(data + offset, bytesAllocated, &objectIndex);

            LinkLargePageDataObjectChunks(prevDataObject, largeDataPage->GetPageId(), objectIndex);

            InsertLargeDataObjectPointerToRow(row, availableBytesInPage, objectIndex, largeDataPage->GetPageId(), largeBlockIndex);

            offset += bytesAllocated;
        }
    }
}

LargeDataPage* Table::GetOrCreateLargeDataPage()
{
    LargeDataPage* largeDataPage = nullptr;
    const auto& lastLargePageId = this->database->GetLastLargeDataPageId();

    if(lastLargePageId > 0)
        largeDataPage = this->database->GetLargeDataPage(lastLargePageId, *this);

    if (largeDataPage == nullptr)
        largeDataPage = this->database->CreateLargeDataPage();

    if (largeDataPage->GetBytesLeft() == 0
        || largeDataPage->GetBytesLeft() < OBJECT_METADATA_SIZE_T + 1)
    {
        largeDataPage->SetNextPageId(this->database->GetLastLargeDataPageId() + 1);

        largeDataPage = this->database->CreateLargeDataPage();
    }

    return largeDataPage;
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
        , const page_offset_t& offset
        , const large_page_index_t& objectIndex
        , const page_id_t& lastLargePageId
        , const column_index_t& largeBlockIndex)
{
    if (offset == 0)
    {
        DataObjectPointer objectPointer(objectIndex, lastLargePageId);

        row->GetData()[largeBlockIndex]->SetData(&objectPointer, sizeof(DataObjectPointer));

        row->UpdateRowSize();
    }
}

vector<Row> Table::GetRowByBlock(const Block& block, const vector<Column*>& selectedColumns) const
{
    vector<Row> selectedRows;
    const column_index_t& blockIndex = block.GetColumnIndex();
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

column_number_t Table::GetNumberOfColumns() const { return this->columns.size();}

const TableMetaData& Table::GetTableMetadata() const { return this->metadata; }

const vector<Column*>& Table::GetColumns() const { return this->columns; }

LargeDataPage* Table::GetLargeDataPage(const page_id_t &pageId) const { return this->database->GetLargeDataPage(pageId, *this); }

void Table::SelectRows(vector<Row>* selectedRows, const size_t& count) const
{
    page_id_t pageId = this->metadata.firstPageId;

   const size_t rowsToSelect = (count == -1)
                        ? numeric_limits<size_t>::max()
                        : count;

    while(pageId > 0)
    {
        Page* page = this->database->GetPage(pageId, *this);
        vector<Row> pageRows;
        page->GetRows(&pageRows, *this);

        if(selectedRows->size() + pageRows.size() > rowsToSelect)
        {
            selectedRows->insert(selectedRows->end(), pageRows.begin(), pageRows.begin() + ( rowsToSelect - selectedRows->size()));
            break;
        }

        selectedRows->insert(selectedRows->end(), pageRows.begin(), pageRows.end());
        pageId = page->GetNextPageId();
    }
}

string& Table::GetTableName(){ return this->metadata.tableName; }

row_size_t& Table::GetMaxRowSize(){ return this->metadata.maxRowSize; }
