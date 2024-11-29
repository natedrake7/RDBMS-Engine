#include "Table.h"

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
    this->metadata.tableName = tableName;
    this->metadata.numberOfColumns = columns.size();
    this->metadata.columnsNullBitMap = new BitMap(this->metadata.numberOfColumns);
    this->metadata.tableId = tableId;

    uint16_t counter = 0;
    for(const auto& column : columns)
    {
        this->metadata.columnsNullBitMap->Set(counter, column->GetAllowNulls());
        
        this->metadata.maxRowSize += column->GetColumnSize();
        column->SetColumnIndex(counter);
        
        counter++;
    }
}

Table::Table(const TableHeader &tableHeader, Database *database)
{
    this->metadata = tableHeader;
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

    if(this->metadata.indexAllocationMapPageId > 0)
    {
        // this->InsertLargeObjectToPage(row, 0,  row->GetLargeBlocks());
        if (this->database->InsertRowToPage(*this, row))
            return;
    }

    Page* newPage = this->database->CreatePage(this->metadata.tableId);

    // this->InsertLargeObjectToPage(row, 0, row->GetLargeBlocks());

    newPage->InsertRow(row);
}

// void Table::InsertLargeObjectToPage(Row* row, page_offset_t offset, const vector<column_index_t>& largeBlocksIndexes)
// {
//     if(largeBlocksIndexes.empty())
//         return;
//
//     RowHeader* rowHeader = row->GetHeader();
//     
//     const auto& rowData = row->GetData();
//
//     for(const auto& largeBlockIndex : largeBlocksIndexes)
//     {
//         rowHeader->largeObjectBitMap->Set(largeBlockIndex, true);
//
//         DataObject* dataObject = nullptr;
//
//         offset = 0;
//
//         block_size_t blockSize = rowData[largeBlockIndex]->GetBlockSize();
//
//         const auto& data = rowData[largeBlockIndex]->GetBlockData();
//
//         while(true)
//         {
//             LargeDataPage* largeDataPage = this->GetOrCreateLargeDataPage();
//
//             blockSize -= offset;
//
//             const auto& availableBytesInPage = largeDataPage->GetBytesLeft();
//
//             const auto& dataSize = blockSize + OBJECT_METADATA_SIZE_T;
//
//             large_page_index_t objectIndex;
//
//             if (availableBytesInPage >= dataSize)
//             {
//                 largeDataPage->InsertObject(data + offset, blockSize, &objectIndex);
//
//                 LinkLargePageDataObjectChunks(dataObject, largeDataPage->GetPageId(), objectIndex);
//
//                 InsertLargeDataObjectPointerToRow(row, offset, objectIndex, largeDataPage->GetPageId(), largeBlockIndex);
//
//                 break;
//             }
//
//             const auto bytesAllocated = availableBytesInPage - OBJECT_METADATA_SIZE_T;//add page Id maybe;
//
//             DataObject* prevDataObject = nullptr;
//
//             if (dataObject != nullptr)
//                 prevDataObject = dataObject;
//
//             dataObject = largeDataPage->InsertObject(data + offset, bytesAllocated, &objectIndex);
//
//             LinkLargePageDataObjectChunks(prevDataObject, largeDataPage->GetPageId(), objectIndex);
//
//             InsertLargeDataObjectPointerToRow(row, availableBytesInPage, objectIndex, largeDataPage->GetPageId(), largeBlockIndex);
//
//             offset += bytesAllocated;
//         }
//     }
// }

// LargeDataPage* Table::GetOrCreateLargeDataPage()
// {
//     LargeDataPage* largeDataPage = nullptr;
//     const auto& lastLargePageId = this->database->GetLastLargeDataPageId();
//
//     if(lastLargePageId > 0)
//         largeDataPage = this->database->GetLargeDataPage(lastLargePageId, *this);
//
//     if (largeDataPage == nullptr)
//         largeDataPage = this->database->CreateLargeDataPage();
//
//     if (largeDataPage->GetBytesLeft() == 0
//         || largeDataPage->GetBytesLeft() < OBJECT_METADATA_SIZE_T + 1)
//     {
//         largeDataPage->SetNextPageId(this->database->GetLastLargeDataPageId() + 1);
//
//         largeDataPage = this->database->CreateLargeDataPage();
//     }
//
//     return largeDataPage;
// }

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

column_number_t Table::GetNumberOfColumns() const { return this->columns.size();}

const TableHeader& Table::GetTableHeader() const { return this->metadata; }

const vector<Column*>& Table::GetColumns() const { return this->columns; }

LargeDataPage* Table::GetLargeDataPage(const page_id_t &pageId) const { return this->database->GetLargeDataPage(pageId, *this); }

void Table::SelectRows(vector<Row>* selectedRows, const vector<RowCondition*>* conditions, const size_t& count) const
{
   const size_t rowsToSelect = (count == -1)
                        ? numeric_limits<size_t>::max()
                        : count;


    vector<Page*> tablePages;
    this->database->GetTablePages(*this, &tablePages);

    for (const auto& tablePage : tablePages)
    {
        vector<Row> pageRows;
        tablePage->GetRows(&pageRows, *this, conditions);

        if(selectedRows->size() + pageRows.size() > rowsToSelect)
        {
            selectedRows->insert(selectedRows->end(), pageRows.begin(), pageRows.begin() + ( rowsToSelect - selectedRows->size()));
            break;
        }

        selectedRows->insert(selectedRows->end(), pageRows.begin(), pageRows.end());
    }
}

// void Table::UpdateRows(const vector<Block> *updates, const vector<RowCondition *>* conditions)
// {
//     page_id_t pageId = this->metadata.indexAllocationMapPageId;
//
//     while(pageId > 0)
//     {
//         Page* page = this->database->GetPage(*this);
//
//         
//     }
// }

void Table::UpdateIndexAllocationMapPageId(const page_id_t &indexAllocationMapPageId) { this->metadata.indexAllocationMapPageId = indexAllocationMapPageId; }

bool Table::IsColumnNullable(const column_index_t &columnIndex) const { return this->metadata.columnsNullBitMap->Get(columnIndex); }

void Table::AddColumn(Column *column) { this->columns.push_back(column); }

string& Table::GetTableName(){ return this->metadata.tableName; }

row_size_t& Table::GetMaxRowSize(){ return this->metadata.maxRowSize; }

const table_id_t& Table::GetTableId() const { return this->metadata.tableId; }