#include "Table.h"

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
    this->metadata.firstPageId = -1;
    this->metadata.lastPageId = -1;
    this->metadata.numberOfColumns = columns.size();

    size_t counter = 0;
    for(const auto& column : columns)
    {
        this->metadata.maxRowSize += column->GetColumnSize();
        const size_t columnHash = counter++;
        column->SetColumnIndex(columnHash);
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
        {
            // const uint64_t primaryHashKey = this->database->InsertToHashTable(inputData[i].c_str());
            string convertedString = inputData[i] + '\0';;
            block->SetData(convertedString.c_str(), convertedString.size() + 1);
        }
        else
            throw invalid_argument("Unsupported column type");

        row->InsertColumnData(block, this->columns[i]->GetColumnIndex());
    }

    Page* lastPage = nullptr;
    if(this->metadata.lastPageId > 0)
    {
        lastPage = this->database->GetPage(this->metadata.lastPageId, *this);

        if(lastPage->GetBytesLeft() >= row->GetRowSize())
        {
            lastPage->InsertRow(row);
            return;
        }
    }

    Page* newPage = this->database->CreatePage();

    const int newPageId = newPage->GetPageId();

    if(lastPage != nullptr)
        lastPage->SetNextPageId(newPageId);

    if(this->metadata.firstPageId == -1)
        this->metadata.firstPageId = newPageId;

    this->metadata.lastPageId = newPageId;

    newPage->InsertRow(row);

    // this->rows.push_back(row);
    // cout<<"Row Affected: 1"<<'\n';
}

vector<Row> Table::GetRowByBlock(const Block& block, const vector<Column*>& selectedColumns) const
{
    vector<Row> selectedRows;
    const size_t& blockIndex = block.GetColumnIndex();
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

size_t Table::GetNumberOfColumns() const { return this->columns.size();}

const TableMetaData& Table::GetTableMetadata() const { return this->metadata; }

const vector<Column*>& Table::GetColumns() const { return this->columns; }

vector<Row> Table::SelectRows(const size_t& count) const
{
    vector<Row> selectedRows;
    int pageId = this->metadata.firstPageId;

   const size_t rowsToSelect = (count == -1)
                        ? numeric_limits<size_t>::max()
                        : count;


    while(pageId != -1)
    {
        Page* page = this->database->GetPage(pageId, *this);
        const vector<Row> pageRows = page->GetRows();

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
