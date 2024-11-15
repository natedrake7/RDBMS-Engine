#include "Table.h"

Table::Table(const string& tableName, const vector<Column*>& columns, const Database* database)
{
    this->tableName = tableName;
    this->columns = columns;
    this->maxRowSize = 0;
    this->database = database;

    size_t counter = 0;
    for(const auto& column : columns)
    {
        this->maxRowSize += column->GetColumnSize();
        const size_t columnHash = counter++;
        column->SetColumnIndex(columnHash);
    }
}

Table::~Table()
{
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
        else if (columnType == ColumnType::String)//ths poutanas tha ginei dictionary encoding incoming
        {

            const uint64_t primaryHashKey = this->database->InsertToHashTable(inputData[i].c_str());
            block->SetData(&primaryHashKey, sizeof(uint64_t));
        }
        else
            throw invalid_argument("Unsupported column type");

        row->InsertColumnData(block, this->columns[i]->GetColumnIndex());

    }

    this->rows.push_back(row);
    // cout<<"Row Affected: 1"<<'\n';
}

vector<Row> Table::GetRowByBlock(const Block& block, const vector<Column*>& selectedColumns) const
{
    vector<Row> selectedRows;
    const size_t& blockIndex = block.GetColumnIndex();
    const ColumnType& columnType = this->columns[blockIndex]->GetColumnType();
    const auto searchBlockData = block.GetBlockData();

    const unsigned char* temp;

    if(columnType == ColumnType::String)
    {
        const char* castBlockData = reinterpret_cast<const char*>(searchBlockData);

        const uint64_t primaryHashKey = this->database->Hash(castBlockData);

        temp = reinterpret_cast<const unsigned char*>(&primaryHashKey);
    }
    else
        temp = searchBlockData;

    for(const auto& row : this->rows)
    {
        const auto& rowData = row->GetData();
        if(memcmp(rowData[blockIndex]->GetBlockData(), temp, rowData[blockIndex]->GetBlockSize()) == 0)
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
        this->rows[i]->PrintRow(this->database);
}

size_t Table::GetNumberOfColumns() const { return this->columns.size();}

string& Table::GetTableName(){ return this->tableName; }

size_t& Table::GetMaxRowSize(){ return this->maxRowSize; }
