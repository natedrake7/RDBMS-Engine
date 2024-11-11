#include "Table.h"

#include "../Database.h"

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
    // Row* row = new Row(*this);
    for(size_t i = 0;i < inputData.size(); ++i)
    {
        Block* block = new Block(columns[i]);
        const ColumnType columnType = columns[i]->GetColumnType();

        if(columnType == ColumnType::Integer)
        {
            //set size only not convert to int
            //bitoperations to make value smaller
            long long int convertedInt = stoi(inputData[i]);
            block->SetData(&convertedInt, sizeof(int));
        }
        else if(columnType == ColumnType::String)//ths poutanas tha ginei dictionary encoding incoming
        {

            const uint32_t primaryHashKey = this->database->InsertToHashTables(inputData[i].c_str());
            block->SetData(&primaryHashKey, sizeof(uint32_t));
        }
        else
            throw invalid_argument("Unsupported column type");

        this->columns[i]->InsertBlock(block);
    }
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

        const uint32_t primaryHashKey = this->database->Hash(castBlockData);

        temp = reinterpret_cast<const unsigned char*>(&primaryHashKey);
    }
    else
        temp = searchBlockData;

    int rowIndex = 0;
    for(const auto& columnBlock : this->columns[blockIndex]->GetData())
    {
        if(memcmp(columnBlock->GetBlockData(), temp, columnBlock->GetBlockSize()) == 0)
        {
            vector<Block*> selectedBlocks;
            if(selectedColumns.empty())
                for(const auto& column : this->columns)
                       selectedBlocks.push_back(column->GetData()[rowIndex]);
            else
                for(const auto& column : selectedColumns)
                    selectedBlocks.push_back(this->columns[column->GetColumnIndex()]->GetData()[rowIndex]);

            selectedRows.emplace_back(*this, selectedBlocks);
        }
        rowIndex++;
    }

    return selectedRows;
}

void Table::PrintTable(size_t maxNumberOfItems) const
{
    if(maxNumberOfItems == -1)
        maxNumberOfItems = this->columns[0]->GetData().size();

    for (const auto& column : columns)
    {
        const auto& columnData = column->GetData();
        cout << column->GetColumnName() << " || ";
    }
}

void Table::CastPropertyToAppropriateType(void* data, Column* column, size_t& dataSize)
{
    const ColumnType columnType = column->GetColumnType();

    if(columnType == ColumnType::Integer)
        cout<< *static_cast<int*>(data) << "\n";
    else if(columnType == ColumnType::String)
        cout<< *static_cast<string*>(data) << '\n';
    else
        throw invalid_argument("Unsupported column type");
}

int* Table::CastPropertyToInt(void* data){ return static_cast<int*>(data); }

string* Table::CastPropertyToString(void* data){ return static_cast<string*>(data);}
size_t generateRandomSeed() {
    // Generate a random seed using the current time or another source
    std::random_device rd;
    return rd();  // Random seed
}

size_t Table::GetNumberOfColumns() const { return this->columns.size();}

string& Table::GetTableName(){ return this->tableName; }

size_t& Table::GetMaxRowSize(){ return this->maxRowSize; }
