#include "Table.h"

Table::Table(const string& tableName, const vector<Column*>& columns)
{
    this->tableName = tableName;
    this->columns = columns;
    this->maxRowSize = 0;

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

    for(const auto& row : this->rows)
        delete row;
}

void Table::InsertRow(vector<string>& inputData)
{
    Row* row = new Row(*this);
    for(size_t i = 0;i < inputData.size(); ++i)
    {
        Block* block = new Block(columns[i]);
        const ColumnType columnType = columns[i]->GetColumnType();

        if(columnType == ColumnType::Integer)
        {
            //set size only not convert to int
            int convertedInt = stoi(inputData[i]);

            block->SetData(&convertedInt, sizeof(int));
        }
        else if(columnType == ColumnType::String)
        {
            inputData[i] += '\0';

            const char* temp = inputData[i].c_str();
                            
            block->SetData(temp, inputData[i].length() + 1);
        }
        else
            throw invalid_argument("Unsupported column type");

        row->InsertColumnData(block);
    }

    this->rows.push_back(row);
    // cout<<"Row Affected: 1"<<'\n';
}

void Table::PrintTable(size_t maxNumberOfItems) const
{
    if(maxNumberOfItems == -1)
        maxNumberOfItems = this->rows.size();

    for (const auto& column : columns)
        cout << column->GetColumnName() << " || ";

    cout << endl;
    for(size_t i = 0;i < maxNumberOfItems; i++)
    {
        const vector<Block*> data = this->rows[i]->GetRowData();

        for(const auto& block : data)
            block->PrintBlockData();

        cout << endl;
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

//have a hashing algorithm for the table to map the properties to the table
size_t Table::HashColumn(Column* column, const size_t& numOfColumns)
{
    string columnData = column->GetColumnName() + to_string(column->GetColumnSize()); //+ column->GetColumnType();
    const uint8_t* data = reinterpret_cast<const uint8_t*>(columnData.c_str());
    uint32_t len = columnData.length();
    uint32_t hash = 31;
    uint32_t c1 = 0xcc9e2d51;
    uint32_t c2 = 0x1b873593;

    uint32_t roundedEnd = len / 4 * 4;
    for (uint32_t i = 0; i < roundedEnd; i += 4) {
        uint32_t k = data[i] | (data[i+1] << 8) | (data[i+2] << 16) | (data[i+3] << 24);
        k *= c1;
        k = (k << 15) | (k >> 17);
        k *= c2;

        hash ^= k;
        hash = (hash << 13) | (hash >> 19);
        hash = hash * 5 + 0xe6546b64;
    }

    uint32_t k = 0;
    switch (len & 3) {
    case 3: k ^= data[roundedEnd + 2] << 16;
    case 2: k ^= data[roundedEnd + 1] << 8;
    case 1: k ^= data[roundedEnd];
        k *= c1;
        k = (k << 15) | (k >> 17);
        k *= c2;
        hash ^= k;
    }

    hash ^= len;
    hash ^= (hash >> 16);
    hash *= 0x85ebca6b;
    hash ^= (hash >> 13);
    hash *= 0xc2b2ae35;
    hash ^= (hash >> 16);

    return hash % numOfColumns;
}

size_t Table::GetNumberOfColumns() const { return this->columns.size();}

string& Table::GetTableName(){ return this->tableName; }

size_t& Table::GetMaxRowSize(){ return this->maxRowSize; }
