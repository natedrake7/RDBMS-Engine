#include "Row.h"

Row::Row(const Table& table)
{
    this->table = &table;
    this->data.resize(this->table->GetNumberOfColumns());
    this->rowSize = new size_t(0);
    this->isOriginalRow = true;
}

Row::Row(const Table& table, const vector<Block*>& data) {
    this->table = &table;
    this->data = data;
    this->isOriginalRow = false;
}

Row::~Row(){
    if(this->isOriginalRow)
    {
        for(const auto& block : this->data)
            delete block;

        delete this->rowSize;
    }
}

void Row::InsertColumnData(Block *block, const size_t &columnIndex)
{
    this->data[columnIndex] = block;
    this->rowSize += block->GetBlockSize();
}

const vector<Block *> & Row::GetData() const { return this->data; }

void Row::PrintRow(const Database* db) const
{
    for(size_t i = 0; i < this->data.size(); i++)
    {
        const ColumnType columnType = this->data[i]->GetColumnType();

        if (columnType == ColumnType::TinyInt)
        {
            int8_t tinyIntValue;
            memcpy(&tinyIntValue, this->data[i]->GetBlockData(), sizeof(int8_t));
            cout << static_cast<int>(tinyIntValue);
        }
        else if (columnType == ColumnType::SmallInt)
        {
            int16_t smallIntValue;
            memcpy(&smallIntValue, this->data[i]->GetBlockData(), sizeof(int16_t));
            cout << smallIntValue;
        }
        else if(columnType == ColumnType::Int)
        {
            int32_t intValue;
            memcpy(&intValue, this->data[i]->GetBlockData(), sizeof(int32_t));
            cout << intValue;
        }
        else if (columnType == ColumnType::BigInt)
        {
            int64_t bigIntValue;
            memcpy(&bigIntValue, this->data[i]->GetBlockData(), sizeof(int64_t));
            cout << bigIntValue;
        }
        else if(columnType == ColumnType::String)
        {
            // const char* hashKey;
            // memcpy(&hashKey, this->data[i]->GetBlockData(), sizeof(uint64_t));
            // const uint64_t hashKey = reinterpret_cast<const uint64_t>(this->data);
            cout << reinterpret_cast<const char*>(this->data[i]->GetBlockData());
        }

        if(i == this->data.size() - 1)
            cout << '\n';
        else
            cout << " || ";
    }
}


