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

        if(columnType == ColumnType::Int)
        {
            int integerValue;
            memcpy(&integerValue, this->data[i]->GetBlockData(), sizeof(int));
            cout<< integerValue;
        }
        else if(columnType == ColumnType::String)
        {
            uint64_t hashKey;
            memcpy(&hashKey, this->data[i]->GetBlockData(), sizeof(uint64_t));
            // const uint64_t hashKey = reinterpret_cast<const uint64_t>(this->data);
            cout << db->GetStringByHashKey(hashKey);
        }

        if(i == this->data.size() - 1)
            cout << '\n';
        else
            cout << " || ";
    }
}


