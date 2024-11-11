#include "Row.h"

Row::Row(const Table& table)
{
    this->table = &table;
    this->data.resize(this->table->GetNumberOfColumns());
}

Row::Row(const Table& table, const vector<Block*>& data) {
    this->table = &table;
    this->data = data;
}