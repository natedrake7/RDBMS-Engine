#include "Field.h"

Field::Field()
{
    this->data = nullptr;
    this->isNull = true;
    this->columnIndex = 0;
}

Field::Field(const string &data, const column_index_t& columnIndex, const bool &isNull)
{
    this->data = data;
    this->isNull = isNull;
    this->columnIndex = columnIndex;
}

Field::~Field() = default;

const string& Field::GetData() const { return this->data; }

const bool & Field::GetIsNull() const { return this->isNull; }

const column_index_t & Field::GetColumnIndex() const { return this->columnIndex;}

void Field::SetData(const string &data) { this->data = data; }

void Field::SetIsNull(const bool &isNull) { this->isNull = isNull; }

void Field::SetColumnIndex(const column_index_t &columnIndex) { this->columnIndex = columnIndex; }