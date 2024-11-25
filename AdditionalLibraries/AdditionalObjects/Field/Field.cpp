#include "Field.h"

Field::Field()
{
    this->data = nullptr;
    this->isNull = true;
}

Field::Field(const string &data, const bool &isNull)
{
    this->data = data;
    this->isNull = isNull;
}

Field::~Field() = default;

const string& Field::GetData() const { return this->data; }

const bool & Field::GetIsNull() const { return this->isNull; }

void Field::SetData(const string &data) { this->data = data; }

void Field::SetIsNull(const bool &isNull) { this->isNull = isNull; }

