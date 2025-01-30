#include "Field.h"

using namespace Constants;

Field::Field()
{
    this->isNull = true;
    this->columnIndex = 0;
}

Field::Field(const string &data, const Constants::column_index_t& columnIndex, const bool &isNull)
{
    this->data = data;
    this->isNull = isNull;
    this->columnIndex = columnIndex;
}

Field::Field(const u16string &data, const Constants::column_index_t &columnIndex, const bool &isNull)
{
    this->unicodeData = data;
    this->isNull = isNull;
    this->columnIndex = columnIndex;
}

Field::Field(const string& data, const Constants::column_index_t& columnIndex , const Operator& operatorType, const ConditionType& conditionType, const bool& isNull)
{
    this->data = data;
    this->columnIndex = columnIndex;
    this->operatorType = operatorType;
    this->conditionType = conditionType;
    this->isNull = isNull;
}

Field::~Field() = default;

const string& Field::GetData() const { return this->data; }

const u16string & Field::GetUnicodeData() const { return this->unicodeData; }

const bool & Field::GetIsNull() const { return this->isNull; }

const Constants::column_index_t & Field::GetColumnIndex() const { return this->columnIndex;}

void Field::SetData(const string &data) { this->data = data; }

void Field::SetIsNull(const bool &isNull) { this->isNull = isNull; }

void Field::SetColumnIndex(const Constants::column_index_t &columnIndex) { this->columnIndex = columnIndex; }

const Constants::ConditionType& Field::GetConditionType() const { return this->conditionType; }

const Constants::Operator& Field::GetOperatorType() const { return this->operatorType; }

const vector<Field>& Field::GetChildren() const { return this->children; }