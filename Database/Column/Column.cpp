#include "Column.h"

Column::Column(const string& columnName, const string&  columnTypeLiteral, const int&  recordSize, const bool& allowNulls)
{
    this->columnName = columnName;
    this->columnTypeLiteral = columnTypeLiteral;
    this->recordSize = recordSize;
    this->allowNulls = allowNulls;
    this->columnType = this->SetColumnType();
    this->columnIndex = -1;
}

string& Column::GetColumnName() { return this->columnName; }

const ColumnType& Column::GetColumnType() const { return this->columnType; }

const size_t& Column::GetColumnSize() const { return this->recordSize; }

bool& Column::GetAllowNulls() { return this->allowNulls;}

const size_t& Column::GetColumnIndex() const { return this->columnIndex; }

void Column::InsertBlock(Block *block) { this->data.push_back(block); }

const vector<Block*>& Column::GetData() const { return this->data; }

void Column::SetColumnIndex(const size_t& columnIndex) { this->columnIndex = columnIndex; }

ColumnType Column::SetColumnType() const {
    if(this->columnTypeLiteral == "Int")
        return ColumnType::Integer;
    if(this->columnTypeLiteral == "Float")
        return ColumnType::Float;
    if(this->columnTypeLiteral == "String")
        return ColumnType::String;
    if(this->columnTypeLiteral == "Bool")
        return ColumnType::Bool;

    throw runtime_error("Column type not recognized");
}



