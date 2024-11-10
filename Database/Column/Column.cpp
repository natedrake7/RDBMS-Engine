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

ColumnType& Column::GetColumnType() { return this->columnType; }

size_t& Column::GetColumnSize() { return this->recordSize; }

bool& Column::GetAllowNulls() { return this->allowNulls;}

size_t& Column::GetColumnIndex() { return this->columnIndex; }

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



