#include "Column.h"

Column::Column(const string& columnName, const string&  recordType, const int&  recordSize, const bool& allowNulls)
{
    this->columnName = columnName;
    this->recordType = recordType;
    this->recordSize = recordSize;
    this->allowNulls = allowNulls;
    this->hashIndex = -1;
}

string& Column::GetColumnName() { return this->columnName; }

string& Column::GetColumnType() { return this->recordType; }

size_t& Column::GetColumnSize() { return this->recordSize; }

bool& Column::GetAllowNulls() { return this->allowNulls;}

size_t& Column::GetColumnHashIndex() { return this->hashIndex; }

void Column::SetColumnHashIndex(const size_t& hashIndex) { this->hashIndex = hashIndex; }


