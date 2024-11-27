#include "Column.h"

#include "../Table/Table.h"

Column::Column(const string& columnName, const string& columnTypeLiteral, const row_size_t&  recordSize, const bool& allowNulls)
{
    this->metadata.columnName = columnName;
    this->metadata.columnTypeLiteral = columnTypeLiteral;
    this->metadata.recordSize = recordSize;
    this->allowNulls = allowNulls;
    this->metadata.columnType = this->SetColumnType();
    this->metadata.columnIndex = 0;
    this->table = nullptr;
}

Column::Column(const ColumnMetaData &metadata)
{
    this->metadata = metadata;
    this->allowNulls = false;
    this->table = nullptr;
}

string& Column::GetColumnName() { return this->metadata.columnName; }

const ColumnType& Column::GetColumnType() const { return this->metadata.columnType; }

const row_size_t& Column::GetColumnSize() const { return this->metadata.recordSize; }

bool Column::IsColumnNullable() const { return this->table->IsColumnNullable(this->metadata.columnIndex); }

const bool& Column::GetAllowNulls() const { return this->allowNulls; }

const column_index_t& Column::GetColumnIndex() const { return this->metadata.columnIndex; }

const ColumnMetaData& Column::GetColumnMetadata() const { return this->metadata; }

void Column::SetColumnIndex(const column_index_t& columnIndex) { this->metadata.columnIndex = columnIndex; }

ColumnType Column::SetColumnType() const {
    if (this->metadata.columnTypeLiteral == "TinyInt")
        return ColumnType::TinyInt;
    if (this->metadata.columnTypeLiteral == "SmallInt")
        return ColumnType::SmallInt;
    if (this->metadata.columnTypeLiteral == "Int")
        return ColumnType::Int;
    if (this->metadata.columnTypeLiteral == "BigInt")
        return ColumnType::BigInt;
    if (this->metadata.columnTypeLiteral == "Decimal")
        return ColumnType::Decimal;
    if (this->metadata.columnTypeLiteral == "String")
        return ColumnType::String;
    if (this->metadata.columnTypeLiteral == "Bool")
        return ColumnType::Bool;

    throw runtime_error("Column type not recognized");
}