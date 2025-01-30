#include "Column.h"
#include "../Table/Table.h"

namespace DatabaseEngine::StorageTypes {
     Column::Column(const string& columnName, const string& columnTypeLiteral, const row_size_t&  recordSize, const bool& allowNulls)
    {
        this->header.columnName = columnName;
        this->header.columnTypeLiteral = columnTypeLiteral;
        this->header.recordSize = recordSize;
        this->allowNulls = allowNulls;
        this->header.columnType = this->SetColumnType();
        this->header.columnIndex = 0;
        this->table = nullptr;
    }

    Column::Column(const ColumnHeader& header, const Table* table)
    {
        this->header = header;
        this->allowNulls = false;
        this->table = table;
    }

    Column::~Column() = default;

    string& Column::GetColumnName() { return this->header.columnName; }

    const ColumnType& Column::GetColumnType() const { return this->header.columnType; }

    const row_size_t& Column::GetColumnSize() const { return this->header.recordSize; }

    bool Column::IsColumnNullable() const { return this->table->IsColumnNullable(this->header.columnIndex); }

    const bool& Column::GetAllowNulls() const { return this->allowNulls; }

    const column_index_t& Column::GetColumnIndex() const { return this->header.columnIndex; }

    const ColumnHeader& Column::GetColumnHeader() const { return this->header; }

    bool Column::isColumnLOB() const { return this->header.recordSize >= LARGE_DATA_OBJECT_SIZE; }

    void Column::SetColumnIndex(const column_index_t& columnIndex) { this->header.columnIndex = columnIndex; }

    ColumnType Column::SetColumnType() const {
        if (this->header.columnTypeLiteral == "TinyInt")
            return ColumnType::TinyInt;
        if (this->header.columnTypeLiteral == "SmallInt")
            return ColumnType::SmallInt;
        if (this->header.columnTypeLiteral == "Int")
            return ColumnType::Int;
        if (this->header.columnTypeLiteral == "BigInt")
            return ColumnType::BigInt;
        if (this->header.columnTypeLiteral == "Decimal")
            return ColumnType::Decimal;
        if (this->header.columnTypeLiteral == "String")
            return ColumnType::String;
        if (this->header.columnTypeLiteral == "UnicodeString")
            return ColumnType::UnicodeString;
        if (this->header.columnTypeLiteral == "Bool")
            return ColumnType::Bool;
        if (this->header.columnTypeLiteral == "DateTime")
            return ColumnType::DateTime;

        throw runtime_error("Column type not recognized");
    }   
}