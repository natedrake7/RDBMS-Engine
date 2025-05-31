#include "Column.h"
#include "../../AdditionalLibraries/StringFunctions/StringFunctions.h"
#include "../Table/Table.h"

namespace DatabaseEngine::StorageTypes {
    

     Column::Column(const std::string& columnName, const ColumnType& type, const row_size_t&  recordSize, const column_index_t& index, const bool& allowNulls)
    {
        this->name = columnName;
        this->header.recordSize = recordSize;
        this->allowNulls = allowNulls;
        this->header.columnType = type;
        this->header.columnIndex = index;
        this->table = nullptr;
        this->isOverflowed = false;
    }

    Column::Column(const Headers::ColumnHeader& masterDbHeader, const Table* table)
    {
        this->header.id = masterDbHeader.id;
        this->name = masterDbHeader.name;
        this->allowNulls = masterDbHeader.isNullable;
        this->header.columnType = static_cast<Constants::ColumnType>(masterDbHeader.dataType);
        this->header.recordSize = masterDbHeader.recordSize;
        this->header.columnIndex = masterDbHeader.ordinalPosition;
        this->table = table;
        this->isOverflowed = false;
    }

    Column::Column(const Headers::sysColumn& header, const column_index_t& tablePos , const Table* table)
    {
        const auto normalizedType = AdditionalLibraries::NormalizeString(header.type);

        this->name = header.name;
        this->allowNulls = false;
        this->header.columnType = ColumnTypesDictionary.Get(normalizedType);

        const auto size = ColumnTypeSizes.Get(normalizedType);

        this->header.recordSize = size == 0 ? header.size : size;
        this->header.columnIndex = tablePos;
        this->table = table;
        this->isOverflowed = false;
    }

    Column::~Column() = default;

    string& Column::GetColumnName() { return this->name; }

    const ColumnType& Column::GetColumnType() const { return this->header.columnType; }

    const row_size_t& Column::GetColumnSize() const { return this->header.recordSize; }

    bool Column::IsColumnNullable() const { return this->table->IsColumnNullable(this->header.columnIndex); }

    const bool& Column::GetAllowNulls() const { return this->allowNulls; }

    const column_index_t& Column::GetColumnIndex() const { return this->header.columnIndex; }

    const ColumnHeader& Column::GetColumnHeader() const { return this->header; }

    bool Column::isColumnLOB() const { return this->header.recordSize >= LARGE_DATA_OBJECT_SIZE; }

    void Column::SetColumnIndex(const column_index_t& columnIndex) { this->header.columnIndex = columnIndex; }

    bool Column::isColumnOverflowed() const{ return this->isOverflowed; }

    void Column::SetIsOverflowed(const bool & isOverflowed){ this->isOverflowed = isOverflowed; }

    const int32_t& Column::GetColumnId() const{ return this->header.id; }
}