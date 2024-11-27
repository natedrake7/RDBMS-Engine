#pragma once

#include <string>
#include <stdexcept>
#include <vector>
#include "../Constants.h"

class Table;
class Block;
using namespace std;

enum class ColumnType : uint8_t
{
    TinyInt = 0,
    SmallInt = 1,
    Int = 2,
    BigInt = 3,
    Decimal = 4,
    Double = 5,
    LongDouble = 6,
    String = 7,
    UnicodeString = 8,
    Bool = 9,
    Null = 10,
};

typedef struct ColumnMetaData {
    metadata_literal_t columnNameSize;
    string columnName;
    metadata_literal_t columnTypeLiteralSize;
    string columnTypeLiteral;
    ColumnType columnType;
    column_index_t columnIndex;
    row_size_t recordSize;
}ColumnMetaData;

class Column {
    ColumnMetaData metadata;
    const Table* table;
    bool allowNulls;

    protected:
        ColumnType SetColumnType() const;

    public:
        Column(const string& columnName, const string&  recordType, const row_size_t&  recordSize, const bool& allowNulls);

        explicit Column(const ColumnMetaData& metadata, const Table* table);

        string& GetColumnName();

        const ColumnType& GetColumnType() const;

        const row_size_t& GetColumnSize() const;

        bool IsColumnNullable() const;

        const bool& GetAllowNulls() const;

        void SetColumnIndex(const column_index_t& columnIndex);

        const column_index_t& GetColumnIndex() const;

        const ColumnMetaData& GetColumnMetadata() const;
};