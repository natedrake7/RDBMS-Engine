#pragma once

#include <string>
#include <stdexcept>
#include <vector>
#include "../Constants.h"

class Block;
using namespace std;

enum class ColumnType : uint8_t {
    TinyInt = 0,
    SmallInt = 1,
    Int = 2,
    BigInt = 3,
    Decimal = 4,
    Double = 5,
    LongDouble = 6,
    String = 7,
    UnicodeString = 8,
    Bool = 9
};

typedef struct ColumnMetadata {
    metadata_literal_t columnNameSize;
    string columnName;
    metadata_literal_t columnTypeLiteralSize;
    string columnTypeLiteral;
    ColumnType columnType;
    column_index_t columnIndex;
    record_size_t recordSize;
    bool allowNulls;
}ColumnMetadata;

class Column {
    ColumnMetadata metadata;

    protected:
        ColumnType SetColumnType() const;

    public:
        Column(const string& columnName, const string&  recordType, const record_size_t&  recordSize, const bool& allowNulls);

        explicit Column(const ColumnMetadata& metadata);

        string& GetColumnName();

        const ColumnType& GetColumnType() const;

        const record_size_t& GetColumnSize() const;

        bool& GetAllowNulls();

        void SetColumnIndex(const column_index_t& columnIndex);

        const column_index_t& GetColumnIndex() const;

        const ColumnMetadata& GetColumnMetadata() const;
};