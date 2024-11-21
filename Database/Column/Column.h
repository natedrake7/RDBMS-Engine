#pragma once

#include <string>
#include <stdexcept>
#include <vector>

class Block;
using namespace std;

enum class ColumnType {
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
    uint16_t columnNameSize;
    string columnName;
    uint16_t columnTypeLiteralSize;
    string columnTypeLiteral;
    ColumnType columnType;
    uint16_t columnIndex;
    uint32_t recordSize;
    bool allowNulls;
}ColumnMetadata;

class Column {
    ColumnMetadata metadata;

    protected:
        ColumnType SetColumnType() const;

    public:
        Column(const string& columnName, const string&  recordType, const int&  recordSize, const bool& allowNulls);

        explicit Column(const ColumnMetadata& metadata);

        string& GetColumnName();

        const ColumnType& GetColumnType() const;

        const uint32_t& GetColumnSize() const;

        bool& GetAllowNulls();

        void SetColumnIndex(const uint16_t& columnIndex);

        const uint16_t& GetColumnIndex() const;

        const ColumnMetadata& GetColumnMetadata() const;
};