#ifndef COLUMN_H
#define COLUMN_H

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

class Column {
    private:
        string columnName;
        string columnTypeLiteral;
        ColumnType columnType;
        size_t columnIndex;
        size_t recordSize;
        bool allowNulls;

    protected:
        ColumnType SetColumnType() const;

    public:
        Column(const string& columnName, const string&  recordType, const int&  recordSize, const bool& allowNulls);

        string& GetColumnName();

        const ColumnType& GetColumnType() const;

        const size_t& GetColumnSize() const;

        bool& GetAllowNulls();

        void SetColumnIndex(const size_t& columnIndex);

        const size_t& GetColumnIndex() const;

        void InsertBlock(Block* block);
};



#endif //COLUMN_H
