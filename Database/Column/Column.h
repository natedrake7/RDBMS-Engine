#ifndef COLUMN_H
#define COLUMN_H

#include <string>
#include <stdexcept>

using namespace std;

enum class ColumnType {
    SmallInt = 0,
    Integer = 1,
    Long = 2,
    Float = 3,
    Double = 4,
    LongDouble = 5,
    String = 6,
    UnicodeString = 7,
    Bool = 8
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

        ColumnType& GetColumnType();

        size_t& GetColumnSize();

        bool& GetAllowNulls();

        void SetColumnIndex(const size_t& columnIndex);

        size_t& GetColumnIndex();
};



#endif //COLUMN_H
