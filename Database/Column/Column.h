#ifndef COLUMN_H
#define COLUMN_H

#include <string>

using namespace std;

class Column {
    private:
        string columnName;
        string recordType;
        size_t hashIndex;
        size_t recordSize;
        bool allowNulls;

    public:
        Column(const string& columnName, const string&  recordType, const int&  recordSize, const bool& allowNulls);

        string& GetColumnName();

        string& GetColumnType();

        size_t& GetColumnSize();

        bool& GetAllowNulls();

        void SetColumnHashIndex(const size_t& hashIndex);

        size_t& GetColumnHashIndex();
};



#endif //COLUMN_H
