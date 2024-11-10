#ifndef TABLE_H
#define TABLE_H

#include <string>
#include <vector>
#include <random>
#include "../Column/Column.h"
#include "../Row/Row.h"

using namespace std;

class Row;
class Block;

class Table {

    private:
        string tableName;
        size_t maxRowSize;
        vector<Column*> columns;
        vector<Row*> rows;

    protected:
        static void CastPropertyToAppropriateType(void* data, Column* column, size_t& dataSize);
        static int* CastPropertyToInt(void* data);
        static string* CastPropertyToString(void* data);
        static size_t HashColumn(Column* column, const size_t& numOfColumns);

    public:
        Table(const string& tableName,const vector<Column*>& columns);

        ~Table();

        void InsertRow(vector<string>& inputData);

        vector<Row*> GetRowByBlock(const Block &block) const;

        string& GetTableName();

        size_t& GetMaxRowSize();

        void PrintTable(size_t maxNumberOfItems = -1) const;

        size_t GetNumberOfColumns() const;
};



#endif //TABLE_H
