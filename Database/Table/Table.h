#ifndef TABLE_H
#define TABLE_H

#include <string>
#include <vector>
#include <random>
#include "../Column/Column.h"
#include "../Row/Row.h"
#include "../Database.h"
#include "../../AdditionalLibraries/SafeConverter/SafeConverter.h"

using namespace std;

class Database;
class Row;
class Block;

class Table {
    string tableName;
    size_t maxRowSize;
    vector<Column*> columns;
    vector<Row*> rows;
    const Database* database;

    public:
        Table(const string& tableName,const vector<Column*>& columns, const Database* database);

        ~Table();

        void InsertRow(const vector<string>& inputData);

        vector<Row> GetRowByBlock(const Block& block, const vector<Column*>& selectedColumns = vector<Column*>()) const;

        string& GetTableName();

        size_t& GetMaxRowSize();

        void PrintTable(size_t maxNumberOfItems = -1) const;

        size_t GetNumberOfColumns() const;
};



#endif //TABLE_H
