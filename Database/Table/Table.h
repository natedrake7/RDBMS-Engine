#pragma once

#include <string>
#include <vector>
#include <random>

using namespace std;

typedef struct TableMetaData {
    int tableNameSize;
    string tableName;
    size_t maxRowSize;
    uint16_t firstPageId;
    uint16_t lastPageId;
    uint16_t numberOfColumns;
}TableMetaData;

#include "../Column/Column.h"
#include "../Row/Row.h"
#include "../Database.h"
#include "../../AdditionalLibraries/SafeConverter/SafeConverter.h"

typedef struct TableFullMetaData {
    TableMetaData tableMetaData;
    vector<ColumnMetadata> columnsMetaData;

    TableFullMetaData();
    TableFullMetaData(const TableFullMetaData& tableMetaData);
}TableFullMetaData;

class Page;
class Database;
class Row;
class Block;

class Table {
    TableMetaData metadata;
    vector<Column*> columns;
    vector<Row*> rows;
    Database* database;

    public:
        Table(const string& tableName,const vector<Column*>& columns, Database* database);

        Table(const TableMetaData &tableMetaData, const vector<Column*>& columns, Database* database);

        ~Table();

        void InsertRow(const vector<string>& inputData);

        vector<Row> GetRowByBlock(const Block& block, const vector<Column*>& selectedColumns = vector<Column*>()) const;

        string& GetTableName();

        size_t& GetMaxRowSize();

        void PrintTable(size_t maxNumberOfItems = -1) const;

        uint16_t GetNumberOfColumns() const;

        const TableMetaData& GetTableMetadata() const;

        const vector<Column*>& GetColumns() const;

        vector<Row> SelectRows(const size_t& count = -1) const;
};