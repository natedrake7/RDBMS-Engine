#pragma once

#include <string>
#include <vector>
#include <random>
#include "../Constants.h"
#include "../../AdditionalLibraries/AdditionalObjects/RowCondition.h"
#include "../../AdditionalLibraries/AdditionalObjects/Field/Field.h"

using namespace std;

typedef struct TableMetaData {
    metadata_literal_t tableNameSize;
    string tableName;
    row_size_t maxRowSize;
    page_id_t firstPageId;
    page_id_t lastPageId;
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
class LargeDataPage;
struct DataObject;

class Table {
    TableMetaData metadata;
    vector<Column*> columns;
    vector<Row*> rows;
    Database* database;

    protected:
        void InsertLargeObjectToPage(Row* row, uint16_t offset, const vector<uint16_t>& largeBlocksIndexes);
        LargeDataPage* GetOrCreateLargeDataPage();
        void InsertRow(const vector<Field>& inputData);
        static void LinkLargePageDataObjectChunks(DataObject* dataObject, const page_id_t& lastLargePageId, const large_page_index_t& objectIndex);
        static void InsertLargeDataObjectPointerToRow(Row* row
                            , const page_offset_t& offset
                            , const large_page_index_t& objectIndex
                            , const page_id_t& lastLargePageId
                            , const column_index_t& largeBlockIndex);


    public:
        Table(const string& tableName,const vector<Column*>& columns, Database* database);

        Table(const TableMetaData &tableMetaData, const vector<Column*>& columns, Database* database);

        ~Table();

        void InsertRows(const vector<vector<Field>>& inputData);

        vector<Row> GetRowByBlock(const Block& block, const vector<Column*>& selectedColumns = vector<Column*>()) const;

        string& GetTableName();

        row_size_t& GetMaxRowSize();

        void PrintTable(size_t maxNumberOfItems = -1) const;

        column_number_t GetNumberOfColumns() const;

        const TableMetaData& GetTableMetadata() const;

        const vector<Column*>& GetColumns() const;

        LargeDataPage* GetLargeDataPage(const page_id_t& pageId) const;

        void SelectRows(vector<Row>* selectedRows, const vector<RowCondition*>* conditions = nullptr, const size_t& count = -1) const;

        void UpdateRows();
};
