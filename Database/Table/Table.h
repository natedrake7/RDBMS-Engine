#pragma once

#include <string>
#include <vector>
#include <random>
#include "../Constants.h"
#include "../../AdditionalLibraries/AdditionalObjects/RowCondition.h"
#include "../../AdditionalLibraries/AdditionalObjects/Field/Field.h"
#include "../../AdditionalLibraries/BitMap/BitMap.h"

using namespace std;

typedef struct TableHeader {
    table_id_t tableId;
    header_literal_t tableNameSize;
    string tableName;
    row_size_t maxRowSize;
    page_id_t firstPageId;
    page_id_t lastPageId;
    column_number_t numberOfColumns;
    extent_id_t lastExtentId;
    
    BitMap* columnsNullBitMap;
    
    TableHeader();
    ~TableHeader();
    TableHeader& operator=(const TableHeader& tableHeader);
}TableHeader;

#include "../Column/Column.h"
#include "../Row/Row.h"
#include "../Database.h"
#include "../../AdditionalLibraries/SafeConverter/SafeConverter.h"

typedef struct TableFullHeader {
    TableHeader tableHeader;
    vector<ColumnHeader> columnsHeaders;

    TableFullHeader();
    TableFullHeader(const TableFullHeader& tableHeader);
}TableFullHeader;

class Page;
class Database;
class Row;
class Block;
class LargeDataPage;
struct DataObject;

class Table {
    TableHeader metadata;
    vector<Column*> columns;
    Database* database;

    protected:
        void InsertLargeObjectToPage(Row* row, page_offset_t offset, const vector<column_index_t>& largeBlocksIndexes);
        LargeDataPage* GetOrCreateLargeDataPage();
        void InsertRow(const vector<Field>& inputData);
        static void LinkLargePageDataObjectChunks(DataObject* dataObject, const page_id_t& lastLargePageId, const large_page_index_t& objectIndex);
        static void InsertLargeDataObjectPointerToRow(Row* row
                            , const page_offset_t& offset
                            , const large_page_index_t& objectIndex
                            , const page_id_t& lastLargePageId
                            , const column_index_t& largeBlockIndex);


    public:
        Table(const string& tableName, const table_id_t& tableId, const vector<Column*>& columns, Database* database);

        Table(const TableHeader &tableHeader, Database* database);

        ~Table();

        void InsertRows(const vector<vector<Field>>& inputData);

        string& GetTableName();

        row_size_t& GetMaxRowSize();

        column_number_t GetNumberOfColumns() const;

        const TableHeader& GetTableHeader() const;

        const vector<Column*>& GetColumns() const;

        LargeDataPage* GetLargeDataPage(const page_id_t& pageId) const;

        void SelectRows(vector<Row>* selectedRows, const vector<RowCondition*>* conditions = nullptr, const size_t& count = -1) const;

        void UpdateRows(const vector<Block>* updates, const vector<RowCondition*>* conditions = nullptr);

        void UpdateFirstPageId(const page_id_t& firstPageId);
    
        void UpdateLastPageId(const page_id_t& lastPageId);

        void UpdateLastExtentId(const extent_id_t& lastExtentId);

        bool IsColumnNullable(const column_index_t& columnIndex) const;

        void AddColumn(Column* column);

        const table_id_t& GetTableId() const;
};
