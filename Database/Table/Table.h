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
    page_id_t indexAllocationMapPageId;
    column_number_t numberOfColumns;
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
    TableHeader header;
    vector<Column*> columns;
    Database* database;

    protected:
        void InsertLargeObjectToPage(Row* row);
        LargeDataPage* GetOrCreateLargeDataPage();
        static void LinkLargePageDataObjectChunks(DataObject* dataObject, const page_id_t& lastLargePageId, const large_page_index_t& objectIndex);
        void InsertLargeDataObjectPointerToRow(Row* row
                            , const bool& isFirstRecursion
                            , const large_page_index_t& objectIndex
                            , const page_id_t& lastLargePageId
                            , const column_index_t& largeBlockIndex);
        void RecursiveInsertToLargePage(Row*& row
                                        , page_offset_t& offset
                                        , const column_index_t& columnIndex
                                        , block_size_t& remainingBlockSize
                                        , const bool& isFirstRecursion
                                        , DataObject** previousDataObject);
        void InsertRow(const vector<Field>& inputData, vector<extent_id_t>& allocatedExtents, extent_id_t& startingExtentIndex);

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

        void Select(vector<Row>& selectedRows, const vector<RowCondition*>* conditions = nullptr, const size_t& count = -1) const;

        void Update(const vector<Block*>* updates, const vector<RowCondition*>* conditions = nullptr);

        void UpdateIndexAllocationMapPageId(const page_id_t& indexAllocationMapPageId);
    
        bool IsColumnNullable(const column_index_t& columnIndex) const;

        void AddColumn(Column* column);

        const table_id_t& GetTableId() const;

        void InsertRow(const vector<Field>& inputData);

};
