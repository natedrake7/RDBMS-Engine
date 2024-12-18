#pragma once
#include <string>
#include <vector>
#include <random>
#include "../Constants.h"
#include "../Database.h"
using namespace std;
using namespace Constants;

class RowCondition;
class Field;

namespace DatabaseEngine {
    class Database;
    
    namespace StorageTypes {
        class Row;
    }
}

namespace Pages {
    class Page;
    class LargeDataPage;
    struct DataObject;
}

namespace ByteMaps {
    class BitMap;
}

namespace DatabaseEngine::StorageTypes {
    enum class ColumnType : uint8_t;
    class Block;
    class Column;
    struct ColumnHeader;

     typedef struct TableHeader {
        table_id_t tableId;

        header_literal_t tableNameSize;
        string tableName;

        row_size_t maxRowSize;
        page_id_t indexAllocationMapPageId;
        column_number_t numberOfColumns;
        
        page_id_t clusteredIndexPageId;
        ByteMaps::BitMap* columnsNullBitMap;


        //bitmaps to store the composite key 
        ByteMaps::BitMap* clusteredIndexes;
        ByteMaps::BitMap* nonClusteredIndexes;
        
        TableHeader();
        ~TableHeader();
        TableHeader& operator=(const TableHeader& tableHeader);
    }TableHeader;

    typedef struct TableFullHeader {
        TableHeader tableHeader;
        vector<ColumnHeader> columnsHeaders;

        TableFullHeader();
        TableFullHeader(const TableFullHeader& tableHeader);
    }TableFullHeader;

    class Table {
        TableHeader header;
        vector<Column*> columns;
        Database* database;

        protected:
            void InsertLargeObjectToPage(Row* row);
            [[nodiscard]] Pages::LargeDataPage* GetOrCreateLargeDataPage() const;
            static void LinkLargePageDataObjectChunks(Pages::DataObject* dataObject, const page_id_t& lastLargePageId, const large_page_index_t& objectIndex);
            void InsertLargeDataObjectPointerToRow(Row* row
                                , const bool& isFirstRecursion
                                , const large_page_index_t& objectIndex
                                , const page_id_t& lastLargePageId
                                , const column_index_t& largeBlockIndex) const;
            void RecursiveInsertToLargePage(Row*& row
                                            , page_offset_t& offset
                                            , const column_index_t& columnIndex
                                            , block_size_t& remainingBlockSize
                                            , const bool& isFirstRecursion
                                            , Pages::DataObject** previousDataObject);
            void InsertRow(const vector<Field>& inputData, vector<extent_id_t>& allocatedExtents, extent_id_t& startingExtentIndex);
            static void SetBlockDataByColumnType(Block *&block, const ColumnType &columnType, const Field& inputData);

        public:
            Table(const string& tableName, const table_id_t& tableId, const vector<Column*>& columns, DatabaseEngine::Database* database);

            Table(const TableHeader &tableHeader, Database* database);

            ~Table();

            void InsertRows(const vector<vector<Field>>& inputData);

            string& GetTableName();

            row_size_t& GetMaxRowSize();

            [[nodiscard]] column_number_t GetNumberOfColumns() const;

            [[nodiscard]] const TableHeader& GetTableHeader() const;

            [[nodiscard]] const vector<Column*>& GetColumns() const;

            [[nodiscard]] Pages::LargeDataPage* GetLargeDataPage(const page_id_t& pageId) const;

            void Select(vector<Row>& selectedRows, const vector<RowCondition*>* conditions = nullptr, const size_t& count = -1) const;

            void Update(const vector<Field>& updates, const vector<RowCondition*>* conditions) const;

            void UpdateIndexAllocationMapPageId(const page_id_t& indexAllocationMapPageId);
        
            [[nodiscard]] bool IsColumnNullable(const column_index_t& columnIndex) const;

            void AddColumn(Column* column);

            [[nodiscard]] const table_id_t& GetTableId() const;

            void InsertRow(const vector<Field>& inputData);

            [[nodiscard]] TableType GetTableType() const;

            [[nodiscard]] row_size_t GetMaximumRowSize() const;
    };   
}
