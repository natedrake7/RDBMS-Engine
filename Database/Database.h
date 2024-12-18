#pragma once
#include <mutex>
#include <string>
#include <vector>
#include "Constants.h"

using namespace Constants;
using namespace std;

class Block;
class Column;
class Row;
class RowCondition;

class Table;
struct TableFullHeader;

namespace Storage {
    class FileManager;
    class PageManager;
}

namespace Pages {
    class Page;
    class LargeDataPage;
    class PageFreeSpacePage;
    class IndexAllocationMapPage;
}

namespace DatabaseEngine{
    enum
    {
        MAX_TABLE_SIZE = 10*1024
    };

    typedef struct DatabaseHeader{
        table_number_t numberOfTables;
        header_literal_t databaseNameSize;
        string databaseName;
        table_id_t lastTableId;
        page_id_t lastPageFreeSpacePageId;
        page_id_t lastGamPageId;

        DatabaseHeader();
        DatabaseHeader(const string &databaseName, const table_number_t& numberOfTables, const page_id_t& lastPageFreeSpacePageId, const page_id_t& lastGamPageId);
        DatabaseHeader(const DatabaseHeader& dbHeader);
        DatabaseHeader& operator=(const DatabaseHeader &dbHeader);
    }DatabaseHeader;
    
    class Database {
        DatabaseHeader header;
        string filename;
        string fileExtension;
        vector<Table*> tables;
        Storage::PageManager* pageManager;
        mutex pageSelectMutex;

        protected:
            void ValidateTableCreation(Table* table) const;
            void WriteHeaderToFile();
            static bool IsSystemPage(const page_id_t& pageId);
            static page_id_t GetGamAssociatedPage(const page_id_t& pageId);
            static page_id_t GetPfsAssociatedPage(const page_id_t& pageId);
            static page_id_t CalculateSystemPageOffset(const page_id_t& pageId);
            static Constants::byte GetObjectSizeToCategory(const row_size_t& size);
            bool AllocateNewExtent( Pages::PageFreeSpacePage**  pageFreeSpacePage
                                , page_id_t* lowerLimit
                                , page_id_t* newPageId
                                , extent_id_t* newExtentId
                                , const table_id_t& tableId);
            const Table* GetTable(const table_id_t& tableId) const;
            void ThreadSelect(const Table* table
                , const Pages::IndexAllocationMapPage* tableMapPage
                , const extent_id_t& extentId
                , const size_t& rowsToSelect
                , const vector<RowCondition*>* conditions
                , vector<Row>* selectedRows);
            bool InsertRowToClusteredIndex(const Table& table, vector<extent_id_t>& allocatedExtents, extent_id_t& lastExtentIndex, Row* row) const;
            bool InsertRowToHeapTable(const Table& table, vector<extent_id_t>& allocatedExtents, extent_id_t& lastExtentIndex, Row* row) const;
        
        public:
            explicit Database(const string& dbName, Storage::PageManager* pageManager);

            ~Database();

            Table* CreateTable(const string& tableName, const vector<Column*>& columns);

            void CreateTable(const TableFullHeader& tableMetaData);

            Table* OpenTable(const string& tableName) const;

            void DeleteDatabase() const;

            bool InsertRowToPage(const Table &table,vector<extent_id_t>& allocatedExtents, extent_id_t& lastExtentIndex, Row *row) const;

            void SelectTableRows(const table_id_t& tableId, vector<Row>* selectedRows, const size_t& rowsToSelect, const vector<RowCondition*>* conditions);

            void UpdateTableRows(const table_id_t& tableId, const vector<Block*>& updates, const vector<RowCondition*>* conditions);

            Pages::Page* CreateDataPage(const table_id_t& tableId);

            Pages::LargeDataPage* CreateLargeDataPage(const table_id_t& tableId);
        
            Pages::LargeDataPage* GetTableLastLargeDataPage(const table_id_t& tableId, const page_size_t& minObjectSize);

            Pages::LargeDataPage* GetLargeDataPage(const page_id_t &pageId, const table_id_t& tableId);

            void SetPageMetaDataToPfs(const Pages::Page* page) const;

            string GetFileName() const;

            static page_id_t CalculateSystemPageOffsetByExtentId(const extent_id_t& extentId);
    };

    void CreateDatabase(const string& dbName, Storage::FileManager* fileManager, Storage::PageManager* pageManager);

    void UseDatabase(const string& dbName, Database** db, Storage::PageManager* pageManager);

    void PrintRows(const vector<Row>& rows);
};