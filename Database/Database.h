#pragma once
#include <mutex>
#include <string>
#include <vector>
#include "Constants.h"

using namespace Constants;
using namespace std;

class RowCondition;

namespace DatabaseEngine::StorageTypes {
    class Table;
    struct TableFullHeader;
    class Block;
    class Column;
    class Row;
}

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
        vector<StorageTypes::Table*> tables;
        Storage::PageManager* pageManager;
        mutex pageSelectMutex;

        protected:
            void ValidateTableCreation(StorageTypes::Table* table) const;
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
            [[nodiscard]] const StorageTypes::Table* GetTable(const table_id_t& tableId) const;
            void ThreadSelect(const StorageTypes::Table* table
                , const Pages::IndexAllocationMapPage* tableMapPage
                , const extent_id_t& extentId
                , const size_t& rowsToSelect
                , const vector<RowCondition*>* conditions
                , vector<StorageTypes::Row>* selectedRows);
            bool InsertRowToClusteredIndex(const StorageTypes::Table& table, vector<extent_id_t>& allocatedExtents, extent_id_t& lastExtentIndex, StorageTypes::Row* row) const;
            bool InsertRowToHeapTable(const StorageTypes::Table& table, vector<extent_id_t>& allocatedExtents, extent_id_t& lastExtentIndex, StorageTypes::Row* row) const;
        
        public:
            explicit Database(const string& dbName, Storage::PageManager* pageManager);

            ~Database();

            StorageTypes::Table* CreateTable(const string& tableName, const vector<StorageTypes::Column*>& columns);

            void CreateTable(const StorageTypes::TableFullHeader& tableMetaData);

            [[nodiscard]] StorageTypes::Table* OpenTable(const string& tableName) const;

            void DeleteDatabase() const;

            bool InsertRowToPage(const StorageTypes::Table &table,vector<extent_id_t>& allocatedExtents, extent_id_t& lastExtentIndex, StorageTypes::Row *row) const;

            void SelectTableRows(const table_id_t& tableId, vector<StorageTypes::Row>* selectedRows, const size_t& rowsToSelect, const vector<RowCondition*>* conditions);

            void UpdateTableRows(const table_id_t& tableId, const vector<StorageTypes::Block*>& updates, const vector<RowCondition*>* conditions);

            Pages::Page* CreateDataPage(const table_id_t& tableId);

            Pages::LargeDataPage* CreateLargeDataPage(const table_id_t& tableId);
        
            Pages::LargeDataPage* GetTableLastLargeDataPage(const table_id_t& tableId, const page_size_t& minObjectSize);

            Pages::LargeDataPage* GetLargeDataPage(const page_id_t &pageId, const table_id_t& tableId);

            void SetPageMetaDataToPfs(const Pages::Page* page) const;

            [[nodiscard]] string GetFileName() const;

            static page_id_t CalculateSystemPageOffsetByExtentId(const extent_id_t& extentId);
    };

    void CreateDatabase(const string& dbName, Storage::FileManager* fileManager, Storage::PageManager* pageManager);

    void UseDatabase(const string& dbName, Database** db, Storage::PageManager* pageManager);

    void PrintRows(const vector<StorageTypes::Row>& rows);
};