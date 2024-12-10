#pragma once
#include <string>
#include <fstream>
#include <iostream>
#include "Constants.h"

using namespace std;

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
}DatabaseHeader;

#include "../AdditionalLibraries/PageManager/PageManager.h"
#include "Table/Table.h"

class FileManager;
class PageManager;
class Page;
class LargeDataPage;
class Table;
class PageFreeSpacePage;

enum
{
    MAX_TABLE_SIZE = 10*1024
};

class  Database {
    DatabaseHeader header;
    string filename;
    string fileExtension;
    vector<Table*> tables;
    PageManager* pageManager;

    protected:
        void ValidateTableCreation(Table* table) const;
        void WriteHeaderToFile();
        static bool IsSystemPage(const page_id_t& pageId);
        static page_id_t GetGamAssociatedPage(const page_id_t& pageId);
        static page_id_t GetPfsAssociatedPage(const page_id_t& pageId);
        static page_id_t CalculateSystemPageOffset(const page_id_t& pageId);
        static byte GetObjectSizeToCategory(const row_size_t& size);
        bool AllocateNewExtent( PageFreeSpacePage**  pageFreeSpacePage
                            , page_id_t* lowerLimit
                            , page_id_t* newPageId
                            , extent_id_t* newExtentId
                            , const table_id_t& tableId);
        const Table* GetTable(const table_id_t& tableId) const;
    
    public:
        explicit Database(const string& dbName, PageManager* pageManager);

        ~Database();

        Table* CreateTable(const string& tableName, const vector<Column*>& columns);

        void CreateTable(const TableFullHeader& tableMetaData);

        Table* OpenTable(const string& tableName) const;

        void DeleteDatabase() const;

        bool InsertRowToPage(const Table &table,vector<extent_id_t>& allocatedExtents, extent_id_t& lastExtentIndex, Row *row);

        void SelectTableRows(const table_id_t& tableId, vector<Row>& selectedRows, const size_t& rowsToSelect, const vector<RowCondition*>* conditions) const;

        void UpdateTableRows(const table_id_t& tableId, const vector<Block*>& updates, const vector<RowCondition*>* conditions);

        Page* CreateDataPage(const table_id_t& tableId);

        LargeDataPage* CreateLargeDataPage(const table_id_t& tableId);
    
        LargeDataPage* GetTableLastLargeDataPage(const table_id_t& tableId, const page_size_t& minObjectSize);

        LargeDataPage* GetLargeDataPage(const page_id_t &pageId, const table_id_t& tableId);

        void SetPageMetaDataToPfs(const Page* page) const;

        string GetFileName() const;

        static page_id_t CalculateSystemPageOffsetByExtentId(const extent_id_t& extentId);
};

void CreateDatabase(const string& dbName, FileManager* fileManager, PageManager* pageManager);

void UseDatabase(const string& dbName, Database** db, PageManager* pageManager);

void PrintRows(const vector<Row>& rows);