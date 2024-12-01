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
        static page_id_t GetPageIdByExtentId(const extent_id_t& extentId);
        static page_id_t CalculateSystemPageOffset(const page_id_t& pageId);
    
    public:
        explicit Database(const string& dbName, PageManager* pageManager);

        ~Database();

        Table* CreateTable(const string& tableName, const vector<Column*>& columns);

        void CreateTable(const TableFullHeader& tableMetaData);

        Table* OpenTable(const string& tableName) const;

        void DeleteDatabase() const;

        bool InsertRowToPage(const Table& table, Row* row);

        void GetTablePages(const Table &table, vector<Page*>* pages) const;

        Page* CreatePage(const table_id_t& tableId);

        LargeDataPage* GetLargeDataPage(const page_id_t& pageId, const Table& table);

        string GetFileName() const;

        static page_id_t CalculateSystemPageOffsetByExtentId(const extent_id_t& extentId);
};

void CreateDatabase(const string& dbName, FileManager* fileManager, PageManager* pageManager);

void UseDatabase(const string& dbName, Database** db, PageManager* pageManager);

void PrintRows(const vector<Row>& rows);