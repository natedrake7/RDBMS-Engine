﻿#pragma once
#include <string>
#include <fstream>
#include <iostream>
#include "Constants.h"

using namespace std;

typedef struct DatabaseHeader{
    table_number_t numberOfTables;
    page_id_t lastLargePageId;
    metadata_literal_t databaseNameSize;
    string databaseName;
    extent_id_t lastExtentId;
    extent_id_t lastLargeExtentId;
    table_id_t lastTableId;

    DatabaseHeader();
    DatabaseHeader(const string& databaseName, const table_number_t& numberOfTables);
    DatabaseHeader(const DatabaseHeader& dbMetaData);
}DatabaseHeader;

#include "../AdditionalLibraries/PageManager/PageManager.h"
#include "Table/Table.h"

class FileManager;
class PageManager;
class Page;
class LargeDataPage;
class Table;
struct TableFullMetaData;

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
    // HashTable* hashTable;

    protected:
        void ValidateTableCreation(Table* table) const;
        void WriteHeaderToFile();

    public:
        explicit Database(const string& dbName, PageManager* pageManager);

        ~Database();

        Table* CreateTable(const string& tableName, const vector<Column*>& columns);

        void CreateTable(const TableFullHeader& tableMetaData);

        Table* OpenTable(const string& tableName) const;

        void DeleteDatabase() const;

        Page* GetPage(const Table& table, const row_size_t& rowSize);

        void GetTablePages(const Table &table, vector<Page*>* pages) const;

        Page* CreatePage(const table_id_t& tableId);

        page_id_t GetLastLargeDataPageId() const;

        LargeDataPage* CreateLargeDataPage();

        LargeDataPage* GetLargeDataPage(const page_id_t& pageId, const Table& table);

        string GetFileName() const;
};

void CreateDatabase(const string& dbName, FileManager* fileManager, PageManager* pageManager);

void UseDatabase(const string& dbName, Database** db, PageManager* pageManager);

void PrintRows(const vector<Row>& rows);