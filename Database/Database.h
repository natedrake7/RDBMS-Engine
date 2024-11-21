#pragma once
#include <string>
#include <fstream>
#include <iostream>
#include "Constants.h"

using namespace std;

typedef struct DatabaseMetaData{
    page_id_t lastPageId;
    uint16_t numberOfTables;
    page_id_t lastLargePageId;
    metadata_literal_t databaseNameSize;
    string databaseName;

    DatabaseMetaData();
    DatabaseMetaData(const string& databaseName, const page_id_t& lastPageId, const int& numberOfTables);
    DatabaseMetaData(const DatabaseMetaData& dbMetaData);
}DatabaseMetaData;

#include "../AdditionalLibraries/PageManager/PageManager.h"
#include "Table/Table.h"

class FileManager;
class PageManager;
class Page;
class LargeDataPage;
class Table;
struct TableFullMetaData;
// class HashTable;

enum
{
    MAX_TABLE_SIZE = 10*1024
};

class Database {
    DatabaseMetaData metadata;
    string filename;
    string fileExtension = ".db";
    vector<Table*> tables;
    PageManager* pageManager;
    // HashTable* hashTable;

    protected:
        void ValidateTableCreation(Table* table) const;
        void WriteMetaDataToFile();

    public:
        explicit Database(const string& dbName, PageManager* pageManager);

        ~Database();

        Table* CreateTable(const string& tableName, const vector<Column*>& columns);

        void CreateTable(TableFullMetaData& tableMetaData);

        Table* OpenTable(const string& tableName);

        void DeleteDatabase() const;

        Page* GetPage(const page_id_t& pageId, const Table& table);

        Page* CreatePage();

        page_id_t GetLastLargeDataPageId() const;

        LargeDataPage* CreateLargeDataPage();

        LargeDataPage* GetLargeDataPage(const page_id_t& pageId, const Table& table);

        string GetFileName() const;

        // uint64_t InsertToHashTable(const char* inputString) const;

        // static uint64_t Hash(const char* inputString);

        // const char* GetStringByHashKey(const uint64_t& hashKey) const;
};

void CreateDatabase(const string& dbName, FileManager* fileManager, PageManager* pageManager);

void UseDatabase(const string& dbName, Database** db, PageManager* pageManager);