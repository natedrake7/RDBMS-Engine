#pragma once
#include <string>
#include <fstream>
#include <iostream>

using namespace std;

typedef struct DatabaseMetaData{
    int lastPageId{};
    int numberOfTables{};
    int databaseNameSize{};
    string databaseName;

    DatabaseMetaData();
    DatabaseMetaData(const string& databaseName, const int& lastPageId, const int& numberOfTables);
    DatabaseMetaData(const DatabaseMetaData& dbMetaData);
}DatabaseMetaData;

#include "../AdditionalLibraries/PageManager/PageManager.h"
#include "Table/Table.h"

class FileManager;
class PageManager;
class Page;
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

        Page* GetPage(const int& pageId, const Table& table);

        Page* CreatePage();

        string GetFileName() const;

        // uint64_t InsertToHashTable(const char* inputString) const;

        // static uint64_t Hash(const char* inputString);

        // const char* GetStringByHashKey(const uint64_t& hashKey) const;
};

void CreateDatabase(const string& dbName, FileManager* fileManager, PageManager* pageManager);

void UseDatabase(const string& dbName, Database** db, PageManager* pageManager);