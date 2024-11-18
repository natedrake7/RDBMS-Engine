#ifndef DATABASE_H
#define DATABASE_H

#include <string>
#include <fstream>
#include <iostream>

#include "../AdditionalLibraries/FileManager/FileManager.h"
#include "../AdditionalLibraries/HashTable/HashTable.h"
#include "../AdditionalLibraries/FileManager/FileManager.h"
#include "Table/Table.h"

using namespace std;
class Table;
class HashTable;

enum
{
    MAX_TABLE_SIZE = 10*1024
};


class Database {
    string filename;
    string fileExtension = ".db";
    vector<Table*> tables;
    // HashTable* hashTable;

    protected:
        void ValidateTableCreation(Table* table) const;

    public:
        explicit Database(const string& dbName);

        ~Database();

        void CreateTable(Table* table);

        void DeleteDatabase() const;

        // uint64_t InsertToHashTable(const char* inputString) const;

        // static uint64_t Hash(const char* inputString);

        // const char* GetStringByHashKey(const uint64_t& hashKey) const;
};

void CreateDatabase(const string& dbName, FileManager* fileManager);

void UseDatabase(const string& dbName, Database** db);

#endif //DATABASE_H
