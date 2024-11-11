#ifndef DATABASE_H
#define DATABASE_H

#include <string>
#include <fstream>
#include <iostream>

#include "../HashTable/HashTable.h"
#include "Table/Table.h"

using namespace std;

enum
{
    MAX_TABLE_SIZE = 10*1024
};


class Database {
    private:
        fstream* fileDescriptor;
        string filename;
        string fileExtension = ".db";
        vector<Table*> tables;
        HashTable* primaryHashTable;
        HashTable* secondaryHashTable;

    protected:
        void ValidateTableCreation(Table* table) const;

    public:
        Database(fstream* fileDescriptor, const string& dbName);

        ~Database();

        void CreateTable(Table* table);

        void InsertRecord();

        void DeleteDatabase() const;

        uint64_t InsertToHashTables(const char* inputString) const;

        uint64_t Hash(const char* inputString) const;

};

void CreateDatabase(const string& dbName);

void UseDatabase(const string& dbName, Database** db);

#endif //DATABASE_H
