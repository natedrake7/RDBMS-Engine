#include "Database.h"

void Database::ValidateTableCreation(Table* table) const
{
    for (const auto& dbTable : this->tables)
        if(dbTable->GetTableName() == table->GetTableName())
            throw runtime_error("Table already exists");

    if(table->GetMaxRowSize() > MAX_TABLE_SIZE)
        throw runtime_error("Table size exceeds limit");

}

Database::Database(const string& dbName)
{
    this->filename = dbName;
    this->hashTable = new HashTable(100);
}

Database::~Database()
{
    for (const auto& dbTable : this->tables)
        delete dbTable;

    delete this->hashTable;
}

void Database::CreateTable(Table* table)
{
    this->ValidateTableCreation(table);
    this->tables.push_back(table);
}

void CreateDatabase(const string& dbName, FileManager* fileManager)
{
    fileManager->CreateFile(dbName, ".db");
}

void UseDatabase(const string& dbName, Database** db)
{
    *db = new Database(dbName);
}

void Database::DeleteDatabase() const
{
    const string path = this->filename + this->fileExtension;

    if(remove(path.c_str()) != 0)
        throw runtime_error("Database " + this->filename + " could not be deleted");
}

uint64_t Database::InsertToHashTable(const char *inputString) const { return this->hashTable->Insert(inputString); }

uint64_t Database::Hash(const char *inputString) { return HashTable::Hash(inputString); }

const char* Database::GetStringByHashKey(const uint64_t& hashKey) const { return this->hashTable->GetStringByHashKey(hashKey); }

//Creates new file in db storage
//1 file for each db? 1.000.000 1 gb
//1.000.000