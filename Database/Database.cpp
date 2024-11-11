#include "Database.h"

void Database::ValidateTableCreation(Table* table) const
{
    for (const auto& dbTable : this->tables)
        if(dbTable->GetTableName() == table->GetTableName())
            throw runtime_error("Table already exists");

    if(table->GetMaxRowSize() > MAX_TABLE_SIZE)
        throw runtime_error("Table size exceeds limit");

}

Database::Database(fstream* fileDescriptor,const string& dbName)
{
    this->fileDescriptor = fileDescriptor;
    this->filename = dbName;
    this->primaryHashTable = new HashTable(10);
    this->secondaryHashTable = new HashTable(100);
}

Database::~Database()
{
    fileDescriptor->close();
    delete this->fileDescriptor;

    for (const auto& dbTable : this->tables)
        delete dbTable;

    delete this->primaryHashTable;
    delete this->secondaryHashTable;
}

void Database:: CreateTable(Table* table)
{
    this->ValidateTableCreation(table);
    this->tables.push_back(table);
}

// void Database::CreateTable(const string& tableName, )
// {
//
// }

void CreateDatabase(const string& dbName)
{
    ifstream fileExists(dbName + ".db");

    if(fileExists)
        throw runtime_error("Database with name: " + dbName +" already exists");

    fileExists.close();

    ofstream file(dbName + ".db");

    if(!file)
        throw runtime_error("Database " + dbName + " could not be created");

    file.close();
}

void UseDatabase(const string& dbName, Database** db)
{
    fstream* file = new fstream(dbName + ".db");

    if(!file->is_open())
        throw runtime_error("Database " + dbName + " does not exist");

    *db = new Database(file, dbName);
}

void Database::DeleteDatabase() const
{
    const string path = this->filename + this->fileExtension;

    this->fileDescriptor->close();

    if(remove(path.c_str()) != 0)
        throw runtime_error("Database " + this->filename + " could not be deleted");
}

uint64_t Database::InsertToHashTables(const char *inputString) const {
    const uint64_t primaryHashKey = this->primaryHashTable->Insert(inputString);
    this->secondaryHashTable->Insert(primaryHashKey, inputString);

    return primaryHashKey;
}

uint64_t Database::Hash(const char *inputString) const { return this->primaryHashTable->Hash(inputString); }

//Creates new file in db storage
//1 file for each db? 1.000.000 1 gb
//1.000.000