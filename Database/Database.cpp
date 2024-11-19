#include "Database.h"

#include "../AdditionalLibraries/PageManager/PageManager.h"

void Database::ValidateTableCreation(Table* table) const
{
    for (const auto& dbTable : this->tables)
        if(dbTable->GetTableName() == table->GetTableName())
            throw runtime_error("Table already exists");

    if(table->GetMaxRowSize() > MAX_TABLE_SIZE)
        throw runtime_error("Table size exceeds limit");

}

void Database::WriteMetaDataToFile()
{
    MetaDataPage* metaDataPage = this->pageManager->GetMetaDataPage();

    metaDataPage->SetMetaData(this->metadata, this->tables);
}


Database::Database(const string &dbName, PageManager *pageManager)
{
    this->filename = dbName;
    this->pageManager = pageManager;

    MetaDataPage* page = pageManager->GetMetaDataPage();

    this->metadata = page->GetDatabaseMetaData();
    vector<TableFullMetaData> tableFullMetaData = page->GetTableFullMetaData();

    for(auto& tableMetaData : tableFullMetaData)
        this->CreateTable(tableMetaData);
}

Database::~Database()
{
    //save db metadata;
    this->WriteMetaDataToFile();
    for (const auto& dbTable : this->tables)
        delete dbTable;

    // delete this->hashTable;
}

Table* Database::CreateTable(const string &tableName, const vector<Column *> &columns)
{
    Table* table = new Table(tableName, columns, this);

    // this->ValidateTableCreation(table);

    this->tables.push_back(table);

    this->metadata.numberOfTables = this->tables.size();

    return table;
}

void Database::CreateTable(TableFullMetaData &tableMetaData)
{
    vector<Column*> columns;
    for(const auto& columnMetaData : tableMetaData.columnsMetaData)
    {
        Column* column = new Column(columnMetaData);
        columns.push_back(column);
    }

    Table* table = new Table(tableMetaData.tableMetaData, columns, this);

    this->tables.push_back(table);
}

Table* Database::OpenTable(const string &tableName)
{
    for (const auto& table : this->tables)
    {
        if(table->GetTableMetadata().tableName == tableName)
            return table;
    }

    return nullptr;
}

void CreateDatabase(const string& dbName, FileManager* fileManager, PageManager* pageManager)
{
    fileManager->CreateFile(dbName, ".db");
    MetaDataPage* metaDataPage = pageManager->CreateMetaDataPage();

    metaDataPage->SetMetaData(DatabaseMetaData(dbName, 0, 0), vector<Table*>());
}

void UseDatabase(const string& dbName, Database** db, PageManager* pageManager)
{
    *db = new Database(dbName, pageManager);
}

void Database::DeleteDatabase() const
{
    const string path = this->filename + this->fileExtension;

    if(remove(path.c_str()) != 0)
        throw runtime_error("Database " + this->filename + " could not be deleted");
}


Page* Database::GetPage(const int &pageId, const Table& table) { return this->pageManager->GetPage(pageId, &table); }

Page* Database::CreatePage()
{
    this->metadata.lastPageId++;

    return this->pageManager->CreatePage(this->metadata.lastPageId);
}

string Database::GetFileName() const { return this->filename + this->fileExtension; }

DatabaseMetaData::DatabaseMetaData()
{
    this->databaseNameSize = 0;
    this->lastPageId = 0;
    this->numberOfTables = 0;
}

DatabaseMetaData::DatabaseMetaData(const string &databaseName, const int &lastPageId, const int& numberOfTables)
{
    this->databaseName = databaseName;
    this->lastPageId = lastPageId;
    this->numberOfTables = numberOfTables;
    this->databaseNameSize = 0;
}

DatabaseMetaData::DatabaseMetaData(const DatabaseMetaData &dbMetaData)
{
    this->databaseName = dbMetaData.databaseName;
    this->lastPageId = dbMetaData.lastPageId;
    this->numberOfTables = dbMetaData.numberOfTables;
    this->databaseNameSize = this->databaseName.size();
}

// uint64_t Database::InsertToHashTable(const char *inputString) const { return this->hashTable->Insert(inputString); }

// uint64_t Database::Hash(const char *inputString) { return HashTable::Hash(inputString); }

// const char* Database::GetStringByHashKey(const uint64_t& hashKey) const { return this->hashTable->GetStringByHashKey(hashKey); }

//Creates new file in db storage
//1 file for each db? 1.000.000 1 gb
//1.000.000