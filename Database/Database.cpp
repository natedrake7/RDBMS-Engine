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
    MetaDataPage* metaDataPage = this->pageManager->GetMetaDataPage(this->filename + this->fileExtension);

    metaDataPage->SetMetaData(this->metadata, this->tables);
}


Database::Database(const string &dbName, PageManager *pageManager)
{
    this->filename = dbName;
    this->pageManager = pageManager;

    MetaDataPage* page = pageManager->GetMetaDataPage(this->filename + this->fileExtension);

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
    Table* table = nullptr;

    if (this->metadata.lastTableId == 0)
        table = new Table(tableName, this->metadata.lastTableId , columns, this);
    else
    {
        this->metadata.lastTableId++;
        table = new Table(tableName, this->metadata.lastTableId , columns, this);
    }

    this->tables.push_back(table);

    this->metadata.numberOfTables = this->tables.size();

    return table;
}

void Database::CreateTable(const TableFullMetaData &tableMetaData)
{
    Table* table = new Table(tableMetaData.tableMetaData, this);

    for(const auto& columnMetaData : tableMetaData.columnsMetaData)
        table->AddColumn(new Column(columnMetaData, table));

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
    MetaDataPage* metaDataPage = pageManager->CreateMetaDataPage(dbName + ".db");

    //start the the paging of the data in the extent 1 not 0
    metaDataPage->SetMetaData(DatabaseMetaData(dbName, 0), vector<Table*>());
}

void UseDatabase(const string& dbName, Database** db, PageManager* pageManager)
{
    *db = new Database(dbName, pageManager);
}

void PrintRows(const vector<Row>& rows)
{
    uint16_t rowCount = 0;
    for(const auto& row: rows)
    {
        row.PrintRow();
        rowCount++;
    }

    cout<< "Rows printed: "<< rowCount << endl;
}

void Database::DeleteDatabase() const
{
    const string path = this->filename + this->fileExtension;

    if(remove(path.c_str()) != 0)
        throw runtime_error("Database " + this->filename + " could not be deleted");
}


Page* Database::GetPage(const page_id_t& pageId, const Table& table) { return this->pageManager->GetPage(pageId, &table); }

Page* Database::CreatePage(const string& tableName)
{
    Table* table = nullptr;
    for (auto& dbTable : this->tables)
    {
        if(dbTable->GetTableName() == tableName)
            table = dbTable;
    }

    if (table == nullptr)
        throw runtime_error("Table " + tableName + " could not be found");
    
    const auto& tableMetaData = table->GetTableMetadata();

    page_id_t lastPageId = 0;

    //counting of extents starts from 0
    if ((tableMetaData.lastPageId + 1 < (tableMetaData.lastExtentId + 1) * EXTENT_SIZE)
        && tableMetaData.lastExtentId > 0)
    {
        lastPageId = tableMetaData.lastPageId + 1;
        
        table->UpdateLastPageId(lastPageId);
        
        return this->pageManager->CreatePage(lastPageId);
    }

    lastPageId = (this->metadata.lastExtentId + 1) * EXTENT_SIZE;

    this->metadata.lastExtentId++;
    
    table->UpdateLastPageId(lastPageId);

    table->UpdateLastExtentId(this->metadata.lastExtentId);

    return this->pageManager->CreatePage(lastPageId);
}

LargeDataPage* Database::CreateLargeDataPage()
{
    if ((this->metadata.lastLargePageId + 1 < (this->metadata.lastLargeExtentId + 1) * EXTENT_SIZE)
        && this->metadata.lastLargeExtentId > 0)
        this->metadata.lastLargePageId++;
    else
    {
        this->metadata.lastLargePageId = (this->metadata.lastExtentId + 1) * EXTENT_SIZE;

        this->metadata.lastExtentId++;

        this->metadata.lastLargeExtentId = this->metadata.lastExtentId;
    }

    return this->pageManager->CreateLargeDataPage(this->metadata.lastLargePageId);
}

LargeDataPage * Database::GetLargeDataPage(const page_id_t& pageId, const Table& table) { return this->pageManager->GetLargeDataPage(pageId, &table); }

page_id_t Database::GetLastLargeDataPageId() const { return this->metadata.lastLargePageId; }

string Database::GetFileName() const { return this->filename + this->fileExtension; }

DatabaseMetaData::DatabaseMetaData()
{
    this->databaseNameSize = 0;
    this->numberOfTables = 0;
    this->lastLargePageId = 0;
    this->lastExtentId = 0;
    this->lastLargeExtentId = 0;
    this->lastTableId = 0;
}

DatabaseMetaData::DatabaseMetaData(const string &databaseName, const table_number_t& numberOfTables)
{
    this->databaseName = databaseName;
    this->numberOfTables = numberOfTables;
    this->databaseNameSize = 0;
    this->lastLargePageId = 0;
    this->lastExtentId = 0;
    this->lastLargeExtentId = 0;
    this->lastTableId = 0;
}

DatabaseMetaData::DatabaseMetaData(const DatabaseMetaData &dbMetaData)
{
    this->databaseName = dbMetaData.databaseName;
    this->numberOfTables = dbMetaData.numberOfTables;
    this->databaseNameSize = this->databaseName.size();
    this->lastLargePageId = dbMetaData.lastLargePageId;
    this->lastExtentId = dbMetaData.lastExtentId;
    this->lastLargeExtentId = dbMetaData.lastLargeExtentId;
    this->lastTableId = dbMetaData.lastTableId;
}