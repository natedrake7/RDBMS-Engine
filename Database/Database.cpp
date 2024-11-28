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

void Database::WriteHeaderToFile()
{
    HeaderPage* metaDataPage = this->pageManager->GetHeaderPage(this->filename + this->fileExtension);

    metaDataPage->SetHeaders(this->header, this->tables);
}


Database::Database(const string &dbName, PageManager *pageManager)
{
    this->filename = dbName;
    this->fileExtension = ".db";
    this->pageManager = pageManager;

    HeaderPage* page = pageManager->GetHeaderPage(this->filename + this->fileExtension);

    this->header = page->GetDatabaseHeader();
    const vector<TableFullHeader> tablesFullHeaders = page->GetTablesFullHeaders();

    for(auto& tableHeader : tablesFullHeaders)
        this->CreateTable(tableHeader);
}

Database::~Database()
{
    //save db header;
    this->WriteHeaderToFile();
    for (const auto& dbTable : this->tables)
        delete dbTable;

    // delete this->hashTable;
}

Table* Database::CreateTable(const string &tableName, const vector<Column *> &columns)
{
    Table* table = nullptr;

    if (this->header.lastTableId == 0)
        table = new Table(tableName, this->header.lastTableId , columns, this);
    else
    {
        this->header.lastTableId++;
        table = new Table(tableName, this->header.lastTableId , columns, this);
    }

    this->tables.push_back(table);

    this->header.numberOfTables = this->tables.size();

    return table;
}

void Database::CreateTable(const TableFullHeader &tableFullHeader)
{
    Table* table = new Table(tableFullHeader.tableHeader, this);

    for(const auto& columnHeader : tableFullHeader.columnsHeaders)
        table->AddColumn(new Column(columnHeader, table));

    this->tables.push_back(table);
}

Table* Database::OpenTable(const string &tableName) const
{
    for (const auto& table : this->tables)
    {
        if(table->GetTableHeader().tableName == tableName)
            return table;
    }

    return nullptr;
}

void CreateDatabase(const string& dbName, FileManager* fileManager, PageManager* pageManager)
{
    fileManager->CreateFile(dbName, ".db");
    
    HeaderPage* metaDataPage = pageManager->CreateHeaderPage(dbName + ".db");
    pageManager->CreateGlobalAllocationMapPage(dbName + ".db");

    //start the the paging of the data in the extent 1 not 0
    metaDataPage->SetHeaders(DatabaseHeader(dbName, 0), vector<Table*>());
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


Page* Database::GetPage(const Table& table, const row_size_t& rowSize)
{
    IndexAllocationMapPage* tableMapPage = pageManager->GetIndexAllocationMapPage(table.GetTableHeader().firstPageId);

    const extent_id_t lastAllocatedExtent = tableMapPage->GetLastAllocatedExtent();

    const page_id_t extentFirstPageId = ( lastAllocatedExtent * EXTENT_SIZE ) + 3;

    page_id_t pageId = (tableMapPage->GetPageId() != extentFirstPageId)
                    ? extentFirstPageId
                    : extentFirstPageId + 1;

    Page* page = nullptr;

    while (true)
    {
        if (pageId >= ( ( lastAllocatedExtent + 1 ) * EXTENT_SIZE ) + 3)
            break;
        
        page = this->pageManager->GetPage(pageId, &table);
        
        if (page->GetBytesLeft() >= rowSize)
            return page;

        pageId++;
    }
    
    return nullptr;
}

void Database::GetTablePages(const Table &table, vector<Page*>* pages) const
{
    IndexAllocationMapPage* tableMapPage = pageManager->GetIndexAllocationMapPage(table.GetTableHeader().firstPageId);

    vector<extent_id_t> allExtentsIds;
    tableMapPage->GetAllocatedExtents(&allExtentsIds);

    for (const auto& extentId : allExtentsIds)
    {
        const page_id_t extentFirstPageId = ( extentId * EXTENT_SIZE ) + 3;

        const page_id_t pageId = (tableMapPage->GetPageId() != extentFirstPageId)
                        ? extentFirstPageId
                        : extentFirstPageId + 1;

        for (page_id_t extentPageId = pageId; extentPageId < ( ( extentId  + 1 ) * EXTENT_SIZE ) + 3; extentPageId++)
        {
            Page* page = this->pageManager->GetPage(extentPageId, &table);
            
            if (page->GetPageSize() == 0)
                continue;
            
            pages->push_back(page);
        }
    }
}

Page* Database::CreatePage(const table_id_t& tableId)
{
    GlobalAllocationMapPage* gamPage = this->pageManager->GetGlobalAllocationMapPage();

    IndexAllocationMapPage* tableMapPage = nullptr;
    
    Table* table = nullptr;
    for (const auto& dbTable : this->tables)
    {
        if(dbTable->GetTableId() == tableId)
        {
            table = dbTable;
            break;            
        }
    }

    if(table == nullptr)
        return nullptr;

    const page_id_t& firstPageId = table->GetTableHeader().firstPageId;

    const extent_id_t newExtentId = gamPage->AllocateExtent();

    const page_id_t newPageId = ( newExtentId * EXTENT_SIZE ) + 3;
    
    bool isFirstExtent = false;
    if (firstPageId == 0)
    {
        isFirstExtent = true;
        tableMapPage = this->pageManager->CreateIndexAllocationMapPage(tableId, newPageId);
        table->UpdateFirstPageId(newPageId);
    }
    else
        tableMapPage = this->pageManager->GetIndexAllocationMapPage(firstPageId);
    
    tableMapPage->SetAllocatedExtent(newExtentId);

    const page_id_t lowerLimit = (isFirstExtent)
                    ? newPageId
                    : newPageId - 1;

    //allocate extent
    Page* newPage = nullptr;
    for (page_id_t pageId = newPageId + EXTENT_SIZE - 1; pageId > lowerLimit; pageId--)
        newPage = this->pageManager->CreatePage(pageId);

    return newPage;
}

LargeDataPage* Database::CreateLargeDataPage()
{
    if ((this->header.lastLargePageId + 1 < (this->header.lastLargeExtentId + 1) * EXTENT_SIZE)
        && this->header.lastLargeExtentId > 0)
        this->header.lastLargePageId++;
    else
    {
        this->header.lastLargePageId = (this->header.lastExtentId + 1) * EXTENT_SIZE;

        this->header.lastExtentId++;

        this->header.lastLargeExtentId = this->header.lastExtentId;
    }

    return this->pageManager->CreateLargeDataPage(this->header.lastLargePageId);
}

LargeDataPage* Database::GetLargeDataPage(const page_id_t& pageId, const Table& table) { return this->pageManager->GetLargeDataPage(pageId, &table); }

page_id_t Database::GetLastLargeDataPageId() const { return this->header.lastLargePageId; }

string Database::GetFileName() const { return this->filename + this->fileExtension; }

DatabaseHeader::DatabaseHeader()
{
    this->databaseNameSize = 0;
    this->numberOfTables = 0;
    this->lastLargePageId = 0;
    this->lastExtentId = 0;
    this->lastLargeExtentId = 0;
    this->lastTableId = 0;
}

DatabaseHeader::DatabaseHeader(const string &databaseName, const table_number_t& numberOfTables)
{
    this->databaseName = databaseName;
    this->numberOfTables = numberOfTables;
    this->databaseNameSize = 0;
    this->lastLargePageId = 0;
    this->lastExtentId = 0;
    this->lastLargeExtentId = 0;
    this->lastTableId = 0;
}

DatabaseHeader::DatabaseHeader(const DatabaseHeader &dbMetaData)
{
    this->databaseName = dbMetaData.databaseName;
    this->numberOfTables = dbMetaData.numberOfTables;
    this->databaseNameSize = this->databaseName.size();
    this->lastLargePageId = dbMetaData.lastLargePageId;
    this->lastExtentId = dbMetaData.lastExtentId;
    this->lastLargeExtentId = dbMetaData.lastLargeExtentId;
    this->lastTableId = dbMetaData.lastTableId;
}