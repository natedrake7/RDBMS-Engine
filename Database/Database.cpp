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
    pageManager->CreatePageFreeSpacePage(dbName + ".db");

    metaDataPage->SetHeaders(DatabaseHeader(dbName, 0), vector<Table*>());
}

void  UseDatabase(const string& dbName, Database** db, PageManager* pageManager)
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

bool Database::InsertRowToPage(const Table &table, Row *row)
{
    const IndexAllocationMapPage* tableMapPage = pageManager->GetIndexAllocationMapPage(table.GetTableHeader().indexAllocationMapPageId);

    const extent_id_t lastAllocatedExtent = tableMapPage->GetLastAllocatedExtent();

    const page_id_t extentFirstPageId = ( lastAllocatedExtent * EXTENT_SIZE ) + 3;

    const page_id_t firstDataPageId = (tableMapPage->GetPageId() != extentFirstPageId)
                        ? extentFirstPageId
                        : extentFirstPageId + 1;

    PageFreeSpacePage* pageFreeSpacePage = pageManager->GetPageFreeSpacePage(2);

    const byte rowCategory = row->RowSizeToCategory();
    for (page_id_t pageId = firstDataPageId; pageId < ( lastAllocatedExtent + 1 ) * EXTENT_SIZE + 3; pageId++)
    {
        const byte pageSizeCategory = pageFreeSpacePage->GetPageSizeCategory(pageId);

        //find potential candidate
        if ( rowCategory <= pageSizeCategory)
        {
            Page* page = this->pageManager->GetPage(pageId, lastAllocatedExtent, &table);
            
            if (row->GetTotalRowSize() > page->GetBytesLeft())
                continue;
            
            page->InsertRow(row);
            pageFreeSpacePage->SetPageMetaData(page);

            return true;
        }
    }

    return false;
}

void Database::GetTablePages(const Table &table, vector<Page*>* pages) const
{
    IndexAllocationMapPage* tableMapPage = pageManager->GetIndexAllocationMapPage(table.GetTableHeader().indexAllocationMapPageId);

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
            Page* page = this->pageManager->GetPage(extentPageId, extentId, &table);
            
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

    const page_id_t& indexAllocationMapPageId = table->GetTableHeader().indexAllocationMapPageId;
    
    PageFreeSpacePage* pageFreeSpacePage = this->pageManager->GetPageFreeSpacePage(2);

    const extent_id_t newExtentId = gamPage->AllocateExtent();
    const page_id_t newPageId = ( newExtentId * EXTENT_SIZE ) + 3;

    const bool isFirstExtent = indexAllocationMapPageId == 0;
    if (isFirstExtent)
    {
        tableMapPage = this->pageManager->CreateIndexAllocationMapPage(tableId, newPageId);

        pageFreeSpacePage->SetPageMetaData(tableMapPage);
        
        table->UpdateIndexAllocationMapPageId(newPageId);
    }
    else
        tableMapPage = this->pageManager->GetIndexAllocationMapPage(indexAllocationMapPageId);

    tableMapPage->SetAllocatedExtent(newExtentId);

    const page_id_t lowerLimit = (isFirstExtent)
                    ? newPageId + 1
                    : newPageId;

    for (page_id_t pageId = lowerLimit; pageId < newPageId + EXTENT_SIZE; pageId++)
        pageFreeSpacePage->SetPageMetaData(this->pageManager->CreatePage(pageId));

    return this->pageManager->GetPage(lowerLimit, newExtentId, table);
}

LargeDataPage* Database::GetLargeDataPage(const page_id_t& pageId, const Table& table) { return this->pageManager->GetLargeDataPage(pageId, &table); }

string Database::GetFileName() const { return this->filename + this->fileExtension; }

DatabaseHeader::DatabaseHeader()
{
    this->databaseNameSize = 0;
    this->numberOfTables = 0;
    this->lastTableId = 0;
}

DatabaseHeader::DatabaseHeader(const string &databaseName, const table_number_t& numberOfTables)
{
    this->databaseName = databaseName;
    this->numberOfTables = numberOfTables;
    this->databaseNameSize = 0;
    this->lastTableId = 0;
}

DatabaseHeader::DatabaseHeader(const DatabaseHeader &dbMetaData)
{
    this->databaseName = dbMetaData.databaseName;
    this->numberOfTables = dbMetaData.numberOfTables;
    this->databaseNameSize = this->databaseName.size();
    this->lastTableId = dbMetaData.lastTableId;
}