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

bool Database::IsSystemPage(const page_id_t &pageId) { return pageId == 0 || pageId == 1 || pageId == 2 || pageId % PAGE_FREE_SPACE_SIZE == 1 || pageId % GAM_NUMBER_OF_PAGES == 2; }

page_id_t Database::GetGamAssociatedPage(const page_id_t& pageId) { return (pageId / PAGE_FREE_SPACE_SIZE) * PAGE_FREE_SPACE_SIZE + 1;}

page_id_t Database::GetPfsAssociatedPage(const page_id_t& pageId) { return (pageId / GAM_NUMBER_OF_PAGES) * GAM_NUMBER_OF_PAGES + 2; }

page_id_t Database::CalculateSystemPageOffset(const page_id_t& pageId)
{
    page_id_t pfsPages = pageId / PAGE_FREE_SPACE_SIZE;

    if (pfsPages == 0)
        pfsPages = 1;
    
    page_id_t gamPages = pageId / GAM_NUMBER_OF_PAGES;

    if (gamPages == 0)
        gamPages = 1;

    return pageId + pfsPages + gamPages + 1;
}

extent_id_t Database::CalculateSystemPageOffsetByExtentId(const extent_id_t& extentId)
{
    const page_id_t pageId = extentId * EXTENT_SIZE;
    
    page_id_t pfsPages = pageId / PAGE_FREE_SPACE_SIZE;

    if (pfsPages == 0)
        pfsPages = 1;
    
    page_id_t gamPages = pageId / GAM_NUMBER_OF_PAGES;

    if (gamPages == 0)
        gamPages = 1;

    return pageId + pfsPages + gamPages + 1;
}

page_id_t Database::GetPageIdByExtentId(const extent_id_t &extentId)
{
    const page_id_t extentPageId = extentId * EXTENT_SIZE;

    const page_id_t systemPagesOffset = Database::CalculateSystemPageOffset(extentPageId);

    return extentPageId + systemPagesOffset;
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

    constexpr page_id_t firstGamePageId = 2;
    constexpr page_id_t firstPfsPageId = 1;
    
    pageManager->CreateGlobalAllocationMapPage(dbName + ".db", firstGamePageId);
    pageManager->CreatePageFreeSpacePage(dbName + ".db", firstPfsPageId);

    metaDataPage->SetHeaders(DatabaseHeader(dbName, 0, firstPfsPageId, firstGamePageId), vector<Table*>());
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

    const page_id_t extentFirstPageId = Database::CalculateSystemPageOffsetByExtentId(lastAllocatedExtent);

    const page_id_t firstDataPageId = (tableMapPage->GetPageId() != extentFirstPageId)
                        ? extentFirstPageId
                        : extentFirstPageId + 1;

    PageFreeSpacePage* pageFreeSpacePage = pageManager->GetPageFreeSpacePage(this->header.lastPageFreeSpacePageId);

    const byte rowCategory = row->RowSizeToCategory();
    for (page_id_t pageId = firstDataPageId; pageId < extentFirstPageId + EXTENT_SIZE; pageId++)
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
        const page_id_t extentFirstPageId = Database::CalculateSystemPageOffset(extentId * EXTENT_SIZE);

        const page_id_t pageId = (tableMapPage->GetPageId() != extentFirstPageId)
                        ? extentFirstPageId
                        : extentFirstPageId + 1;

        for (page_id_t extentPageId = pageId; extentPageId < extentFirstPageId + EXTENT_SIZE; extentPageId++)
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
    GlobalAllocationMapPage* gamPage = this->pageManager->GetGlobalAllocationMapPage(this->header.lastGamPageId);

    IndexAllocationMapPage* tableMapPage = nullptr;
    
    if (tableId >= this->tables.size())
        return nullptr;

    const page_id_t& indexAllocationMapPageId = this->tables[tableId]->GetTableHeader().indexAllocationMapPageId;
    
    PageFreeSpacePage* pageFreeSpacePage = this->pageManager->GetPageFreeSpacePage(this->header.lastPageFreeSpacePageId);
    
    if (pageFreeSpacePage->IsFull())
    {
        pageFreeSpacePage = this->pageManager->CreatePageFreeSpacePage(pageFreeSpacePage->GetPageId() + PAGE_FREE_SPACE_SIZE);
        this->header.lastPageFreeSpacePageId = pageFreeSpacePage->GetPageId();
    }

    extent_id_t newExtentId = 0;
    page_id_t newPageId = 0;
    if (gamPage->IsFull())
    {
        gamPage = this->pageManager->CreateGlobalAllocationMapPage(gamPage->GetPageId() + GAM_NUMBER_OF_PAGES);

        //get the last iam page always
        IndexAllocationMapPage* previousTableMapPage = this->pageManager->GetIndexAllocationMapPage(indexAllocationMapPageId);

        newExtentId = gamPage->AllocateExtent();

        newPageId = Database::CalculateSystemPageOffsetByExtentId(newExtentId);
        
        previousTableMapPage->SetNextPageId(newPageId);

        tableMapPage = this->pageManager->CreateIndexAllocationMapPage(tableId, newPageId, newExtentId);
        this->tables[tableId]->UpdateIndexAllocationMapPageId(newPageId);

        pageFreeSpacePage->SetPageMetaData(tableMapPage);
    }
    else
    {
        newExtentId = gamPage->AllocateExtent();
        newPageId = Database::CalculateSystemPageOffsetByExtentId(newExtentId);
    }

    const bool isFirstExtent = indexAllocationMapPageId == 0;
    if (isFirstExtent && tableMapPage == nullptr)
    {
        tableMapPage = this->pageManager->CreateIndexAllocationMapPage(tableId, newPageId, newExtentId);

        pageFreeSpacePage->SetPageMetaData(tableMapPage);
        
        this->tables[tableId]->UpdateIndexAllocationMapPageId(newPageId);
    }
    else
        tableMapPage = this->pageManager->GetIndexAllocationMapPage(indexAllocationMapPageId);

    tableMapPage->SetAllocatedExtent(newExtentId, gamPage);

    const page_id_t lowerLimit = (isFirstExtent)
                    ? newPageId + 1
                    : newPageId;

    for (page_id_t pageId = lowerLimit; pageId < newPageId + EXTENT_SIZE; pageId++)
        pageFreeSpacePage->SetPageMetaData(this->pageManager->CreatePage(pageId));

    return this->pageManager->GetPage(lowerLimit, newExtentId, this->tables[tableId]);
}

LargeDataPage* Database::GetLargeDataPage(const page_id_t& pageId, const Table& table) { return this->pageManager->GetLargeDataPage(pageId, &table); }

string Database::GetFileName() const { return this->filename + this->fileExtension; }

DatabaseHeader::DatabaseHeader()
{
    this->databaseNameSize = 0;
    this->numberOfTables = 0;
    this->lastTableId = 0;
    this->lastPageFreeSpacePageId = 0;
    this->lastGamPageId = 0;
}

DatabaseHeader::DatabaseHeader(const string &databaseName, const table_number_t& numberOfTables, const page_id_t& lastPageFreeSpacePageId, const page_id_t& lastGamPageId)
{
    this->databaseName = databaseName;
    this->numberOfTables = numberOfTables;
    this->databaseNameSize = 0;
    this->lastTableId = 0;
    this->lastPageFreeSpacePageId = lastPageFreeSpacePageId;
    this->lastGamPageId = lastGamPageId;
}

DatabaseHeader::DatabaseHeader(const DatabaseHeader &dbMetaData)
{
    this->databaseName = dbMetaData.databaseName;
    this->numberOfTables = dbMetaData.numberOfTables;
    this->databaseNameSize = this->databaseName.size();
    this->lastTableId = dbMetaData.lastTableId;
    this->lastPageFreeSpacePageId = dbMetaData.lastPageFreeSpacePageId;
    this->lastGamPageId = dbMetaData.lastGamPageId;
}