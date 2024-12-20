#include "Database.h"
#include <fstream>
#include <iostream>
#include <thread>
#include "./Pages/Header/HeaderPage.h"
#include "./Pages/GlobalAllocationMap/GlobalAllocationMapPage.h"
#include "./Pages/IndexMapAllocation/IndexAllocationMapPage.h"
#include "./Pages/PageFreeSpace/PageFreeSpacePage.h"
#include "./Pages/IndexPage/IndexPage.h"
#include "Table/Table.h"
#include "Column/Column.h"
#include "Row/Row.h"
#include "Pages/LargeObject/LargeDataPage.h"
#include "Storage/FileManager/FileManager.h"
#include "Storage/PageManager/PageManager.h"
#include "../AdditionalLibraries/B+Tree/BPlusTree.h"
#include "Block/Block.h"
#include <unordered_set>

using namespace Pages;
using namespace DatabaseEngine::StorageTypes;
using namespace Indexing;

namespace DatabaseEngine
{
    void Database::ValidateTableCreation(Table *table) const
    {
        for (const auto &dbTable : this->tables)
            if (dbTable->GetTableName() == table->GetTableName())
                throw runtime_error("Table already exists");

        if (table->GetMaxRowSize() > MAX_TABLE_SIZE)
            throw runtime_error("Table size exceeds limit");
    }

    void Database::WriteHeaderToFile()
    {
        HeaderPage *metaDataPage = this->pageManager->GetHeaderPage(this->filename + this->fileExtension);

        metaDataPage->SetHeaders(this->header, this->tables);
    }

    bool Database::IsSystemPage(const page_id_t &pageId) { return pageId == 0 || pageId == 1 || pageId == 2 || pageId % PAGE_FREE_SPACE_SIZE == 1 || pageId % GAM_NUMBER_OF_PAGES == 2; }

    page_id_t Database::GetPfsAssociatedPage(const page_id_t &pageId) { return (pageId / PAGE_FREE_SPACE_SIZE) * PAGE_FREE_SPACE_SIZE + 1; }

    page_id_t Database::GetGamAssociatedPage(const page_id_t &pageId) { return (pageId / GAM_NUMBER_OF_PAGES) * GAM_NUMBER_OF_PAGES + 1; }

    page_id_t Database::CalculateSystemPageOffset(const page_id_t &pageId)
    {
        page_id_t pfsPages = pageId / PAGE_FREE_SPACE_SIZE;

        if (pfsPages == 0)
            pfsPages = 1;

        page_id_t gamPages = pageId / GAM_NUMBER_OF_PAGES;

        if (gamPages == 0)
            gamPages = 1;

        return pageId + pfsPages + gamPages + 1;
    }

    Constants::byte Database::GetObjectSizeToCategory(const row_size_t &size)
    {
        static const Constants::byte categories[] = {0, 1, 5, 9, 13, 17, 21, 25, 29};
        const float freeSpacePercentage = static_cast<float>(size) / PAGE_SIZE;
        const int index = static_cast<int>(freeSpacePercentage * 8); // Map 0.0–1.0 to 0–8

        return categories[index];
    }

    extent_id_t Database::CalculateSystemPageOffsetByExtentId(const extent_id_t &extentId)
    {
        const page_id_t pageId = extentId * EXTENT_SIZE;

        const page_id_t pfsPages = pageId / PAGE_FREE_SPACE_SIZE + 1;

        const page_id_t gamPages = pageId / GAM_NUMBER_OF_PAGES + 1;

        return pageId + pfsPages + gamPages + 1;
    }

    Database::Database(const string &dbName, Storage::PageManager *pageManager)
    {
        this->filename = dbName;
        this->fileExtension = ".db";
        this->pageManager = pageManager;

        const HeaderPage *page = pageManager->GetHeaderPage(this->filename + this->fileExtension);

        this->header = *page->GetDatabaseHeader();
        const vector<TableFullHeader> tablesFullHeaders = page->GetTablesFullHeaders();

        for (auto &tableHeader : tablesFullHeaders)
            this->CreateTable(tableHeader);
    }

    Database::~Database()
    {
        // save db header;
        this->WriteHeaderToFile();
        for (const auto &dbTable : this->tables)
            delete dbTable;
    }

    Table *Database::CreateTable(const string &tableName, const vector<StorageTypes::Column *> &columns, const vector<column_index_t> *clusteredKeyIndexes, const vector<column_index_t> *nonClusteredIndexes)
    {
        Table *table = new Table(tableName, this->header.lastTableId, columns, this, clusteredKeyIndexes, nonClusteredIndexes);

        this->header.lastTableId++;

        this->tables.push_back(table);

        this->header.numberOfTables = this->tables.size();

        return table;
    }

    void Database::CreateTable(const TableFullHeader &tableFullHeader)
    {
        Table *table = new Table(tableFullHeader.tableHeader, this);

        for (const auto &columnHeader : tableFullHeader.columnsHeaders)
            table->AddColumn(new Column(columnHeader, table));

        this->tables.push_back(table);
    }

    Table *Database::OpenTable(const string &tableName) const
    {
        for (const auto &table : this->tables)
        {
            if (table->GetTableHeader().tableName == tableName)
                return table;
        }

        return nullptr;
    }

    void CreateDatabase(const string &dbName, Storage::FileManager *fileManager, Storage::PageManager *pageManager)
    {
        fileManager->CreateFile(dbName, ".db");

        HeaderPage *headerPage = pageManager->CreateHeaderPage(dbName + ".db");

        constexpr page_id_t firstGamePageId = 2;
        constexpr page_id_t firstPfsPageId = 1;

        pageManager->CreateGlobalAllocationMapPage(dbName + ".db", firstGamePageId);
        pageManager->CreatePageFreeSpacePage(dbName + ".db", firstPfsPageId);

        headerPage->SetHeaders(DatabaseHeader(dbName, 0, firstPfsPageId, firstGamePageId), vector<Table *>());
    }

    void UseDatabase(const string &dbName, Database **db, Storage::PageManager *pageManager)
    {
        *db = new Database(dbName, pageManager);
    }

    void PrintRows(const vector<Row> &rows)
    {
        uint16_t rowCount = 0;
        for (const auto &row : rows)
        {
            row.PrintRow();
            rowCount++;
        }

        cout << "Rows printed: " << rowCount << endl;
    }

    void Database::DeleteDatabase() const
    {
        const string path = this->filename + this->fileExtension;

        if (remove(path.c_str()) != 0)
            throw runtime_error("Database " + this->filename + " could not be deleted");
    }

    bool Database::InsertRowToPage(const Table &table, vector<extent_id_t> &allocatedExtents, extent_id_t &lastExtentIndex, Row *row)
    {
        return (table.GetTableType() == TableType::CLUSTERED)
                   ? this->InsertRowToClusteredIndex(table, allocatedExtents, lastExtentIndex, row)
                   : this->InsertRowToHeapTable(table, allocatedExtents, lastExtentIndex, row);
    }

    bool Database::InsertRowToClusteredIndex(const Table &table, vector<extent_id_t> &allocatedExtents, extent_id_t &lastExtentIndex, Row *row)
    {
        const auto &tableHeader = table.GetTableHeader();

        IndexPage *indexPage = nullptr;

        if (tableHeader.clusteredIndexPageId == 0)
        {
            indexPage = this->CreateIndexPage(table.GetTableId());
            this->tables[table.GetTableId()]->SetIndexPageId(indexPage->GetPageId());
        }
        else
            indexPage = this->pageManager->GetIndexPage(tableHeader.clusteredIndexPageId);

        vector<column_index_t> indexedColumns = indexPage->GetIndexedColumns();

        if (indexedColumns.empty())
            table.GetIndexedColumnKeys(&indexedColumns);

        const auto &keyBlock = row->GetData()[indexedColumns[0]];

        const Key key(keyBlock->GetBlockData(), keyBlock->GetBlockSize());

        int indexPosition = 0;

        Node *node = indexPage->FindAppropriateNodeForInsert(&table, key, &indexPosition);

        const Constants::byte rowCategory = Database::GetObjectSizeToCategory(row->GetTotalRowSize());

        page_id_t rowInsertedPageId = 0;
        extent_id_t rowInsertedExtentId = 0;

        if (!node->data.empty())
        {
            const auto &previousObject = node->data.begin() + indexPosition - 1;

            const page_id_t pageFreeSpacePageId = Database::GetPfsAssociatedPage(previousObject->pageId);

            PageFreeSpacePage *pageFreeSpacePage = this->pageManager->GetPageFreeSpacePage(pageFreeSpacePageId);

            const Constants::byte pageSizeCategory = pageFreeSpacePage->GetPageSizeCategory(previousObject->pageId);
            // rowCategory < pageSizeCategory &&
            if (this->TryInsertRowToPage(table, pageFreeSpacePage, previousObject->pageId, previousObject->extentId, row, indexPosition))
            {
                rowInsertedPageId = previousObject->pageId;
                rowInsertedExtentId = previousObject->extentId;
            }
            else
            {
                const page_id_t extentFirstPageId = Database::CalculateSystemPageOffsetByExtentId(previousObject->extentId);

                // check if there is at least one page available left in the extent
                if (previousObject->pageId < extentFirstPageId + EXTENT_SIZE - 1 && this->TryInsertRowToPage(table, pageFreeSpacePage, ++previousObject->pageId, previousObject->extentId, row, indexPosition))
                {
                    rowInsertedPageId = ++previousObject->pageId;
                    rowInsertedExtentId = previousObject->extentId;
                }
                else
                {
                    Page *newPage = this->CreateDataPage(table.GetTableId());

                    const page_id_t newPageAssosiatedPfs = Database::GetPfsAssociatedPage(previousObject->pageId);

                    pageFreeSpacePage = this->pageManager->GetPageFreeSpacePage(newPageAssosiatedPfs);

                    // should never fail
                    if (!this->TryInsertRowToPage(table, pageFreeSpacePage, newPage, row, indexPosition))
                        throw std::invalid_argument("Database::InsertRowToClusteredIndex: Failed to insert to new page");

                    rowInsertedPageId = ++previousObject->pageId;
                    rowInsertedExtentId = previousObject->extentId;
                }
            }

            node->keys.insert(node->keys.begin() + indexPosition, key);

            node->data.insert(node->data.begin() + indexPosition, BPlusTreeData(rowInsertedPageId, rowInsertedExtentId));

            return true;
        }

        extent_id_t newExtentId = 0;

        Page *newPage = this->CreateDataPage(table.GetTableId(), &newExtentId);

        const page_id_t &newPageId = newPage->GetPageId();

        const page_id_t pageFreeSpacePageId = Database::GetPfsAssociatedPage(newPageId);

        PageFreeSpacePage *pageFreeSpacePage = this->pageManager->GetPageFreeSpacePage(pageFreeSpacePageId);

        // should never fail
        if (!this->TryInsertRowToPage(table, pageFreeSpacePage, newPage, row, indexPosition))
            throw std::invalid_argument("Failed to insert to new page");

        rowInsertedPageId = newPageId;
        rowInsertedExtentId = newExtentId;

        node->keys.insert(node->keys.begin() + indexPosition, key);

        node->data.insert(node->data.begin() + indexPosition, BPlusTreeData(rowInsertedPageId, rowInsertedExtentId));

        return true;
    }

    bool Database::TryInsertRowToPage(const Table &table, PageFreeSpacePage *pageFreeSpacePage, const page_id_t &pageId, const extent_id_t &extentId, Row *row, const int &indexPosition)
    {
        Page *page = this->pageManager->GetPage(pageId, extentId, &table);
        if (row->GetTotalRowSize() <= page->GetBytesLeft())
        {
            page->InsertRow(row, indexPosition);
            pageFreeSpacePage->SetPageMetaData(page);

            return true;
        }

        return false;
    }

    bool Database::TryInsertRowToPage(const Table &table, PageFreeSpacePage *pageFreeSpacePage, Page *page, Row *row, const int &indexPosition)
    {
        if (row->GetTotalRowSize() <= page->GetBytesLeft())
        {
            page->InsertRow(row, indexPosition);
            pageFreeSpacePage->SetPageMetaData(page);

            return true;
        }

        return false;
    }

    bool Database::InsertRowToHeapTable(const Table &table, vector<extent_id_t> &allocatedExtents, extent_id_t &lastExtentIndex, Row *row)
    {
        const IndexAllocationMapPage *tableMapPage = pageManager->GetIndexAllocationMapPage(table.GetTableHeader().indexAllocationMapPageId);

        tableMapPage->GetAllocatedExtents(&allocatedExtents, lastExtentIndex);
        lastExtentIndex = allocatedExtents.size() - 1;

        const Constants::byte rowCategory = Database::GetObjectSizeToCategory(row->GetTotalRowSize());

        for (const auto &extentId : allocatedExtents)
        {
            const page_id_t extentFirstPageId = Database::CalculateSystemPageOffsetByExtentId(extentId);

            const page_id_t firstDataPageId = (tableMapPage->GetPageId() != extentFirstPageId)
                                                  ? extentFirstPageId
                                                  : extentFirstPageId + 1;

            for (page_id_t pageId = firstDataPageId; pageId < extentFirstPageId + EXTENT_SIZE; pageId++)
            {
                const page_id_t pageFreeSpacePageId = Database::GetPfsAssociatedPage(pageId);
                PageFreeSpacePage *pageFreeSpacePage = pageManager->GetPageFreeSpacePage(pageFreeSpacePageId);

                if (pageFreeSpacePage->GetPageType(pageId) != PageType::DATA)
                    break;

                const Constants::byte pageSizeCategory = pageFreeSpacePage->GetPageSizeCategory(pageId);

                // find potential candidate
                if (rowCategory <= pageSizeCategory)
                {
                    Page *page = this->pageManager->GetPage(pageId, extentId, &table);

                    if (row->GetTotalRowSize() > page->GetBytesLeft())
                        continue;

                    page->InsertRow(row);
                    pageFreeSpacePage->SetPageMetaData(page);

                    return true;
                }
            }
        }

        Page *newPage = this->CreateDataPage(table.GetTableId());

        newPage->InsertRow(row);

        return true;
    }

    void Database::SelectTableRows(const table_id_t &tableId, vector<Row> *selectedRows, const size_t &rowsToSelect, const vector<RowCondition *> *conditions)
    {
        const Table *table = this->GetTable(tableId);

        if (table->GetTableType() == TableType::HEAP)
        {
            this->SelectRowsFromHeapTable(table, selectedRows, rowsToSelect, conditions);
            return;
        }

        this->SelectRowFromClusteredTable(table, selectedRows, rowsToSelect, conditions);
    }

    void Database::SelectRowsFromHeapTable(const Table *table, vector<Row> *selectedRows, const size_t &rowsToSelect, const vector<RowCondition *> *conditions)
    {
        const IndexAllocationMapPage *tableMapPage = pageManager->GetIndexAllocationMapPage(table->GetTableHeader().indexAllocationMapPageId);

        vector<extent_id_t> tableExtentIds;
        tableMapPage->GetAllocatedExtents(&tableExtentIds);

        vector<thread> workerThreads;
        for (const auto &extentId : tableExtentIds)
        {
            workerThreads.emplace_back([this, selectedRows, conditions, rowsToSelect, extentId, tableMapPage, table]
                                       { ThreadSelect(table, tableMapPage, extentId, rowsToSelect, conditions, selectedRows); });
        }

        for (auto &workerThread : workerThreads)
            workerThread.join();
    }

    void Database::SelectRowFromClusteredTable(const Table *table, vector<Row> *selectedRows, const size_t &rowsToSelect, const vector<RowCondition *> *conditions)
    {
        const auto &tableHeader = table->GetTableHeader();

        IndexPage *indexPage = this->pageManager->GetIndexPage(tableHeader.clusteredIndexPageId);

        vector<QueryData> results;
        int minKey = 12, maxKey = 25;

        indexPage->RangeQuery(Key(&minKey, sizeof(int)), Key(&maxKey, sizeof(int)), results);

        const Page *page = (!results.empty())
                         ? this->pageManager->GetPage(results[0].treeData.pageId, results[0].treeData.extentId, table)
                         : nullptr;

        for (const auto &result : results)
        {
            // get new page else use current one
            if (result.treeData.pageId != page->GetPageId())
                page = this->pageManager->GetPage(result.treeData.pageId, result.treeData.extentId, table);

            selectedRows->push_back(page->GetRowByIndex(*table, result.indexPosition));
        }
    }

    void Database::ThreadSelect(const Table *table, const IndexAllocationMapPage *tableMapPage, const extent_id_t &extentId, const size_t &rowsToSelect, const vector<RowCondition *> *conditions, vector<Row> *selectedRows)
    {
        const page_id_t extentFirstPageId = Database::CalculateSystemPageOffset(extentId * EXTENT_SIZE);

        const page_id_t pfsPageId = Database::GetPfsAssociatedPage(extentFirstPageId);

        const PageFreeSpacePage *pageFreeSpacePage = this->pageManager->GetPageFreeSpacePage(pfsPageId);

        const page_id_t pageId = (tableMapPage->GetPageId() != extentFirstPageId)
                                     ? extentFirstPageId
                                     : extentFirstPageId + 1;

        for (page_id_t extentPageId = pageId; extentPageId < extentFirstPageId + EXTENT_SIZE; extentPageId++)
        {
            if (pageFreeSpacePage->GetPageType(extentPageId) != PageType::DATA)
                break;

            const Page *page = this->pageManager->GetPage(extentPageId, extentId, table);

            if (page->GetPageSize() == 0)
                continue;

            this->pageSelectMutex.lock();

            page->GetRows(selectedRows, *table, rowsToSelect, conditions);

            this->pageSelectMutex.unlock();

            if (selectedRows->size() >= rowsToSelect)
                return;
        }
    }

    void Database::UpdateTableRows(const table_id_t &tableId, const vector<Block *> &updates, const vector<RowCondition *> *conditions)
    {
        const Table *table = this->GetTable(tableId);

        const IndexAllocationMapPage *tableMapPage = pageManager->GetIndexAllocationMapPage(table->GetTableHeader().indexAllocationMapPageId);

        vector<extent_id_t> tableExtentIds;
        tableMapPage->GetAllocatedExtents(&tableExtentIds);

        for (const auto &extentId : tableExtentIds)
        {
            const page_id_t extentFirstPageId = Database::CalculateSystemPageOffset(extentId * EXTENT_SIZE);

            const page_id_t pfsPageId = Database::GetPfsAssociatedPage(extentFirstPageId);

            const PageFreeSpacePage *pageFreeSpacePage = pageManager->GetPageFreeSpacePage(pfsPageId);

            const page_id_t pageId = (tableMapPage->GetPageId() != extentFirstPageId)
                                         ? extentFirstPageId
                                         : extentFirstPageId + 1;

            for (page_id_t extentPageId = pageId; extentPageId < extentFirstPageId + EXTENT_SIZE; extentPageId++)
            {
                if (pageFreeSpacePage->GetPageType(extentPageId) != PageType::DATA)
                    break;

                Page *page = this->pageManager->GetPage(extentPageId, extentId, table);

                page->UpdateRows(conditions);
            }
        }
    }

    Page *Database::CreateDataPage(const table_id_t &tableId, extent_id_t *allocatedExtentId)
    {
        PageFreeSpacePage *pageFreeSpacePage = nullptr;
        extent_id_t newExtentId = 0;
        page_id_t lowerLimit = 0, newPageId = 0;

        if (!this->AllocateNewExtent(&pageFreeSpacePage, &lowerLimit, &newPageId, &newExtentId, tableId))
            return nullptr;

        for (page_id_t pageId = lowerLimit; pageId < newPageId + EXTENT_SIZE; pageId++)
            pageFreeSpacePage->SetPageMetaData(this->pageManager->CreatePage(pageId));

        if (allocatedExtentId != nullptr)
            *allocatedExtentId = newExtentId;

        return this->pageManager->GetPage(lowerLimit, newExtentId, this->tables[tableId]);
    }

    LargeDataPage *Database::CreateLargeDataPage(const table_id_t &tableId)
    {
        PageFreeSpacePage *pageFreeSpacePage = nullptr;
        extent_id_t newExtentId = 0;
        page_id_t lowerLimit = 0, newPageId = 0;

        if (!this->AllocateNewExtent(&pageFreeSpacePage, &lowerLimit, &newPageId, &newExtentId, tableId))
            return nullptr;

        for (page_id_t pageId = lowerLimit; pageId < newPageId + EXTENT_SIZE; pageId++)
            pageFreeSpacePage->SetPageMetaData(this->pageManager->CreateLargeDataPage(pageId));

        return this->pageManager->GetLargeDataPage(lowerLimit, newExtentId, this->tables[tableId]);
    }

    IndexPage *Database::CreateIndexPage(const table_id_t &tableId)
    {
        PageFreeSpacePage *pageFreeSpacePage = nullptr;
        extent_id_t newExtentId = 0;
        page_id_t lowerLimit = 0, newPageId = 0;

        if (!this->AllocateNewExtent(&pageFreeSpacePage, &lowerLimit, &newPageId, &newExtentId, tableId))
            return nullptr;

        for (page_id_t pageId = lowerLimit; pageId < newPageId + EXTENT_SIZE; pageId++)
            pageFreeSpacePage->SetPageMetaData(this->pageManager->CreateIndexPage(pageId));

        return this->pageManager->GetIndexPage(lowerLimit, newExtentId);
    }

    bool Database::AllocateNewExtent(PageFreeSpacePage **pageFreeSpacePage, page_id_t *lowerLimit, page_id_t *newPageId, extent_id_t *newExtentId, const table_id_t &tableId)
    {
        GlobalAllocationMapPage *gamPage = this->pageManager->GetGlobalAllocationMapPage(this->header.lastGamPageId);

        IndexAllocationMapPage *tableMapPage = nullptr;

        if (tableId >= this->tables.size())
            return false;

        const page_id_t &indexAllocationMapPageId = this->tables[tableId]->GetTableHeader().indexAllocationMapPageId;

        *pageFreeSpacePage = this->pageManager->GetPageFreeSpacePage(this->header.lastPageFreeSpacePageId);

        if ((*pageFreeSpacePage)->IsFull())
        {
            *pageFreeSpacePage = this->pageManager->CreatePageFreeSpacePage((*pageFreeSpacePage)->GetPageId() + PAGE_FREE_SPACE_SIZE);
            this->header.lastPageFreeSpacePageId = (*pageFreeSpacePage)->GetPageId();
        }

        if (gamPage->IsFull())
        {
            gamPage = this->pageManager->CreateGlobalAllocationMapPage(gamPage->GetPageId() + GAM_NUMBER_OF_PAGES);

            // get the last iam page always
            IndexAllocationMapPage *previousTableMapPage = this->pageManager->GetIndexAllocationMapPage(indexAllocationMapPageId);

            *newExtentId = gamPage->AllocateExtent();

            *newPageId = Database::CalculateSystemPageOffsetByExtentId(*newExtentId);

            previousTableMapPage->SetNextPageId(*newPageId);

            tableMapPage = this->pageManager->CreateIndexAllocationMapPage(tableId, *newPageId, *newExtentId);
            this->tables[tableId]->UpdateIndexAllocationMapPageId(*newPageId);

            (*pageFreeSpacePage)->SetPageMetaData(tableMapPage);
        }
        else
        {
            *newExtentId = gamPage->AllocateExtent();
            *newPageId = Database::CalculateSystemPageOffsetByExtentId(*newExtentId);
        }

        const bool isFirstExtent = indexAllocationMapPageId == 0;
        if (isFirstExtent && tableMapPage == nullptr)
        {
            tableMapPage = this->pageManager->CreateIndexAllocationMapPage(tableId, *newPageId, *newExtentId);

            (*pageFreeSpacePage)->SetPageMetaData(tableMapPage);

            this->tables[tableId]->UpdateIndexAllocationMapPageId(*newPageId);
        }
        else
            tableMapPage = this->pageManager->GetIndexAllocationMapPage(indexAllocationMapPageId);

        tableMapPage->SetAllocatedExtent(*newExtentId, gamPage);

        *lowerLimit = (isFirstExtent)
                          ? *newPageId + 1
                          : *newPageId;

        return true;
    }

    const Table *Database::GetTable(const table_id_t &tableId) const
    {
        if (tableId >= this->tables.size())
            throw out_of_range("No table with ID: " + to_string(tableId) + " exists");

        return this->tables[tableId];
    }

    LargeDataPage *Database::GetTableLastLargeDataPage(const table_id_t &tableId, const page_size_t &minObjectSize)
    {
        if (tableId >= this->tables.size())
            return nullptr;

        const IndexAllocationMapPage *tableMapPage = pageManager->GetIndexAllocationMapPage(this->tables[tableId]->GetTableHeader().indexAllocationMapPageId);
        LargeDataPage *lastLargeDataPage = nullptr;

        vector<extent_id_t> allocatedExtents;
        tableMapPage->GetAllocatedExtents(&allocatedExtents);

        for (const auto &extentId : allocatedExtents)
        {
            const page_id_t firstExtentPageId = Database::CalculateSystemPageOffsetByExtentId(extentId);

            for (page_id_t pageId = firstExtentPageId; pageId < firstExtentPageId + EXTENT_SIZE; pageId++)
            {
                const page_id_t correspondingPfsPageId = Database::GetPfsAssociatedPage(pageId);

                const PageFreeSpacePage *pageFreeSpace = pageManager->GetPageFreeSpacePage(correspondingPfsPageId);
                const Constants::byte objectSizeCategory = Database::GetObjectSizeToCategory(minObjectSize);

                if (pageFreeSpace->GetPageType(pageId) != PageType::LOB)
                    break;

                if (objectSizeCategory > pageFreeSpace->GetPageSizeCategory(pageId))
                    continue;

                lastLargeDataPage = this->pageManager->GetLargeDataPage(pageId, extentId, this->tables[tableId]);

                if (lastLargeDataPage->GetBytesLeft() >= minObjectSize)
                    return lastLargeDataPage;
            }
        }

        return nullptr;
    }

    LargeDataPage *Database::GetLargeDataPage(const page_id_t &pageId, const table_id_t &tableId)
    {
        if (tableId >= this->tables.size())
            return nullptr;

        const page_id_t tableMapPageId = this->tables[tableId]->GetTableHeader().indexAllocationMapPageId;

        IndexAllocationMapPage *tableMapPage = this->pageManager->GetIndexAllocationMapPage(tableMapPageId);

        vector<extent_id_t> allocatedExtents;
        tableMapPage->GetAllocatedExtents(&allocatedExtents);

        extent_id_t associatedExtentId = 0;
        bool extentFound = false;
        for (const auto &extentId : allocatedExtents)
        {
            const page_id_t firstExtentPageId = Database::CalculateSystemPageOffsetByExtentId(extentId);

            if (pageId >= firstExtentPageId && pageId < firstExtentPageId + EXTENT_SIZE)
            {
                associatedExtentId = extentId;
                extentFound = true;
                break;
            }
        }

        return (extentFound)
                   ? this->pageManager->GetLargeDataPage(pageId, associatedExtentId, this->tables[tableId])
                   : nullptr;
    }

    void Database::SetPageMetaDataToPfs(const Page *page) const
    {
        const page_id_t correspondingPfsPageId = Database::GetPfsAssociatedPage(page->GetPageId());

        PageFreeSpacePage *pageFreeSpacePage = this->pageManager->GetPageFreeSpacePage(correspondingPfsPageId);

        pageFreeSpacePage->SetPageMetaData(page);
    }

    string Database::GetFileName() const { return this->filename + this->fileExtension; }
    DatabaseHeader::DatabaseHeader()
    {
        this->databaseNameSize = 0;
        this->numberOfTables = 0;
        this->lastTableId = 0;
        this->lastPageFreeSpacePageId = 0;
        this->lastGamPageId = 0;
    }

    DatabaseHeader::DatabaseHeader(const string &databaseName, const table_number_t &numberOfTables, const page_id_t &lastPageFreeSpacePageId, const page_id_t &lastGamPageId)
    {
        this->databaseName = databaseName;
        this->numberOfTables = numberOfTables;
        this->databaseNameSize = 0;
        this->lastTableId = 0;
        this->lastPageFreeSpacePageId = lastPageFreeSpacePageId;
        this->lastGamPageId = lastGamPageId;
    }

    DatabaseHeader::DatabaseHeader(const DatabaseHeader &dbHeader)
    {
        this->databaseName = dbHeader.databaseName;
        this->numberOfTables = dbHeader.numberOfTables;
        this->databaseNameSize = this->databaseName.size();
        this->lastTableId = dbHeader.lastTableId;
        this->lastPageFreeSpacePageId = dbHeader.lastPageFreeSpacePageId;
        this->lastGamPageId = dbHeader.lastGamPageId;
    }

    DatabaseHeader &DatabaseHeader::operator=(const DatabaseHeader &dbHeader)
    {
        if (&dbHeader == this)
            return *this;

        this->databaseName = dbHeader.databaseName;
        this->databaseNameSize = dbHeader.databaseNameSize;
        this->numberOfTables = dbHeader.numberOfTables;
        this->lastTableId = dbHeader.lastTableId;
        this->lastGamPageId = dbHeader.lastGamPageId;
        this->lastPageFreeSpacePageId = dbHeader.lastPageFreeSpacePageId;

        return *this;
    }
}
