#include "Database.h"
#include <cstdint>
#include <stdexcept>
#include <vcruntime_new_debug.h>
#include <vector>
#include "./Pages/Header/HeaderPage.h"
#include "./Pages/GlobalAllocationMap/GlobalAllocationMapPage.h"
#include "./Pages/PageFreeSpace/PageFreeSpacePage.h"
#include "./Pages/IndexMapAllocation/IndexAllocationMapPage.h"
#include "./Pages/IndexPage/IndexPage.h"
#include "Constants.h"
#include "Table/Table.h"
#include "Column/Column.h"
#include "Row/Row.h"
#include "Pages/LargeObject/LargeDataPage.h"
#include "Storage/StorageManager/StorageManager.h"
#include "../AdditionalLibraries/B+Tree/BPlusTree.h"
#include "Block/Block.h"
#include "../AdditionalLibraries/BitMap/BitMap.h"

using namespace Pages;
using namespace DatabaseEngine::StorageTypes;
using namespace Storage;
using namespace Indexing;
using namespace std;
using namespace ByteMaps;

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
        HeaderPage *metaDataPage = StorageManager::Get().GetHeaderPage(this->filename + this->fileExtension);

        metaDataPage->SetDbHeader(this->header);
    }

    bool Database::IsSystemPage(const page_id_t &pageId) { return pageId == 0 || pageId == 1 || pageId == 2 || pageId % PAGE_FREE_SPACE_SIZE == 1 || pageId % GAM_NUMBER_OF_PAGES == 2; }

    page_id_t Database::GetPfsAssociatedPage(const page_id_t &pageId) { return (pageId / PAGE_FREE_SPACE_SIZE) * PAGE_FREE_SPACE_SIZE + 1; }

    page_id_t Database::GetGamAssociatedPage(const page_id_t &pageId) { return (pageId / GAM_NUMBER_OF_PAGES) * GAM_NUMBER_OF_PAGES + 2; }

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

    Database::Database(const string &dbName)
    {
        this->filename = dbName;
        this->fileExtension = ".db";

        const HeaderPage *headerPage = StorageManager::Get().GetHeaderPage(this->filename + this->fileExtension);

        this->header = *headerPage->GetDatabaseHeader();
        const vector<TableFullHeader> tablesFullHeaders = headerPage->GetTablesFullHeaders();

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
        for (const auto& table : this->tables)
        {
            if(table->GetTableName() == tableName)
                throw invalid_argument("Database::CreateTable: Table with name " + tableName + " already exists!");
        }

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

        const string exceptionMsg = "Database::OpenTable: No table with name " + tableName + " exists.";

        throw invalid_argument(exceptionMsg);
    }

    void CreateDatabase(const string &dbName)
    {
        StorageManager::Get().CreateFile(dbName, ".db");

        constexpr page_id_t firstGamePageId = 2;
        constexpr page_id_t firstPfsPageId = 1;
        
        StorageManager::Get().CreateGlobalAllocationMapPage(dbName + ".db", firstGamePageId);
        StorageManager::Get().CreatePageFreeSpacePage(dbName + ".db", firstPfsPageId);

        HeaderPage *headerPage = StorageManager::Get().CreateHeaderPage(dbName + ".db");

        headerPage->SetDbHeader(DatabaseHeader(dbName, 0, firstPfsPageId, firstGamePageId));
    }

    void UseDatabase(const string &dbName, Database **db)
    {
        *db = new Database(dbName);
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

    void Database::InsertRowToPage(const Table &table, vector<extent_id_t> &allocatedExtents, extent_id_t &lastExtentIndex, Row *row)
    {
        if (table.GetTableType() != TableType::CLUSTERED)
        {
            this->InsertRowToHeapTable(table, allocatedExtents, lastExtentIndex, row);
            return;
        }

        this->InsertRowToClusteredIndex(table, row);
    }

    void Database::InsertRowToClusteredIndex(const Table &table, Row *row)
    {
        const auto &tableHeader = table.GetTableHeader();

        const table_id_t& tableId = table.GetTableId();

        BPlusTree* tree = this->tables[tableId]->GetClusteredIndexedTree();

        vector<column_index_t> indexedColumns;

        table.GetIndexedColumnKeys(&indexedColumns);

        const auto &keyBlock = row->GetData()[indexedColumns[0]];

        const Key key(keyBlock->GetBlockData(), keyBlock->GetBlockSize(), KeyType::Int);

        int indexPosition = 0;

        vector<pair<Node *, Node *>> splitLeaves;

        Node *node = tree->FindAppropriateNodeForInsert(key, &indexPosition, &splitLeaves);

        SplitPage(splitLeaves, tree->GetBranchingFactor(), table);

        const Constants::byte rowCategory = Database::GetObjectSizeToCategory(row->GetTotalRowSize());

        if (node->data.pageId != 0)
        {
            this->InsertRowToNonEmptyNode(node, table, row, key, indexPosition);
            return;
        }

        extent_id_t newExtentId = 0;

        Page *newPage = this->CreateDataPage(table.GetTableId(), &newExtentId);

        const page_id_t &newPageId = newPage->GetPageId();

        const page_id_t pageFreeSpacePageId = Database::GetPfsAssociatedPage(newPageId);

        PageFreeSpacePage *pageFreeSpacePage = StorageManager::Get().GetPageFreeSpacePage(pageFreeSpacePageId);

        // should never fail
        Database::InsertRowToPage(pageFreeSpacePage, newPage, row, indexPosition);

        node->keys.insert(node->keys.begin() + indexPosition, key);

        node->data.pageId = newPageId;
        node->data.extentId = newExtentId;
    }

    void Database::SplitPage(vector<pair<Node *, Node *>> &splitLeaves, const int &branchingFactor, const Table &table)
    {
        if (splitLeaves.empty())
            return;

        for (auto &[firstNode, secondNode] : splitLeaves)
        {
            if (!firstNode->isLeaf || !secondNode->isLeaf)
                continue;

            const page_id_t pageId = firstNode->data.pageId;
            const extent_id_t extentId = firstNode->data.extentId;

            Page *page = StorageManager::Get().GetPage(pageId, extentId, &table);

            const page_id_t extentFirstPageId = Database::CalculateSystemPageOffsetByExtentId(extentId);

            // check if there is at least one page available left in the extent

            extent_id_t nextExtentId = 0;

            const page_id_t pageFreeSpacePageId = Database::GetPfsAssociatedPage(pageId);

            PageFreeSpacePage *pageFreeSpacePage = StorageManager::Get().GetPageFreeSpacePage(pageFreeSpacePageId);

            Page *nextLeafPage = this->FindOrAllocateNextDataPage(pageFreeSpacePage, pageId, extentFirstPageId, extentId, table, &nextExtentId);

            const page_id_t nextLeafPageId = nextLeafPage->GetPageId();
            const extent_id_t nextLeafExtentId = (nextExtentId == 0)
                                                     ? firstNode->data.extentId
                                                     : nextExtentId;

            page->SplitPageRowByBranchingFactor(nextLeafPage, branchingFactor, table);

            pageFreeSpacePage->SetPageMetaData(page);
            pageFreeSpacePage->SetPageMetaData(nextLeafPage);

            secondNode->data.pageId = nextLeafPageId;
            secondNode->data.extentId = nextLeafExtentId;
        }
    }

    Page *Database::FindOrAllocateNextDataPage(PageFreeSpacePage *&pageFreeSpacePage, const page_id_t &pageId, const page_id_t &extentFirstPageId, const extent_id_t &extentId, const Table &table, extent_id_t *nextExtentId)
    {
        Page *nextLeafPage = nullptr;
        if (pageId < extentFirstPageId + EXTENT_SIZE - 1)
        {
            for (page_id_t nextLeafPageId = pageId + 1; nextLeafPageId < extentFirstPageId + EXTENT_SIZE; nextLeafPageId++)
            {
                const auto &pageSizeCategory = pageFreeSpacePage->GetPageSizeCategory(nextLeafPageId);

                if (pageSizeCategory == 0)
                    continue;

                nextLeafPage = StorageManager::Get().GetPage(nextLeafPageId, extentId, &table);

                if (nextLeafPage->GetPageSize() == 0)
                    break;
            }
        }

        if (nextLeafPage == nullptr || nextLeafPage->GetPageSize() > 0)
        {
            nextLeafPage = this->CreateDataPage(table.GetTableId(), nextExtentId);

            const page_id_t newPageAssociatedPfs = Database::GetPfsAssociatedPage(nextLeafPage->GetPageId());

            pageFreeSpacePage = StorageManager::Get().GetPageFreeSpacePage(newPageAssociatedPfs);
        }

        return nextLeafPage;
    }

    void Database::InsertRowToNonEmptyNode(Node *node, const Table &table, Row *row, const Key &key, const int &indexPosition)
    {
        const page_id_t pageFreeSpacePageId = Database::GetPfsAssociatedPage(node->data.pageId);

        PageFreeSpacePage *pageFreeSpacePage = StorageManager::Get().GetPageFreeSpacePage(pageFreeSpacePageId);

        Page *page = StorageManager::Get().GetPage(node->data.pageId, node->data.extentId, &table);

        Database::InsertRowToPage(pageFreeSpacePage, page, row, indexPosition);

        node->keys.insert(node->keys.begin() + indexPosition, key);
    }

    void Database::InsertRowToPage(PageFreeSpacePage *pageFreeSpacePage, Page *page, Row *row, const int &indexPosition)
    {
        if (row->GetTotalRowSize() <= page->GetBytesLeft())
        {
            page->InsertRow(row, indexPosition);
            pageFreeSpacePage->SetPageMetaData(page);
        }
    }

    void Database::InsertRowToHeapTable(const Table &table, vector<extent_id_t> &allocatedExtents, extent_id_t &lastExtentIndex, Row *row)
    {
        const IndexAllocationMapPage *tableMapPage = StorageManager::Get().GetIndexAllocationMapPage(table.GetTableHeader().indexAllocationMapPageId);

        if (tableMapPage == nullptr)
        {
           Page *newPage = this->CreateDataPage(table.GetTableId());

            newPage->InsertRow(row);

            return;
        }
        
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
                PageFreeSpacePage *pageFreeSpacePage = StorageManager::Get().GetPageFreeSpacePage(pageFreeSpacePageId);

                if (pageFreeSpacePage->GetPageType(pageId) != PageType::DATA)
                    break;

                const Constants::byte pageSizeCategory = pageFreeSpacePage->GetPageSizeCategory(pageId);

                // find potential candidate
                if (rowCategory <= pageSizeCategory)
                {
                    Page *page = StorageManager::Get().GetPage(pageId, extentId, &table);

                    if (row->GetTotalRowSize() > page->GetBytesLeft())
                        continue;

                    page->InsertRow(row);
                    pageFreeSpacePage->SetPageMetaData(page);

                    return;
                }
            }
        }

        Page *newPage = this->CreateDataPage(table.GetTableId());

        newPage->InsertRow(row);
    }

    void Database::UpdateTableRows(const table_id_t &tableId, const vector<Field> &updates, const vector<Field> *conditions)
    {
        const Table *table = this->GetTable(tableId);

        const IndexAllocationMapPage *tableMapPage = StorageManager::Get().GetIndexAllocationMapPage(table->GetTableHeader().indexAllocationMapPageId);

        vector<extent_id_t> tableExtentIds;
        tableMapPage->GetAllocatedExtents(&tableExtentIds);

        const auto& columns = table->GetColumns();

        vector<Row*> rowsToBeInserted;

        for (const auto &extentId : tableExtentIds)
        {
            const page_id_t extentFirstPageId = Database::CalculateSystemPageOffset(extentId * EXTENT_SIZE);

            const page_id_t pfsPageId = Database::GetPfsAssociatedPage(extentFirstPageId);

            PageFreeSpacePage *pageFreeSpacePage = StorageManager::Get().GetPageFreeSpacePage(pfsPageId);

            const page_id_t pageId = (tableMapPage->GetPageId() != extentFirstPageId)
                                         ? extentFirstPageId
                                         : extentFirstPageId + 1;

            for (page_id_t extentPageId = pageId; extentPageId < extentFirstPageId + EXTENT_SIZE; extentPageId++)
            {
                if (pageFreeSpacePage->GetPageType(extentPageId) != PageType::DATA)
                    break;

                Page *page = StorageManager::Get().GetPage(extentPageId, extentId, table);

                vector<Row*>* rows = page->GetDataRowsUnsafe();

                vector<Row*>::iterator it;

                for(it = rows->begin(); it != rows->end(); it++)
                {
                    Row* row = *it;

                    if(row == nullptr)
                        continue;

                    RowHeader* rowHeader = row->GetHeader();

                    const row_size_t rowPreviousSize = row->GetTotalRowSize();

                    for(const auto& update: updates)
                    {
                        const column_index_t& columnIndex = update.GetColumnIndex();

                        if(rowHeader->largeObjectBitMap->Get(columnIndex))
                        {
                            //do for LOBS

                            continue;
                        }

                        if(rowHeader->nullBitMap->Get(columnIndex) && update.GetIsNull())
                            continue;
                        else if(rowHeader->nullBitMap->Get(columnIndex) && !update.GetIsNull())
                            rowHeader->nullBitMap->Set(columnIndex, false);

                        //leverage table inserts to do this better
                        const string& data = update.GetData();

                        row->InsertColumnData(new Block(data.c_str(), data.size(), columns[columnIndex]), columnIndex);
                    }

                    if(row->GetTotalRowSize() > page->GetBytesLeft() + rowPreviousSize)
                    {
                        rowsToBeInserted.push_back(row);
                        rows->erase(it);
                        continue;
                    }

                    //update bytesleft is slow and better implementation should be created
                    //but it sets dirty to true
                    page->UpdateBytesLeft();
                    pageFreeSpacePage->SetPageMetaData(page);
                }
                
                if(rowsToBeInserted.empty())
                    continue;

                for(it = rowsToBeInserted.begin(); it != rowsToBeInserted.end(); it++)
                {
                    Row* row = *it;

                    if(page->GetBytesLeft() == 0)
                        break;

                    if(row->GetTotalRowSize() > page->GetBytesLeft())
                        continue;

                    page->InsertRow(row);

                    pageFreeSpacePage->SetPageMetaData(page);

                    rowsToBeInserted.erase(it);
                }
            }

            //leverage heap insert optimization to skips extents with index less than the specified one
            extent_id_t lastExtentIndex = tableExtentIds.size() - 1;
            tableExtentIds.clear();

            for(auto& row: rowsToBeInserted)
                this->InsertRowToHeapTable(*table, tableExtentIds, lastExtentIndex, row);

            //rows need to be inserted to new page
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
            pageFreeSpacePage->SetPageMetaData(StorageManager::Get().CreatePage(pageId));

        if (allocatedExtentId != nullptr)
            *allocatedExtentId = newExtentId;

        return StorageManager::Get().GetPage(lowerLimit, newExtentId, this->tables[tableId]);
    }

    LargeDataPage *Database::CreateLargeDataPage(const table_id_t &tableId)
    {
        PageFreeSpacePage *pageFreeSpacePage = nullptr;
        extent_id_t newExtentId = 0;
        page_id_t lowerLimit = 0, newPageId = 0;

        if (!this->AllocateNewExtent(&pageFreeSpacePage, &lowerLimit, &newPageId, &newExtentId, tableId))
            return nullptr;

        for (page_id_t pageId = lowerLimit; pageId < newPageId + EXTENT_SIZE; pageId++)
            pageFreeSpacePage->SetPageMetaData(StorageManager::Get().CreateLargeDataPage(pageId));

        return StorageManager::Get().GetLargeDataPage(lowerLimit, newExtentId, this->tables[tableId]);
    }

    IndexPage *Database::CreateIndexPage(const table_id_t &tableId)
    {
        PageFreeSpacePage *pageFreeSpacePage = nullptr;
        extent_id_t newExtentId = 0;
        page_id_t lowerLimit = 0, newPageId = 0;

        if (!this->AllocateNewExtent(&pageFreeSpacePage, &lowerLimit, &newPageId, &newExtentId, tableId))
            return nullptr;

        for (page_id_t pageId = lowerLimit; pageId < newPageId + EXTENT_SIZE; pageId++)
            pageFreeSpacePage->SetPageMetaData(StorageManager::Get().CreateIndexPage(pageId));

        return StorageManager::Get().GetIndexPage(lowerLimit, newExtentId);
    }

    bool Database::AllocateNewExtent(PageFreeSpacePage **pageFreeSpacePage, page_id_t *lowerLimit, page_id_t *newPageId, extent_id_t *newExtentId, const table_id_t &tableId)
    {
        GlobalAllocationMapPage *gamPage = StorageManager::Get().GetGlobalAllocationMapPage(this->header.lastGamPageId);

        IndexAllocationMapPage *tableMapPage = nullptr;

        if (tableId >= this->tables.size())
            return false;

        const page_id_t &indexAllocationMapPageId = this->tables[tableId]->GetTableHeader().indexAllocationMapPageId;

        *pageFreeSpacePage = StorageManager::Get().GetPageFreeSpacePage(this->header.lastPageFreeSpacePageId);

        if ((*pageFreeSpacePage)->IsFull())
        {
            *pageFreeSpacePage = StorageManager::Get().CreatePageFreeSpacePage((*pageFreeSpacePage)->GetPageId() + PAGE_FREE_SPACE_SIZE);
            this->header.lastPageFreeSpacePageId = (*pageFreeSpacePage)->GetPageId();
        }

        if (gamPage->IsFull())
        {
            gamPage = StorageManager::Get().CreateGlobalAllocationMapPage(gamPage->GetPageId() + GAM_NUMBER_OF_PAGES);

            // get the last iam page always
            IndexAllocationMapPage *previousTableMapPage = StorageManager::Get().GetIndexAllocationMapPage(indexAllocationMapPageId);

            *newExtentId = gamPage->AllocateExtent();

            *newPageId = Database::CalculateSystemPageOffsetByExtentId(*newExtentId);

            previousTableMapPage->SetNextPageId(*newPageId);

            tableMapPage = StorageManager::Get().CreateIndexAllocationMapPage(tableId, *newPageId, *newExtentId);
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
            tableMapPage = StorageManager::Get().CreateIndexAllocationMapPage(tableId, *newPageId, *newExtentId);

            (*pageFreeSpacePage)->SetPageMetaData(tableMapPage);

            this->tables[tableId]->UpdateIndexAllocationMapPageId(*newPageId);
        }
        else
            tableMapPage = StorageManager::Get().GetIndexAllocationMapPage(indexAllocationMapPageId);

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

        const IndexAllocationMapPage *tableMapPage = StorageManager::Get().GetIndexAllocationMapPage(this->tables[tableId]->GetTableHeader().indexAllocationMapPageId);
        LargeDataPage *lastLargeDataPage = nullptr;

        vector<extent_id_t> allocatedExtents;
        tableMapPage->GetAllocatedExtents(&allocatedExtents);

        for (const auto &extentId : allocatedExtents)
        {
            const page_id_t firstExtentPageId = Database::CalculateSystemPageOffsetByExtentId(extentId);

            for (page_id_t pageId = firstExtentPageId; pageId < firstExtentPageId + EXTENT_SIZE; pageId++)
            {
                const page_id_t correspondingPfsPageId = Database::GetPfsAssociatedPage(pageId);

                const PageFreeSpacePage *pageFreeSpace = StorageManager::Get().GetPageFreeSpacePage(correspondingPfsPageId);
                const Constants::byte objectSizeCategory = Database::GetObjectSizeToCategory(minObjectSize);

                if (pageFreeSpace->GetPageType(pageId) != PageType::LOB)
                    break;

                if (objectSizeCategory > pageFreeSpace->GetPageSizeCategory(pageId))
                    continue;

                lastLargeDataPage = StorageManager::Get().GetLargeDataPage(pageId, extentId, this->tables[tableId]);

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

        IndexAllocationMapPage *tableMapPage = StorageManager::Get().GetIndexAllocationMapPage(tableMapPageId);

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
                   ? StorageManager::Get().GetLargeDataPage(pageId, associatedExtentId, this->tables[tableId])
                   : nullptr;
    }

    void Database::SetPageMetaDataToPfs(const Page *page) const
    {
        const page_id_t correspondingPfsPageId = Database::GetPfsAssociatedPage(page->GetPageId());

        PageFreeSpacePage *pageFreeSpacePage = StorageManager::Get().GetPageFreeSpacePage(correspondingPfsPageId);

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