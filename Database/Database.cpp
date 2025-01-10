#include "Database.h"
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

    void Database::WriteHeaderToFile() const
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

    page_id_t Database::CalculateSystemPageOffsetByExtentId(const extent_id_t &extentId)
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

    Table *Database::CreateTable(const string &tableName, const vector<StorageTypes::Column *> &columns, const vector<column_index_t> *clusteredKeyIndexes, const vector<vector<column_index_t>> *nonClusteredIndexes)
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

    void Database::DeleteTable(const string& tableName)
    {
        Table* table = nullptr;
        vector<Table*>::iterator it;

        for (it = this->tables.begin(); it != this->tables.end(); it++)
        {
            if ((*it)->GetTableName() == tableName)
            {
                table = *it;
                break;
            }
        }

        if (table == nullptr)
        {
            const string exceptionMsg = "Database::OpenTable: No table with name " + tableName + " exists.";

            throw invalid_argument(exceptionMsg);
        }

        const auto& tableHeader = table->GetTableHeader();

        const IndexAllocationMapPage* indexAllocationMapPage = StorageManager::Get().GetIndexAllocationMapPage(tableHeader.indexAllocationMapPageId);

        if (indexAllocationMapPage == nullptr)
        {
            this->tables.erase(it);
            delete table;
            return;
        }

        const page_id_t globalAllocationMapPageId = Database::GetGamAssociatedPage(tableHeader.indexAllocationMapPageId);

        const GlobalAllocationMapPage* globalAllocationMapPage = StorageManager::Get().GetGlobalAllocationMapPage(globalAllocationMapPageId);

        vector<extent_id_t> allocatedExtents;
        indexAllocationMapPage->GetAllocatedExtents(&allocatedExtents);

        //deallocate pages as well
        for (const auto& extentId : allocatedExtents)
        {

        }
        //deletes all rows
        table->Delete(nullptr);
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

    void Database::InsertRowToPage(const table_id_t &tableId, vector<extent_id_t> &allocatedExtents, extent_id_t &lastExtentIndex, Row *row)
    {
        const Table* table = this->GetTable(tableId);

        page_id_t rowPageId;
        extent_id_t rowExtentId;
        int rowIndexPosition;

        if (table->GetTableType() == TableType::CLUSTERED)
            this->InsertRowToClusteredIndex(*table, row, &rowPageId, &rowExtentId, &rowIndexPosition);
        else
            this->InsertRowToHeapTable(*table, allocatedExtents, lastExtentIndex, row, &rowPageId, &rowExtentId, &rowIndexPosition);

        //insert to Non Clustered Indexes
        if(!table->HasNonClusteredIndexes())
            return;

        vector<vector<column_index_t>> nonClusteredIndexedColumns;
        table->GetNonClusteredIndexedColumnKeys(&nonClusteredIndexedColumns);

        BPlusTreeNonClusteredData nonClusteredData(rowPageId, rowExtentId, rowIndexPosition);

        for (int i = 0; i < nonClusteredIndexedColumns.size(); i++)
            this->InsertRowToNonClusteredIndex(tableId, row, i, nonClusteredIndexedColumns[i], nonClusteredData);
    }

    void Database::InsertRowToClusteredIndex(const Table &table, Row *row, page_id_t* rowPageId, extent_id_t* rowExtentId, int* rowIndex)
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

        *rowIndex = indexPosition;

        SplitPage(splitLeaves, tree->GetBranchingFactor(), table);

        //const Constants::byte rowCategory = Database::GetObjectSizeToCategory(row->GetTotalRowSize());

        if (node->data.pageId != 0)
        {
            this->InsertRowToNonEmptyNode(node, table, row, key, indexPosition);

            *rowPageId = node->data.pageId;
            *rowExtentId = node->data.extentId;
            return;
        }

        extent_id_t newExtentId = 0;

        Page *newPage = this->CreateDataPage(table.GetTableId(), &newExtentId);

        const page_id_t &newPageId = newPage->GetPageId();

        PageFreeSpacePage *pageFreeSpacePage =  this->GetAssociatedPfsPage(newPageId);

        // should never fail
        Database::InsertRowToPage(pageFreeSpacePage, newPage, row, indexPosition);

        node->keys.insert(node->keys.begin() + indexPosition, key);

        node->data.pageId = newPageId;
        node->data.extentId = newExtentId;

        *rowPageId = newPageId;
        *rowExtentId = newExtentId;
    }

    void Database::InsertRowToNonClusteredIndex(const table_id_t& tableId, Row* row, const int& nonClusteredIndexId, const vector<column_index_t>& indexedColumns, const BPlusTreeNonClusteredData& data)
    {
        Table* table = this->tables.at(tableId);

        BPlusTree* tree = table->GetNonClusteredIndexTree(nonClusteredIndexId);

        const auto &keyBlock = row->GetData()[indexedColumns[0]];

        const Key key(keyBlock->GetBlockData(), keyBlock->GetBlockSize(), KeyType::Int);

        int indexPosition = 0;

        vector<pair<Node *, Node *>> splitLeaves;

        Node *node = tree->FindAppropriateNodeForInsert(key, &indexPosition, &splitLeaves);

        const int& branchingFactor = tree->GetBranchingFactor();

        this->SplitNonClusteredData(splitLeaves, branchingFactor);

        const page_size_t prevNodeSize = node->GetNodeSize();

        node->keys.insert(node->keys.begin() + indexPosition, key);

        node->nonClusteredData.insert(node->nonClusteredData.begin() + indexPosition, data);

        this->SplitNodeFromIndexPage(*table, node, prevNodeSize);
    }

    void Database::SplitNonClusteredData(vector<pair<Indexing::Node*,Indexing::Node*>>& splitLeaves, const int & branchingFactor)
    {
        if (splitLeaves.empty())
            return;

        for (auto &[firstNode, secondNode] : splitLeaves)
        {
            if (!firstNode->isLeaf || !secondNode->isLeaf)
                continue;

            secondNode->nonClusteredData.assign(firstNode->nonClusteredData.begin() + branchingFactor, firstNode->nonClusteredData.end());
            firstNode->nonClusteredData.resize(branchingFactor);
        }
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

            PageFreeSpacePage *pageFreeSpacePage = this->GetAssociatedPfsPage(pageId);

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

            UpdateNonClusteredData(table, nextLeafPage, nextLeafPageId, nextLeafExtentId);
        }
    }

    void Database::UpdateNonClusteredData(const Table& table, Page* nextLeafPage, const page_id_t& nextLeafPageId, const extent_id_t& nextLeafExtentId)
    {
       if(!table.HasNonClusteredIndexes())
            return;

        Table* tablePtr = this->tables.at(table.GetTableId());
            
        //update any nonClusteredIndexes
        vector<vector<column_index_t>> nonClusteredIndexedColumns;
        tablePtr->GetNonClusteredIndexedColumnKeys(&nonClusteredIndexedColumns);

        for (int i = 0; i < nonClusteredIndexedColumns.size(); i++)
        {
            BPlusTree* nonClusteredTree = tablePtr->GetNonClusteredIndexTree(i);

            const auto& rows = nextLeafPage->GetDataRowsUnsafe();

            for (page_offset_t index = 0; index < rows->size(); index++)
            {
                const auto &keyBlock = (*rows)[index]->GetData()[nonClusteredIndexedColumns[i][0]];

                const Key key(keyBlock->GetBlockData(), keyBlock->GetBlockSize(), KeyType::Int);

                int64_t val = 0;
                memcpy(&val, key.value.data(), key.value.size());

                nonClusteredTree->UpdateRowData(key, BPlusTreeNonClusteredData(nextLeafPageId, nextLeafExtentId, index));
            }
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

            pageFreeSpacePage = this->GetAssociatedPfsPage(nextLeafPage->GetPageId());
        }

        return nextLeafPage;
    }

    IndexPage* Database::FindOrAllocateNextIndexPage(const table_id_t& tableId, const page_id_t &indexPageId, const int& nodeSize, const int& nonClusteredIndexId)
    {
        const auto& tableHeader = this->GetTable(tableId)->GetTableHeader();

        //tree has no indexPage
        if (indexPageId == 0)
        {
            IndexPage* indexPage = this->CreateIndexPage(tableId);
            Table* table = this->tables.at(tableId);

            const page_id_t& newIndexPageId = indexPage->GetPageId();
            indexPage->SetTreeId(newIndexPageId);

            if (nonClusteredIndexId != -1)
            {
                table->SetNonClusteredIndexPageId(newIndexPageId, nonClusteredIndexId);
                return indexPage;
            }

            table->SetClusteredIndexPageId(newIndexPageId);
            return indexPage;
        }

        IndexAllocationMapPage* indexAllocationMapPage = StorageManager::Get().GetIndexAllocationMapPage(tableHeader.indexAllocationMapPageId);
        
        vector<extent_id_t> allocatedExtents;
        indexAllocationMapPage->GetAllocatedExtents(&allocatedExtents);
        
        for(const auto& extentId: allocatedExtents)
        {
            const page_id_t firstExtentPageId = Database::CalculateSystemPageOffsetByExtentId(extentId);

            for(page_id_t nextIndexPageId = firstExtentPageId; nextIndexPageId < firstExtentPageId + EXTENT_SIZE; nextIndexPageId++)
            {
                PageFreeSpacePage *pageFreeSpacePage = this->GetAssociatedPfsPage(indexPageId);

                if(pageFreeSpacePage->GetPageType(nextIndexPageId) != PageType::INDEX)
                    continue;

                //page is free
                if(pageFreeSpacePage->GetPageSizeCategory(nextIndexPageId) == 0)
                    continue;

                IndexPage* indexPage = StorageManager::Get().GetIndexPage(nextIndexPageId);

                const auto& treeId = indexPage->GetTreeId();
                if(indexPage->GetBytesLeft() < nodeSize || ( treeId != indexPageId && treeId != 0 ))
                    continue;

                indexPage->SetTreeId(indexPageId);

                return indexPage;
            }
        }

        IndexPage* indexPage = this->CreateIndexPage(tableHeader.tableId);
        indexPage->SetTreeId(indexPageId);

        return indexPage;
    }

    void Database::InsertRowToNonEmptyNode(Node *node, const Table &table, Row *row, const Key &key, const int &indexPosition)
    {
        PageFreeSpacePage *pageFreeSpacePage = this->GetAssociatedPfsPage(node->data.pageId);

        Page *page = StorageManager::Get().GetPage(node->data.pageId, node->data.extentId, &table);

        Database::InsertRowToPage(pageFreeSpacePage, page, row, indexPosition);

        const page_size_t previousNodeSize = node->GetNodeSize();

        node->keys.insert(node->keys.begin() + indexPosition, key);

        this->SplitNodeFromIndexPage(table, node, previousNodeSize);
    }

    void Database::SplitNodeFromIndexPage(const Table& table, Node*& node, const page_size_t& previousNodeSize)
    {
        IndexPage* indexPage = StorageManager::Get().GetIndexPage(node->header.pageId);

        const auto& nodeSize = node->GetNodeSize();

        if (nodeSize <= indexPage->GetBytesLeft() + previousNodeSize)
            return;

        indexPage->DeleteNode(node->header.indexPosition);

        const NodeHeader previousHeader = node->header;

        indexPage = this->FindOrAllocateNextIndexPage(table.GetTableId(), indexPage->GetPageId(), nodeSize);

        indexPage->InsertNode(node, &node->header.indexPosition);
        node->header.pageId = indexPage->GetPageId();

        if (node->parentHeader.pageId != 0)
        {
            IndexPage* parentNodeIndexPage = StorageManager::Get().GetIndexPage(node->parentHeader.pageId);

            Node* parentNode = parentNodeIndexPage->GetNodeByIndex(node->parentHeader.indexPosition);

            for (auto& child : parentNode->childrenHeaders)
                if (child.pageId == previousHeader.pageId && child.indexPosition == previousHeader.indexPosition)
                    parentNodeIndexPage->UpdateNodeChildHeader(node->parentHeader.indexPosition, child.indexPosition, node->header);
        }

        if (node->isLeaf)
        {
            if (node->previousNodeHeader.pageId != 0)
            {
                IndexPage* previousLeafNodeIndexPage = StorageManager::Get().GetIndexPage(node->previousNodeHeader.pageId);

                previousLeafNodeIndexPage->UpdateNodeNextLeafHeader(node->previousNodeHeader.indexPosition, node->header);
            }

            if (node->nextNodeHeader.pageId != 0)
            {
                IndexPage* nextLeafNodeIndexPage = StorageManager::Get().GetIndexPage(node->nextNodeHeader.pageId);

                nextLeafNodeIndexPage->UpdateNodePreviousLeafHeader(node->nextNodeHeader.indexPosition, node->header);
            }

            return; //leaf has not children so return
        }

        for (auto& child : node->childrenHeaders)
        {
            IndexPage* childIndexPage = StorageManager::Get().GetIndexPage(child.pageId);

            childIndexPage->UpdateNodeParentHeader(child.indexPosition, node->header);
        }
    }

    PageFreeSpacePage * Database::GetAssociatedPfsPage(const page_id_t & pageId)
    {
        const page_id_t pageFreeSpacePageId = Database::GetPfsAssociatedPage(pageId);

        return StorageManager::Get().GetPageFreeSpacePage(pageFreeSpacePageId);
    }

    void Database::InsertRowToPage(PageFreeSpacePage *pageFreeSpacePage, Page *page, Row *row, const int &indexPosition)
    {
        if (row->GetTotalRowSize() <= page->GetBytesLeft())
        {
            page->InsertRow(row, indexPosition);
            pageFreeSpacePage->SetPageMetaData(page);
        }
    }

    void Database::InsertRowToHeapTable(const Table &table, vector<extent_id_t> &allocatedExtents, extent_id_t &lastExtentIndex, Row *row, page_id_t* rowPageId, extent_id_t* rowExtentId, int* rowIndex)
    {
        const IndexAllocationMapPage *tableMapPage = StorageManager::Get().GetIndexAllocationMapPage(table.GetTableHeader().indexAllocationMapPageId);

        if (tableMapPage == nullptr)
        {
            Page *newPage = this->CreateDataPage(table.GetTableId(), rowExtentId);
            newPage->InsertRow(row, rowIndex);
            *rowPageId = newPage->GetPageId();

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

                    page->InsertRow(row, rowIndex);
                    pageFreeSpacePage->SetPageMetaData(page);

                    *rowExtentId = extentId;
                    *rowPageId = pageId;

                    return;
                }
            }
        }

        Page *newPage = this->CreateDataPage(table.GetTableId(), rowExtentId);
        newPage->InsertRow(row, rowIndex);
        *rowPageId = newPage->GetPageId();
    }

    void Database::UpdateTableRows(const table_id_t &tableId, const vector<Block*> &updateBlocks, const vector<Field> *conditions)
    {
        //this is update heap table rows (which should be called if no index is selected)
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

                    for(const auto& update: updateBlocks)
                    {
                        const column_index_t& columnIndex = update->GetColumnIndex();

                        if(rowHeader->largeObjectBitMap->Get(columnIndex))
                        {
                            //do for LOBS

                            continue;
                        }

                        const bool isNull = update->GetBlockData() == nullptr;

                        if(rowHeader->nullBitMap->Get(columnIndex) && isNull)
                            continue;
                        else if(rowHeader->nullBitMap->Get(columnIndex) && !isNull)
                            rowHeader->nullBitMap->Set(columnIndex, false);

                        //leverage table inserts to do this better
                        Block* newBlock = new Block(update);

                        row->InsertColumnData(newBlock, columnIndex);
                    }

                    const row_size_t currentRowSize = row->GetTotalRowSize();

                    if(currentRowSize > page->GetBytesLeft() + rowPreviousSize)
                    {
                        rowsToBeInserted.push_back(row);
                        rows->erase(it);
                        continue;
                    }

                    //it sets dirty to true
                    page->UpdateBytesLeft(rowPreviousSize, currentRowSize);
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
        }
        //leverage heap insert optimization to skips extents with index less than the specified one
        //rows need to be inserted to new page

        extent_id_t lastExtentIndex = tableExtentIds.size() - 1;
        tableExtentIds.clear();

        //skip inserting to non clustered indexes again and just update their position(if index values changes handle)

        for(auto& row: rowsToBeInserted)
            this->InsertRowToPage(table->GetTableId(), tableExtentIds, lastExtentIndex, row);
    }

    void Database::DeleteTableRows(const table_id_t& tableId, const vector<Field>* conditions)
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

                for(const auto& row: *rows)
                    delete row;

                rows->clear();

                page->UpdateBytesLeft();
                page->UpdatePageSize();
                pageFreeSpacePage->SetPageMetaData(page);
            }
        }
    }

    void Database::TruncateTable(const table_id_t & tableId)
    {
        Table *table = this->tables.at(tableId);

        page_id_t indexAllocationMapPageId = table->GetTableHeader().indexAllocationMapPageId;

        while (indexAllocationMapPageId != 0)
        {
            const IndexAllocationMapPage *tableMapPage = StorageManager::Get().GetIndexAllocationMapPage(indexAllocationMapPageId);

            vector<extent_id_t> allocatedExtents;
            tableMapPage->GetAllocatedExtents(&allocatedExtents);

            const page_id_t globalAllocationMapPageId = Database::GetGamAssociatedPage(indexAllocationMapPageId);

            GlobalAllocationMapPage* gamPage = StorageManager::Get().GetGlobalAllocationMapPage(globalAllocationMapPageId);

            indexAllocationMapPageId = tableMapPage->GetNextPageId();

            for (const auto& extentId : allocatedExtents)
            {
                const page_id_t extentFirstPageId = Database::CalculateSystemPageOffsetByExtentId(extentId);

                for (page_id_t pageId = extentFirstPageId; pageId < extentFirstPageId + EXTENT_SIZE; pageId++)
                {
                    const page_id_t pageFreeSpacePageId = Database::GetPfsAssociatedPage(pageId);
                
                    PageFreeSpacePage* pageFreeSpacePage = StorageManager::Get().GetPageFreeSpacePage(pageFreeSpacePageId);

                    pageFreeSpacePage->SetPageFreed(pageId);
                }

                gamPage->DeallocateExtent(extentId);
            }
        }

        table->SetIndexAllocationMapPageId(0);
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