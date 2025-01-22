#include "Database.h"
#include <cstdint>
#include <vector>
#include "./Pages/PageFreeSpace/PageFreeSpacePage.h"
#include "./Pages/IndexMapAllocation/IndexAllocationMapPage.h"
#include "./Pages/IndexPage/IndexPage.h"
#include "Constants.h"
#include "B+Tree/BPlusTree.h"
#include "Pages/Page.h"
#include "Table/Table.h"
#include "Row/Row.h"
#include "Storage/StorageManager/StorageManager.h"
#include "Block/Block.h"

using namespace Pages;
using namespace DatabaseEngine::StorageTypes;
using namespace Storage;
using namespace Indexing;
using namespace std;
using namespace ByteMaps;

namespace DatabaseEngine {
    void Database::InsertRowToClusteredIndex(const table_id_t& tableId, Row *row, page_id_t* rowPageId, extent_id_t* rowExtentId, int* rowIndex)
    {
        Table* table = this->tables.at(tableId);

        BPlusTree* tree = table->GetClusteredIndexedTree();

        vector<column_index_t> indexedColumns;

        table->GetIndexedColumnKeys(&indexedColumns);

        const auto &keyBlock = row->GetData()[indexedColumns[0]];

        const Key key(keyBlock->GetBlockData(), keyBlock->GetBlockSize(), KeyType::Int);

        int indexPosition = 0;

        Node *node = tree->FindAppropriateNodeForInsert(key, &indexPosition);

        *rowIndex = indexPosition;

        if (node->dataPageId != 0)
        {
            this->InsertRowToNonEmptyNode(node, *table, row, key, indexPosition);

            *rowPageId = node->dataPageId;
            return;
        }

        //first Insert
        Page *newPage = this->CreateDataPage(table->GetTableId());

        const page_id_t &newPageId = newPage->GetPageId();

        PageFreeSpacePage *pageFreeSpacePage =  this->GetAssociatedPfsPage(newPageId);

        // should never fail
        Database::InsertRowToPage(pageFreeSpacePage, newPage, row, indexPosition);

        node->keys.insert(node->keys.begin() + indexPosition, key);
        node->currentNodeSize += key.GetKeySize();

        node->dataPageId = newPageId;
        *rowPageId = newPageId;

        this->SplitNodeFromIndexPage(tableId, node);
    }

    void Database::InsertRowToNonClusteredIndex(const table_id_t& tableId, Row* row, const int& nonClusteredIndexId, const vector<column_index_t>& indexedColumns, const BPlusTreeNonClusteredData& data)
    {
        Table* table = this->tables.at(tableId);

        BPlusTree* tree = table->GetNonClusteredIndexTree(nonClusteredIndexId);

        const auto &keyBlock = row->GetData()[indexedColumns[0]];

        const Key key(keyBlock->GetBlockData(), keyBlock->GetBlockSize(), KeyType::Int);

        int indexPosition = 0;
        Node *node = tree->FindAppropriateNodeForInsert(key, &indexPosition);

        node->keys.insert(node->keys.begin() + indexPosition, key);
        node->nonClusteredData.insert(node->nonClusteredData.begin() + indexPosition, data);
        node->prevNodeSize = node->currentNodeSize;
        node->currentNodeSize = node->GetNodeSize();

        this->SplitNodeFromIndexPage(tableId, node, nonClusteredIndexId);
    }

    void Database::SplitPage(Node*& firstNode, Node*& secondNode, const int &branchingFactor, const table_id_t& tableId)
    {
        Table* table = this->tables.at(tableId);

        const page_id_t pageId = firstNode->dataPageId;

        const extent_id_t pageExtentId = Database::CalculateExtentIdByPageId(pageId);

        Page *page = StorageManager::Get().GetPage(pageId, pageExtentId, table);

        const page_id_t extentFirstPageId = Database::CalculateSystemPageOffsetByExtentId(pageExtentId);

        // check if there is at least one page available left in the extent
        extent_id_t nextExtentId = 0;
        PageFreeSpacePage *pageFreeSpacePage = this->GetAssociatedPfsPage(pageId);

        Page *nextLeafPage = this->FindOrAllocateNextDataPage(pageFreeSpacePage, pageId, extentFirstPageId, pageExtentId, *table, &nextExtentId);

        const page_id_t nextLeafPageId = nextLeafPage->GetPageId();
        page->SplitPageRowByBranchingFactor(nextLeafPage, branchingFactor, *table);

        pageFreeSpacePage->SetPageMetaData(page);
        pageFreeSpacePage->SetPageMetaData(nextLeafPage);

        secondNode->dataPageId = nextLeafPageId;
        UpdateNonClusteredData(*table, nextLeafPage, nextLeafPageId);
    }

    void Database::UpdateNonClusteredData(const Table& table, Page* nextLeafPage, const page_id_t& nextLeafPageId)
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

                nonClusteredTree->UpdateRowData(key, BPlusTreeNonClusteredData(nextLeafPageId, index));
            }
        }
    }

	IndexPage* Database::FindOrAllocateNextIndexPage(const table_id_t& tableId, const page_id_t &indexPageId, const int& nodeSize, const int& nonClusteredIndexId, const bool& findPageDifferentFromCurrent)
    {
        const auto& tableHeader = this->GetTable(tableId)->GetTableHeader();

        Table* table = this->tables.at(tableId);

        const bool isNonClusteredIndex = nonClusteredIndexId != -1;

        const uint8_t indexId = isNonClusteredIndex
                                ? table->GetNonClusteredIndexId(nonClusteredIndexId)
                                : 0;

        if(indexPageId == 0)
        {
            IndexPage* newIndexPage = this->CreateIndexPage(tableId, indexId);
            
            newIndexPage->SetTreeType(isNonClusteredIndex 
                                    ? TreeType::NonClustered 
                                    : TreeType::Clustered);

            return newIndexPage;
        }

        IndexAllocationMapPage* indexAllocationMapPage = StorageManager::Get().GetIndexAllocationMapPage(tableHeader.indexAllocationMapPageId);
        
        vector<extent_id_t> allocatedExtents;
        indexAllocationMapPage->GetAllocatedExtents(&allocatedExtents);

        for(const auto& extentId: allocatedExtents)
        {
            const page_id_t firstExtentPageId = Database::CalculateSystemPageOffsetByExtentId(extentId);

            for(page_id_t nextIndexPageId = firstExtentPageId; nextIndexPageId < firstExtentPageId + EXTENT_SIZE; nextIndexPageId++)
            {
                if(findPageDifferentFromCurrent 
                    && nextIndexPageId == indexPageId)
                    continue;

                PageFreeSpacePage * pageFreeSpacePage = this->GetAssociatedPfsPage(nextIndexPageId);

                if (pageFreeSpacePage->GetPageType(nextIndexPageId) != PageType::INDEX)
                    continue;

                //page is free
                if(pageFreeSpacePage->GetPageSizeCategory(nextIndexPageId) == 0)
                    continue;

                IndexPage* indexPage = StorageManager::Get().GetIndexPage(nextIndexPageId);

                if(indexPage->GetBytesLeft() < nodeSize
                    || indexPage->GetTreeId() != indexId)
                    continue;

                indexPage->SetTreeType(isNonClusteredIndex 
                                        ? TreeType::NonClustered 
                                        : TreeType::Clustered);
                
                return indexPage;
            }
        }

        IndexPage* newIndexPage = this->CreateIndexPage(tableId, indexId);
            
        newIndexPage->SetTreeType(isNonClusteredIndex 
                                ? TreeType::NonClustered 
                                : TreeType::Clustered);

        return newIndexPage;
    }

    void Database::SplitNodeFromIndexPage(const table_id_t& tableId, Node*& node, const int& nonClusteredIndexId)
    {
        IndexPage* overflowedPage = StorageManager::Get().GetIndexPage(node->header.pageId);

        overflowedPage->UpdateBytesLeft();

        if(overflowedPage->GetBytesLeft() > 0)
        {
            PageFreeSpacePage* overflowedPagePFS = this->GetAssociatedPfsPage(overflowedPage->GetPageId());
            overflowedPagePFS->SetPageMetaData(overflowedPage);
            return;
        }

        vector<Node*>* overflowedPageNodes = overflowedPage->GetNodesUnsafe();

        const int splitFactor = overflowedPage->GetPageSize() / 2;

        const page_size_t availablePageBytes = PAGE_SIZE - PageHeader::GetPageHeaderSize() - IndexPageAdditionalHeader::GetAdditionalHeaderSize();

        IndexPage* nextIndexPage = this->FindOrAllocateNextIndexPage(tableId, overflowedPage->GetPageId(), availablePageBytes, nonClusteredIndexId, true);

        vector<Node*>* nextIndexPageNodes = nextIndexPage->GetNodesUnsafe();

        const page_id_t& nextIndexPageId = nextIndexPage->GetPageId();

        page_offset_t indexPosition = 0;

        for(page_offset_t i = splitFactor; i < overflowedPageNodes->size(); i++)
        {
           const NodeHeader newNodeHeader(nextIndexPageId, indexPosition);

            this->UpdateNodeConnections((*overflowedPageNodes)[i], newNodeHeader);

            (*overflowedPageNodes)[i]->header = newNodeHeader;

            this->UpdateTableIndexes(tableId, (*overflowedPageNodes)[i], nonClusteredIndexId);

            nextIndexPageNodes->push_back((*overflowedPageNodes)[i]);

            indexPosition++;
        }

        overflowedPage->ResizeNodes(splitFactor);
        overflowedPage->UpdateBytesLeft();
        
        nextIndexPage->UpdateBytesLeft();
        nextIndexPage->UpdatePageSize();

        PageFreeSpacePage* overflowedPagePFS = this->GetAssociatedPfsPage(overflowedPage->GetPageId());
        overflowedPagePFS->SetPageMetaData(overflowedPage);

        PageFreeSpacePage* nextIndexPagePFS = this->GetAssociatedPfsPage(nextIndexPageId);
        nextIndexPagePFS->SetPageMetaData(nextIndexPage);
    }

    void Database::UpdateNodeConnections(Node *& node, const NodeHeader& newNodeHeader)
    {
        if (node->parentHeader.pageId != 0)
        {
            IndexPage* parentNodeIndexPage = StorageManager::Get().GetIndexPage(node->parentHeader.pageId);
            Node* parentNode = parentNodeIndexPage->GetNodeByIndex(node->parentHeader.indexPosition);

            for (int index = 0; index < parentNode->childrenHeaders.size(); index++)
            {
                if (parentNode->childrenHeaders[index].pageId == node->header.pageId
                    && parentNode->childrenHeaders[index].indexPosition == node->header.indexPosition)
                {
                    parentNodeIndexPage->UpdateNodeChildHeader(parentNode->header.indexPosition, index, newNodeHeader);
                    break;
                }
            }
        }

        if (node->isLeaf)
        {
            if (node->previousNodeHeader.pageId != 0)
            {
                IndexPage* previousLeafNodeIndexPage = StorageManager::Get().GetIndexPage(node->previousNodeHeader.pageId);

                previousLeafNodeIndexPage->UpdateNodeNextLeafHeader(node->previousNodeHeader.indexPosition, newNodeHeader);
            }

            if (node->nextNodeHeader.pageId != 0)
            {
                IndexPage* nextLeafNodeIndexPage = StorageManager::Get().GetIndexPage(node->nextNodeHeader.pageId);

                nextLeafNodeIndexPage->UpdateNodePreviousLeafHeader(node->nextNodeHeader.indexPosition, newNodeHeader);
            }

            return; //leaf has no children so return
        }

        for (auto& child : node->childrenHeaders)
        {
            IndexPage* childIndexPage = StorageManager::Get().GetIndexPage(child.pageId);

            childIndexPage->UpdateNodeParentHeader(child.indexPosition, newNodeHeader);
        }
    }

    void Database::UpdateNodeConnections(Node*& node)
    {
        if (node->parentHeader.pageId != 0)
        {
            IndexPage* parentNodeIndexPage = StorageManager::Get().GetIndexPage(node->parentHeader.pageId);
            Node* parentNode = parentNodeIndexPage->GetNodeByIndex(node->parentHeader.indexPosition);

            for (int index = 0; index < parentNode->childrenHeaders.size(); index++)
            {
                if (parentNode->childrenHeaders[index].pageId == node->header.pageId
                    && parentNode->childrenHeaders[index].indexPosition == node->header.indexPosition)
                {
                    parentNodeIndexPage->UpdateNodeChildHeader(parentNode->header.indexPosition, index, node->header);
                    break;
                }
            }
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

            return; //leaf has no children so return
        }

        for (auto& child : node->childrenHeaders)
        {
            IndexPage* childIndexPage = StorageManager::Get().GetIndexPage(child.pageId);

            childIndexPage->UpdateNodeParentHeader(child.indexPosition, node->header);
        }  
    }

    void Database::UpdateTableIndexes(const table_id_t & tableId, Indexing::Node *& node, const int & nonClusteredIndexId)
    {
        if(!node->isRoot)
            return;

        Table* table = this->tables.at(tableId);

        const bool isNonClusteredIndex = nonClusteredIndexId != -1;

        if (isNonClusteredIndex)
        {
            table->SetNonClusteredIndexPageId(node->header.pageId, nonClusteredIndexId);
            return;
        }

        table->SetClusteredIndexPageId(node->header.pageId);
    }

    void Database::InsertRowToNonEmptyNode(Node *node, const Table &table, Row *row, const Key &key, const int &indexPosition)
    {
        PageFreeSpacePage *pageFreeSpacePage = this->GetAssociatedPfsPage(node->dataPageId);

        const extent_id_t pageExtentId = Database::CalculateExtentIdByPageId(node->dataPageId);

        Page *page = StorageManager::Get().GetPage(node->dataPageId, pageExtentId, &table);

        Database::InsertRowToPage(pageFreeSpacePage, page, row, indexPosition);

        //node->prevNodeSize = (node->prevNodeSize > 0) ? node->prevNodeSize : node->GetNodeSize();

        node->keys.insert(node->keys.begin() + indexPosition, key);
        this->SplitNodeFromIndexPage(table.GetTableId(), node);
    }
}