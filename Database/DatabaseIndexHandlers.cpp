#include "Database.h"
#include <vector>
#include "./Pages/PageFreeSpace/PageFreeSpacePage.h"
#include "./Pages/IndexMapAllocation/IndexAllocationMapPage.h"
#include "./Pages/IndexPage/IndexPage.h"
#include "Constants.h"
#include "../AdditionalLibraries/AdditionalDataTypes/ErrorHandling.h"
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
    Key Database::CreateKey(const vector<column_index_t>& indexedColumns, const Row* row)
    {
        Key key;
        for (const auto &columnId : indexedColumns)
        {
            const auto &keyBlock = row->GetData()[columnId];
            key.InsertKey(Key(keyBlock->GetBlockData(), keyBlock->GetBlockSize(), keyBlock->GetColumnType()));
        }

        return key;
    }

    AdditionalDataTypes::ResultStatus Database::InsertRowToClusteredIndex(const table_id_t& tableId, Row* row, page_id_t* rowPageId, int* rowIndex)
    {
        Table* table = this->tables.at(tableId);

        BPlusTree* tree = table->GetClusteredIndexedTree();

        const auto key = Database::CreateKey(table->GetClusteredIndex(), row);

        int indexPosition = 0;

        AdditionalDataTypes::ResultStatus status;

        auto *node = tree->FindAppropriateNodeForInsert(key, &indexPosition, status);

        if (status.code != AdditionalDataTypes::ResultCode::Ok)
            return status;

        *rowIndex = indexPosition;

        PageFreeSpacePage *pageFreeSpacePage =  Database::GetAssociatedPfsPage(node->GetPageId());

        // should never fail
        Database::InsertRowToPage(pageFreeSpacePage, node, row, indexPosition);

        auto* keys = node->GetKeysUnsafe();

        keys->insert(keys->begin() + indexPosition, new Key(key));

        node->UpdateBytesLeft();

        *rowPageId = node->GetPageId();

        // this->SplitNodeFromIndexPage(tableId, node);
        return {};
    }

    AdditionalDataTypes::ResultStatus Database::InsertRowToNonClusteredIndex(const table_id_t& tableId, const Row* row, const int& nonClusteredIndexId, const vector<column_index_t>& indexedColumns, const BPlusTreeNonClusteredData& data)
    {
        Table* table = this->tables.at(tableId);

        BPlusTree* tree = table->GetNonClusteredIndexTree(nonClusteredIndexId);
        const auto key = Database::CreateKey(indexedColumns, row);

        int indexPosition = 0;
        AdditionalDataTypes::ResultStatus status;

        // Node *node = tree->FindAppropriateNodeForInsert(key, &indexPosition, status);

        // if (status.code != AdditionalDataTypes::ResultCode::Ok)
        //     return status;

        // node->keys.insert(node->keys.begin() + indexPosition, key);
        // node->nonClusteredData.insert(node->nonClusteredData.begin() + indexPosition, data);
        // node->prevNodeSize = node->currentNodeSize;
        // node->currentNodeSize = node->GetNodeSize();

        // this->SplitNodeFromIndexPage(tableId, node, nonClusteredIndexId);

        return status;
    }


    void Database::UpdateNonClusteredData(const Table& table, Page* nextLeafPage, const page_id_t& nextLeafPageId) const
    {
       if(!table.HasNonClusteredIndexes())
            return;

        Table* tablePtr = this->tables.at(table.GetTableId());

        const auto& nonClusteredIndexes = tablePtr->GetNonClusteredIndexes();
            

        for (int i = 0; i < nonClusteredIndexes.size(); i++)
        {
            const BPlusTree* nonClusteredTree = tablePtr->GetNonClusteredIndexTree(i);

            const auto& rows = nextLeafPage->GetDataRowsUnsafe();

            for (page_offset_t index = 0; index < rows->size(); index++)
            {
                const auto key = Database::CreateKey(nonClusteredIndexes[i], (*rows)[index]);

                nonClusteredTree->UpdateRowData(key, BPlusTreeNonClusteredData(nextLeafPageId, index));
            }
        }
    }

	IndexPage* Database::FindOrAllocateNextIndexPage(const table_id_t& tableId, const page_id_t &indexPageId, const int& nonClusteredIndexId, const bool& findPageDifferentFromCurrent)
    {
        const auto& tableHeader = this->GetTable(tableId)->GetTableHeader();

        const Table* table = this->tables.at(tableId);

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

        const IndexAllocationMapPage* indexAllocationMapPage = StorageManager::Get().GetIndexAllocationMapPage(this->filename, tableHeader.indexAllocationMapPageId);
        
        vector<extent_id_t> allocatedExtents;
        indexAllocationMapPage->GetAllocatedExtents(&allocatedExtents);

        for(const auto& extentId: allocatedExtents)
        {
            const page_id_t firstExtentPageId = Database::CalculateSystemPageOffsetByExtentId(extentId);

            for(page_id_t nextIndexPageId = firstExtentPageId; nextIndexPageId < firstExtentPageId + EXTENT_SIZE; nextIndexPageId++)
            {
                const PageFreeSpacePage * pageFreeSpacePage = Database::GetAssociatedPfsPage(nextIndexPageId);

                if (pageFreeSpacePage->GetPageType(nextIndexPageId) != PageType::INDEX)
                    continue;

                //page is free
                if(pageFreeSpacePage->GetPageSizeCategory(nextIndexPageId) == 0)
                    continue;

                IndexPage* indexPage = StorageManager::Get().GetIndexPage(this->filename, nextIndexPageId, extentId, table);

                if(!indexPage->isEmpty())
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
        // IndexPage* overflowedPage = StorageManager::Get().GetIndexPage(this->filename, node->header.pageId);

        // overflowedPage->UpdateBytesLeft();

        // if(overflowedPage->GetBytesLeft() > 0)
        // {
        //     PageFreeSpacePage* overflowedPagePFS = Database::GetAssociatedPfsPage(overflowedPage->GetPageId());
        //     overflowedPagePFS->SetPageMetaData(overflowedPage);
        //     return;
        // }

        // auto* overflowedPageNodes = overflowedPage->GetNodesUnsafe();

        // const int splitFactor = overflowedPage->GetPageSize() / 2;

        // const page_size_t availablePageBytes = PAGE_SIZE - PageHeader::GetPageHeaderSize() - IndexPageAdditionalHeader::GetAdditionalHeaderSize();

        // IndexPage* nextIndexPage = this->FindOrAllocateNextIndexPage(tableId, overflowedPage->GetPageId(), availablePageBytes, nonClusteredIndexId, true);

        // auto* nextIndexPageNodes = nextIndexPage->GetNodesUnsafe();

        // const page_id_t& nextIndexPageId = nextIndexPage->GetPageId();

        // page_offset_t counter = 0;
        // for (auto& [key, node] : *overflowedPageNodes) {
        //     if (counter < splitFactor)
        //         continue;

        //     page_offset_t indexPosition = 0;

        //     nextIndexPage->InsertNode(node, &indexPosition);

        //     const NodeHeader newNodeHeader(nextIndexPageId, indexPosition);

        //     Database::UpdateNodeConnections(node, newNodeHeader);

        //     node->header = newNodeHeader;

        //     this->UpdateTableIndexes(tableId, node, nonClusteredIndexId);

        // }

        // overflowedPage->ResizeNodes(splitFactor);
        // overflowedPage->UpdateBytesLeft();

        // nextIndexPage->UpdateBytesLeft();
        // nextIndexPage->UpdatePageSize();

        // PageFreeSpacePage* overflowedPagePFS = Database::GetAssociatedPfsPage(overflowedPage->GetPageId());
        // overflowedPagePFS->SetPageMetaData(overflowedPage);

        // PageFreeSpacePage* nextIndexPagePFS = Database::GetAssociatedPfsPage(nextIndexPageId);
        // nextIndexPagePFS->SetPageMetaData(nextIndexPage);
    }

    void Database::UpdateNodeConnections(Node *& node, const NodeHeader& newNodeHeader)
    {
        // if (node->parentHeader.pageId != node->header.pageId)
        // {
        //     IndexPage* parentNodeIndexPage = StorageManager::Get().GetIndexPage(this->filename, node->parentHeader.pageId);
        //     const Node* parentNode = parentNodeIndexPage->GetNodeByIndex(node->parentHeader.indexPosition);

        //     for (int index = 0; index < parentNode->childrenHeaders.size(); index++)
        //     {
        //         if (parentNode->childrenHeaders[index].pageId == node->header.pageId
        //             && parentNode->childrenHeaders[index].indexPosition == node->header.indexPosition)
        //         {
        //             parentNodeIndexPage->UpdateNodeChildHeader(parentNode->header.indexPosition, index, newNodeHeader);
        //             break;
        //         }
        //     }
        // }

        // if (node->isLeaf)
        // {
        //     if (node->previousNodeHeader.pageId != node->header.pageId)
        //     {
        //         IndexPage* previousLeafNodeIndexPage = StorageManager::Get().GetIndexPage(this->filename, node->previousNodeHeader.pageId);

        //         previousLeafNodeIndexPage->UpdateNodeNextLeafHeader(node->previousNodeHeader.indexPosition, newNodeHeader);
        //     }

        //     if (node->nextNodeHeader.pageId != node->header.pageId)
        //     {
        //         IndexPage* nextLeafNodeIndexPage = StorageManager::Get().GetIndexPage(this->filename, node->nextNodeHeader.pageId);

        //         nextLeafNodeIndexPage->UpdateNodePreviousLeafHeader(node->nextNodeHeader.indexPosition, newNodeHeader);
        //     }

        //     return; //leaf has no children so return
        // }

        // for (auto& child : node->childrenHeaders)
        // {
        //     if (child.pageId == node->header.pageId)
        //         continue;

        //     IndexPage* childIndexPage = StorageManager::Get().GetIndexPage(this->filename, child.pageId);

        //     childIndexPage->UpdateNodeParentHeader(child.indexPosition, newNodeHeader);
        // }
    }

    // void Database::UpdateNodeConnectionsOnDelete(Indexing::Node *&node, Indexing::Node* deletedNode, const Indexing::NodeHeader &newNodeHeader){
    //     if (node->parentHeader.pageId != 0)
    //     {
    //         IndexPage* parentNodeIndexPage = StorageManager::Get().GetIndexPage(this->filename, node->parentHeader.pageId);
    //         const Node* parentNode = parentNodeIndexPage->GetNodeByKey(node->parentHeader.indexPosition);
    //
    //         for (int index = 0; index < parentNode->childrenHeaders.size(); index++)
    //         {
    //             if (parentNode->childrenHeaders[index].pageId == node->header.pageId
    //                 && parentNode->childrenHeaders[index].indexPosition == node->header.indexPosition)
    //             {
    //                 parentNodeIndexPage->UpdateNodeChildHeaderById(node->parentHeader.indexPosition, index, newNodeHeader);
    //                 break;
    //             }
    //         }
    //     }
    //
    //     if (node->isLeaf)
    //     {
    //         if (node->previousNodeHeader.pageId != 0)
    //         {
    //             IndexPage* previousLeafNodeIndexPage = StorageManager::Get().GetIndexPage(this->filename, node->previousNodeHeader.pageId);
    //
    //             previousLeafNodeIndexPage->UpdateNodeNextLeafHeaderById(node->previousNodeHeader.indexPosition, newNodeHeader);
    //         }
    //
    //         if (node->nextNodeHeader.pageId != 0)
    //         {
    //             IndexPage* nextLeafNodeIndexPage = StorageManager::Get().GetIndexPage(this->filename, node->nextNodeHeader.pageId);
    //
    //             nextLeafNodeIndexPage->UpdateNodePreviousLeafHeaderById(node->nextNodeHeader.indexPosition, newNodeHeader);
    //         }
    //
    //         return; //leaf has no children so return
    //     }
    //
    //     for (auto& child : node->childrenHeaders)
    //     {
    //         IndexPage* childIndexPage = StorageManager::Get().GetIndexPage(this->filename, child.pageId);
    //
    //         childIndexPage->UpdateNodeParentHeaderById(child.indexPosition, newNodeHeader);
    //     }
    // }

    void Database::UpdateNodeConnections(Node*& node)
    {
        // if (node->parentHeader.pageId != 0 && node->parentHeader.pageId != node->header.pageId)
        // {
        //     IndexPage* parentNodeIndexPage = StorageManager::Get().GetIndexPage(this->filename, node->parentHeader.pageId);
        //     const Node* parentNode = parentNodeIndexPage->GetNodeByIndex(node->parentHeader.indexPosition);

        //     for (int index = 0; index < parentNode->childrenHeaders.size(); index++)
        //     {
        //         if (parentNode->childrenHeaders[index].pageId == node->header.pageId
        //             && parentNode->childrenHeaders[index].indexPosition == node->header.indexPosition)
        //         {
        //             parentNodeIndexPage->UpdateNodeChildHeader(parentNode->header.indexPosition, index, node->header);
        //             break;
        //         }
        //     }
        // }

        // if (node->isLeaf)
        // {
        //     if (node->previousNodeHeader.pageId != 0 && node->previousNodeHeader.pageId != node->header.pageId)
        //     {
        //         IndexPage* previousLeafNodeIndexPage = StorageManager::Get().GetIndexPage(this->filename, node->previousNodeHeader.pageId);

        //         previousLeafNodeIndexPage->UpdateNodeNextLeafHeader(node->previousNodeHeader.indexPosition, node->header);
        //     }

        //     if (node->nextNodeHeader.pageId != 0 && node->nextNodeHeader.pageId != node->header.pageId)
        //     {
        //         IndexPage* nextLeafNodeIndexPage = StorageManager::Get().GetIndexPage(this->filename, node->nextNodeHeader.pageId);

        //         nextLeafNodeIndexPage->UpdateNodePreviousLeafHeader(node->nextNodeHeader.indexPosition, node->header);
        //     }

        //     return; //leaf has no children so return
        // }

        // for (auto& child : node->childrenHeaders)
        // {
        //     if (child.pageId == node->header.pageId)
        //         continue;

        //     IndexPage* childIndexPage = StorageManager::Get().GetIndexPage(this->filename, child.pageId);

        //     childIndexPage->UpdateNodeParentHeader(child.indexPosition, node->header);
        // }  
    }

    void Database::UpdateTableIndexes(const table_id_t & tableId, Indexing::Node *& node, const int & nonClusteredIndexId) const
    {
//        if(!node->isRoot)
//            return;
//
//        Table* table = this->tables.at(tableId);
//
//        const bool isNonClusteredIndex = nonClusteredIndexId != -1;
//
//        if (isNonClusteredIndex)
//        {
//            table->SetNonClusteredIndexPageId(node->header.pageId, nonClusteredIndexId);
//            return;
//        }
//
//        table->SetClusteredIndexPageId(node->header.pageId);
    }

    void Database::InsertRowToNonEmptyNode(Node *node, const Table &table, Row *row, const Key &key, const int &indexPosition)
    {
//        PageFreeSpacePage *pageFreeSpacePage = Database::GetAssociatedPfsPage(node->dataPageId);
//
//        const extent_id_t pageExtentId = Database::CalculateExtentIdByPageId(node->dataPageId);
//
//        Page *page = StorageManager::Get().GetPage(this->filename, node->dataPageId, pageExtentId, &table);
//
//        Database::InsertRowToPage(pageFreeSpacePage, page, row, indexPosition);
//
//        //node->prevNodeSize = (node->prevNodeSize > 0) ? node->prevNodeSize : node->GetNodeSize();
//
//        node->keys.insert(node->keys.begin() + indexPosition, key);
//        this->SplitNodeFromIndexPage(table.GetTableId(), node);
    }

}