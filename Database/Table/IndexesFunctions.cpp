#include "Table.h"
#include "../../AdditionalLibraries/B+Tree/BPlusTree.h"
#include "../Block/Block.h"
#include "../Column/Column.h"
#include "../Constants.h"
#include "../Database.h"
#include "../Pages/LargeObject/LargeDataPage.h"
#include "../Pages/IndexPage/IndexPage.h"
#include "../Pages/IndexMapAllocation/IndexAllocationMapPage.h"
#include "../Pages/Header/HeaderPage.h"
#include "../Pages/PageFreeSpace/PageFreeSpacePage.h"
#include "../Storage/StorageManager/StorageManager.h"
#include "../Pages/Page.h"
#include "../Row/Row.h"


using namespace Pages;
using namespace ByteMaps;
using namespace Indexing;
using namespace Storage;

namespace DatabaseEngine::StorageTypes {
    
    BPlusTree* Table::GetClusteredIndexedTree() 
    {
        if(this->clusteredIndexedTree == nullptr)
        {
            this->clusteredIndexedTree = new BPlusTree(this);

            this->GetClusteredIndexFromDisk();
        }

        return this->clusteredIndexedTree; 
    }

    void Table::WriteIndexesToDisk()
    {
        IndexPage* indexPage = nullptr;

        if (this->header.clusteredIndexPageId == 0)
        {
            indexPage = this->database->CreateIndexPage(this->header.tableId);
            this->header.clusteredIndexPageId = indexPage->GetPageId();
        }
        else
            indexPage = StorageManager::Get().GetIndexPage(this->header.clusteredIndexPageId);

        Node* root = this->clusteredIndexedTree->GetRoot();

        page_offset_t offSet = 0;

        this->WriteNodeToPage(root, indexPage, offSet);
    }

    void Table::WriteNodeToPage(Node* node, IndexPage*& indexPage, page_offset_t &offSet)
    {
        const page_size_t nodeSize = node->GetNodeSize();

        const page_id_t indexPageId = indexPage->GetPageId();

        const page_id_t freeSpacePageId = Database::GetPfsAssociatedPage(indexPageId);

        PageFreeSpacePage* pageFreeSpacePage = StorageManager::Get().GetPageFreeSpacePage(freeSpacePageId);

        if(indexPage->GetBytesLeft() < nodeSize)
        {
            IndexAllocationMapPage* indexAllocationMapPage = StorageManager::Get().GetIndexAllocationMapPage(this->header.indexAllocationMapPageId);

            vector<extent_id_t> allocatedExtents;
            indexAllocationMapPage->GetAllocatedExtents(&allocatedExtents);

            bool newPageFound = false;
            for(const auto& extentId: allocatedExtents)
            {
                const page_id_t firstExtentPageId = Database::CalculateSystemPageOffsetByExtentId(extentId);

                for(page_id_t nextIndexPageId = firstExtentPageId; nextIndexPageId < firstExtentPageId + EXTENT_SIZE; nextIndexPageId++)
                {
                    if(pageFreeSpacePage->GetPageType(nextIndexPageId) != PageType::INDEX)
                        break;

                    //page is free
                    if(pageFreeSpacePage->GetPageSizeCategory(nextIndexPageId) == 0)
                        continue;

                    IndexPage* nextIndexPage = StorageManager::Get().GetIndexPage(nextIndexPageId);

                    if(nextIndexPage->GetBytesLeft() < nodeSize)
                        continue;

                    indexPage->SetNextPageId(nextIndexPageId);
                    indexPage = nextIndexPage;

                    newPageFound = true;

                    break;
                }
            }

            if(!newPageFound)
            {
                IndexPage* prevIndexPage = indexPage;
                indexPage = this->database->CreateIndexPage(this->header.tableId);

                prevIndexPage->SetNextPageId(indexPage->GetPageId());
            }

            offSet = 0;
        }

        indexPage->WriteTreeDataToPage(node, offSet);
        pageFreeSpacePage->SetPageMetaData(indexPage);

        for (auto &child : node->children)
            this->WriteNodeToPage(child, indexPage, offSet);
    }

    void Table::GetClusteredIndexFromDisk()
    {
        IndexPage* indexPage = nullptr;

        Node* root = this->clusteredIndexedTree->GetRoot();

        indexPage = StorageManager::Get().GetIndexPage(this->header.clusteredIndexPageId);

        int currentNodeIndex = 0;
        page_offset_t offSet = 0;

        Node* prevLeafNode = nullptr;

        root = this->GetNodeFromDisk(indexPage, currentNodeIndex, offSet, prevLeafNode);

        this->clusteredIndexedTree->SetRoot(root);
    }

    Node* Table::GetNodeFromDisk(IndexPage*& indexPage, int& currentNodeIndex, page_offset_t& offSet, Node*& prevLeafNode)
    {
        //next page must be loaded
        if(currentNodeIndex == indexPage->GetPageSize() && indexPage->GetNextPageId() != 0)
        {
            indexPage = StorageManager::Get().GetIndexPage(indexPage->GetNextPageId());

            currentNodeIndex = 0;
            offSet = 0;
        }

        const auto& treeData = indexPage->GetTreeData();

        bool isLeaf;
        memcpy(&isLeaf, treeData + offSet, sizeof(bool));
        offSet += sizeof(bool);

        Node *node = new Node(isLeaf);

        uint16_t numOfKeys;
        memcpy(&numOfKeys, treeData + offSet, sizeof(uint16_t));
        offSet += sizeof(uint16_t);

        for (int i = 0; i < numOfKeys; i++)
        {
            key_size_t keySize;
            memcpy(&keySize, treeData + offSet, sizeof(key_size_t));
            offSet += sizeof(key_size_t);

            vector<object_t> keyValue(keySize);
            memcpy(keyValue.data(), treeData + offSet, keySize);
            offSet += keySize;

            node->keys.emplace_back(keyValue.data(), keySize, KeyType::Int);
        }

        currentNodeIndex++;

        if (node->isLeaf)
        {
            memcpy(&node->data, treeData + offSet, sizeof(BPlusTreeData));
            offSet += sizeof(BPlusTreeData);

            if(prevLeafNode != nullptr)
                prevLeafNode->next = node;

            prevLeafNode = node;
        }
        else
        {
            uint16_t numberOfChildren;
            memcpy(&numberOfChildren, treeData + offSet, sizeof(uint16_t));
            offSet += sizeof(uint16_t);

            node->children.resize(numberOfChildren);

            
            for (int i = 0; i < numberOfChildren; i++)
                node->children[i] = this->GetNodeFromDisk(indexPage, currentNodeIndex, offSet, prevLeafNode);
        }
        
        return node;
    }
}
