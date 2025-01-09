#include "Table.h"
#include "Table.h"
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
            this->clusteredIndexedTree = new BPlusTree(this, this->header.clusteredIndexPageId, TreeType::Clustered);

            this->GetClusteredIndexFromDisk();
        }

        return this->clusteredIndexedTree; 
    }

    Indexing::BPlusTree * Table::GetNonClusteredIndexTree(const int & nonClusteredIndexId)
    {
        if(this->header.nonClusteredIndexesBitMap.empty())
            return nullptr;

        if(this->nonClusteredIndexedTrees.empty())
            this->nonClusteredIndexedTrees.resize(this->header.nonClusteredIndexesBitMap.size());

        BPlusTree*& nonClusteredTree = this->nonClusteredIndexedTrees.at(nonClusteredIndexId);

        if (nonClusteredTree == nullptr)
        {
            nonClusteredTree = new BPlusTree(this, this->header.nonClusteredIndexPageIds[nonClusteredIndexId], TreeType::NonClustered);

            this->GetNonClusteredIndexFromDisk(nonClusteredIndexId);
        }

        return nonClusteredTree;
    }

    void Table::WriteClusteredIndexToPage()
    {
        this->header.clusteredIndexPageId = this->clusteredIndexedTree->GetFirstIndexPageId();
        //IndexPage* indexPage = nullptr;

        //if (this->header.clusteredIndexPageId == 0)
        //{
        //    indexPage = this->database->CreateIndexPage(this->header.tableId);
        //    this->header.clusteredIndexPageId = indexPage->GetPageId();
        //}
        //else
        //    indexPage = StorageManager::Get().GetIndexPage(this->header.clusteredIndexPageId);

        //Node* root = this->clusteredIndexedTree->GetRoot();

        //page_offset_t offSet = 0;

        //this->WriteNodeToPage(root, indexPage, offSet);
    }

    void Table::WriteNonClusteredIndexToPage(const int & indexId)
    {
        IndexPage* indexPage = nullptr;

        if (this->header.nonClusteredIndexPageIds[indexId] == 0)
        {
            indexPage = this->database->CreateIndexPage(this->header.tableId);
            this->header.nonClusteredIndexPageIds[indexId] = indexPage->GetPageId();
        }
        else
            indexPage = StorageManager::Get().GetIndexPage(this->header.nonClusteredIndexPageIds[indexId]);

        Node*& root = this->nonClusteredIndexedTrees[indexId]->GetRoot();

        page_offset_t offSet = 0;

        this->WriteNodeToPage(root, indexPage, offSet);
    }

    void Table::WriteNodeToPage(Node* node, IndexPage*& indexPage, page_offset_t &offSet)
    {
        //const page_size_t nodeSize = node->GetNodeSize();

        //const page_id_t indexPageId = indexPage->GetPageId();

        //const page_id_t freeSpacePageId = Database::GetPfsAssociatedPage(indexPageId);

        //PageFreeSpacePage* pageFreeSpacePage = StorageManager::Get().GetPageFreeSpacePage(freeSpacePageId);

        //if(indexPage->GetBytesLeft() < nodeSize)
        //{


        //    offSet = 0;
        //}

        //indexPage->WriteTreeDataToPage(node, offSet);
        //pageFreeSpacePage->SetPageMetaData(indexPage);

        //for (auto &child : node->children)
        //    this->WriteNodeToPage(child, indexPage, offSet);
    }

    void Table::GetClusteredIndexFromDisk() 
    {
        IndexPage* indexPage = StorageManager::Get().GetIndexPage(this->header.clusteredIndexPageId);

        Node* root = indexPage->GetRoot();

        this->clusteredIndexedTree->SetRoot(root);

        this->clusteredIndexedTree->SetTreeType(TreeType::Clustered);
    }

    void Table::GetNonClusteredIndexFromDisk(const int& indexId)
    {
        if(this->header.nonClusteredIndexPageIds[indexId] == 0)
            return;

        Node*& root = this->nonClusteredIndexedTrees[indexId]->GetRoot();

        this->GetIndexFromDisk(root, this->header.nonClusteredIndexPageIds[indexId], TreeType::NonClustered);

        this->nonClusteredIndexedTrees[indexId]->SetTreeType(TreeType::NonClustered);
    }

    void Table::GetIndexFromDisk(Node*& root, const page_id_t& indexPageId, const TreeType& treeType)
    {
        IndexPage* indexPage = nullptr;

        indexPage = StorageManager::Get().GetIndexPage(indexPageId);

        int currentNodeIndex = 0;
        page_offset_t offSet = 0;

        Node* prevLeafNode = nullptr;

        root = this->GetNodeFromDisk(indexPage, treeType, currentNodeIndex, offSet, prevLeafNode);
    }

    Node* Table::GetNodeFromDisk(Pages::IndexPage*& indexPage, const TreeType& treeType, int& currentNodeIndex, page_offset_t& offSet, Indexing::Node*& prevLeafNode)
    {
        //next page must be loaded
       /* if(currentNodeIndex == indexPage->GetPageSize() && indexPage->GetNextPageId() != 0)
        {
            indexPage = StorageManager::Get().GetIndexPage(indexPage->GetNextPageId());

            currentNodeIndex = 0;
            offSet = 0;
        }

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
            if (treeType == TreeType::Clustered)
            {
                memcpy(&node->data, treeData + offSet, sizeof(BPlusTreeData));
                offSet += sizeof(BPlusTreeData);
            }
            else
            {
                uint16_t numberOfRows;
                memcpy(&numberOfRows, treeData + offSet, sizeof(uint16_t));
                offSet += sizeof(uint16_t);

                for (uint16_t rowIndex = 0; rowIndex < numberOfRows; rowIndex++)
                {
                    BPlusTreeNonClusteredData nonClusteredData;
                    memcpy(&nonClusteredData, treeData + offSet, sizeof(BPlusTreeData) + sizeof(page_offset_t));
                    offSet += (sizeof(BPlusTreeData) + sizeof(page_offset_t));

                    node->nonClusteredData.push_back(nonClusteredData);
                }
            }

            if(prevLeafNode != nullptr)
                prevLeafNode->next = node;

            prevLeafNode = node;

            return node;
        }

        uint16_t numberOfChildren;
        memcpy(&numberOfChildren, treeData + offSet, sizeof(uint16_t));
        offSet += sizeof(uint16_t);

        node->children.resize(numberOfChildren);
            
        for (int i = 0; i < numberOfChildren; i++)
            node->children[i] = this->GetNodeFromDisk(indexPage, treeType, currentNodeIndex, offSet, prevLeafNode);
        
        return node;*/
    }
}
