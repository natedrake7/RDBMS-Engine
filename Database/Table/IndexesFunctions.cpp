#include "Table.h"
#include "../Column/Column.h"
#include "../Constants.h"
#include "../Database.h"
#include "../Pages/IndexPage/IndexPage.h"
#include "../Storage/StorageManager/StorageManager.h"
#include "../B+Tree/BPlusTree.h"

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

    BPlusTree * Table::GetNonClusteredIndexTree(const int & nonClusteredIndexId)
    {
        if(this->header.nonClusteredColumnIndexes.empty())
            return nullptr;

        if(this->nonClusteredIndexedTrees.empty())
            this->nonClusteredIndexedTrees.resize(this->header.nonClusteredColumnIndexes.size());

        BPlusTree*& nonClusteredTree = this->nonClusteredIndexedTrees.at(nonClusteredIndexId);

        if (nonClusteredTree == nullptr)
        {
            nonClusteredTree = new BPlusTree(this, this->header.nonClusteredIndexPageIds[nonClusteredIndexId], TreeType::NonClustered, nonClusteredIndexId);

            this->GetNonClusteredIndexFromDisk(nonClusteredIndexId);
        }

        return nonClusteredTree;
    }

    void Table::GetClusteredIndexFromDisk() const
    {
        Node* root = Table::GetIndexFromDisk(this->header.clusteredIndexPageId);

        this->clusteredIndexedTree->SetRoot(root);

        this->clusteredIndexedTree->SetTreeType(TreeType::Clustered);
    }

    void Table::GetNonClusteredIndexFromDisk(const int& indexId) const
    {
        if(this->header.nonClusteredIndexPageIds[indexId] == 0)
            return;

        Node* root =  Table::GetIndexFromDisk(this->header.nonClusteredIndexPageIds[indexId]);

        this->nonClusteredIndexedTrees[indexId]->SetRoot(root);

        this->nonClusteredIndexedTrees[indexId]->SetTreeType(TreeType::NonClustered);
    }

    Node* Table::GetIndexFromDisk(const page_id_t & indexPageId) const
    {
        const extent_id_t indexPageExtentId = Database::CalculateExtentIdByPageId(indexPageId);

        IndexPage* indexPage = StorageManager::Get().GetIndexPage(indexPageId, indexPageExtentId, this);

        return indexPage->GetRoot();
    }
}
