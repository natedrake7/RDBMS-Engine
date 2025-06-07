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

            if (this->header.clusteredIndexPageId == INVALID_PAGE_ID)
                return this->clusteredIndexedTree;
            
            this->GetClusteredIndexFromDisk();
        }

        return this->clusteredIndexedTree; 
    }

    BPlusTree * Table::GetNonClusteredIndexTree(const int & nonClusteredIndexId)
    {
        const auto numOfIndexes = this->header.nonClusteredIndexes.size();

        if(this->nonClusteredIndexedTrees.empty())
            this->nonClusteredIndexedTrees.resize(numOfIndexes);

        if (this->header.nonClusteredIndexPageIds.size() < numOfIndexes)
            this->header.nonClusteredIndexPageIds.resize(numOfIndexes, INVALID_PAGE_ID);

        BPlusTree*& nonClusteredTree = this->nonClusteredIndexedTrees.at(nonClusteredIndexId);

        if (nonClusteredTree == nullptr)
        {
            const auto& indexPageId = this->header.nonClusteredIndexPageIds.at(nonClusteredIndexId);

            nonClusteredTree = new BPlusTree(this, indexPageId, TreeType::NonClustered, nonClusteredIndexId);

            if (indexPageId == INVALID_PAGE_ID)
                return nonClusteredTree;

            this->GetNonClusteredIndexFromDisk(nonClusteredIndexId);
        }

        return nonClusteredTree;
    }

    void Table::GetClusteredIndexFromDisk() const
    {
        auto* root = Table::GetIndexFromDisk(this->header.clusteredIndexPageId);

        this->clusteredIndexedTree->SetRoot(root);

        this->clusteredIndexedTree->SetTreeType(TreeType::Clustered);
    }

    void Table::GetNonClusteredIndexFromDisk(const int& indexId) const
    {
        auto* root =  Table::GetIndexFromDisk(this->header.nonClusteredIndexPageIds[indexId]);

        this->nonClusteredIndexedTrees[indexId]->SetRoot(root);

        this->nonClusteredIndexedTrees[indexId]->SetTreeType(TreeType::NonClustered);
    }

    Pages::IndexPage* Table::GetIndexFromDisk(const page_id_t & indexPageId) const
    {
        const auto& filename = this->database->GetFileName();

        const extent_id_t indexPageExtentId = Database::CalculateExtentIdByPageId(indexPageId);

        return StorageManager::Get().GetIndexPage(filename, indexPageId, indexPageExtentId, this);
    }
}
