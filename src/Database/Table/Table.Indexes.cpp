#include "Table.h"
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
    void Table::ClusteredIndexSeek(std::vector<const Row*> *selectedRows, const DataTypes::Indexing::Key *minimumValue, const DataTypes::Indexing::Key *maximumValue){
        const auto* tree = this->GetClusteredIndexedTree();

        tree->IndexSeek(*minimumValue, *maximumValue, selectedRows);
    }

    void Table::ClusteredIndexScan(
      std::vector<const Row*> *selectedRows,
      QueryPipeline::PhysicalPlan::IndexState& state,
      const int& rowsToSelect,
      const Expressions::Expression* expression){
        if (this->header.indexAllocationMapPageId == INVALID_PAGE_ID)
          return;

        const auto* tree = this->GetClusteredIndexedTree();

        if(expression != nullptr){
          tree->IndexScan(selectedRows, state, expression, rowsToSelect);
          return;
        }

        tree->IndexScan(selectedRows, state, rowsToSelect);
    }

    void Table::ClusteredIndexScan(std::vector<const Row*> *selectedRows, const Expressions::Expression *expression){
        if (this->header.indexAllocationMapPageId == INVALID_PAGE_ID)
          return;

        const auto* tree = this->GetClusteredIndexedTree();

        if(expression != nullptr){
          tree->IndexScan(selectedRows, expression);
          return;
        }

        tree->IndexScan(selectedRows);
    }

    void Table::NonClusteredIndexScan(
      std::vector<const Row*> *selectedRows,
      const int &indexPos,
      QueryPipeline::PhysicalPlan::IndexState& state,
      const int& rowsToSelect,
      const Expressions::Expression *expression){

        auto* tree = this->GetNonClusteredIndexTree(indexPos);

        std::vector<Headers::RowIdentifier> rowIds;
        tree->IndexScan(&rowIds, state, rowsToSelect);

        if (expression != nullptr) {

          for (const auto& rowId : rowIds) {
            const auto page = StorageManager::Get().GetPage(this->GetFileName(), rowId.pageId, this);

            auto* row = page->GetRow(rowId.indexId);

            const auto conditionResult = expression->Evaluate(row);

            if (!conditionResult.GetBool())
              continue;

            selectedRows->push_back(row);
          }

          return;
        }

        for (const auto& rowId : rowIds) {
          const auto page = StorageManager::Get().GetPage(this->GetFileName(), rowId.pageId, this);

          selectedRows->push_back(page->GetRow(rowId.indexId));
        }
    }

    key_size_t Table::CalculateIndexKeySize(const int& indexPos) const {
            HashSet<column_index_t> clusteredColumns;

            if (indexPos != -1)
                return this->CalculateNonClusteredIndexKeySize(indexPos);

            key_size_t keySize = 0;
            for(const auto& column : this->header.clusteredIndex.columns)
                clusteredColumns.Add(column);

            for (const auto &column : this->columns)
                if(clusteredColumns.Contains(column->GetColumnIndex()))
                    keySize += column->GetColumnSize();

            return keySize;
    }

    key_size_t Table::CalculateNonClusteredIndexKeySize(const int &indexPos) const{
            HashSet<column_index_t> clusteredColumns;

            key_size_t keySize = 0;
            for(const auto& column : this->header.nonClusteredIndexes.at(indexPos).columns)
                clusteredColumns.Add(column);

            for (const auto &column : this->columns)
                if(clusteredColumns.Contains(column->GetColumnIndex()))
                    keySize += column->GetColumnSize();

            return keySize;
    }

    BPlusTree* Table::GetClusteredIndexedTree() 
    {
        if(this->clusteredIndexedTree != nullptr)
            return this->clusteredIndexedTree;

        this->clusteredIndexedTree = new BPlusTree(this, this->header.clusteredIndexPageId, TreeType::Clustered);

        if (this->header.clusteredIndexPageId == INVALID_PAGE_ID)
            return this->clusteredIndexedTree;

        this->GetClusteredIndexFromDisk();

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
        auto root = Table::GetIndexFromDisk(this->header.clusteredIndexPageId);

        this->clusteredIndexedTree->SetTreeType(TreeType::Clustered);
    }

    void Table::GetNonClusteredIndexFromDisk(const int& indexId) const
    {
        auto root =  Table::GetIndexFromDisk(this->header.nonClusteredIndexPageIds[indexId]);

        this->nonClusteredIndexedTrees[indexId]->SetTreeType(TreeType::NonClustered);
    }

    Pages::PageGuard<Pages::IndexPage> Table::GetIndexFromDisk(const page_id_t & indexPageId) const
    {
        const auto& filename = this->database->GetFileName();

        return StorageManager::Get().GetIndexPage(filename, indexPageId, this);
    }
}
