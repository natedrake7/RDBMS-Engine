#pragma once
#include "../../Systemic/DataTypes/Value/Value.h"
#include "../../Systemic/Indexing/Key.h"

#include <vector>
#include "../../Database/Constants.h"
#include "../../QueryPipeline/PhysicalPlan/PhysicalPlan.h"

#include <fstream>

#include "../Column/Column.h"
#include "../Pages/PageGuard/PageGuard.h"
#include "../Row/Row.h"

namespace DatabaseEngine{
    class Database;
}

namespace Pages{
    class IndexPage;
}

namespace Indexing
{
    class BPlusTree final
    {
        page_id_t indexPageId;

        int degree;
        int keySize;

        TreeType type;
        int nonClusteredIndexId;

        DatabaseEngine::Database* database;
        DatabaseEngine::StorageTypes::Table* table;

        void SplitChild(Pages::PageGuard<Pages::IndexPage>& parent, const int &index, Pages::PageGuard<Pages::IndexPage>& child);
        Pages::PageGuard<Pages::IndexPage> GetNonFullNode(Pages::PageGuard<Pages::IndexPage>& node, const DataTypes::Indexing::Key &key, int *indexPosition, Errors::RuntimeStatus& status);
        [[nodiscard]] Pages::PageGuard<Pages::IndexPage> SearchKey(const DataTypes::Indexing::Key &key) const;
        [[nodiscard]] Pages::PageGuard<Pages::IndexPage> SearchKeyWithAncestors(const DataTypes::Indexing::Key &key, std::vector<Pages::PageGuard<Pages::IndexPage>>& ancestors) const;
        [[nodiscard]] Pages::PageGuard<Pages::IndexPage> SearchLeftMostLeafNode() const;

        [[nodiscard]] Pages::PageGuard<Pages::IndexPage> GetNode(const Constants::page_id_t& pageId) const;
        [[nodiscard]] int CalculateTreeDegree(const DatabaseEngine::StorageTypes::Table* otherTable, const TreeType& treeType, const int& nonClusteredId)const;

        [[nodiscard]] Pages::PageGuard<Pages::IndexPage> AllocateNewPage(const Constants::page_id_t& parentPageId);

        void HandleUnderflow(Pages::PageGuard<Pages::IndexPage>& node, std::vector<Pages::PageGuard<Pages::IndexPage>>& ancestors, int& parentIndex);
        void HandleRootUnderflow();

        bool TryBorrowFromLeftSibling(Pages::PageGuard<Pages::IndexPage>& node, Pages::PageGuard<Pages::IndexPage>& parent, const int& index)const;
        bool TryBorrowFromRightSibling(Pages::PageGuard<Pages::IndexPage>& node, Pages::PageGuard<Pages::IndexPage>& parent, const int& index)const;

        void MergeNodes(
          Pages::PageGuard<Pages::IndexPage>& leftNode,
          Pages::PageGuard<Pages::IndexPage>& rightNode,
          Pages::PageGuard<Pages::IndexPage>& parent,
          int parentKeyIndex,
        std::vector<Pages::PageGuard<Pages::IndexPage>>& ancestors,
          int& parentIndex);

    public:
        explicit BPlusTree(DatabaseEngine::StorageTypes::Table *table, const Constants::page_id_t& indexPageId, const Constants::TreeType& treeType, const int& nonClusteredIndexId = -1);
        BPlusTree();
        ~BPlusTree();

        Pages::PageGuard<Pages::IndexPage> FindAppropriateNodeForInsert(const DataTypes::Indexing::Key &key, int *indexPosition, Errors::RuntimeStatus& status);

        void IndexSeek(
            const DataTypes::Indexing::Key &minKey,
            const DataTypes::Indexing::Key &maxKey,
            vector<DataTypes::Indexing::QueryData> &result
        )const;

        void IndexSeek(
            const QueryPipeline::PhysicalPlan::PhysicalPlanExecutionProperties& properties,
            const DataTypes::Indexing::Key &minKey,
            const DataTypes::Indexing::Key &maxKey,
            std::vector<const DatabaseEngine::StorageTypes::Row*>* result
        )const;

        void IndexScan(vector<DataTypes::Indexing::QueryData> &result)const;

        void IndexScan(
            const QueryPipeline::PhysicalPlan::PhysicalPlanExecutionProperties& properties,
            std::vector<const DatabaseEngine::StorageTypes::Row*> *result,
            QueryPipeline::PhysicalPlan::IndexState& state
        )const;

        void IndexScan(
            const QueryPipeline::PhysicalPlan::PhysicalPlanExecutionProperties& properties,
            std::vector<const DatabaseEngine::StorageTypes::Row*> *result,
            QueryPipeline::PhysicalPlan::IndexState& state,
            const Expressions::Expression* expression
        )const;

        void IndexScan(
            const QueryPipeline::PhysicalPlan::PhysicalPlanExecutionProperties& properties,
            std::vector<const DatabaseEngine::StorageTypes::Row*> *result,
            const Expressions::Expression* expression
        )const;

        void IndexScan(
            const QueryPipeline::PhysicalPlan::PhysicalPlanExecutionProperties& properties,
            std::vector<const DatabaseEngine::StorageTypes::Row*> *result
        )const;

        void IndexScan(
            vector<Headers::RowIdentifier>* result,
            QueryPipeline::PhysicalPlan::IndexState& state,
            const int& rowsToSelect
        )const;

        void IndexScan(vector<Headers::RowIdentifier>* result, const Expressions::Expression* expression)const;

        void IndexScanUpdate(
            const QueryPipeline::PhysicalPlan::PhysicalPlanExecutionProperties& properties,
            const Expressions::Expression* expression,
            const vector<Value> & updates
        )const;

        [[nodiscard]] Errors::RuntimeStatus IndexScanUpdate(
            const QueryPipeline::PhysicalPlan::PhysicalPlanExecutionProperties& properties,
            const Expressions::Expression* expression,
            const vector<QueryPipeline::Statements::UpdateColumn*> & updates
        )const;

        [[nodiscard]] Errors::RuntimeStatus IndexScanUpdate(
            const QueryPipeline::PhysicalPlan::PhysicalPlanExecutionProperties& properties,
            const vector<QueryPipeline::Statements::UpdateColumn*> & updates
        )const;

        Errors::RuntimeStatus IndexSeekUpdate(
            const QueryPipeline::PhysicalPlan::PhysicalPlanExecutionProperties& properties,
            const Expressions::Expression* expression,
            const DataTypes::Indexing::Key* minKey,
            const DataTypes::Indexing::Key* maxKey,
            const vector<Value> & updates
        )const;
        Errors::RuntimeStatus IndexSeekUpdate(
            const QueryPipeline::PhysicalPlan::PhysicalPlanExecutionProperties& properties,
            const DataTypes::Indexing::Key* minKey,
            const DataTypes::Indexing::Key* maxKey,
            const vector<Value> & updates
        )const;

        void SearchKey(const DataTypes::Indexing::Key &key, DataTypes::Indexing::QueryData &result) const;

        void Remove(const DataTypes::Indexing::Key& key);

        void SetBranchingFactor(const int &branchingFactor);

        [[nodiscard]] const int &GetBranchingFactor() const;

        void SetTreeType(const Constants::TreeType& treeType);

        [[nodiscard]] const Constants::page_id_t& GetFirstIndexPageId() const;

        void InsertRowsToOtherTree(const int& indexPos)const;

        void InsertColumnToRow(const Constants::column_index_t& index, const Value& defaultValue)const;

        void RemoveColumnFromRow(const Constants::column_index_t& index)const;
    };
}
