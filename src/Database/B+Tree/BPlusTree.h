#pragma once
#include "../../Systemic/DataTypes/Value/Value.h"
#include "../../Systemic/Indexing/Key.h"

#include <vector>
#include "../../Database/Constants.h"
#include "../../QueryPipeline/PhysicalPlan/PhysicalPlan.h"

#include <fstream>

#include "../Column/Column.h"
#include "../Row/Row.h"

namespace DatabaseEngine
{
    class Database;
}

namespace Pages
{
    class IndexPage;
}

namespace Indexing
{
    class BPlusTree final
    {
        table_id_t tableId;
        int16_t tablePosition;
        page_id_t indexPageId;

        int t;
        int keySize;

        Pages::IndexPage *root;

        TreeType type;
        int nonClusteredIndexId;

        DatabaseEngine::Database* database;
        DatabaseEngine::StorageTypes::Table* table;

        void SplitChild(Pages::IndexPage *parent, const int &index, Pages::IndexPage *child)const;
        Pages::IndexPage *GetNonFullNode(Pages::IndexPage *node, const DataTypes::Indexing::Key &key, int *indexPosition, Errors::ResultStatus& status);
        [[nodiscard]] Pages::IndexPage *SearchKey(const DataTypes::Indexing::Key &key) const;
        [[nodiscard]] Pages::IndexPage *SearchKeyWithAncestors(const DataTypes::Indexing::Key &key, std::vector<Pages::IndexPage*>& ancestors) const;
        [[nodiscard]] Pages::IndexPage *SearchLeftMostLeafNode() const;

        [[nodiscard]] Pages::IndexPage * GetNode(const Constants::page_id_t& pageId) const;
        [[nodiscard]] int CalculateTreeDegree(const DatabaseEngine::StorageTypes::Table* table, const TreeType& treeType, const int& nonClusteredIndexId)const;

        [[nodiscard]] Pages::IndexPage* AllocateNewPage(const Constants::page_id_t& parentPageId)const;

        void HandleUnderflow(Pages::IndexPage* node, const std::vector<Pages::IndexPage*>& ancestors, int& parentIndex);
        void HandleRootUnderflow();

        bool TryBorrowFromLeftSibling(Pages::IndexPage* node, Pages::IndexPage* parent, const int& index)const;
        bool TryBorrowFromRightSibling(Pages::IndexPage* node, Pages::IndexPage* parent, const int& index)const;

        void MergeNodes(
          Pages::IndexPage* leftNode,
          Pages::IndexPage* rightNode,
          Pages::IndexPage* parent,
          int parentKeyIndex,
          const std::vector<Pages::IndexPage*>& ancestors,
          int& parentIndex);

    public:
        explicit BPlusTree(DatabaseEngine::StorageTypes::Table *table, const Constants::page_id_t& indexPageId, const Constants::TreeType& treeType, const int& nonClusteredIndexId = -1);
        BPlusTree();
        ~BPlusTree();

        Pages::IndexPage *FindAppropriateNodeForInsert(const DataTypes::Indexing::Key &key, int *indexPosition, Errors::ResultStatus& status);

        void IndexSeek(const DataTypes::Indexing::Key &minKey, const DataTypes::Indexing::Key &maxKey, vector<DataTypes::Indexing::QueryData> &result) const;

        void IndexSeek(const DataTypes::Indexing::Key &minKey, const DataTypes::Indexing::Key &maxKey, std::vector<const DatabaseEngine::StorageTypes::Row*>* result);

        void IndexScan(vector<DataTypes::Indexing::QueryData> &result)const;

        void IndexScan(
            std::vector<const DatabaseEngine::StorageTypes::Row*> *result,
            const QueryPipeline::PhysicalPlan::IndexState& state,
            const int& rowsToSelect);

        void IndexScan(
            std::vector<const DatabaseEngine::StorageTypes::Row*> *result,
            const QueryPipeline::PhysicalPlan::IndexState& state,
            const int& rowsToSelect,
            const Expressions::Expression* expression);

        void IndexScan(
            std::vector<const DatabaseEngine::StorageTypes::Row*> *result,
            const Expressions::Expression* expression);

        void IndexScan(std::vector<const DatabaseEngine::StorageTypes::Row*> *result);

        void IndexScan(
            vector<Headers::RowIdentifier>* result,
            QueryPipeline::PhysicalPlan::IndexState& state,
            const int& rowsToSelect);

        void IndexScan(vector<Headers::RowIdentifier>* result, const Expressions::Expression* expression);

        void IndexScanUpdate(const Expressions::Expression* expression, const vector<Value> & updates);

        void IndexScanUpdate(const Expressions::Expression* expression, const vector<QueryPipeline::Statements::UpdateColumn*> & updates);

        void IndexScanUpdate(const vector<QueryPipeline::Statements::UpdateColumn*> & updates);

        void IndexSeekUpdate(Expressions::Expression* expression, const DataTypes::Indexing::Key* minKey, const DataTypes::Indexing::Key* maxKey, const vector<Value> & updates);

        void SearchKey(const DataTypes::Indexing::Key &key, DataTypes::Indexing::QueryData &result) const;

        void Remove(const DataTypes::Indexing::Key& key);

        Pages::IndexPage*& GetRoot();

        void SetRoot(Pages::IndexPage *&node);

        void SetBranchingFactor(const int &branchingFactor);

        [[nodiscard]] const int &GetBranchingFactor() const;

        void SetTreeType(const Constants::TreeType& treeType);

        [[nodiscard]] const Constants::page_id_t& GetFirstIndexPageId() const;

        void InsertRowsToOtherTree(const int& indexPos);

        void InsertColumnToRow(const Constants::column_index_t& index, const Value& defaultValue);

        void RemoveColumnFromRow(const Constants::column_index_t& index);
    };
}
