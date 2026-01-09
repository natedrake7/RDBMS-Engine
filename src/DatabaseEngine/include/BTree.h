#pragma once
#include "../../Systemic/include/DataTypes/Value.h"
#include "../../Systemic/include/Key.h"
#include "../../Systemic/include/DataStructures/SortedDictionary.h"

#include <vector>
#include "PipelineConstants.h"
#include "../../QueryPipeline/include/PhysicalPlan.h"

#include "Pages/PageGuard.h"
#include "DataStorage/Row.h"

namespace Statistics {
    struct ValueFrequency;
}

namespace MultiThreading {
    class ReaderGuard;
}

namespace DatabaseEngine{
    class Database;
}

namespace Pages{
    class IndexPage;
}

namespace Indexing{
    class BTree final{
        page_id_t indexPageId;

        int degree;
        int keySize;

        TreeType type;
        int nonClusteredIndexId;

        DatabaseEngine::Database* database;
        DatabaseEngine::StorageTypes::Table* table;

        static int LowerBound(const std::vector<DataTypes::Indexing::Key*>* keys, const DataTypes::Indexing::Key &key);
        static int PartialLowerBound(const std::vector<DataTypes::Indexing::Key*>* keys, const DataTypes::Indexing::Key &key);
        static bool IsDuplicateKey(
            const std::vector<DataTypes::Indexing::Key*>* keys,
            const DataTypes::Indexing::Key &key,
            const int& indexPos
        );
        static void CreateDuplicateKeyError(Errors::RuntimeStatus& status, const DataTypes::Indexing::Key &key);

        Pages::PageGuard<Pages::IndexPage> CreateRootPage(int& indexPosition, const int& pagesToAllocate);

        void SplitRoot(Pages::PageGuard<Pages::IndexPage>& root, MultiThreading::ReaderGuard& rootLock, const int& pagesToAllocate);

        void SplitChild(
            Pages::PageGuard<Pages::IndexPage>& parent,
            MultiThreading::ReaderGuard& parentReadLock,
            const int &index,
            Pages::PageGuard<Pages::IndexPage>& child,
            MultiThreading::ReaderGuard& childReadLock,
            const int& pagesToAllocate
        );

        void SplitLeafNoLock(
            Pages::PageGuard<Pages::IndexPage>& parent,
            Pages::PageGuard<Pages::IndexPage>& child,
            Pages::PageGuard<Pages::IndexPage>& newChild,
            const int& index
        )const;

        void SplitInternalNodeNoLock(
            Pages::PageGuard<Pages::IndexPage>& parent,
            Pages::PageGuard<Pages::IndexPage>& child,
            Pages::PageGuard<Pages::IndexPage>& newChild,
            const int& index
        )const;

        void SplitChildNoLock(
            Pages::PageGuard<Pages::IndexPage>& parent,
            const int &index,
            Pages::PageGuard<Pages::IndexPage>& child,
            const int& pagesToAllocate
        );
        Pages::PageGuard<Pages::IndexPage> GetNonFullNode(
            Pages::PageGuard<Pages::IndexPage>& parent,
            const DataTypes::Indexing::Key &key,
            const int& pagesToAllocate,
            int& indexPosition,
            Errors::RuntimeStatus& status
        );

        static int GetLeafNodeInsertPosition(
            const std::vector<DataTypes::Indexing::Key*>*& parentKeys,
            const DataTypes::Indexing::Key &key,
            Errors::RuntimeStatus& status
        );

        [[nodiscard]] Pages::PageGuard<Pages::IndexPage> SearchKey(const DataTypes::Indexing::Key &key) const;
        [[nodiscard]] Pages::PageGuard<Pages::IndexPage> SearchKeyWithAncestors(const DataTypes::Indexing::Key &key, std::vector<Pages::PageGuard<Pages::IndexPage>>& ancestors) const;
        [[nodiscard]] Pages::PageGuard<Pages::IndexPage> SearchLeftMostLeafNode() const;
        [[nodiscard]] Pages::PageGuard<Pages::IndexPage> SearchLeftMostLeafNode(int8_t& depth) const;

        [[nodiscard]] Pages::PageGuard<Pages::IndexPage> GetNode(const page_id_t& pageId) const;
        [[nodiscard]] int CalculateTreeDegree(const DatabaseEngine::StorageTypes::Table* otherTable, const TreeType& treeType, const int& nonClusteredId)const;

        [[nodiscard]] Pages::PageGuard<Pages::IndexPage> AllocateNewPage(const page_id_t& parentPageId, const int& pagesToAllocate);

        void HandleUnderflow(Pages::PageGuard<Pages::IndexPage>& node, std::vector<Pages::PageGuard<Pages::IndexPage>>& ancestors, int& parentIndex);
        void HandleRootUnderflow();

        bool TryBorrowFromLeftSibling(Pages::PageGuard<Pages::IndexPage>& node, Pages::PageGuard<Pages::IndexPage>& parent, const int& index)const;
        bool TryBorrowFromRightSibling(Pages::PageGuard<Pages::IndexPage>& node, Pages::PageGuard<Pages::IndexPage>& parent, const int& index)const;

        // Leaf redistribution methods for improved space utilization
        [[nodiscard]] bool TryRedistributeLeaf(
            Pages::PageGuard<Pages::IndexPage>& parent,
            MultiThreading::ReaderGuard& parentLock,
            Pages::PageGuard<Pages::IndexPage>& child,
            MultiThreading::ReaderGuard& childLock,
            const int& childIndex
        )const;

        [[nodiscard]] bool TryRedistributeLeafWithLeftSibling(
            Pages::PageGuard<Pages::IndexPage>& child,
            Pages::PageGuard<Pages::IndexPage>& sibling,
            MultiThreading::ReaderGuard& childLock,
            MultiThreading::ReaderGuard& siblingLock
        )const;

        [[nodiscard]] bool TryRedistributeLeafWithRightSibling(
            Pages::PageGuard<Pages::IndexPage>& child,
            Pages::PageGuard<Pages::IndexPage>& sibling,
            MultiThreading::ReaderGuard& childLock,
            MultiThreading::ReaderGuard& siblingLock
        )const;

        void MergeNodes(
            Pages::PageGuard<Pages::IndexPage>& leftNode,
            Pages::PageGuard<Pages::IndexPage>& rightNode,
            Pages::PageGuard<Pages::IndexPage>& parent,
            int parentKeyIndex,
            std::vector<Pages::PageGuard<Pages::IndexPage>>& ancestors,
            int& parentIndex
        );

        void CalculateClusteredStatistics(
            Pages::PageGuard<Pages::IndexPage>& currentNode,
            Headers::IndexStatistics& indexStatistics,
            Headers::TableStatistics& tableStatistics,
            std::vector<Headers::ColumnStatistics>& columnStatistics,
            Dictionary<int32_t, SortedDictionary<Value, int64_t, ValueComparator>>& sortedValues
        )const;

    public:
        explicit BTree(DatabaseEngine::StorageTypes::Table *table, const page_id_t& indexPageId, const Constants::TreeType& treeType, const int& nonClusteredIndexId = -1);
        BTree();
        ~BTree();

        Pages::PageGuard<Pages::IndexPage> FindInsertNode(
            const DataTypes::Indexing::Key &key,
            const int& pagesToAllocate,
            int &indexPosition,
            Errors::RuntimeStatus& status
        );

        void IndexSeekRange(
            const DataTypes::Indexing::Key &minKey,
            const DataTypes::Indexing::Key &maxKey,
            vector<DataTypes::Indexing::QueryData> &result
        )const;

        void IndexSeekRange(
            const DatabaseEngine::ExecutionProperties& properties,
            const DataTypes::Indexing::Key &minKey,
            const DataTypes::Indexing::Key &maxKey,
            std::vector<Pointer<DatabaseEngine::StorageTypes::Row>>* result
        )const;

        void IndexSeekRange(
            const DatabaseEngine::ExecutionProperties& properties,
            const DataTypes::Indexing::Key &minKey,
            const DataTypes::Indexing::Key &maxKey,
            std::vector<Pointer<DatabaseEngine::StorageTypes::Row>>* result,
            const Expressions::Expression* expression
        )const;

        void IndexSeek(
            const DatabaseEngine::ExecutionProperties& properties,
            const DataTypes::Indexing::Key &key,
            std::vector<Pointer<DatabaseEngine::StorageTypes::Row>>* result
        )const;

        void IndexSeek(
            const DatabaseEngine::ExecutionProperties& properties,
            const DataTypes::Indexing::Key &key,
            std::vector<Pointer<DatabaseEngine::StorageTypes::Row>>* result,
            const Expressions::Expression* expression
        )const;

        void IndexScan(vector<DataTypes::Indexing::QueryData> &result)const;

        void IndexScan(
            const DatabaseEngine::ExecutionProperties& properties,
            std::vector<Pointer<DatabaseEngine::StorageTypes::Row>> *result,
            DatabaseEngine::IndexState& state
        )const;

        void IndexScan(
            const DatabaseEngine::ExecutionProperties& properties,
            std::vector<Pointer<DatabaseEngine::StorageTypes::Row>> *result,
            DatabaseEngine::IndexState& state,
            const Expressions::Expression* expression
        )const;

        void IndexScan(
            const DatabaseEngine::ExecutionProperties& properties,
            std::vector<Pointer<DatabaseEngine::StorageTypes::Row>> *result,
            const Expressions::Expression* expression
        )const;

        void IndexScan(
            const DatabaseEngine::ExecutionProperties& properties,
            std::vector<Pointer<DatabaseEngine::StorageTypes::Row>> *result
        )const;

        void IndexScan(
            vector<Headers::RowIdentifier>* result,
            DatabaseEngine::IndexState& state,
            const int& rowsToSelect
        )const;

        void IndexScan(vector<Headers::RowIdentifier>* result, const Expressions::Expression* expression)const;

        void IndexScanUpdate(
            const DatabaseEngine::ExecutionProperties& properties,
            const Expressions::Expression* expression,
            const vector<Value> & updates
        )const;

        [[nodiscard]] Errors::RuntimeStatus IndexScanUpdate(
            const DatabaseEngine::ExecutionProperties& properties,
            const Expressions::Expression* expression,
            const vector<QueryPipeline::Statements::UpdateColumn*> & updates
        )const;

        [[nodiscard]] Errors::RuntimeStatus IndexScanUpdate(
            const DatabaseEngine::ExecutionProperties& properties,
            const vector<QueryPipeline::Statements::UpdateColumn*> & updates
        )const;

        [[nodiscard]] Errors::RuntimeStatus IndexSeekUpdate(
            const DatabaseEngine::ExecutionProperties& properties,
            const DataTypes::Indexing::Key& key,
            const std::vector<Value> & updates
        )const;

        Errors::RuntimeStatus IndexSeekUpdate(
            const DatabaseEngine::ExecutionProperties& properties,
            const Expressions::Expression* expression,
            const DataTypes::Indexing::Key* minKey,
            const DataTypes::Indexing::Key* maxKey,
            const vector<Value> & updates
        )const;
        Errors::RuntimeStatus IndexSeekUpdate(
            const DatabaseEngine::ExecutionProperties& properties,
            const DataTypes::Indexing::Key* minKey,
            const DataTypes::Indexing::Key* maxKey,
            const vector<Value> & updates
        )const;

        void SearchKey(const DataTypes::Indexing::Key &key, DataTypes::Indexing::QueryData &result) const;

        void Remove(const DataTypes::Indexing::Key& key);

        void SetBranchingFactor(const int &branchingFactor);

        [[nodiscard]] const int &GetBranchingFactor() const;

        void SetTreeType(const Constants::TreeType& treeType);

        [[nodiscard]] const page_id_t& GetFirstIndexPageId() const;

        void InsertRowsToOtherTree(const int& indexPos, const int& pagesToAllocate)const;

        void InsertColumnToRow(const column_index_t& index, const Value& defaultValue)const;

        void RemoveColumnFromRow(const column_index_t& index)const;

        [[nodiscard]] bool IsEmpty()const;

        void CalculateIndexStatistics(
            Headers::IndexStatistics& indexStatistics,
            Headers::TableStatistics& tableStatistics,
            std::vector<Headers::ColumnStatistics>& columnStatistics,
            Dictionary<int32_t, SortedDictionary<Value, int64_t, ValueComparator>>& sortedValues
        )const;
    };
}
