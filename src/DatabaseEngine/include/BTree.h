#pragma once
#include "../../Systemic/include/DataTypes/Value.h"
#include "../../Systemic/include/Key.h"
#include "../../Systemic/include/DataStructures/SortedDictionary.h"

#include <vector>
#include "DatabaseConstants.h"
#include "../../QueryPipeline/include/PhysicalPlan.h"

#include "Pages/PageGuard.h"
#include "DataStorage/Row.h"

namespace Pages{
    struct LeafNodeTuple;
}

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
        page_id_t rootPageId;

        Int degree;
        Int keySize;

        TreeType type;
        Int nonClusteredIndexId;

        DatabaseEngine::Database* database;
        DatabaseEngine::StorageTypes::Table* table;

        static void AssignLeavesConnections(
            Pages::PageGuard<Pages::IndexPage>& child,
            Pages::PageGuard<Pages::IndexPage>& newChild
        );

        static Int LeafLowerBound(const Pages::PageGuard<Pages::IndexPage>& page, const DataTypes::Indexing::Key& key);
        static Int LeafPartialLowerBound(const Pages::PageGuard<Pages::IndexPage>& page, const DataTypes::Indexing::Key& key);

        static Int InternalNodeLowerBound(const Pages::PageGuard<Pages::IndexPage>& page, const DataTypes::Indexing::Key& key);
        static Int InternalNodePartialLowerBound(const Pages::PageGuard<Pages::IndexPage>& page, const DataTypes::Indexing::Key& key);

        static Int LowerBound(const Pages::PageGuard<Pages::IndexPage>& page, const DataTypes::Indexing::Key& key);
        static Int PartialLowerBound(const Pages::PageGuard<Pages::IndexPage>& page, const DataTypes::Indexing::Key& key);
        static bool IsDuplicateKey(
            const std::vector<DataTypes::Indexing::Key*>* keys,
            const DataTypes::Indexing::Key& key,
            Int indexPos
        );
        static Errors::RuntimeStatus CreateDuplicateKeyError(const DataTypes::Indexing::Key& key);

        Pages::PageGuard<Pages::IndexPage> CreateRootPage(Int& indexPosition, Int pagesToAllocate);

        void SplitRoot(Pages::PageGuard<Pages::IndexPage>& root, MultiThreading::ReaderGuard& rootLock, Int pagesToAllocate);

        void SplitChild(
            Pages::PageGuard<Pages::IndexPage>& parent,
            MultiThreading::ReaderGuard& parentReadLock,
            Int index,
            Pages::PageGuard<Pages::IndexPage>& child,
            MultiThreading::ReaderGuard& childReadLock,
            Int pagesToAllocate
        );

        void SplitLeafNoLock(
            Pages::PageGuard<Pages::IndexPage>& parent,
            Pages::PageGuard<Pages::IndexPage>& child,
            Pages::PageGuard<Pages::IndexPage>& newChild,
            Int index
        )const;

        void SplitInternalNodeNoLock(
            Pages::PageGuard<Pages::IndexPage>& parent,
            Pages::PageGuard<Pages::IndexPage>& child,
            Pages::PageGuard<Pages::IndexPage>& newChild,
            Int index
        )const;

        void SplitChildNoLock(
            Pages::PageGuard<Pages::IndexPage>& parent,
            Int index,
            Pages::PageGuard<Pages::IndexPage>& child,
            Int pagesToAllocate
        );
        Errors::RuntimeStatus InsertToNonFullNode(
            Pages::PageGuard<Pages::IndexPage>& parent,
            const Pages::LeafNodeTuple& tuple,
            Int pagesToAllocate,
            Int& indexPosition
        );

        Errors::RuntimeStatus InsertToNode(
            Pages::PageGuard<Pages::IndexPage>& parent,
            const Pages::LeafNodeTuple& tuple,
            Int& indexPosition
        ) const;

        [[nodiscard]] Pages::PageGuard<Pages::IndexPage> SearchKey(const DataTypes::Indexing::Key& key) const;
        [[nodiscard]] Pages::PageGuard<Pages::IndexPage> SearchKeyWithAncestors(const DataTypes::Indexing::Key& key, std::vector<Pages::PageGuard<Pages::IndexPage>>& ancestors) const;
        [[nodiscard]] Pages::PageGuard<Pages::IndexPage> SearchLeftMostLeafNode() const;
        [[nodiscard]] Pages::PageGuard<Pages::IndexPage> SearchLeftMostLeafNode(TinyInt& depth) const;

        [[nodiscard]] Pages::PageGuard<Pages::IndexPage> GetNode(page_id_t pageId) const;
        [[nodiscard]] Int CalculateTreeDegree(const DatabaseEngine::StorageTypes::Table* otherTable, TreeType treeType, Int nonClusteredId)const;

        [[nodiscard]] Pages::PageGuard<Pages::IndexPage> AllocateNewPage(page_id_t parentPageId, Int pagesToAllocate);

        void HandleUnderflow(Pages::PageGuard<Pages::IndexPage>& node, std::vector<Pages::PageGuard<Pages::IndexPage>>& ancestors, Int& parentIndex);
        void HandleRootUnderflow();

        bool TryBorrowFromLeftSibling(Pages::PageGuard<Pages::IndexPage>& node, Pages::PageGuard<Pages::IndexPage>& parent, Int index)const;
        bool TryBorrowFromRightSibling(Pages::PageGuard<Pages::IndexPage>& node, Pages::PageGuard<Pages::IndexPage>& parent, Int index)const;

        // Leaf redistribution methods for improved space utilization
        [[nodiscard]] bool TryRedistributeLeaf(
            Pages::PageGuard<Pages::IndexPage>& parent,
            MultiThreading::ReaderGuard& parentLock,
            Pages::PageGuard<Pages::IndexPage>& child,
            MultiThreading::ReaderGuard& childLock,
            Int childIndex
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
            Int parentKeyIndex,
            std::vector<Pages::PageGuard<Pages::IndexPage>>& ancestors,
            Int& parentIndex
        );

        void CalculateClusteredStatistics(
            Pages::PageGuard<Pages::IndexPage>& currentNode,
            Headers::IndexStatistics& indexStatistics,
            Headers::TableStatistics& tableStatistics,
            std::vector<Headers::ColumnStatistics>& columnStatistics,
            Dictionary<Int, SortedDictionary<Value, BigInt, ValueComparator>>& sortedValues
        )const;

        void UpdatePfsPage(Pages::PageGuard<Pages::IndexPage>& node)const;

    public:
        explicit BTree(
            DatabaseEngine::StorageTypes::Table *table,
            page_id_t indexPageId,
            TreeType treeType,
            Int nonClusteredIndexId = -1
        );
        BTree();
        ~BTree();

        Errors::RuntimeStatus InsertRow(
            const Pages::LeafNodeTuple& tuple,
            Int pagesToAllocate,
            Int& indexPosition
        );

        void IndexSeekRange(
            const DataTypes::Indexing::Key& minKey,
            const DataTypes::Indexing::Key& maxKey,
            vector<DataTypes::Indexing::QueryData>& result
        )const;

        void IndexSeekRange(
            const DatabaseEngine::ExecutionProperties& properties,
            const DataTypes::Indexing::Key& minKey,
            const DataTypes::Indexing::Key& maxKey,
            std::vector<DatabaseEngine::StorageTypes::Row>* result
        )const;

        void IndexSeekRange(
            const DatabaseEngine::ExecutionProperties& properties,
            const DataTypes::Indexing::Key& minKey,
            const DataTypes::Indexing::Key& maxKey,
            std::vector<DatabaseEngine::StorageTypes::Row>* result,
            const Expressions::Expression* expression
        )const;

        void IndexSeek(
            const DatabaseEngine::ExecutionProperties& properties,
            const DataTypes::Indexing::Key& key,
            std::vector<DatabaseEngine::StorageTypes::Row>* result
        )const;

        void IndexSeek(
            const DatabaseEngine::ExecutionProperties& properties,
            const DataTypes::Indexing::Key& key,
            std::vector<DatabaseEngine::StorageTypes::Row>* result,
            const Expressions::Expression* expression
        )const;

        void IndexScan(vector<DataTypes::Indexing::QueryData>& result)const;

        void IndexScan(
            const DatabaseEngine::ExecutionProperties& properties,
            std::vector<DatabaseEngine::StorageTypes::Row>* result,
            DatabaseEngine::IndexState& state
        )const;

        void IndexScan(
            const DatabaseEngine::ExecutionProperties& properties,
            std::vector<DatabaseEngine::StorageTypes::Row>* result,
            DatabaseEngine::IndexState& state,
            const Expressions::Expression* expression
        )const;

        void IndexScan(
            const DatabaseEngine::ExecutionProperties& properties,
            std::vector<DatabaseEngine::StorageTypes::Row>* result,
            const Expressions::Expression* expression
        )const;

        void IndexScan(
            const DatabaseEngine::ExecutionProperties& properties,
            std::vector<DatabaseEngine::StorageTypes::Row>* result
        )const;

        void IndexScan(
            vector<Headers::RowIdentifier>* result,
            DatabaseEngine::IndexState& state,
            Int rowsToSelect
        )const;

        void IndexScan(vector<Headers::RowIdentifier>* result, const Expressions::Expression* expression)const;

        void IndexScanUpdate(
            const DatabaseEngine::ExecutionProperties& properties,
            const Expressions::Expression* expression,
            const vector<Value>& updates
        )const;

        [[nodiscard]] Errors::RuntimeStatus IndexScanUpdate(
            const DatabaseEngine::ExecutionProperties& properties,
            const Expressions::Expression* expression,
            const vector<QueryPipeline::Statements::UpdateColumn*>&  updates
        )const;

        [[nodiscard]] Errors::RuntimeStatus IndexScanUpdate(
            const DatabaseEngine::ExecutionProperties& properties,
            const vector<QueryPipeline::Statements::UpdateColumn*>&  updates
        )const;

        [[nodiscard]] Errors::RuntimeStatus IndexSeekUpdate(
            const DatabaseEngine::ExecutionProperties& properties,
            const DataTypes::Indexing::Key& key,
            const std::vector<Value>& updates
        )const;

        Errors::RuntimeStatus IndexSeekUpdate(
            const DatabaseEngine::ExecutionProperties& properties,
            const Expressions::Expression* expression,
            const DataTypes::Indexing::Key* minKey,
            const DataTypes::Indexing::Key* maxKey,
            const std::vector<Value>& updates
        )const;
        Errors::RuntimeStatus IndexSeekUpdate(
            const DatabaseEngine::ExecutionProperties& properties,
            const DataTypes::Indexing::Key* minKey,
            const DataTypes::Indexing::Key* maxKey,
            const std::vector<Value>& updates
        )const;

        void SearchKey(const DataTypes::Indexing::Key& key, DataTypes::Indexing::QueryData& result) const;

        void Remove(const DataTypes::Indexing::Key& key);

        void SetBranchingFactor(Int branchingFactor);

        [[nodiscard]] Int GetBranchingFactor() const;

        void SetTreeType(TreeType treeType);

        [[nodiscard]] page_id_t GetFirstIndexPageId() const;

        void InsertRowsToOtherTree(Int indexPos, Int pagesToAllocate)const;

        void InsertColumnToRow(column_index_t index, const Value& defaultValue)const;

        void RemoveColumnFromRow(column_index_t index)const;

        [[nodiscard]] bool IsEmpty()const;

        void CalculateIndexStatistics(
            Headers::IndexStatistics& indexStatistics,
            Headers::TableStatistics& tableStatistics,
            std::vector<Headers::ColumnStatistics>& columnStatistics,
            Dictionary<Int, SortedDictionary<Value, BigInt, ValueComparator>>& sortedValues
        )const;
    };
}
