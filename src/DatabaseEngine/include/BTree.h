#pragma once
#include "../../Systemic/include/DataTypes/Value.h"
#include "../../Systemic/include/Key.h"
#include "../../Systemic/include/DataStructures/SortedDictionary.h"

#include <vector>
#include "DatabaseConstants.h"
#include "../../QueryPipeline/include/PhysicalPlan.h"

#include "DataStorage/Row.h"
#include "Pages/IndexPageView.h"

namespace Pages{
    struct IndexInsertTuple;
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
            const Pages::IndexPageView& child,
            const Pages::IndexPageView& newChild
        );

        static Int LeafLowerBound(const Pages::IndexPageView& page, const DataTypes::Indexing::Key& key);
        static Int LeafPartialLowerBound(const Pages::IndexPageView& page, const DataTypes::Indexing::Key& key);

        static Int InternalNodeLowerBound(const Pages::IndexPageView& page, const DataTypes::Indexing::Key& key);
        static Int InternalNodePartialLowerBound(const Pages::IndexPageView& page, const DataTypes::Indexing::Key& key);

        static Errors::RuntimeStatus CreateDuplicateKeyError(const DataTypes::Indexing::Key& key);

        Pages::IndexPageView CreateRootPage(Int& indexPosition, Int pagesToAllocate);

        void SplitRoot(Pages::IndexPageView& root, MultiThreading::ReaderGuard& rootLock, Int pagesToAllocate);

        void SplitChild(
            const Pages::IndexPageView& parent,
            MultiThreading::ReaderGuard& parentReadLock,
            Int index,
            const Pages::IndexPageView& child,
            MultiThreading::ReaderGuard& childReadLock,
            Int pagesToAllocate
        );

        void SplitLeafNoLock(
            const Pages::IndexPageView& parent,
            const Pages::IndexPageView& child,
            const Pages::IndexPageView& newChild,
            Int index
        )const;

        void SplitInternalNodeNoLock(
            const Pages::IndexPageView& parent,
            const Pages::IndexPageView& child,
            const Pages::IndexPageView& newChild,
            Int index
        )const;

        void SplitChildNoLock(
            const Pages::IndexPageView& parent,
            Int index,
            const Pages::IndexPageView& child,
            Int pagesToAllocate
        );
        Errors::RuntimeStatus InsertToNonFullNode(
            const Pages::IndexPageView& parent,
            const Pages::IndexInsertTuple& tuple,
            Int pagesToAllocate,
            Int& indexPosition
        );

        static Errors::RuntimeStatus InsertToNode(
            const Pages::IndexPageView& parent,
            const Pages::IndexInsertTuple& tuple,
            Int& indexPosition
        );

        [[nodiscard]] Pages::IndexPageView SearchKey(const DataTypes::Indexing::Key& key) const;
        [[nodiscard]] Pages::IndexPageView SearchKeyWithAncestors(const DataTypes::Indexing::Key& key, std::vector<Pages::IndexPageView>& ancestors) const;
        [[nodiscard]] Pages::IndexPageView SearchLeftMostLeafNode() const;
        [[nodiscard]] Pages::IndexPageView SearchLeftMostLeafNode(TinyInt& depth) const;

        [[nodiscard]] Pages::IndexPageView GetNode(page_id_t pageId) const;
        [[nodiscard]] Int CalculateTreeDegree(const DatabaseEngine::StorageTypes::Table* otherTable, TreeType treeType, Int nonClusteredId)const;

        [[nodiscard]] Pages::IndexPageView AllocateNewPage(page_id_t parentPageId, Int pagesToAllocate);

        void HandleUnderflow(const Pages::IndexPageView& node, std::vector<Pages::IndexPageView>& ancestors, Int& parentIndex);
        void HandleRootUnderflow();

        bool TryBorrowFromLeftSibling(Pages::IndexPageView& node, Pages::IndexPageView& parent, Int index)const;
        bool TryBorrowFromRightSibling(Pages::IndexPageView& node, Pages::IndexPageView& parent, Int index)const;

        // Leaf redistribution methods for improved space utilization
        [[nodiscard]] bool TryRedistributeLeaf(
            const Pages::IndexPageView& parent,
            MultiThreading::ReaderGuard& parentLock,
            Pages::IndexPageView& child,
            MultiThreading::ReaderGuard& childLock,
            Int childIndex
        )const;

        [[nodiscard]] bool TryRedistributeLeafWithLeftSibling(
            Pages::IndexPageView& child,
            Pages::IndexPageView& sibling,
            MultiThreading::ReaderGuard& childLock,
            MultiThreading::ReaderGuard& siblingLock
        )const;

        [[nodiscard]] bool TryRedistributeLeafWithRightSibling(
            Pages::IndexPageView& child,
            Pages::IndexPageView& sibling,
            MultiThreading::ReaderGuard& childLock,
            MultiThreading::ReaderGuard& siblingLock
        )const;

        void MergeNodes(
            Pages::IndexPageView& leftNode,
            Pages::IndexPageView& rightNode,
            Pages::IndexPageView& parent,
            Int parentKeyIndex,
            std::vector<Pages::IndexPageView>& ancestors,
            Int& parentIndex
        );

        void CalculateClusteredStatistics(
            Pages::IndexPageView& currentNode,
            Headers::IndexStatistics& indexStatistics,
            Headers::TableStatistics& tableStatistics,
            std::vector<Headers::ColumnStatistics>& columnStatistics,
            Dictionary<Int, SortedDictionary<Value, BigInt, ValueComparator>>& sortedValues
        )const;

        void UpdatePfsPage(const Pages::IndexPageView& node)const;

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
            const Pages::IndexInsertTuple& tuple,
            Int pagesToAllocate,
            Int& indexPosition
        );

        void IndexSeekRange(
            const DataTypes::Indexing::Key& minKey,
            const DataTypes::Indexing::Key& maxKey,
            std::vector<DataTypes::Indexing::QueryData>& result
        )const;

        void IndexSeekRange(
            const DatabaseEngine::ExecutionProperties& properties,
            const DataTypes::Indexing::Key& minKey,
            const DataTypes::Indexing::Key& maxKey,
            std::vector<Pages::RowReference>* result
        )const;

        void IndexSeekRange(
            const DatabaseEngine::ExecutionProperties& properties,
            const DataTypes::Indexing::Key& minKey,
            const DataTypes::Indexing::Key& maxKey,
            std::vector<Pages::RowReference>* result,
            const Expressions::Expression* expression
        )const;

        void IndexSeek(
            const DatabaseEngine::ExecutionProperties& properties,
            const DataTypes::Indexing::Key& key,
            std::vector<Pages::RowReference>* result
        )const;

        void IndexSeek(
            const DatabaseEngine::ExecutionProperties& properties,
            const DataTypes::Indexing::Key& key,
            std::vector<Pages::RowReference>* result,
            const Expressions::Expression* expression
        )const;

        void IndexScan(std::vector<DataTypes::Indexing::QueryData>& result)const;

        void IndexScan(
            const DatabaseEngine::ExecutionProperties& properties,
            std::vector<Pages::RowReference>* result,
            DatabaseEngine::IndexState& state
        )const;

        void IndexScan(
            const DatabaseEngine::ExecutionProperties& properties,
            std::vector<Pages::RowReference>* result,
            DatabaseEngine::IndexState& state,
            const Expressions::Expression* expression
        )const;

        void IndexScan(
            const DatabaseEngine::ExecutionProperties& properties,
            std::vector<Pages::RowReference>* result,
            const Expressions::Expression* expression
        )const;

        void IndexScan(
            const DatabaseEngine::ExecutionProperties& properties,
            std::vector<Pages::RowReference>* result
        )const;

        void IndexScan(
            std::vector<DataTypes::RowIdentifier>* result,
            DatabaseEngine::IndexState& state,
            Int rowsToSelect
        )const;

        void IndexScan(std::vector<DataTypes::RowIdentifier>* result, const Expressions::Expression* expression)const;

        void IndexScanUpdate(
            const DatabaseEngine::ExecutionProperties& properties,
            const Expressions::Expression* expression,
            const std::vector<Value> &updates
        )const;

        [[nodiscard]] Errors::RuntimeStatus IndexScanUpdate(
            const DatabaseEngine::ExecutionProperties& properties,
            const Expressions::Expression* expression,
           const std::vector<Expressions::Expression*>& updates
        )const;

        [[nodiscard]] Errors::RuntimeStatus IndexScanUpdate(
            const DatabaseEngine::ExecutionProperties& properties,
            const std::vector<Expressions::Expression*>& updates
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
