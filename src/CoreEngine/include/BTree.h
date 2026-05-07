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

namespace CoreEngine{
    class Database;
}

namespace Pages{
    class IndexPage;
}

namespace Indexing{
    class BTree final{
        CoreEngine::Database* database;
        CoreEngine::StorageTypes::Table* table;

        Int degree;
        Int keySize;

        Int nonClusteredIndexId;
        page_id_t rootPageId;
        Constants::TreeType type;

        [[nodiscard]] bool ShouldSplit(const Pages::IndexPageView& node)const;

        static void AssignLeavesConnections(
            const Pages::IndexPageView& child,
            const Pages::IndexPageView& newChild
        );

        static inline Int LeafLowerBound(
            const ::Memory::IAllocator* allocator,
            const Pages::IndexPageView& page,
            const DataTypes::Indexing::Key& key
        );
        static inline Int InternalNodeLowerBound(
            const Pages::IndexPageView& page,
            const DataTypes::Indexing::Key& key
        );

        static Errors::RuntimeStatus CreateDuplicateKeyError(
            const DataTypes::Indexing::Key& key,
            const ::Memory::IAllocator* allocator
        );

        Pages::IndexPageView CreateRootPage(
            const CoreEngine::ExecutionContext& context,
            Int& indexPosition,
            Int pagesToAllocate
        );

        void SplitRoot(
            const CoreEngine::ExecutionContext& context,
            Pages::IndexPageView& root,
            MultiThreading::ReaderGuard& rootLock,
            Int pagesToAllocate
        );

        void SplitChild(
            const CoreEngine::ExecutionContext& context,
            const Pages::IndexPageView& parent,
            MultiThreading::ReaderGuard& parentReadLock,
            Int index,
            const Pages::IndexPageView& child,
            MultiThreading::ReaderGuard& childReadLock,
            Int pagesToAllocate
        );

        static void SplitLeafNoLock(
            const CoreEngine::ExecutionContext& context,
            const Pages::IndexPageView& parent,
            const Pages::IndexPageView& child,
            const Pages::IndexPageView& newChild,
            Int index
        );
        static void SplitInternalNodeNoLock(
            const CoreEngine::ExecutionContext& context,
            const Pages::IndexPageView& parent,
            const Pages::IndexPageView& child,
            const Pages::IndexPageView& newChild,
            Int index
        );

        void SplitChildNoLock(
            const CoreEngine::ExecutionContext& context,
            const Pages::IndexPageView& parent,
            Int index,
            const Pages::IndexPageView& child,
            Int pagesToAllocate
        );
        Errors::RuntimeStatus InsertToNonFullNode(
            const CoreEngine::ExecutionContext& context,
            const Pages::IndexPageView& parent,
            const Pages::IndexInsertTuple& tuple,
            Int pagesToAllocate,
            Int& indexPosition
        );

        static Errors::RuntimeStatus InsertToNode(
            const CoreEngine::ExecutionContext& context,
            const Pages::IndexPageView& parent,
            const Pages::IndexInsertTuple& tuple,
            Int& indexPosition
        );

        [[nodiscard]] Pages::IndexPageView SearchKey(const ::Memory::IAllocator* allocator, const DataTypes::Indexing::Key& key) const;
        [[nodiscard]] Pages::IndexPageView SearchKeyWithAncestors(const DataTypes::Indexing::Key& key, DataStructures::PolymorphicArray<Pages::IndexPageView>& ancestors) const;
        [[nodiscard]] Pages::IndexPageView SearchLeftMostLeafNode(const ::Memory::IAllocator* allocator) const;
        [[nodiscard]] Pages::IndexPageView SearchLeftMostLeafNode(const ::Memory::IAllocator* allocator, TinyInt& depth) const;

        [[nodiscard]] Pages::IndexPageView GetNode(page_id_t pageId) const;
        [[nodiscard]] Int CalculateTreeDegree(const CoreEngine::StorageTypes::Table* otherTable, Constants::TreeType treeType, Int nonClusteredId)const;

        [[nodiscard]] Pages::IndexPageView AllocateNewPage(
            const ::Memory::IAllocator* allocator,
            page_id_t parentPageId,
            page_id_t splitChildPageId,
            Int pagesToAllocate
        );

        void HandleUnderflow(const Pages::IndexPageView& node, DataStructures::PolymorphicArray<Pages::IndexPageView>& ancestors, Int& parentIndex);
        void HandleRootUnderflow();

        bool TryBorrowFromLeftSibling(Pages::IndexPageView& node, Pages::IndexPageView& parent, Int index)const;
        bool TryBorrowFromRightSibling(Pages::IndexPageView& node, Pages::IndexPageView& parent, Int index)const;

        // Leaf redistribution methods for improved space utilization
        [[nodiscard]] bool TryRedistributeLeaf(
            const CoreEngine::ExecutionContext& context,
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
            DataStructures::PolymorphicArray<Pages::IndexPageView>& ancestors,
            Int& parentIndex
        );

        void CalculateClusteredStatistics(
            const ::Memory::IAllocator* allocator,
            Pages::IndexPageView& currentNode,
            Headers::IndexStatistics& indexStatistics,
            Headers::TableStatistics& tableStatistics,
            DataStructures::PolymorphicArray<Headers::ColumnStatistics>& columnStatistics,
            Dictionary<Int, SortedDictionary<Value, BigInt, ValueComparator>>& sortedValues
        )const;

        void UpdatePfsPage(const Pages::IndexPageView& node)const;

    public:
        explicit BTree(
            CoreEngine::StorageTypes::Table *table,
            page_id_t indexPageId,
            Constants::TreeType treeType,
            Int nonClusteredIndexId = -1
        );
        BTree();
        ~BTree();

        Errors::RuntimeStatus InsertRow(
            const CoreEngine::ExecutionContext& context,
            const Pages::IndexInsertTuple& tuple,
            Int pagesToAllocate,
            Int& indexPosition
        );

        void IndexSeekRange(
            const DataTypes::Indexing::Key& minKey,
            const DataTypes::Indexing::Key& maxKey,
            DataStructures::PolymorphicArray<DataTypes::Indexing::QueryData>& result
        )const;

        void IndexSeekRange(
            const CoreEngine::ExecutionContext& context,
            const DataTypes::Indexing::Key& minKey,
            const DataTypes::Indexing::Key& maxKey,
            DataStructures::PolymorphicArray<Pages::RowReference>* result
        )const;

        void IndexSeekRange(
            const CoreEngine::ExecutionContext& context,
            const DataTypes::Indexing::Key& minKey,
            const DataTypes::Indexing::Key& maxKey,
            DataStructures::PolymorphicArray<Pages::RowReference>* result,
            const Expressions::Expression* expression
        )const;

        void IndexSeek(
            const CoreEngine::ExecutionContext& context,
            const DataTypes::Indexing::Key& key,
            DataStructures::PolymorphicArray<Pages::RowReference>* result
        )const;

        void IndexSeek(
            const CoreEngine::ExecutionContext& context,
            const DataTypes::Indexing::Key& key,
            DataStructures::PolymorphicArray<Pages::RowReference>* result,
            const Expressions::Expression* expression
        )const;
        void SystemIndexSeek(
            const ::Memory::IAllocator* allocator,
            const DataTypes::Indexing::Key& key,
            DataStructures::PolymorphicArray<Pages::RowReference>* result
        )const;
        void SystemIndexSeek(
            const ::Memory::IAllocator* allocator,
            const DataTypes::Indexing::Key& key,
            DataStructures::PolymorphicArray<Pages::RowReference>* result,
            const Expressions::Expression* expression
        )const;

        void IndexScan(DataStructures::PolymorphicArray<DataTypes::Indexing::QueryData>& result)const;

        void IndexScan(
            const CoreEngine::ExecutionContext& context,
            DataStructures::PolymorphicArray<Pages::RowReference>* result,
            CoreEngine::IndexState& state
        )const;

        void IndexScan(
            const CoreEngine::ExecutionContext& context,
            DataStructures::PolymorphicArray<Pages::RowReference>* result,
            CoreEngine::IndexState& state,
            const Expressions::Expression* expression
        )const;

        void IndexScan(
            const CoreEngine::ExecutionContext& context,
            DataStructures::PolymorphicArray<Pages::RowReference>* result
        )const;
        void IndexScan(
            const CoreEngine::ExecutionContext& context,
            DataStructures::PolymorphicArray<Pages::RowReference>* result,
            const Expressions::Expression* expression
        )const;
        void SystemIndexScan(
            const ::Memory::IAllocator* allocator,
            DataStructures::PolymorphicArray<Pages::RowReference>* result,
            const Expressions::Expression* expression
        )const;
        void SystemIndexScan(
            const ::Memory::IAllocator* allocator,
            DataStructures::PolymorphicArray<Pages::RowReference>* result
        )const;
        void IndexScan(
            DataStructures::PolymorphicArray<DataTypes::RowIdentifier>* result,
            CoreEngine::IndexState& state,
            Int rowsToSelect
        )const;

        void IndexScan(DataStructures::PolymorphicArray<DataTypes::RowIdentifier>* result, const Expressions::Expression* expression)const;

        void IndexScanUpdate(
            const CoreEngine::ExecutionContext& context,
            const Expressions::Expression* expression,
            const DataStructures::PolymorphicArray<Value> &updates
        )const;

        [[nodiscard]] Errors::RuntimeStatus IndexScanUpdate(
            const CoreEngine::ExecutionContext& context,
            const Expressions::Expression* expression,
           const DataStructures::PolymorphicArray<Expressions::Expression*>& updates
        )const;

        [[nodiscard]]
        Errors::RuntimeStatus IndexScanUpdate(
            const CoreEngine::ExecutionContext& context,
            const DataStructures::PolymorphicArray<Expressions::Expression*>& updates
        )const;

        [[nodiscard]]
        Errors::RuntimeStatus IndexSeekUpdate(
            const CoreEngine::ExecutionContext& context,
            const DataTypes::Indexing::Key& key,
            const DataStructures::PolymorphicArray<Value>& updates
        )const;
        [[nodiscard]]
        Errors::RuntimeStatus IndexSeekUpdate(
            const CoreEngine::ExecutionContext& context,
            const Expressions::Expression* expression,
            const DataTypes::Indexing::Key* minKey,
            const DataTypes::Indexing::Key* maxKey,
            const DataStructures::PolymorphicArray<Value>& updates
        )const;
        [[nodiscard]]
        Errors::RuntimeStatus IndexSeekUpdate(
            const CoreEngine::ExecutionContext& context,
            const DataTypes::Indexing::Key* minKey,
            const DataTypes::Indexing::Key* maxKey,
            const DataStructures::PolymorphicArray<Value>& updates
        )const;
        [[nodiscard]]
        Errors::RuntimeStatus SystemIndexSeekUpdate(
            const ::Memory::IAllocator* allocator,
            const DataTypes::Indexing::Key& key,
            const DataStructures::PolymorphicArray<Value>& updates
        )const;

        void SearchKey(const DataTypes::Indexing::Key& key, DataTypes::Indexing::QueryData& result) const;

        void Remove(const DataTypes::Indexing::Key& key);

        void SetBranchingFactor(Int branchingFactor);

        [[nodiscard]] Int GetBranchingFactor() const;

        void SetTreeType(Constants::TreeType treeType);

        [[nodiscard]] page_id_t GetFirstIndexPageId() const;

        void InsertRowsToOtherTree(Int indexPos, Int pagesToAllocate)const;

        void InsertColumnToRow(column_index_t index, const Value& defaultValue)const;

        void RemoveColumnFromRow(column_index_t index)const;

        [[nodiscard]] bool IsEmpty()const;

        void CalculateIndexStatistics(
            Headers::IndexStatistics& indexStatistics,
            Headers::TableStatistics& tableStatistics,
            DataStructures::PolymorphicArray<Headers::ColumnStatistics>& columnStatistics,
            Dictionary<Int, SortedDictionary<Value, BigInt, ValueComparator>>& sortedValues
        )const;
    };
}
