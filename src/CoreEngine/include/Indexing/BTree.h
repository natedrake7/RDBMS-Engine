#pragma once
#include "../../../Systemic/include/DataTypes/Value.h"
#include "Key.h"
#include "../../../Systemic/include/DataStructures/SortedDictionary.h"

#include "../DatabaseConstants.h"
#include "../Errors.h"
#include "../DataStorage/Row.h"
#include "../Pages/IndexPageView.h"

namespace Headers{
    struct ColumnStatistics;
    struct TableStatistics;
    struct IndexStatistics;
}

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
    namespace StorageTypes
    {
        class ExtentReservation;
    }

    struct IndexState;
    class ExecutionContext;
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

        static inline Int ScanLeafUpperBound(
            const Pages::IndexPageView& page,
            const DataTypes::Indexing::Key& key,
            Int left = 0
        );
        static inline Int ScanLeafLowerBound(
            const Pages::IndexPageView& page,
            const DataTypes::Indexing::Key& key
        );
        static inline Int LeafLowerBound(
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
            CoreEngine::StorageTypes::ExtentReservation& extentReservation,
            Int& indexPosition
        );

        void SplitRoot(
            const CoreEngine::ExecutionContext& context,
            Pages::IndexPageView& root,
            MultiThreading::ReaderGuard& rootLock,
            CoreEngine::StorageTypes::ExtentReservation& extentReservation
        );

        void SplitChild(
            const CoreEngine::ExecutionContext& context,
            const Pages::IndexPageView& parent,
            MultiThreading::ReaderGuard& parentReadLock,
            Int index,
            const Pages::IndexPageView& child,
            MultiThreading::ReaderGuard& childReadLock,
            CoreEngine::StorageTypes::ExtentReservation& extentReservation
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
            CoreEngine::StorageTypes::ExtentReservation& extentReservation
        ) const;
        Errors::RuntimeStatus InsertToNonFullNode(
            const CoreEngine::ExecutionContext& context,
            Pages::IndexPageView& root,
            const Pages::IndexInsertTuple& tuple,
            CoreEngine::StorageTypes::ExtentReservation& extentReservation,
            Int& indexPosition
        );

        static Errors::RuntimeStatus InsertToNode(
            const CoreEngine::ExecutionContext& context,
            const Pages::IndexPageView& node,
            MultiThreading::ReaderGuard& readGuard,
            const Pages::IndexInsertTuple& tuple,
            Int& indexPosition
        );

        [[nodiscard]] Pages::IndexPageView SearchKey(const DataTypes::Indexing::Key& key) const;
        [[nodiscard]] Pages::IndexPageView SearchKeyWithAncestors(const DataTypes::Indexing::Key& key, DataStructures::PolymorphicArray<Pages::IndexPageView>& ancestors) const;
        [[nodiscard]] Pages::IndexPageView SearchLeftMostLeafNode() const;
        [[nodiscard]] Pages::IndexPageView SearchLeftMostLeafNode(TinyInt& depth) const;

        [[nodiscard]] Pages::IndexPageView GetNode(page_id_t pageId) const;
        [[nodiscard]] Int CalculateTreeDegree(const CoreEngine::StorageTypes::Table* otherTable, Constants::TreeType treeType, Int nonClusteredId)const;

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

        void UpdatePfsPage(const Pages::IndexPageView& page)const;

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
            CoreEngine::StorageTypes::ExtentReservation& extentReservation,
            Int& indexPosition
        );

        void IndexSeekRange(
            const CoreEngine::ExecutionContext& context,
            const DataTypes::Indexing::Key& minKey,
            const DataTypes::Indexing::Key& maxKey,
            DataStructures::PolymorphicArray<CoreEngine::StorageTypes::RID>* result
        )const;

        void IndexSeekRange(
            const CoreEngine::ExecutionContext& context,
            const DataTypes::Indexing::Key& minKey,
            const DataTypes::Indexing::Key& maxKey,
            DataStructures::PolymorphicArray<CoreEngine::StorageTypes::RID>* result,
            const Expressions::Expression* expression
        )const;

        void IndexSeek(
            const CoreEngine::ExecutionContext& context,
            const DataTypes::Indexing::Key& key,
            DataStructures::PolymorphicArray<CoreEngine::StorageTypes::RID>* result
        )const;

        void IndexSeek(
            const CoreEngine::ExecutionContext& context,
            const DataTypes::Indexing::Key& key,
            DataStructures::PolymorphicArray<CoreEngine::StorageTypes::RID>* result,
            const Expressions::Expression* expression
        )const;
        void SystemIndexSeek(
            const DataTypes::Indexing::Key& key,
            DataStructures::PolymorphicArray<CoreEngine::StorageTypes::RID>* result
        )const;
        void SystemIndexSeek(
            const ::Memory::IAllocator* allocator,
            const DataTypes::Indexing::Key& key,
            DataStructures::PolymorphicArray<CoreEngine::StorageTypes::RID>* result,
            const Expressions::Expression* expression
        )const;

        void IndexScan(
            const CoreEngine::ExecutionContext& context,
            DataStructures::PolymorphicArray<CoreEngine::StorageTypes::RID>* result,
            CoreEngine::IndexState& state
        )const;

        void IndexScan(
            const CoreEngine::ExecutionContext& context,
            DataStructures::PolymorphicArray<CoreEngine::StorageTypes::RID>* result,
            CoreEngine::IndexState& state,
            const Expressions::Expression* expression
        )const;

        void IndexScan(
            const CoreEngine::ExecutionContext& context,
            DataStructures::PolymorphicArray<CoreEngine::StorageTypes::RID>* result
        )const;
        void IndexScan(
            const CoreEngine::ExecutionContext& context,
            DataStructures::PolymorphicArray<CoreEngine::StorageTypes::RID>* result,
            const Expressions::Expression* expression
        )const;
        void SystemIndexScan(
            const ::Memory::IAllocator* allocator,
            DataStructures::PolymorphicArray<CoreEngine::StorageTypes::RID>* result,
            const Expressions::Expression* expression
        )const;
        void SystemIndexScan(DataStructures::PolymorphicArray<CoreEngine::StorageTypes::RID>* result)const;
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
            const Memory::IAllocator* allocator,
            const DataTypes::Indexing::Key& key,
            const DataStructures::PolymorphicArray<Value>& updates
        )const;

        void Remove(const DataTypes::Indexing::Key& key);

        void SetBranchingFactor(Int branchingFactor);

        [[nodiscard]] Int GetBranchingFactor() const;

        void SetTreeType(Constants::TreeType treeType);

        [[nodiscard]] page_id_t GetFirstIndexPageId() const;

        void InsertRowsToOtherTree(CoreEngine::StorageTypes::ExtentReservation& extentReservation, Int indexPos)const;

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
