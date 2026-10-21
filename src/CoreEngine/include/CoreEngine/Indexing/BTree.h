#pragma once
#include <Systemic/DataTypes/Value.h>
#include <CoreEngine/Indexing/Key.h>
#include <Systemic/DataStructures/SortedDictionary.h>

#include <CoreEngine/DatabaseConstants.h>
#include <CoreEngine/Errors.h>
#include <CoreEngine/DataStorage/Row/Row.h>
#include <CoreEngine/Pages/IndexPageView.h>

#include <Systemic/DataStructures/Dictionary.h>

#include "CoreEngine/Accessors/PageAccessor.h"

namespace Expressions{
    class Expression;
}

namespace CoreEngine::Catalog {
    struct ColumnStatistics;
    struct TableStatistics;
    struct IndexStatistics;
}

namespace Statistics {
    struct ValueFrequency;
}

namespace MultiThreading {
    class WriterGuard;
    class ReaderGuard;
}

namespace CoreEngine{
    class VectorizedPushedDownFilter;

    namespace StorageTypes
    {
        struct FilterColumnInfo;
        class ExtentReservation;
    }

    struct IndexState;
    class ExecutionContext;
    class Database;
}

namespace CoreEngine::Indexing{
    class BTree final{
        StorageTypes::Table* table;

        Int degree;
        Int keySize;

        Int nonClusteredIndexId;
        page_id_t rootPageId;

        Accessors::PageAccessor _pageAccessor;

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

        Errors::RuntimeStatus InsertToEmptyTree(
            StorageTypes::ExtentReservation& extentReservation,
            const Pages::IndexInsertTuple& tuple
        );

        void SplitRootNoLock(
            const ExecutionContext& context,
            Pages::IndexPageView& root,
            MultiThreading::WriterGuard& rootLock,
            StorageTypes::ExtentReservation& extentReservation
        );

        void SplitChild(
            const ExecutionContext& context,
            const Pages::IndexPageView& parent,
            Int index,
            const Pages::IndexPageView& child,
            StorageTypes::ExtentReservation& extentReservation
        ) const;

        static void SplitLeafNoLock(
            const ExecutionContext& context,
            const Pages::IndexPageView& parent,
            const Pages::IndexPageView& child,
            const Pages::IndexPageView& newChild,
            Int index
        );
        static void SplitInternalNodeNoLock(
            const ExecutionContext& context,
            const Pages::IndexPageView& parent,
            const Pages::IndexPageView& child,
            const Pages::IndexPageView& newChild,
            Int index
        );

        void SplitChildNoLock(
            const ExecutionContext& context,
            const Pages::IndexPageView& parent,
            Int index,
            const Pages::IndexPageView& child,
            StorageTypes::ExtentReservation& extentReservation
        ) const;

        Errors::RuntimeStatus InsertRowOptimistic(
            const ExecutionContext& context,
            const Pages::IndexInsertTuple& tuple,
            bool* outSuccess
        ) const;

        Errors::RuntimeStatus InsertRowPessimistic(
            const ExecutionContext& context,
            const Pages::IndexInsertTuple& tuple,
            StorageTypes::ExtentReservation& extentReservation
        );

        static Errors::RuntimeStatus InsertToNodeNoLock(
            const ExecutionContext& context,
            const Pages::IndexPageView& node,
            const Pages::IndexInsertTuple& tuple
        );

        [[nodiscard]] Pages::IndexPageView SearchKey(const DataTypes::Indexing::Key& key) const;
        [[nodiscard]] Pages::IndexPageView SearchKeyWithAncestors(const DataTypes::Indexing::Key& key, DataStructures::PolymorphicArray<Pages::IndexPageView>& ancestors) const;
        [[nodiscard]] Pages::IndexPageView SearchLeftMostLeafNode() const;
        [[nodiscard]] Pages::IndexPageView SearchLeftMostLeafNode(TinyInt& depth) const;

        [[nodiscard]] Int CalculateTreeDegree(const StorageTypes::Table* otherTable, Constants::TreeType treeType, Int nonClusteredId)const;

        void HandleUnderflow(const Pages::IndexPageView& node, DataStructures::PolymorphicArray<Pages::IndexPageView>& ancestors, Int& parentIndex);
        void HandleRootUnderflow();

        bool TryBorrowFromLeftSibling(Pages::IndexPageView& node, Pages::IndexPageView& parent, Int index)const;
        bool TryBorrowFromRightSibling(Pages::IndexPageView& node, Pages::IndexPageView& parent, Int index)const;

        // Leaf redistribution methods for improved space utilization
        [[nodiscard]] bool TryRedistributeLeaf(
            const ExecutionContext& context,
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
            Catalog::IndexStatistics& indexStatistics,
            Catalog::TableStatistics& tableStatistics,
            DataStructures::PolymorphicArray<Catalog::ColumnStatistics>& columnStatistics,
            Dictionary<Int, SortedDictionary<Value, BigInt, ValueComparator>>& sortedValues
        )const;

        void UpdatePfsPageNoLock(const Pages::IndexPageView& page)const;

    public:
        static inline constexpr Int MIN_TREE_DEGREE = 2;

        explicit BTree(
            StorageTypes::Table *table,
            page_id_t indexPageId,
            Constants::TreeType treeType,
            Int nonClusteredIndexId = -1
        );

        Errors::RuntimeStatus InsertRow(
            const ExecutionContext& context,
            const Pages::IndexInsertTuple& tuple,
            StorageTypes::ExtentReservation& extentReservation
        );

        void SeekRange(
            const ExecutionContext& context,
            const DataTypes::Indexing::Key& minKey,
            const DataTypes::Indexing::Key& maxKey,
            DataStructures::PolymorphicArray<StorageTypes::RID>* result
        )const;

        void SeekRange(
            const ExecutionContext& context,
            const DataTypes::Indexing::Key& minKey,
            const DataTypes::Indexing::Key& maxKey,
            DataStructures::PolymorphicArray<StorageTypes::RID>* result,
            VectorizedPushedDownFilter& filter
        )const;

        void Seek(
            const ExecutionContext& context,
            const DataTypes::Indexing::Key& key,
            DataStructures::PolymorphicArray<StorageTypes::RID>* result
        )const;

        void Seek(
            const ExecutionContext& context,
            const DataTypes::Indexing::Key& key,
            DataStructures::PolymorphicArray<StorageTypes::RID>* result,
            VectorizedPushedDownFilter& filter
        )const;
        void SystemSeek(
            const DataTypes::Indexing::Key& key,
            DataStructures::PolymorphicArray<StorageTypes::RID>* result
        )const;
        void SystemSeek(
            const ::Memory::IAllocator* allocator,
            const DataTypes::Indexing::Key& key,
            DataStructures::PolymorphicArray<StorageTypes::RID>* result,
            const Expressions::Expression* expression
        )const;

        void Scan(
            const ExecutionContext& context,
            DataStructures::PolymorphicArray<StorageTypes::RID>* result,
            IndexState& state
        )const;

        void Scan(
            const ExecutionContext& context,
            DataStructures::PolymorphicArray<StorageTypes::RID>* result,
            IndexState& state,
            VectorizedPushedDownFilter& filter
        )const;

        void Scan(
            const ExecutionContext& context,
            DataStructures::PolymorphicArray<StorageTypes::RID>* result
        )const;
        void Scan(
            const ExecutionContext& context,
            DataStructures::PolymorphicArray<StorageTypes::RID>* result,
            const Expressions::Expression* expression
        )const;

        void SystemScan(
            const ::Memory::IAllocator* allocator,
            DataStructures::PolymorphicArray<StorageTypes::RID>* result,
            const Expressions::Expression* expression
        )const;
        void SystemScan(DataStructures::PolymorphicArray<StorageTypes::RID>* result)const;
        void Scan(
            DataStructures::PolymorphicArray<DataTypes::RowIdentifier>* result,
            IndexState& state,
            Int rowsToSelect
        )const;

        void Scan(DataStructures::PolymorphicArray<DataTypes::RowIdentifier>* result, const Expressions::Expression* expression)const;

        void ScanUpdate(
            const ExecutionContext& context,
            const Expressions::Expression* expression,
            const DataStructures::PolymorphicArray<Value> &updates
        )const;

        [[nodiscard]] Errors::RuntimeStatus ScanUpdate(
            const ExecutionContext& context,
            const Expressions::Expression* expression,
           const DataStructures::PolymorphicArray<Expressions::Expression*>& updates
        )const;

        [[nodiscard]]
        Errors::RuntimeStatus ScanUpdate(
            const ExecutionContext& context,
            const DataStructures::PolymorphicArray<Expressions::Expression*>& updates
        )const;

        [[nodiscard]]
        Errors::RuntimeStatus SeekUpdate(
            const ExecutionContext& context,
            const DataTypes::Indexing::Key& key,
            const DataStructures::PolymorphicArray<Value>& updates
        )const;
        [[nodiscard]]
        Errors::RuntimeStatus SeekUpdate(
            const ExecutionContext& context,
            const Expressions::Expression* expression,
            const DataTypes::Indexing::Key* minKey,
            const DataTypes::Indexing::Key* maxKey,
            const DataStructures::PolymorphicArray<Value>& updates
        )const;
        [[nodiscard]]
        Errors::RuntimeStatus SeekUpdate(
            const ExecutionContext& context,
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

        void Remove(const DataTypes::Indexing::Key& key);

        void SetBranchingFactor(Int branchingFactor);

        [[nodiscard]] Int GetBranchingFactor() const;

        void SetTreeType(Constants::TreeType treeType);

        [[nodiscard]] page_id_t GetFirstIndexPageId() const;

        void InsertRowsToOtherTree(StorageTypes::ExtentReservation& extentReservation, Int indexPos)const;

        void InsertColumnToRow(column_index_t index, const Value& defaultValue)const;

        void RemoveColumnFromRow(column_index_t index)const;

        [[nodiscard]] bool IsEmpty()const;

        void CalculateIndexStatistics(
            Catalog::IndexStatistics& indexStatistics,
            Catalog::TableStatistics& tableStatistics,
            DataStructures::PolymorphicArray<CoreEngine::Catalog::ColumnStatistics>& columnStatistics,
            Dictionary<Int, SortedDictionary<Value, BigInt, ValueComparator>>& sortedValues
        )const;
    };
}
