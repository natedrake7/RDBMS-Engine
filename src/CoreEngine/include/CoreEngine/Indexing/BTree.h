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

        Int32_t degree;
        Int32_t keySize;

        Int32_t nonClusteredIndexId;
        page_id_t rootPageId;

        Accessors::PageAccessor _pageAccessor;

        Constants::TreeType type;

        [[nodiscard]] bool ShouldSplit(const Pages::IndexPageView& node)const;

        static void AssignLeavesConnections(
            const Pages::IndexPageView& child,
            const Pages::IndexPageView& newChild
        );

        static inline Int32_t ScanLeafUpperBound(
            const Pages::IndexPageView& page,
            const DataTypes::Indexing::Key& key,
            Int32_t left = 0
        );
        static inline Int32_t ScanLeafLowerBound(
            const Pages::IndexPageView& page,
            const DataTypes::Indexing::Key& key
        );
        static inline Int32_t LeafLowerBound(
            const Pages::IndexPageView& page,
            const DataTypes::Indexing::Key& key
        );
        static inline Int32_t InternalNodeLowerBound(
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
            Int32_t index,
            const Pages::IndexPageView& child,
            StorageTypes::ExtentReservation& extentReservation
        ) const;

        static void SplitLeafNoLock(
            const ExecutionContext& context,
            const Pages::IndexPageView& parent,
            const Pages::IndexPageView& child,
            const Pages::IndexPageView& newChild,
            Int32_t index
        );
        static void SplitInternalNodeNoLock(
            const ExecutionContext& context,
            const Pages::IndexPageView& parent,
            const Pages::IndexPageView& child,
            const Pages::IndexPageView& newChild,
            Int32_t index
        );

        void SplitChildNoLock(
            const ExecutionContext& context,
            const Pages::IndexPageView& parent,
            Int32_t index,
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
        [[nodiscard]] Pages::IndexPageView SearchLeftMostLeafNode(Int8_t& depth) const;

        [[nodiscard]] Int32_t CalculateTreeDegree(const StorageTypes::Table* otherTable, Constants::TreeType treeType, Int32_t nonClusteredId)const;

        void HandleUnderflow(const Pages::IndexPageView& node, DataStructures::PolymorphicArray<Pages::IndexPageView>& ancestors, Int32_t& parentIndex);
        void HandleRootUnderflow();

        bool TryBorrowFromLeftSibling(Pages::IndexPageView& node, Pages::IndexPageView& parent, Int32_t index)const;
        bool TryBorrowFromRightSibling(Pages::IndexPageView& node, Pages::IndexPageView& parent, Int32_t index)const;

        // Leaf redistribution methods for improved space utilization
        [[nodiscard]] bool TryRedistributeLeaf(
            const ExecutionContext& context,
            const Pages::IndexPageView& parent,
            MultiThreading::ReaderGuard& parentLock,
            Pages::IndexPageView& child,
            MultiThreading::ReaderGuard& childLock,
            Int32_t childIndex
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
            Int32_t parentKeyIndex,
            DataStructures::PolymorphicArray<Pages::IndexPageView>& ancestors,
            Int32_t& parentIndex
        );

        void CalculateClusteredStatistics(
            const ::Memory::IAllocator* allocator,
            Pages::IndexPageView& currentNode,
            Catalog::IndexStatistics& indexStatistics,
            Catalog::TableStatistics& tableStatistics,
            DataStructures::PolymorphicArray<Catalog::ColumnStatistics>& columnStatistics,
            Dictionary<Int32_t, SortedDictionary<Value, Int64_t, ValueComparator>>& sortedValues
        )const;

        void UpdatePfsPageNoLock(const Pages::IndexPageView& page)const;

    public:
        static inline constexpr Int32_t MIN_TREE_DEGREE = 2;

        explicit BTree(
            StorageTypes::Table *table,
            page_id_t indexPageId,
            Constants::TreeType treeType,
            Int32_t nonClusteredIndexId = -1
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
            Int32_t rowsToSelect
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

        void SetBranchingFactor(Int32_t branchingFactor);

        [[nodiscard]] Int32_t GetBranchingFactor() const;

        void SetTreeType(Constants::TreeType treeType);

        [[nodiscard]] page_id_t GetFirstIndexPageId() const;

        void InsertRowsToOtherTree(StorageTypes::ExtentReservation& extentReservation, Int32_t indexPos)const;

        void InsertColumnToRow(column_index_t index, const Value& defaultValue)const;

        void RemoveColumnFromRow(column_index_t index)const;

        [[nodiscard]] bool IsEmpty()const;

        void CalculateIndexStatistics(
            Catalog::IndexStatistics& indexStatistics,
            Catalog::TableStatistics& tableStatistics,
            DataStructures::PolymorphicArray<CoreEngine::Catalog::ColumnStatistics>& columnStatistics,
            Dictionary<Int32_t, SortedDictionary<Value, Int64_t, ValueComparator>>& sortedValues
        )const;
    };
}
