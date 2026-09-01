#pragma once
#include "ExtentReservation.h"
#include "SerializedRow.h"
#include "../DatabaseConstants.h"
#include "../../../Systemic/include/Headers.h"
#include "../Indexing/BTree.h"
#include "../Logger/Logger.h"
#include "../BufferPool/FileManager.h"
#include "../Memory/Allocator.h"
#include "../Memory/PersistentAllocator.h"

namespace Expressions
{
    struct EvaluationContext;
}

namespace Pages{
    class LargeObjectView;
}

namespace CoreEngine::StorageTypes{
    class SerializedRow;
}

namespace QueryPipeline::Statements {
    struct Expression;
}

class Value;

namespace Indexing{
    class BTree;
}

namespace CoreEngine{
    struct DataVector;
    struct ScanHandle;
    struct ScanState;
    struct SelectionVector;
    class Database;
}

namespace ByteMaps{
    class BitMap;
}

namespace CoreEngine::StorageTypes{
    struct TableHeader{
        private:
            DataStructures::StaticArray<page_id_t, 10> nonClusteredIndexPageIds;
            mutable page_id_t _allocationPageId;
            mutable page_id_t _clusteredIndexPageId;

            mutable AllocationCursor _allocationCursor;

        public:
            TableHeader();
            TableHeader &operator=(const TableHeader &other);

            page_id_t GetAllocationPageId() const;
            void SetAllocationPageId(page_id_t allocationPageId) const;

            page_id_t GetClusteredIndexPageId() const;
            void SetClusteredIndexPageId(page_id_t indexPageId) const;
    };

    class Table final{
        HashSet<column_id_t> clusteredIndexColumnsCache;
        TableHeader _header;
        DataStructures::PolymorphicArray<Indexing::BTree*> _nonClusteredTrees;
        DataStructures::PolymorphicArray<Column*> _columns;

        Memory::PersistentAllocator _allocator;

        Headers::Index clusteredHeader;
        DataStructures::PolymorphicArray<Headers::Index> nonClusteredHeaders;

        Database* _db;
        Indexing::BTree* _clusteredTree;

        SmallInt _ordinalPosition;
        Int _id;

        protected:
            static bool VectorContainsIndex(const DataStructures::PolymorphicArray<column_index_t>& vector, column_index_t index, int& indexPosition);

        /**
        * @name Index and Pages protected Functions
        * Functions to manage indexes and pages.
        * @{
        */
            void PopulateClusteredIndexCache(const Headers::Index& index);
            bool IsColumnAutoComputedPrimaryKey(const Column* column) const;

            [[nodiscard]] Pages::IndexPageView GetIndexFromDisk(page_id_t indexPageId) const;

            [[nodiscard]]
            page_id_t InsertLargeObject(
                const ::Memory::IAllocator* allocator,
                const Value& value
            )const;
            page_id_t StoreLargeObject(
                ExtentReservation& reservation,
                const Value& value
            )const;

        /** @} End of: Class Constructors and Destructors*/

            void InsertExistingRowsToNonClusteredIndexByClusteredIndex(Int indexPos, Int pagesToAllocate);
            void InsertExistingRowToNonClusteredIndexByHeap(Int indexPos, Int pagesToAllocate);
            void RemoveColumnByClusteredIndex(column_index_t index);
            void RemoveColumnByHeap(column_index_t index)const;
            [[nodiscard]]
            static RID InsertToVersionDatabase(
                const ::Memory::IAllocator* allocator,
                const Pages::RawRowReference& rowRef
            );

        public:
            template<typename ValueProvider>
            SerializedRow SerializeRowGeneric(
                Errors::RuntimeStatus& status,
                const RowSerializationContext& rowContext,
                object_t* buffer,
                ValueProvider&& provider
            ) const;

            SerializedRow SerializeRow(
                Errors::RuntimeStatus& status,
                const RowSerializationContext& rowContext,
                object_t* buffer,
                const DataStructures::PolymorphicArray<Value>& values
            ) const;

            SerializedRow SerializeRow(
                Errors::RuntimeStatus& status,
                const RowSerializationContext& rowContext,
                object_t* buffer,
                const DataStructures::PolymorphicArray<Expressions::Expression*>& expressions,
                const InsertPlan& insertPlan,
                const Expressions::EvaluationContext& evaluationContext
            ) const;
        /**
        * @name Class Constructors and Destructors
        * Functions to create and destroy Table objects.
        * @{
        */
            Table(table_id_t tableId, SmallInt ordinalPosition, Database *db);
            Table(const Headers::TableHeader& masterDbHeader, const TableHeader &tableHeader, Database *database);
            Table(
                const Headers::sysTable& systemHeader,
                const TableHeader &tableHeader,
                const Headers::Index& primaryKey,
                Database *database,
                SmallInt ordinalPosition
            );
            void Destroy()const;

        /** @} End of: Class Constructors and Destructors*/

        /**
        * @name Insert Functions
        * Functions to insert rows into the table.
        * @{
        */
            Errors::RuntimeStatus BatchInsert(
                const ExecutionContext& executionContext,
                DataStructures::PolymorphicArray<QueryResult> &input
            );
            Errors::RuntimeStatus InsertRow(
                const ExecutionContext& executionContext,
                const DataStructures::PolymorphicArray<Value> &inputData
            );
            Errors::RuntimeStatus InsertRow(
                const ExecutionContext& executionContext,
                const DataStructures::PolymorphicArray<Expressions::Expression*> &inputData,
                const InsertPlan& insertPlan
            );
            Errors::RuntimeStatus InsertRowPayload(
                const ExecutionContext& executionContext,
                ExtentReservation& extentReservation,
                SerializedRow& payload
            );
            Errors::RuntimeStatus HeapInsertToNewPage(
                ExtentReservation& extentReservation,
                const SerializedRow& payload
            )const;
            Errors::RuntimeStatus HeapInsert(
                const ExecutionContext& executionContext,
                ExtentReservation& extentReservation,
                const SerializedRow& payload
            )const;
            Errors::RuntimeStatus ClusteredIndexInsert(
                const ExecutionContext& executionContext,
                ExtentReservation& extentReservation,
                SerializedRow& payload
            );
            // Errors::RuntimeStatus NonClusteredIndexInsert(
            //     const Row* row,
            //     Int nonClusteredIndexId,
            //     Int pagesToAllocate,
            //     const DataTypes::RowIdentifier& data
            // );
            Errors::RuntimeStatus NonClusteredIndexInsertExistingRows(
                Int indexPos,
                Int pagesToAllocate
            );

        /** @} End of: Insert Functions*/

        /**
        * @name Metadata Accessor Functions
        * Functions to access table metadata.
        * @{
        */
            [[nodiscard]] Storage::FileKey GetSystemFileKey() const;
            [[nodiscard]] Storage::FileKey GetDataFileKey() const;
            [[nodiscard]] column_number_t GetNumberOfColumns() const;
            [[nodiscard]] const TableHeader &GetHeader() const;
            [[nodiscard]] const DataStructures::PolymorphicArray<Column*>& GetColumns() const;
            [[nodiscard]] const Column* GetColumn(column_index_t index) const;
            void GetConstantColumns(DataStructures::PolymorphicArray<const Column*>* array) const;
            [[nodiscard]] const Headers::Index& GetNonClusteredIndexes(Int indexPos) const;
            [[nodiscard]] const DataStructures::StaticArray<column_index_t, 10>& GetClusteredIndex() const;
            [[nodiscard]] DataStructures::StaticArray<DataType, 10> GetColumnTypeByTreeId(UnsignedTinyInt treeId) const;
            [[nodiscard]] table_id_t GetTableId() const;
            [[nodiscard]] Constants::TableType GetType() const;
            [[nodiscard]] bool IsClustered()const;

        /** @} End of: Metadata Accessor Functions*/

        /**
        * @name Scan and Seek Functions
        * Functions to access rows using scans and seeks.
        * @{
        */
            void ClusteredIndexSeekRange(
                const ExecutionContext& executionContext,
                DataStructures::PolymorphicArray<RID>* selectedRows,
                const DataTypes::Indexing::Key& minKey,
                const DataTypes::Indexing::Key& maxKey,
                const Expressions::Expression* expression,
                const DataStructures::PolymorphicArray<FilterColumnInfo>& filterColumns,
                UnsignedSmallInt slotIndex
            );
            void ClusteredIndexSeek(
                const ExecutionContext& executionContext,
                DataStructures::PolymorphicArray<RID>* selectedRows,
                const DataTypes::Indexing::Key& key,
                const Expressions::Expression* expression,
                const DataStructures::PolymorphicArray<FilterColumnInfo>& filterColumns,
                UnsignedSmallInt slotIndex
            );
            void SystemClusteredIndexSeek(
                const ::Memory::IAllocator* allocator,
                DataStructures::PolymorphicArray<RID>* selectedRows,
                const DataTypes::Indexing::Key& key,
                const Expressions::Expression* expression
            );
            void ClusteredIndexScan(
                const ExecutionContext& executionContext,
                DataStructures::PolymorphicArray<RID>* selectedRows,
                IndexState& state,
                const Expressions::Expression* expression,
                const DataStructures::PolymorphicArray<FilterColumnInfo>& filterColumns,
                UnsignedSmallInt slotIndex
            );
            void ClusteredIndexScan(
                const ExecutionContext& executionContext,
                DataStructures::PolymorphicArray<RID>* selectedRows,
                const Expressions::Expression* expression,
                const DataStructures::PolymorphicArray<FilterColumnInfo>& filterColumns,
                const Storage::FileKey* fileKeys
            );
            void SystemClusteredIndexScan(
                const ::Memory::IAllocator* allocator,
                DataStructures::PolymorphicArray<RID>* selectedRows,
                const Expressions::Expression* expression
            );
            void NonClusteredIndexScan(
                const ExecutionContext& executionContext,
                DataStructures::PolymorphicArray<RID>* selectedRows,
                Int indexPos,
                IndexState& state,
                const Expressions::Expression* expression
            );
            void HeapScan(
                const ExecutionContext& executionContext,
                DataStructures::PolymorphicArray<RID>* result,
                ScanState& state
            )const;
            void TemporaryDatabaseHeapScan(
                DataStructures::PolymorphicArray<RID>* result,
                ScanState& state,
                Int batchSize
            )const;


        /** @} End of: Scan and Seek Functions*/

        /**
        * @name Update Functions
        * Functions to update rows in the table.
        * @{
        */
            Errors::RuntimeStatus HeapUpdate(
                const ExecutionContext& executionContext,
                const Expressions::Expression* expression,
                const DataStructures::PolymorphicArray<Value> &updates
            );
            Errors::RuntimeStatus HeapUpdate(
                const ExecutionContext& executionContext,
                const Expressions::Expression* expression,
                const DataStructures::PolymorphicArray<Expressions::Expression*> &updates
            );
            void ClusteredIndexScanUpdate(
                const ExecutionContext& executionContext,
                const Expressions::Expression* expression,
                const DataStructures::PolymorphicArray<Value> &updates
            );
            [[nodiscard]] Errors::RuntimeStatus ClusteredIndexScanUpdate(
                const ExecutionContext& executionContext,
                const Expressions::Expression* expression,
                const DataStructures::PolymorphicArray<Expressions::Expression*> &updates
            );
            [[nodiscard]] Errors::RuntimeStatus ClusteredIndexSeekUpdate(
                const ExecutionContext& executionContext,
                const Expressions::Expression* expression,
                const DataTypes::Indexing::Key* minimumValue,
                const DataTypes::Indexing::Key* maximumValue,
                const DataStructures::PolymorphicArray<Value> &updates
            );
            [[nodiscard]] Errors::RuntimeStatus ClusteredIndexSeekUpdate(
                const ExecutionContext& executionContext,
                const DataTypes::Indexing::Key& key,
                const DataStructures::PolymorphicArray<Value> &updates
            );
            [[nodiscard]] Errors::RuntimeStatus SystemClusteredIndexSeekUpdate(
                const ::Memory::IAllocator* allocator,
                const DataTypes::Indexing::Key& key,
                const DataStructures::PolymorphicArray<Value> &updates
            );
            [[nodiscard]]
            Errors::RuntimeStatus UpdateRowNoLock(
                const Pages::PageView* page,
                const RID* rid,
                const ExecutionContext& context,
                const DataStructures::PolymorphicArray<Value>& updates
            );
            [[nodiscard]]
            Errors::RuntimeStatus UpdateRowNoLock(
                const Pages::PageView* page,
                const RID* row,
                const ExecutionContext& context,
                const DataStructures::PolymorphicArray<Expressions::Expression*>& updates
            );
            [[nodiscard]]
            Errors::RuntimeStatus SystemUpdateRowNoLock(
                const Pages::PageView* page,
                const RID* row,
                const ::Memory::IAllocator* allocator,
                const DataStructures::PolymorphicArray<Value>& updates
            ) const;
        /** @} End of: Update Functions*/

        /**
        * @name Delete Functions
        * Functions to delete rows in the table.
        * @{
        */
            void HeapDelete(
                const ExecutionContext& executionContext,
                const Expressions::Expression* expression
            ) const;
            void ClusteredIndexScanDelete(
                const ExecutionContext& executionContext,
                const Expressions::Expression* expression,
                IndexState& state
            );
            void ClusteredIndexSeekDelete(
                const ExecutionContext& executionContext,
                const Expressions::Expression* expression,
                IndexState& state
            );

        /** @} End of: Delete Functions*/

        /**
        * @name Calculation and Utility Functions
        * Mostly general utility functions for the table.
        * @{
        */

        /** @} End of: Calculation and Utility Functions*/

        /**
        * @name Materialization Functions
        * @{
        */
            Value MaterializeColumn(const ::Memory::IAllocator* allocator, const RID* rid, column_index_t columnIndex) const;
            QueryResult MaterializeFromPage(const::Memory::IAllocator* allocator, const RID* row) const;

            template<typename T>
            static void MaterializeColumnFromPage(
                const Storage::FileKey* fileKeys,
                const ::Memory::IAllocator* allocator,
                const DataStructures::PolymorphicArray<RID>& rids,
                DataVector* __restrict__ _vector,
                column_index_t ordinalPosition
            );

            template<typename T>
            static void MaterializeColumn(
                const ExecutionContext& context,
                const SelectionVector* sv,
                DataVector* __restrict__ _vector,
                UnsignedSmallInt slotIndex,
                column_index_t ordinalPosition
            );
        /** @} End of: Materialization Functions*/

        /**
        * @name Page and Index Management Functions
        * Functions that manage indexes and pages
        * @{
        */
            Int CreateNonClusteredIndex(const DataStructures::PolymorphicArray<column_index_t>& columnIndices);
            void UpdateAllocationPageId(page_id_t allocationPageId) const;
            page_id_t GetAllocationPageId()const;

            [[nodiscard]] page_id_t GetClusteredIndexPageId() const;
            void SetClusteredIndexPageId(page_id_t pageId) const;

            [[nodiscard]] page_id_t GetNonClusteredIndexPageId(Int indexPosition) const;
            void SetNonClusteredIndexPageId(page_id_t indexPageId, Int indexPosition);

            Indexing::BTree* GetClusteredIndexedTree();
            Indexing::BTree* GetNonClusteredIndexTree(Int nonClusteredIndexId);
            [[nodiscard]] bool HasNonClusteredIndexes() const;

            DataTypes::Indexing::Key CreateKey(
                const ExecutionContext& executionContext,
                const DataStructures::StaticArray<column_index_t, 10>& indexedColumns,
                const SerializedRow& payload
            ) const;

            void DeleteLargeObjectFromPage(RID* rowPtr, const HashSet<column_index_t>& updatedColumns);
            void DeleteOverflowedRowsFromPage(RID* rowPtr, const HashSet<column_index_t>& updatedColumns)const;

            ExtentReservation ReserveExtents(
                const ::Memory::IAllocator* allocator,
                Int requiredPages
            )const;

            ExtentReservation LazyReservation(const ::Memory::IAllocator* allocator)const;

        /** @} End of: Page and Index Management Functions*/

            void Truncate();

            [[nodiscard]] row_size_t GetMaximumRowSize() const;
            [[nodiscard]] row_size_t ReduceMaximumRowSize() const;

            [[nodiscard]] key_size_t CalculateIndexKeySize(Int indexPos = -1) const;
            [[nodiscard]] key_size_t CalculateNonClusteredIndexKeySize(Int indexPos) const;
            [[nodiscard]] row_size_t CalculateInsertPayloadSize()const;
            [[nodiscard]] Database* GetDatabase() const;

            [[nodiscard]] SmallInt GetOrdinalPosition() const;

            int HandleRowOverflow(RID* rowPtr) const;
            int HandleRowOverflow(RID* rowPtr, const Column* column)const;

            [[nodiscard]] bool IsEmpty()const;

            void PopulateColumn(column_index_t index, const Value& defaultValue);
            void PopulateColumnByClusteredIndex(column_index_t index, const Value& defaultValue);
            void PopulateColumnByHeap(column_index_t index, const Value& defaultValue);

            void Rollback(const Snapshot& snapshot, const DataTypes::RowIdentifier& rowId)const;
        /**
        * @name Table Altering Functions
        * Functions that alter the structure of the table.
        * @{
        */
            void AddColumn(Column *column);
            Column* AddColumn(
                const DataTypes::StringView& columnName,
                DataType type,
                row_size_t recordSize,
                column_index_t index,
                bool allowNulls
            );
            void HandleAddColumn(
                const ExecutionContext& executionContext,
                const Pages::PageView* page,
                const RID* rowPtr,
                column_index_t index,
                const Value& defaultValue
            ) const;
            void UpdateColumnName(column_index_t index, const DataTypes::String& name)const;
            void RemoveColumn(const ExecutionContext& context, column_index_t index);
            static void HandleRemoveColumn(Pages::PageView* page, QueryResult& row, column_index_t index);
            void HandleRemoveColumn(column_index_t index);

        /** @} End of System Catalog Integration Functions */

        /**
        * @name System Catalog Integration Functions
        * Functions to retrieve and update system catalog information related to the table.
        * @{
        */
            void UpdateSystemCatalog(const ::Memory::IAllocator* allocator) const;
            void RetrieveDefaultValuesFromCatalog(const ::Memory::IAllocator* allocator)const;
            void RetrieveColumnHeadersFromCatalog(const ::Memory::IAllocator* allocator)const;
            void UpdateCatalogIdentityColumns(const ::Memory::IAllocator* allocator)const;
            void RetrieveIdentityColumnsFromCatalog(const ::Memory::IAllocator* allocator)const;
            void RetrieveIdentityColumnById(const ::Memory::IAllocator* allocator, Int columnId)const;
            void RetrieveIndexesFromCatalog(const ::Memory::IAllocator* allocator);

        /** @} End of System Catalog Integration Functions */

        void SetPrimaryKeyIndexedColumns(const column_index_t* _array, Int size);
    };
}
