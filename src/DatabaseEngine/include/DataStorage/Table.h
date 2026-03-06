#pragma once
#include <string>
#include <vector>

#include "InsertPayload.h"
#include "../DatabaseConstants.h"
#include "../../../Systemic/include/Headers.h"
#include "../BTree.h"
#include "../Logger/Logger.h"
#include "../Pages/OverflowPageView.h"
#include "../BufferPool/FileManager.h"
#include "../Memory/Allocator.h"
#include "../Memory/PersistentAllocator.h"

namespace Pages{
    class LargeObjectView;
}

namespace DatabaseEngine::StorageTypes{
    class InsertPayload;
}

namespace QueryPipeline::Statements {
    struct Expression;
}

class Value;

namespace Indexing{
    class BTree;
}

namespace DatabaseEngine{
    class Database;
}

namespace ByteMaps{
    class BitMap;
}

namespace DatabaseEngine::StorageTypes
{
    struct TableHeader{
        table_id_t tableId;
        SmallInt ordinalPosition;

        page_id_t allocationPageId;
        column_number_t numberOfColumns;

        page_id_t clusteredIndexPageId;
        std::vector<page_id_t> nonClusteredIndexPageIds;

        TableHeader();
        ~TableHeader();
        TableHeader &operator=(const TableHeader &tableHeader);
    };

    class Table final{
        HashSet<column_id_t> clusteredIndexColumnsCache;
        TableHeader header;
        DataStructures::PolymorphicArray<Indexing::BTree*> nonClusteredIndexedTrees;
        DataStructures::PolymorphicArray<Column*> _columns;

        Memory::PersistentAllocator _allocator;

        Headers::Index clusteredIndexHeader;
        DataStructures::PolymorphicArray<Headers::Index> nonClusteredIndexes;

        Database *database;
        Indexing::BTree* clusteredIndexedTree;

        protected:
            static bool VectorContainsIndex(const std::vector<column_index_t>& vector, column_index_t index, int& indexPosition);

        /**
        * @name Index and Pages protected Functions
        * Functions to manage indexes and pages.
        * @{
        */
            void PopulateClusteredIndexCache(const Headers::Index& index);
            bool IsColumnAutoComputedPrimaryKey(const Column* column) const;

            void GetClusteredIndexFromDisk() const;
            void GetNonClusteredIndexFromDisk(Int indexId) const;
            [[nodiscard]] Pages::IndexPageView GetIndexFromDisk(page_id_t indexPageId) const;

            static void LinkLargePageDataObjectChunks(
                const Pages::LargeObjectView* dataObject,
                page_id_t lastLargePageId
            );
            void InsertLargeDataObjectPointerToRow(
                bool isFirstRecursion,
                page_id_t lastLargePageId,
                column_index_t largeBlockIndex
            ) const;
            page_id_t StoreLargeObject(
                const Value& value,
                page_offset_t &offset,
                block_size_t &remainingBlockSize,
                Pages::LargeObjectView* previousDataObject
            )const;
            [[nodiscard]] Pages::LargeObjectView GetOrCreateLargeDataPage() const;

        /** @} End of: Class Constructors and Destructors*/

            void InsertExistingRowsToNonClusteredIndexByClusteredIndex(Int indexPos, Int pagesToAllocate);
            void InsertExistingRowToNonClusteredIndexByHeap(Int indexPos, Int pagesToAllocate);
            void RemoveColumnByClusteredIndex(column_index_t index);
            void RemoveColumnByHeap(column_index_t index)const;
            void InsertToVersionDatabase(const Pages::RawRowReference& rowRef) const;

        public:
            InsertPayload CreateInsertPayload(
                Errors::RuntimeStatus& status,
                const ::Memory::IAllocator* allocator,
                transaction_id_t transactionId,
                const std::vector<Value> &inputData
            ) const;
            InsertPayload CreateUpdatePayload(
                Errors::RuntimeStatus& status,
                const ::Memory::IAllocator* allocator,
                transaction_id_t transactionId,
                const std::vector<Value> &inputData
            ) const;
        /**
        * @name Class Constructors and Destructors
        * Functions to create and destroy Table objects.
        * @{
        */
            Table(table_id_t tableId, Int ordinalPosition, Database *database);
            // Table(
            //   table_id_t tableId,
            //   Int ordinalPosition,
            //   const std::vector<Column *> &columns,
            //   Database *database,
            //   const Headers::Index* clusteredIndex = nullptr,
            //   const std::vector<Headers::Index> *nonClusteredIndexes = nullptr
            // );
            Table(const Headers::TableHeader& masterDbHeader, const TableHeader &tableHeader, Database *database);
            Table(const std::string& tableName, const TableHeader &tableHeader, Database *database);
            Table(
                const Headers::sysTable& systemHeader,
                const TableHeader &tableHeader,
                const Headers::Index& primaryKey,
                Database *database,
                Int ordinalPosition
            );
            ~Table();

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
                const std::vector<Value> &inputData
            );
            Errors::RuntimeStatus InsertRow(
                const ExecutionContext& executionContext,
                InsertPayload& payload,
                Int pagesToAllocate
            );
            Errors::RuntimeStatus HeapInsert(const InsertPayload& payload, Int pagesToAllocate)const;
            Errors::RuntimeStatus ClusteredIndexInsert(
                const ExecutionContext& executionContext,
                InsertPayload& payload,
                Int pagesToAllocate
            );
            Errors::RuntimeStatus NonClusteredIndexInsert(
                const Row* row,
                Int nonClusteredIndexId,
                Int pagesToAllocate,
                const DataTypes::RowIdentifier& data
            );
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
            [[nodiscard]] DataTypes::StringView GetFileNameView() const;
            [[nodiscard]] DataTypes::StringView GetSystemFileNameView() const;
            [[nodiscard]] column_number_t GetNumberOfColumns() const;
            [[nodiscard]] const TableHeader &GetHeader() const;
            [[nodiscard]] const DataStructures::Array<Column*>& GetColumns() const;
            void GetConstantColumns(DataStructures::PolymorphicArray<const Column*>* array) const;
            [[nodiscard]] const Headers::Index& GetNonClusteredIndexes(Int indexPos) const;
            [[nodiscard]] const DataStructures::StaticArray<column_index_t, 10>& GetClusteredIndex() const;
            [[nodiscard]] std::vector<DataType> GetColumnTypeByTreeId(const UnsignedTinyInt& treeId) const;
            [[nodiscard]] table_id_t GetTableId() const;
            [[nodiscard]] TableType GetType() const;
            [[nodiscard]] bool IsClustered()const;

        /** @} End of: Metadata Accessor Functions*/

        /**
        * @name Scan and Seek Functions
        * Functions to access rows using scans and seeks.
        * @{
        */
            void ClusteredIndexSeekRange(
                const ExecutionContext& executionContext,
                DataStructures::Array<Pages::RowReference>* selectedRows,
                const DataTypes::Indexing::Key& minKey,
                const DataTypes::Indexing::Key& maxKey,
                const Expressions::Expression* expression
            );
            void ClusteredIndexSeek(
                const ExecutionContext& executionContext,
                DataStructures::Array<Pages::RowReference>* selectedRows,
                const DataTypes::Indexing::Key& key,
                const Expressions::Expression* expression
            );
            void SystemClusteredIndexSeek(
                const ::Memory::IAllocator* allocator,
                DataStructures::Array<Pages::RowReference>* selectedRows,
                const DataTypes::Indexing::Key& key,
                const Expressions::Expression* expression
            );
            void ClusteredIndexScan(
                const ExecutionContext& executionContext,
                DataStructures::Array<Pages::RowReference>* selectedRows,
                IndexState& state,
                const Expressions::Expression* expression
            );
            void ClusteredIndexScan(
                const ExecutionContext& executionContext,
                DataStructures::Array<Pages::RowReference>* selectedRows,
                const Expressions::Expression* expression
            );
            void SystemClusteredIndexScan(
                const ::Memory::IAllocator* allocator,
                DataStructures::Array<Pages::RowReference>* selectedRows,
                const Expressions::Expression* expression
            );
            void NonClusteredIndexScan(
                const ExecutionContext& executionContext,
                DataStructures::Array<Pages::RowReference>* selectedRows,
                Int indexPos,
                IndexState& state,
                const Expressions::Expression* expression
            );
            void HeapScan(
                const ExecutionContext& executionContext,
                DataStructures::PolymorphicArray<Pages::RowReference> *result,
                ScanState& state
            )const;
            void TemporaryDatabaseHeapScan(
                DataStructures::PolymorphicArray<Pages::RowReference> *result,
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
                const std::vector<Value> &updates
            );
            Errors::RuntimeStatus HeapUpdate(
                const ExecutionContext& executionContext,
                const Expressions::Expression* expression,
                const std::vector<Expressions::Expression*> &updates
            );
            void ClusteredIndexScanUpdate(
                const ExecutionContext& executionContext,
                const Expressions::Expression* expression,
                const std::vector<Value> &updates
            );
            [[nodiscard]] Errors::RuntimeStatus ClusteredIndexScanUpdate(
                const ExecutionContext& executionContext,
                const Expressions::Expression* expression,
                const std::vector<Expressions::Expression*> &updates
            );
            [[nodiscard]] Errors::RuntimeStatus ClusteredIndexSeekUpdate(
                const ExecutionContext& executionContext,
                const Expressions::Expression* expression,
                const DataTypes::Indexing::Key* minimumValue,
                const DataTypes::Indexing::Key* maximumValue,
                const std::vector<Value> &updates
            );
            [[nodiscard]] Errors::RuntimeStatus ClusteredIndexSeekUpdate(
                const ExecutionContext& executionContext,
                const DataTypes::Indexing::Key& key,
                const std::vector<Value> &updates
            );
            [[nodiscard]] Errors::RuntimeStatus SystemClusteredIndexSeekUpdate(
                const ::Memory::IAllocator* allocator,
                const DataTypes::Indexing::Key& key,
                const std::vector<Value> &updates
            );
            [[nodiscard]]
            Errors::RuntimeStatus UpdateRowNoLock(
                const Pages::PageView* page,
                const Pages::RowReference& rowPtr,
                const ExecutionContext& executionContext,
                const std::vector<Value>& updates
            );
            [[nodiscard]]
            Errors::RuntimeStatus UpdateRowNoLock(
                const Pages::PageView* page,
                const Pages::RowReference& rowPtr,
                const ExecutionContext& executionContext,
                const std::vector<Expressions::Expression*>& updates
            );
            [[nodiscard]]
            Errors::RuntimeStatus SystemUpdateRowNoLock(
                const Pages::PageView* page,
                const Pages::RowReference& rowPtr,
                const ::Memory::IAllocator* allocator,
                const std::vector<Value>& updates
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
        * @name Page and Index Management Functions
        * Functions that manage indexes and pages
        * @{
        */
            Int CreateNonClusteredIndex(const std::vector<column_index_t>& columnIndices);
            void UpdateIndexAllocationMapPageId(page_id_t indexAllocationMapPageId);
            page_id_t GetIndexAllocationMapPageId()const;

            [[nodiscard]] page_id_t GetClusteredIndexPageId() const;
            void SetClusteredIndexPageId(page_id_t indexPageId);

            [[nodiscard]] page_id_t GetNonClusteredIndexPageId(Int indexPosition) const;
            void SetNonClusteredIndexPageId(page_id_t indexPageId, Int indexPosition);

            Indexing::BTree* GetClusteredIndexedTree();
            Indexing::BTree* GetNonClusteredIndexTree(Int nonClusteredIndexId);
            [[nodiscard]] bool HasNonClusteredIndexes() const;

            DataTypes::Indexing::Key CreateKey(
                const ExecutionContext& executionContext,
                const DataStructures::StaticArray<column_index_t, 10>& indexedColumns,
                const InsertPayload& payload
            ) const;

            void DeleteLargeObjectFromPage(Pages::RowReference& rowPtr, const HashSet<column_index_t>& updatedColumns);
            void DeleteOverflowedRowsFromPage(Pages::RowReference& rowPtr, const HashSet<column_index_t>& updatedColumns)const;

            [[nodiscard]] Pages::LargeObjectView GetLargeDataPage(page_id_t pageId) const;
            [[nodiscard]] Pages::OverflowPageView GetOverflowPage(page_id_t pageId) const;

        /** @} End of: Page and Index Management Functions*/

            void Truncate();

            [[nodiscard]] row_size_t GetMaximumRowSize() const;

            [[nodiscard]] row_size_t ReduceMaximumRowSize() const;

            [[nodiscard]] key_size_t CalculateIndexKeySize(Int indexPos = -1) const;

            [[nodiscard]] key_size_t CalculateNonClusteredIndexKeySize(Int indexPos) const;

//            void GetIndexedColumnKeys(vector<column_index_t> *vector) const;

//            void GetNonClusteredIndexedColumnKeys(vector<vector<column_index_t>> *vector) const;

            [[nodiscard]] Database* GetDatabase() const;

            int HandleRowOverflow(Pages::RowReference& rowPtr) const;
            int HandleRowOverflow(Pages::RowReference& rowPtr, const Column* column)const;

            void InsertLargeObjectToPage(Pages::RowReference& rowPtr);

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
                const DataTypes::String& columnName,
                DataType type,
                row_size_t recordSize,
                column_index_t index,
                bool allowNulls
            );
            void HandleAddColumn(
                const ExecutionContext& executionContext,
                const Pages::PageView* page,
                const Pages::RowReference& rowPtr,
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
