#pragma once
#include <string>
#include <vector>

#include "InsertPayload.h"
#include "../DatabaseConstants.h"
#include "../../../Systemic/include/Headers.h"
#include "../BTree.h"
#include "../Logger/Logger.h"
#include "../Pages/PageGuard.h"

namespace DatabaseEngine::StorageTypes
{
    struct InsertPayload;
}

namespace QueryPipeline::Statements {
    struct Expression;
}

using namespace std;
using namespace Constants;

class RowCondition;
class Value;

namespace Indexing{
    class BTree;
}

namespace DatabaseEngine{
    class Database;

    namespace StorageTypes{
        class Row;
    }
}

namespace Pages{
    class Page;
    class LargeObjectPage;
    class PageFreeSpacePage;
    class IndexPage;
    class OverflowPage;
    struct LargeDataObject;
}

namespace ByteMaps{
    class BitMap;
}

namespace DatabaseEngine::StorageTypes
{
    class Block;
    class Column;
    struct ColumnHeader;

    struct TableHeader{
        table_id_t tableId;
        int16_t ordinalPosition;

        page_id_t indexAllocationMapPageId;
        column_number_t numberOfColumns;

        page_id_t clusteredIndexPageId;
        std::vector<page_id_t> nonClusteredIndexPageIds;

        // bitmaps to store the composite key
        Headers::Index clusteredIndex;
        std::vector<Headers::Index> nonClusteredIndexes;

        TableHeader();
        ~TableHeader();
        TableHeader &operator=(const TableHeader &tableHeader);
    };

    class Table final{
        TableHeader header;

        std::vector<Column *> columns;
        Database *database;

        HashSet<column_id_t> clusteredIndexColumnsCache;

        Indexing::BTree* clusteredIndexedTree;
        vector<Indexing::BTree*> nonClusteredIndexedTrees;

        protected:
            static bool VectorContainsIndex(const vector<column_index_t>& vector, column_index_t index, int& indexPosition);

        /**
        * @name Index and Pages protected Functions
        * Functions to manage indexes and pages.
        * @{
        */
            void PopulateClusteredIndexCache(const Headers::Index& index);
            bool IsColumnAutoComputedPrimaryKey(const Column* column) const;

            void GetClusteredIndexFromDisk() const;
            void GetNonClusteredIndexFromDisk(Int indexId) const;
            [[nodiscard]] Pages::PageGuard<Pages::IndexPage> GetIndexFromDisk(page_id_t indexPageId) const;

            static void LinkLargePageDataObjectChunks(
                Pages::LargeDataObject *dataObject,
                page_id_t lastLargePageId
            );
            void InsertLargeDataObjectPointerToRow(
                Row* row,
                bool isFirstRecursion,
                page_id_t lastLargePageId,
                column_index_t largeBlockIndex
            ) const;
            void RecursiveInsertToLargePage(
                Row* row,
                page_offset_t &offset,
                column_index_t columnIndex,
                block_size_t &remainingBlockSize,
                bool isFirstRecursion,
                Pages::LargeDataObject **previousDataObject
            );
            [[nodiscard]] Pages::PageGuard<Pages::LargeObjectPage> GetOrCreateLargeDataPage() const;

        /** @} End of: Class Constructors and Destructors*/

        /**
        * @name Row Creation Functions
        * Functions to create rows from input data.
        * @{
        */
            Errors::RuntimeStatus CreateInsertPayload(
                transaction_id_t transactionId,
                const std::vector<Value> &inputData
            );
            [[nodiscard]] Errors::RuntimeStatus BatchCreateRow(
                Row*& rowPtr,
                transaction_id_t transactionId,
                const std::vector<Value>& inputData,
                const std::vector<column_index_t> &columnIndices,
                std::vector<char>& buffer,
                page_offset_t& bufferOffset
            )const;
            [[nodiscard]] Errors::RuntimeStatus CreateRow(
                Row*& row,
                transaction_id_t transactionId,
                const std::vector<Value>& inputData,
                Logging::CheckPoint* checkPoint
            )const;
            [[nodiscard]] Errors::RuntimeStatus CreateRow(
                Row*& row,
                transaction_id_t transactionId,
                const std::vector<Value>& inputData,
                const std::vector<column_index_t>& columnIndices,
                Logging::CheckPoint* checkPoint
            )const;
            [[nodiscard]] Errors::RuntimeStatus CreateRow(
                Row*& row,
                transaction_id_t transactionId,
                const std::vector<Expressions::Expression*>& inputData,
                const std::vector<column_index_t>& columnIndices,
                Logging::CheckPoint* checkPoint
            )const;
            static bool PopulateColumnIdentity(Row*& row, Column*& column, BigInt& outValue);
            static void PopulateDefaultValues(Row*& row, Column*& column);
            static void InsertNullValues(Block *&block, Row* row, column_index_t columnIndex);
            void PopulateAutoComputedColumns(Row*& row)const;

        /** @} End of: Class Constructors and Destructors*/

            void InsertRowToPage(
                Pages::PageGuard<Pages::PageFreeSpacePage>& pageFreeSpacePage,
                Pages::PageGuard<>& page,
                Row*& row,
                const int &indexPosition
            )const;
            void InsertExistingRowsToNonClusteredIndexByClusteredIndex(Int indexPos, Int pagesToAllocate);
            void InsertExistingRowToNonClusteredIndexByHeap(Int indexPos, Int pagesToAllocate);
            void RemoveColumnByClusteredIndex(column_index_t index);
            void RemoveColumnByHeap(column_index_t index)const;
            void InsertToVersionDatabase(Row*& row, transaction_id_t transactionId) const;

        public:
        /**
        * @name Class Constructors and Destructors
        * Functions to create and destroy Table objects.
        * @{
        */
            Table(
              table_id_t tableId,
              Int ordinalPosition,
              const vector<Column *> &columns,
              Database *database,
              const Headers::Index* clusteredIndex = nullptr,
              const vector<Headers::Index> *nonClusteredIndexes = nullptr
            );
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
                const ExecutionProperties& properties,
                const std::vector<QueryResult>& input,
                const std::vector<column_index_t>& columnIndices
            );

            Errors::RuntimeStatus InsertRow(const ExecutionProperties& properties, const vector<Value> &inputData);
            Errors::RuntimeStatus InsertRow(
                const ExecutionProperties& properties,
                const vector<Value> &inputData,
                const std::vector<column_index_t>& columnIndices
            );
            Errors::RuntimeStatus InsertRow(
                const ExecutionProperties& properties,
                const vector<Expressions::Expression*> &inputData,
                const std::vector<column_index_t>& columnIndices
            );
            Errors::RuntimeStatus InsertRow(Row*& row, Int pagesToAllocate);
            Errors::RuntimeStatus HeapInsert(Row*& row, Int pagesToAllocate)const;
            Errors::RuntimeStatus ClusteredIndexInsert(Row*& row, Int pagesToAllocate);
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
            [[nodiscard]] std::string GetFileName() const;
            [[nodiscard]] column_number_t GetNumberOfColumns() const;
            [[nodiscard]] const TableHeader &GetHeader() const;
            [[nodiscard]] const std::vector<Column *> &GetColumns() const;
            [[nodiscard]] std::vector<const Column*> GetConstantColumns() const;
            [[nodiscard]] const Headers::Index& GetNonClusteredIndexes(Int indexPos) const;
            [[nodiscard]] const std::vector<column_index_t>& GetClusteredIndex() const;
            [[nodiscard]] std::vector<DataType> GetColumnTypeByTreeId(const UnsignedTinyInt& treeId) const;
            [[nodiscard]] bool IsColumnNullable(column_index_t columnIndex) const;
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
                const ExecutionProperties& properties,
                std::vector<Pages::RowReference> *selectedRows,
                const DataTypes::Indexing::Key& minKey,
                const DataTypes::Indexing::Key& maxKey,
                const Expressions::Expression* expression
            );
            void ClusteredIndexSeek(
                const ExecutionProperties& properties,
                std::vector<Pages::RowReference> *selectedRows,
                const DataTypes::Indexing::Key& key,
                const Expressions::Expression* expression
            );
            void ClusteredIndexScan(
                const ExecutionProperties& properties,
                std::vector<Pages::RowReference> *selectedRows,
                IndexState& state,
                const Expressions::Expression* expression
            );
            void ClusteredIndexScan(
                const ExecutionProperties& properties,
                std::vector<Pages::RowReference> *selectedRows,
                const Expressions::Expression* expression
            );
            void NonClusteredIndexScan(
                const ExecutionProperties& properties,
                std::vector<Pages::RowReference> *selectedRows,
                Int indexPos,
                IndexState& state,
                const Expressions::Expression* expression
            );
            void HeapScan(
                const ExecutionProperties& properties,
                std::vector<Pages::RowReference> *result,
                ScanState& state
            )const;
            void TemporaryDatabaseHeapScan(
                std::vector<Pages::RowReference> *result,
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
                const ExecutionProperties& properties,
                const Expressions::Expression* expression,
                const vector<Value> &updates
            );
            Errors::RuntimeStatus HeapUpdate(
                const ExecutionProperties& properties,
                const Expressions::Expression* expression,
                const vector<QueryPipeline::Statements::UpdateColumn*> &updates
            );
            void ClusteredIndexScanUpdate(
                const ExecutionProperties& properties,
                const Expressions::Expression* expression,
                const vector<Value> &updates
            );
            [[nodiscard]] Errors::RuntimeStatus ClusteredIndexScanUpdate(
                const ExecutionProperties& properties,
                const Expressions::Expression* expression,
                const vector<QueryPipeline::Statements::UpdateColumn*> &updates
            );
            [[nodiscard]] Errors::RuntimeStatus ClusteredIndexSeekUpdate(
                const ExecutionProperties& properties,
                const Expressions::Expression* expression,
                const DataTypes::Indexing::Key* minimumValue,
                const DataTypes::Indexing::Key* maximumValue,
                const vector<Value> &updates
            );
            [[nodiscard]] Errors::RuntimeStatus ClusteredIndexSeekUpdate(
                const ExecutionProperties& properties,
                const DataTypes::Indexing::Key& key,
                const std::vector<Value> &updates
            );
            [[nodiscard]]
            Errors::RuntimeStatus UpdateRowNoLock(
                Pages::Page* page,
                Pages::RowReference& row,
                const ExecutionProperties& properties,
                const std::vector<Value>& updates,
                Int indexPosition,
                bool isHeap
            );
            [[nodiscard]]
            Errors::RuntimeStatus UpdateRowNoLock(
                Pages::Page* page,
                Pages::RowReference& row,
                const ExecutionProperties& properties,
                const std::vector<QueryPipeline::Statements::UpdateColumn*>& updates,
                const HashSet<column_index_t>& updatedColumns,
                Int indexPosition,
                bool isHeap
            );
        /** @} End of: Update Functions*/

        /**
        * @name Delete Functions
        * Functions to delete rows in the table.
        * @{
        */
            void HeapDelete(
                const ExecutionProperties& properties,
                const Expressions::Expression* expression
            ) const;
            void ClusteredIndexScanDelete(
                const ExecutionProperties& properties,
                const Expressions::Expression* expression,
                IndexState& state
            );
            void ClusteredIndexSeekDelete(
                const ExecutionProperties& properties,
                const Expressions::Expression* expression,
                IndexState& state
            );

        /** @} End of: Update Functions*/

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
            int CreateNonClusteredIndex(vector<column_index_t>& columnIndices);
            void UpdateIndexAllocationMapPageId(page_id_t indexAllocationMapPageId);
            page_id_t GetIndexAllocationMapPageId()const;

            [[nodiscard]] page_id_t GetClusteredIndexPageId() const;
            void SetClusteredIndexPageId(page_id_t indexPageId);

            [[nodiscard]] page_id_t GetNonClusteredIndexPageId(Int indexPosition) const;
            void SetNonClusteredIndexPageId(page_id_t indexPageId, Int indexPosition);

            Indexing::BTree* GetClusteredIndexedTree();
            Indexing::BTree* GetNonClusteredIndexTree(Int nonClusteredIndexId);
            [[nodiscard]] bool HasNonClusteredIndexes() const;

            void DeleteLargeObjectFromPage(Row*& row, const HashSet<column_index_t>& updatedColumns);
            void DeleteOverflowedRowsFromPage(Row*& row, const HashSet<column_index_t>& updatedColumns)const;

            [[nodiscard]] Pages::PageGuard<Pages::LargeObjectPage> GetLargeDataPage(page_id_t pageId) const;
            [[nodiscard]] Pages::PageGuard<Pages::OverflowPage> GetOverflowPage(page_id_t pageId) const;

        /** @} End of: Page and Index Management Functions*/

            void Truncate();

            [[nodiscard]] row_size_t GetMaximumRowSize() const;

            [[nodiscard]] row_size_t ReduceMaximumRowSize() const;

            [[nodiscard]] key_size_t CalculateIndexKeySize(Int indexPos = -1) const;

            [[nodiscard]] key_size_t CalculateNonClusteredIndexKeySize(Int indexPos) const;

//            void GetIndexedColumnKeys(vector<column_index_t> *vector) const;

//            void GetNonClusteredIndexedColumnKeys(vector<vector<column_index_t>> *vector) const;

            [[nodiscard]] Database* GetDatabase() const;

            int HandleRowOverflow(Row* row) const;
            int HandleRowOverflow(Row*& row, const Column* column)const;

            void InsertLargeObjectToPage(Row* row);

            void PopulateColumn(column_index_t index, const Value& defaultValue);
            void PopulateColumnByClusteredIndex(column_index_t index, const Value& defaultValue);
            void PopulateColumnByHeap(column_index_t index, const Value& defaultValue);

            void Rollback(const Snapshot& snapshot, const Headers::RowIdentifier& rowId)const;
        /**
        * @name Table Altering Functions
        * Functions that alter the structure of the table.
        * @{
        */
            void AddColumn(Column *column);
            void HandleAddColumn(Pages::Page* page, Row* row, column_index_t index, const Value& defaultValue);
            void UpdateColumnName(column_index_t index, const std::string& name)const;
            void RemoveColumn(column_index_t index);
            static void HandleRemoveColumn(Pages::Page* page, Row* row, column_index_t index);
            void HandleRemoveColumn(column_index_t index);

        /** @} End of System Catalog Integration Functions */

        /**
        * @name System Catalog Integration Functions
        * Functions to retrieve and update system catalog information related to the table.
        * @{
        */
            void UpdateSystemCatalog() const;

            void RetrieveDefaultValuesFromCatalog()const;

            void RetrieveColumnHeadersFromCatalog()const;

            void UpdateCatalogIdentityColumns()const;

            void RetrieveIdentityColumnsFromCatalog()const;

            void RetrieveIdentityColumnById(const int32_t& columnId)const;

            void RetrieveIndexesFromCatalog();

        /** @} End of System Catalog Integration Functions */

    };
}
