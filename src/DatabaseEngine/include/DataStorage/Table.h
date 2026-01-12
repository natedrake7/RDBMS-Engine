#pragma once
#include <string>
#include <vector>
#include "../DatabaseConstants.h"
#include "../../../Systemic/include/Headers.h"
#include "../BTree.h"
#include "../Logger/Logger.h"
#include "../Pages/PageGuard.h"

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

    struct TableHeader
    {
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

    class Table final
    {
        TableHeader header;

        std::vector<Column *> columns;
        DatabaseEngine::Database *database;

        HashSet<column_id_t> clusteredIndexColumnsCache;

        Indexing::BTree* clusteredIndexedTree;
        vector<Indexing::BTree*> nonClusteredIndexedTrees;

        protected:

            void PopulateClusteredIndexCache(const Headers::Index& index);

            bool IsColumnAutoComputedPrimaryKey(const Column* column) const;

            void PopulateAutoComputedColumns(Pointer<Row>& row)const;

            [[nodiscard]] Pages::PageGuard<Pages::LargeObjectPage> GetOrCreateLargeDataPage() const;

            static void LinkLargePageDataObjectChunks(Pages::LargeDataObject *dataObject, const page_id_t &lastLargePageId);
            void InsertLargeDataObjectPointerToRow(Pointer<Row>& row, const bool &isFirstRecursion, const page_id_t &lastLargePageId, const column_index_t &largeBlockIndex) const;
            void RecursiveInsertToLargePage(Pointer<Row>& row, page_offset_t &offset, const column_index_t &columnIndex, block_size_t &remainingBlockSize, const bool &isFirstRecursion, Pages::LargeDataObject **previousDataObject);

            static void InsertNullValues(Block *&block, Pointer<Row>& row, const column_index_t &associatedColumnIndex);
            static bool VectorContainsIndex(const vector<column_index_t>& vector, const column_index_t& index, int& indexPosition);

            void GetClusteredIndexFromDisk() const;
            void GetNonClusteredIndexFromDisk(const int& indexId) const;
            [[nodiscard]] Pages::PageGuard<Pages::IndexPage> GetIndexFromDisk(const page_id_t& indexPageId) const;

            [[nodiscard]] Errors::RuntimeStatus BatchCreateRow(
                Pointer<Row>& rowPtr,
                const transaction_id_t& transactionId,
                const vector<Value>& inputData,
                const std::vector<column_index_t> &columnIndices,
                Logging::CheckPoint* checkPoint
            )const;

            [[nodiscard]] Errors::RuntimeStatus CreateRow(
                Pointer<Row>& rowPtr,
                const transaction_id_t& transactionId,
                const vector<Value>& inputData,
                Logging::CheckPoint* checkPoint
            )const;

            [[nodiscard]] Errors::RuntimeStatus CreateRow(
                Pointer<Row>& rowPtr,
                const transaction_id_t& transactionId,
                const std::vector<Value>& inputData,
                const std::vector<column_index_t>& columnIndices,
                Logging::CheckPoint* checkPoint
            )const;

            [[nodiscard]] Errors::RuntimeStatus CreateRow(
                Pointer<Row>& rowPtr,
                const transaction_id_t& transactionId,
                const std::vector<Expressions::Expression*>& inputData,
                const std::vector<column_index_t>& columnIndices,
                Logging::CheckPoint* checkPoint
            )const;

            void InsertRowToPage(
                Pages::PageGuard<Pages::PageFreeSpacePage>& pageFreeSpacePage,
                Pages::PageGuard<>& page,
                Pointer<Row>& row,
                const int &indexPosition
            )const;
            void InsertRowToClusteredPage(Pages::PageGuard<Pages::PageFreeSpacePage>& pageFreeSpacePage, Pages::Page* page, Pointer<Row>& row, const int &indexPosition)const;

            static bool PopulateColumnIdentity(Pointer<Row>& row, Column*& column, int64_t& outValue);

            static void PopulateDefaultValues(Pointer<Row>& row, Column*& column);

            void InsertExistingRowsToNonClusteredIndexByClusteredIndex(const int32_t& indexPos, const int& pagesToAllocate);

            void InsertExistingRowToNonClusteredIndexByHeap(const int& indexPos, const int& pagesToAllocate);

            void RemoveColumnByClusteredIndex(const column_index_t& index);

            void RemoveColumnByHeap(const column_index_t& index)const;

        public:
            Table(
              const table_id_t &tableId,
              const int& ordinalPosition,
              const vector<Column *> &columns,
              Database *database,
              const Headers::Index* clusteredIndex = nullptr,
              const vector<Headers::Index> *nonClusteredIndexes = nullptr);

            Table(const Headers::TableHeader& masterDbHeader, const TableHeader &tableHeader, Database *database);

            Table(const std::string& tableName, const TableHeader &tableHeader, Database *database);

            Table(
                const Headers::sysTable& systemHeader,
                const TableHeader &tableHeader,
                const Headers::Index& primaryKey,
                Database *database,
                const int& ordinalPosition
            );

            ~Table();

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

            Errors::RuntimeStatus InsertRow(Pointer<Row>& row, const int& pagesToAllocate);

            void DeleteLargeObjectFromPage(Pointer<Row>& row, const HashSet<column_index_t>& updatedColumns)const;

            void DeleteOverflowedRowsFromPage(Pointer<Row>& row, const HashSet<column_index_t>& updatedColumns)const;

            string GetSchema();

            [[nodiscard]] string GetFileName() const;

            [[nodiscard]] column_number_t GetNumberOfColumns() const;

            [[nodiscard]] const TableHeader &GetHeader() const;

            [[nodiscard]] const vector<Column *> &GetColumns() const;

            [[nodiscard]] std::vector<const Column*> GetConstantColumns() const;

            [[nodiscard]] Pages::PageGuard<Pages::LargeObjectPage> GetLargeDataPage(const page_id_t &pageId) const;

            [[nodiscard]] Pages::PageGuard<Pages::OverflowPage> GetOverflowPage(const page_id_t &pageId) const;

            [[nodiscard]] const Headers::Index& GetNonClusteredIndexes(const int& indexPos) const;

            [[nodiscard]] const vector<column_index_t>& GetClusteredIndex() const;

            void ClusteredIndexSeekRange(
                const ExecutionProperties& properties,
                std::vector<Pointer<Row>> *selectedRows,
                const DataTypes::Indexing::Key& minKey,
                const DataTypes::Indexing::Key& maxKey,
                const Expressions::Expression* expression
            );

            void ClusteredIndexSeek(
                const ExecutionProperties& properties,
                std::vector<Pointer<Row>> *selectedRows,
                const DataTypes::Indexing::Key& key,
                const Expressions::Expression* expression
            );

            void ClusteredIndexScan(
                const ExecutionProperties& properties,
                std::vector<Pointer<Row>> *selectedRows,
                IndexState& state,
                const Expressions::Expression* expression
            );

            void ClusteredIndexScan(
                const ExecutionProperties& properties,
                std::vector<Pointer<Row>> *selectedRows,
                const Expressions::Expression* expression
            );

            void NonClusteredIndexScan(
                const ExecutionProperties& properties,
                std::vector<Pointer<Row>> *selectedRows,
                const int& indexPos,
                IndexState& state,
                const Expressions::Expression* expression
            );

            void HeapScan(
                const ExecutionProperties& properties,
                std::vector<Pointer<Row>> *result,
                ScanState& state
            )const;

            void TemporaryDatabaseHeapScan(
                std::vector<Pointer<Row>> *result,
                ScanState& state
            )const;

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

            Errors::RuntimeStatus HeapInsert(
                Pointer<Row>& row,
                const int& pagesToAllocate,
                Headers::RowIdentifier* rowId
            )const;

            Errors::RuntimeStatus ClusteredIndexInsert(
                Pointer<Row>& row,
                const int& pagesToAllocate,
                Headers::RowIdentifier* rowId
            );

            Errors::RuntimeStatus NonClusteredIndexInsert(
                const Pointer<Row>& row,
                const int& nonClusteredIndexId,
                const int& pagesToAllocate,
                const Headers::RowIdentifier& data
            );

            Errors::RuntimeStatus NonClusteredIndexInsertExistingRows(
                const int& indexPos,
                const int& pagesToAllocate
            );

            int CreateNonClusteredIndex(vector<column_index_t>& columnIndices);

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

            void Truncate();

            void UpdateIndexAllocationMapPageId(const page_id_t &indexAllocationMapPageId);

            page_id_t GetIndexAllocationMapPageId()const;

            [[nodiscard]] bool IsColumnNullable(const column_index_t &columnIndex) const;

            void AddColumn(Column *column);

            [[nodiscard]] const table_id_t &GetTableId() const;

            [[nodiscard]] TableType GetType() const;

            [[nodiscard]] bool IsClustered()const;

            [[nodiscard]] row_size_t GetMaximumRowSize() const;

            [[nodiscard]] row_size_t ReduceMaximumRowSize() const;

            [[nodiscard]] key_size_t CalculateIndexKeySize(const int& indexPos = -1) const;

            [[nodiscard]] key_size_t CalculateNonClusteredIndexKeySize(const int& indexPos) const;

//            void GetIndexedColumnKeys(vector<column_index_t> *vector) const;

//            void GetNonClusteredIndexedColumnKeys(vector<vector<column_index_t>> *vector) const;

            void SetClusteredIndexPageId(const page_id_t &indexPageId);

            [[nodiscard]] const page_id_t& GetClusteredIndexPageId() const;

            void SetNonClusteredIndexPageId(const page_id_t& indexPageId, const int& indexPosition);

            [[nodiscard]] const page_id_t& GetNonClusteredIndexPageId( const int& indexPosition) const;

            Indexing::BTree* GetClusteredIndexedTree();

            Indexing::BTree* GetNonClusteredIndexTree(const int& nonClusteredIndexId);

            [[nodiscard]] bool HasNonClusteredIndexes() const;

            [[nodiscard]] Database* GetDatabase() const;

            [[nodiscard]] vector<DataType> GetColumnTypeByTreeId(const uint8_t& treeId) const;

            int HandleRowOverflow(const Pointer<Row>& row)const;

            int HandleRowOverflow(Pointer<Row>& row, const Column* column)const;

            void InsertLargeObjectToPage(Pointer<Row>& row);

            [[nodiscard]]
            Errors::RuntimeStatus HandleRowUpdate(
                Pages::Page *page,
                Pointer<Row>& row,
                const ExecutionProperties& properties,
                const std::vector<Value> &updates,
                const bool &isHeap = true
            );

            [[nodiscard]]
            Errors::RuntimeStatus  HandleRowUpdate(
                Pages::Page *page,
                Pointer<Row>& row,
                const ExecutionProperties& properties,
                const std::vector<QueryPipeline::Statements::UpdateColumn*> &updates,
                const HashSet<column_index_t>& updatedColumns,
                const bool &isHeap = true
            );

            void GetDefaultValuesHeaders()const;

            void GetColumnsHeaders()const;

            void UpdateIdentityManagersIds()const;

            void GetIdentityColumns()const;

            void GetIdentityColumnById(const int32_t& columnId)const;

            void GetIndexes();

            void UpdateMasterDatabase() const;

            void UpdateColumnName(const column_index_t& index, const std::string& name)const;

            void PopulateColumn(const column_index_t& index, const Value& defaultValue);

            void PopulateColumnByClusteredIndex(const column_index_t& index, const Value& defaultValue);

            void PopulateColumnByHeap(const column_index_t& index, const Value& defaultValue);

            void HandleAddColumn(Pages::Page* page, Pointer<Row>& row, const column_index_t& index, const Value& defaultValue);

            static void HandleRemoveColumn(Pages::Page* page, Pointer<Row>& row, const column_index_t& index);

            void RemoveColumn(const column_index_t& index);

            void HandleRemoveColumn(const column_index_t& index);

            void Rollback(const Snapshot& snapshot, const Headers::RowIdentifier& rowId) const;
    };
}
