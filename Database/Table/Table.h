#pragma once
#include <string>
#include <unordered_set>
#include <vector>
#include "../Constants.h"
#include "../../AdditionalLibraries/AdditionalDataTypes/Headers/Headers.h"
#include "../B+Tree/BPlusTree.h"

namespace QueryPipeline::Statements {
    struct Expression;
}

using namespace std;
using namespace Constants;

class RowCondition;
class Field;

namespace Indexing{
    class BPlusTree;
}

namespace DatabaseEngine
{
    class Database;

    namespace StorageTypes
    {
        class Row;
    }
}

namespace Pages
{
    class Page;
    class LargeDataPage;
    class PageFreeSpacePage;
    class IndexPage;
    class OverflowPage;
    struct DataObject;
}

namespace ByteMaps
{
    class BitMap;
}

namespace DatabaseEngine::StorageTypes
{
    class Block;
    class Column;
    struct ColumnHeader;

    typedef struct TableHeader
    {
        table_id_t tableId;
        int16_t ordinalPosition;

        page_id_t indexAllocationMapPageId;
        column_number_t numberOfColumns;

        page_id_t clusteredIndexPageId;
        vector<page_id_t> nonClusteredIndexPageIds;
        vector<uint8_t> nonClusteredIndexesIds;

        // bitmaps to store the composite key
        Headers::Index clusteredIndex;
        vector<Headers::Index> nonClusteredIndexes;

        TableHeader();
        ~TableHeader();
        TableHeader &operator=(const TableHeader &tableHeader);
    } TableHeader;

    class Table final
    {

        TableHeader header;
        vector<Column *> columns;
        DatabaseEngine::Database *database;

        Indexing::BPlusTree* clusteredIndexedTree;
        vector<Indexing::BPlusTree*> nonClusteredIndexedTrees;

        protected:

            [[nodiscard]] int64_t PopulateAutoComputedColumns(Row* row)const;

            [[nodiscard]] Pages::LargeDataPage *GetOrCreateLargeDataPage() const;

            static void LinkLargePageDataObjectChunks(Pages::DataObject *dataObject, const page_id_t &lastLargePageId, const large_page_index_t &objectIndex);
            void InsertLargeDataObjectPointerToRow(Row *row, const bool &isFirstRecursion, const page_id_t &lastLargePageId, const column_index_t &largeBlockIndex) const;
            void RecursiveInsertToLargePage(Row *&row, page_offset_t &offset, const column_index_t &columnIndex, block_size_t &remainingBlockSize, const bool &isFirstRecursion, Pages::DataObject **previousDataObject);
            AdditionalDataTypes::ResultStatus InsertRow(Row* row, vector<extent_id_t> &allocatedExtents, extent_id_t &startingExtentIndex);

            static void CheckAndInsertNullValues(Block *&block, Row *&row, const column_index_t &associatedColumnIndex);
            static bool VectorContainsIndex(const vector<column_index_t>& vector, const column_index_t& index, int& indexPosition);

            void GetClusteredIndexFromDisk() const;
            void GetNonClusteredIndexFromDisk(const int& indexId) const;
            [[nodiscard]] Pages::IndexPage* GetIndexFromDisk(const page_id_t& indexPageId) const;

            [[nodiscard]] Row* CreateRow(const vector<Field>& inputData, int64_t* primaryKeyVal);

            void InsertRowToPage(Pages::PageFreeSpacePage *pageFreeSpacePage, Pages::Page *page, Row *row, const int &indexPosition)const;
            void InsertRowToClusteredPage(Pages::PageFreeSpacePage *pageFreeSpacePage, Pages::Page *page, Row *row, const int &indexPosition);

            void UpdateColumnIdentity(const int32_t& columnId, const int32_t& lastValue)const;

        public:
            Table(
              const table_id_t &tableId,
              const vector<Column *> &columns,
              DatabaseEngine::Database *database,
              const Headers::Index* clusteredIndex = nullptr,
              const vector<Headers::Index> *nonClusteredIndexes = nullptr);

            Table(const Headers::TableHeader& masterDbHeader, const TableHeader &tableHeader, Database *database);

            Table(const std::string& tableName, const TableHeader &tableHeader, DatabaseEngine::Database *database);

            Table(
                const Headers::sysTable& systemHeader,
                const TableHeader &tableHeader,
                const Headers::Index& primaryKey,
                DatabaseEngine::Database *database,
                const int& ordinalPosition);

            ~Table();

            AdditionalDataTypes::ResultStatus InsertRows(const vector<vector<Field>> &inputData);

            AdditionalDataTypes::ResultStatus InsertRow(const vector<Field> &inputData);

            void DeleteLargeObjectFromPage(Row *row, const HashSet<column_index_t>& updatedColumns);

            void DeleteOverflowedRowsFromPage(Row *row, const HashSet<column_index_t>& updatedColumns)const;

            string GetSchema();

            [[nodiscard]] string GetFileName() const;

            [[nodiscard]] column_number_t GetNumberOfColumns() const;

            [[nodiscard]] const TableHeader &GetTableHeader() const;

            [[nodiscard]] const vector<Column *> &GetColumns() const;

            [[nodiscard]] Pages::LargeDataPage *GetLargeDataPage(const page_id_t &pageId) const;

            [[nodiscard]] Pages::OverflowPage *GetOverflowPage(const page_id_t &pageId) const;

            [[nodiscard]] const vector<vector<column_index_t>>& GetNonClusteredIndexes() const;

            [[nodiscard]] const vector<column_index_t>& GetClusteredIndex() const;

            void ClusteredIndexSeek(
                vector<Row> *selectedRows,
                const Indexing::Key* minimumValue,
                const Indexing::Key* maximumValue);

            void ClusteredIndexScan(vector<Row> *selectedRows, Expressions::Expression* expression = nullptr);

            void HeapScan(vector<Row> *selectedRows, const size_t &rowsToSelect)const;

            void SelectForJoin(vector<Row> &selectedRows, const vector<column_index_t>& selectedColumnIndices, const vector<Block> *conditions = nullptr, const size_t &count = -1);

            void HeapDelete(const Expressions::Expression* expression) const;

            void ClusteredIndexScanDelete(const Expressions::Expression* expression);

            void ClusteredIndexSeekDelete(const Expressions::Expression* expression);

            AdditionalDataTypes::ResultStatus HeapInsert(vector<extent_id_t> &allocatedExtents, extent_id_t &lastExtentIndex, Row *row, page_id_t* rowPageId, int* rowIndex);

            AdditionalDataTypes::ResultStatus ClusteredIndexInsert(Row *row, page_id_t* rowPageId, int* rowIndex);

            AdditionalDataTypes::ResultStatus NonClusteredIndexInsert(
                const StorageTypes::Row *row,
                const int& nonClusteredIndexId,
                const vector<column_index_t>& indexedColumns,
                const Indexing::BPlusTreeNonClusteredData& data);

            void HeapUpdate(const Expressions::Expression* expression, const vector<Field> &updates);

            void ClusteredIndexScanUpdate(Expressions::Expression* expression, const vector<Field> &updates);

            void ClusteredIndexSeekUpdate(
                Expressions::Expression* expression,
                const Indexing::Key* minimumValue,
                const Indexing::Key* maximumValue,
                const vector<Field> &updates);

            void Truncate();

            void UpdateIndexAllocationMapPageId(const page_id_t &indexAllocationMapPageId);

            [[nodiscard]] bool IsColumnNullable(const column_index_t &columnIndex) const;

            void AddColumn(Column *column);

            [[nodiscard]] const table_id_t &GetTableId() const;

            [[nodiscard]] TableType GetTableType() const;

            [[nodiscard]] row_size_t GetMaximumRowSize() const;

            [[nodiscard]] row_size_t ReduceMaximumRowSize() const;

            [[nodiscard]] key_size_t CalculateIndexKeySize() const;

//            void GetIndexedColumnKeys(vector<column_index_t> *vector) const;

//            void GetNonClusteredIndexedColumnKeys(vector<vector<column_index_t>> *vector) const;

            void SetClusteredIndexPageId(const page_id_t &indexPageId);

            [[nodiscard]] const page_id_t& GetClusteredIndexPageId() const;

            void SetNonClusteredIndexPageId(const page_id_t& indexPageId, const int& indexPosition);

            [[nodiscard]] const page_id_t& GetNonClusteredIndexPageId( const int& indexPosition) const;

            [[nodiscard]] const uint8_t& GetNonClusteredIndexId( const int& indexPosition) const;

            Indexing::BPlusTree* GetClusteredIndexedTree();

            Indexing::BPlusTree* GetNonClusteredIndexTree(const int& nonClusteredIndexId);

            [[nodiscard]] bool HasNonClusteredIndexes() const;

            [[nodiscard]] Database* GetDatabase() const;

            [[nodiscard]] vector<ColumnType> GetColumnTypeByTreeId(const uint8_t& treeId) const;

            int HandleRowOverflow(const Row *row)const;

            int HandleRowOverflow(Row *row, Column* column);

            void InsertLargeObjectToPage(Row *row);

            void HandleRowUpdate(Pages::Page *page, Row *row, const std::vector<Field> &updates, const HashSet<column_index_t>& updatedColumns, const bool &isHeap = true);

            void GetIdentityColumns();

            void UpdateMasterDatabase() const;
    };
}
