﻿#pragma once
#include <mutex>
#include <string>
#include <unordered_set>
#include <vector>
#include "../Constants.h"
#include "../Database.h"
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
    struct DataObject;
}

namespace ByteMaps
{
    class BitMap;
}

namespace DatabaseEngine::StorageTypes
{
    enum class ColumnType : uint8_t;
    class Block;
    class Column;
    struct ColumnHeader;

    typedef struct TableHeader
    {
        table_id_t tableId;

        header_literal_t tableNameSize;
        string tableName;

        row_size_t maxRowSize;
        page_id_t indexAllocationMapPageId;
        column_number_t numberOfColumns;

        page_id_t clusteredIndexPageId;
        vector<page_id_t> nonClusteredIndexPageIds;
        vector<uint8_t> nonClusteredIndexesIds;

        ByteMaps::BitMap *columnsNullBitMap;

        // bitmaps to store the composite key
        vector<column_index_t> clusteredColumnIndexes;
        vector<vector<column_index_t>> nonClusteredColumnIndexes;
        // ByteMaps::BitMap *clusteredIndexesBitMap;
        // vector<ByteMaps::BitMap*> nonClusteredIndexesBitMap;
        //

        TableHeader();
        ~TableHeader();
        TableHeader &operator=(const TableHeader &tableHeader);
    } TableHeader;

    typedef struct TableFullHeader
    {
        TableHeader tableHeader;
        vector<ColumnHeader> columnsHeaders;

        TableFullHeader();
        TableFullHeader(const TableFullHeader &tableHeader);
    } TableFullHeader;

    class Table final
    {
        TableHeader header;
        vector<Column *> columns;
        Database *database;
        const vector<void (*)(Block *&block, const Field &inputData)> setBlockDataByDataTypeArray = {&Table::SetTinyIntData, &Table::SetSmallIntData, &Table::SetIntData, &Table::SetBigIntData, &Table::SetDecimalData, &Table::SetStringData, &Table::SetUnicodeStringData, &Table::SetBoolData, &Table::SetDateTimeData};
        Indexing::BPlusTree* clusteredIndexedTree;
        vector<Indexing::BPlusTree*> nonClusteredIndexedTrees;
        mutex pageSelectMutex;

        protected:

            [[nodiscard]] unordered_set<column_index_t> GetClusteredIndexesMap() const;
            
            void InsertLargeObjectToPage(Row *row);
            [[nodiscard]] Pages::LargeDataPage *GetOrCreateLargeDataPage() const;
            
            static void LinkLargePageDataObjectChunks(Pages::DataObject *dataObject, const page_id_t &lastLargePageId, const large_page_index_t &objectIndex);
            void InsertLargeDataObjectPointerToRow(Row *row, const bool &isFirstRecursion, const large_page_index_t &objectIndex, const page_id_t &lastLargePageId, const column_index_t &largeBlockIndex) const;
            void RecursiveInsertToLargePage(Row *&row, page_offset_t &offset, const column_index_t &columnIndex, block_size_t &remainingBlockSize, const bool &isFirstRecursion, Pages::DataObject **previousDataObject);
            void InsertRow(const vector<Field> &inputData, vector<extent_id_t> &allocatedExtents, extent_id_t &startingExtentIndex);
            void SetTableIndexesToHeader(const vector<column_index_t> *clusteredKeyIndexes, const vector<vector<column_index_t>> *nonClusteredIndexes);
        
            static void SetTinyIntData(Block *&block, const Field &inputData);
            static void SetSmallIntData(Block *&block, const Field &inputData);
            static void SetIntData(Block *&block, const Field &inputData);
            static void SetBigIntData(Block *&block, const Field &inputData);
            static void SetStringData(Block *&block, const Field &inputData);
            static void SetUnicodeStringData(Block *&block, const Field &inputData);
            static void SetBoolData(Block *&block, const Field &inputData);
            static void SetDateTimeData(Block *&block, const Field &inputData);
            static void SetDecimalData(Block *&block, const Field &inputData);
            void CheckAndInsertNullValues(Block *&block, Row *&row, const column_index_t &associatedColumnIndex);

            static bool VectorContainsIndex(const vector<column_index_t>& vector, const column_index_t& index, int& indexPosition);
        
            void GetClusteredIndexFromDisk() const;
            void GetNonClusteredIndexFromDisk(const int& indexId) const;
            [[nodiscard]] Indexing::Node* GetIndexFromDisk(const page_id_t& indexPageId) const;
        
            void SelectRowsFromClusteredIndex(vector<Row> *selectedRows, const size_t &rowsToSelect, const Indexing::Key& minimumValue, const Indexing::Key& maximumValue, const bool indexSeek, const vector<column_index_t>& selectedColumnIndices);
            void SelectRowsFromNonClusteredIndex(vector<Row> *selectedRows, const size_t &rowsToSelect, const vector<Field> *conditions, const vector<column_index_t>& selectedColumnIndices);
            void SelectRowsFromHeap(vector<Row> *selectedRows, const size_t &rowsToSelect, const vector<Field> *conditions);
            void ThreadSelect(const Pages::IndexAllocationMapPage *tableMapPage, const extent_id_t &extentId, const size_t &rowsToSelect, const vector<Field> *conditions, vector<Row> *selectedRows);
            
            Row* CreateRow(const vector<Field>& inputData);

        public:
            Table(const string &tableName, const table_id_t &tableId, const vector<Column *> &columns, DatabaseEngine::Database *database, const vector<column_index_t> *clusteredKeyIndexes = nullptr, const vector<vector<column_index_t>> *nonClusteredIndexes = nullptr);

            Table(const TableHeader &tableHeader, Database *database);

            ~Table();

            void InsertRows(const vector<vector<Field>> &inputData);

            string &GetTableName();

            row_size_t &GetMaxRowSize();

            [[nodiscard]] column_number_t GetNumberOfColumns() const;

            [[nodiscard]] const TableHeader &GetTableHeader() const;

            [[nodiscard]] const vector<Column *> &GetColumns() const;

            [[nodiscard]] Pages::LargeDataPage *GetLargeDataPage(const page_id_t &pageId) const;

            [[nodiscard]] const vector<vector<column_index_t>>& GetNonClusteredIndexes() const;

            [[nodiscard]] const vector<column_index_t>& GetClusteredIndex() const;

            void Select(vector<Row> &selectedRows, const vector<column_index_t>& selectedColumnIndices, const vector<Field> *conditions = nullptr, const size_t &count = -1);

            void Update(const vector<Field> &updates, const vector<Field> *conditions = nullptr) const;

            void Delete(const vector<Field> *conditions = nullptr) const;

            void Truncate();

            void UpdateIndexAllocationMapPageId(const page_id_t &indexAllocationMapPageId);

            [[nodiscard]] bool IsColumnNullable(const column_index_t &columnIndex) const;

            void AddColumn(Column *column);

            [[nodiscard]] const table_id_t &GetTableId() const;

            [[nodiscard]] TableType GetTableType() const;

            [[nodiscard]] row_size_t GetMaximumRowSize() const;

            void GetIndexedColumnKeys(vector<column_index_t> *vector) const;

            void GetNonClusteredIndexedColumnKeys(vector<vector<column_index_t>> *vector) const;

            void SetClusteredIndexPageId(const page_id_t &indexPageId);

            [[nodiscard]] const page_id_t& GetClusteredIndexPageId() const;

            void SetNonClusteredIndexPageId(const page_id_t& indexPageId, const int& indexPosition);

            [[nodiscard]] const page_id_t& GetNonClusteredIndexPageId( const int& indexPosition) const;

            [[nodiscard]] const uint8_t& GetNonClusteredIndexId( const int& indexPosition) const;

            void SetIndexAllocationMapPageId(const page_id_t& pageId);

            Indexing::BPlusTree* GetClusteredIndexedTree();

            Indexing::BPlusTree* GetNonClusteredIndexTree(const int& nonClusteredIndexId);

            [[nodiscard]] bool HasNonClusteredIndexes() const;

            [[nodiscard]] Database* GetDatabase() const;

            [[nodiscard]] vector<ColumnType> GetColumnTypeByTreeId(const uint8_t& treeId) const;
    };
}
