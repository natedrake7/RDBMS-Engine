#pragma once
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
        ByteMaps::BitMap *columnsNullBitMap;

        // bitmaps to store the composite key
        ByteMaps::BitMap *clusteredIndexesBitMap;
        ByteMaps::BitMap *nonClusteredIndexesBitMap;

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
        const vector<void (*)(Block *&block, const Field &inputData)> setBlockDataByDataTypeArray = {&Table::SetTinyIntData, &Table::SetSmallIntData, &Table::SetIntData, &Table::SetBigIntData, &Table::SetDecimalData, &Table::SetStringData, &Table::SetBoolData, &Table::SetDateTimeData};
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
            void SetTableIndexesToHeader(const vector<column_index_t> *clusteredKeyIndexes, const vector<column_index_t> *nonClusteredIndexes);
            
            static void SetTinyIntData(Block *&block, const Field &inputData);
            static void SetSmallIntData(Block *&block, const Field &inputData);
            static void SetIntData(Block *&block, const Field &inputData);
            static void SetBigIntData(Block *&block, const Field &inputData);
            static void SetStringData(Block *&block, const Field &inputData);
            static void SetBoolData(Block *&block, const Field &inputData);
            static void SetDateTimeData(Block *&block, const Field &inputData);
            static void SetDecimalData(Block *&block, const Field &inputData);
            void CheckAndInsertNullValues(Block *&block, Row *&row, const column_index_t &associatedColumnIndex);
        
            void WriteIndexesToDisk();
            void WriteNodeToPage(Indexing::Node* node, Pages::IndexPage*& indexPage, page_offset_t &offSet);
            void GetClusteredIndexFromDisk();
            Indexing::Node* GetNodeFromDisk(Pages::IndexPage*& indexPage, int& currentNodeIndex, page_offset_t& offSet, Indexing::Node*& prevLeafNode);
        
            void SelectRowsFromClusteredIndex(vector<Row> *selectedRows, const size_t &rowsToSelect, const vector<Field> *conditions);
            void SelectRowsFromHeap(vector<Row> *selectedRows, const size_t &rowsToSelect, const vector<Field> *conditions);
            void ThreadSelect(const Pages::IndexAllocationMapPage *tableMapPage, const extent_id_t &extentId, const size_t &rowsToSelect, const vector<Field> *conditions, vector<Row> *selectedRows);
            
            void InsertRowToPage(vector<extent_id_t> &allocatedExtents, extent_id_t &lastExtentIndex, Row *row);
            void InsertRowToClusteredIndex(Row *row);
            void SplitPage(vector<pair<Indexing::Node *, Indexing::Node *>> &splitLeaves);

            Row* CreateRow(const vector<Field>& inputData);

        public:
            Table(const string &tableName, const table_id_t &tableId, const vector<Column *> &columns, DatabaseEngine::Database *database, const vector<column_index_t> *clusteredKeyIndexes = nullptr, const vector<column_index_t> *nonClusteredIndexes = nullptr);

            Table(const TableHeader &tableHeader, Database *database);

            ~Table();

            void InsertRows(const vector<vector<Field>> &inputData);

            string &GetTableName();

            row_size_t &GetMaxRowSize();

            [[nodiscard]] column_number_t GetNumberOfColumns() const;

            [[nodiscard]] const TableHeader &GetTableHeader() const;

            [[nodiscard]] const vector<Column *> &GetColumns() const;

            [[nodiscard]] Pages::LargeDataPage *GetLargeDataPage(const page_id_t &pageId) const;

            void Select(vector<Row> &selectedRows, const vector<Field> *conditions = nullptr, const size_t &count = -1);

            void Update(const vector<Field> &updates, const vector<Field> *conditions = nullptr) const;

            void Delete(const vector<Field> *conditions = nullptr) const;

            void UpdateIndexAllocationMapPageId(const page_id_t &indexAllocationMapPageId);

            [[nodiscard]] bool IsColumnNullable(const column_index_t &columnIndex) const;

            void AddColumn(Column *column);

            [[nodiscard]] const table_id_t &GetTableId() const;

            void InsertRow(const vector<Field> &inputData);

            [[nodiscard]] TableType GetTableType() const;

            [[nodiscard]] row_size_t GetMaximumRowSize() const;

            void GetIndexedColumnKeys(vector<column_index_t> *vector) const;

            void SetIndexPageId(const page_id_t &indexPageId);

            Indexing::BPlusTree* GetClusteredIndexedTree();
    };
}
