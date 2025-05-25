#pragma once
#include "Constants.h"
#include "../AdditionalLibraries/AdditionalDataTypes/ErrorHandling.h"
#include "../AdditionalLibraries/AdditionalDataTypes/Field/Field.h"
#include <string>
#include <vector>
#include "../AdditionalLibraries/AdditionalDataTypes/JoinField/JoinField.h"
#include "../Server/Server.h"
#include "B+Tree/BPlusTree.h"
#include "Column/Column.h"
#include "Table/Table.h"

using namespace Constants;
using namespace std;

class RowCondition;

namespace Indexing {
  struct Key;
  struct Node;
  class BPlusTree;
  struct BPlusTreeNonClusteredData;
  struct NodeHeader;
} // namespace Indexing

namespace DatabaseEngine::StorageTypes {
  class Table;
  class Block;
  class Column;
  class Row;
  struct TableHeader;
} // namespace DatabaseEngine::StorageTypes

namespace Storage {
  class FileManager;
  class PageManager;
} // namespace Storage

namespace Pages {
class Page;
  class LargeDataPage;
  class PageFreeSpacePage;
  class IndexAllocationMapPage;
  class IndexPage;
} // namespace Pages

namespace DatabaseEngine {
enum { MAX_TABLE_SIZE = 10 * 1024 };

typedef struct DatabaseHeader {
  table_number_t numberOfTables;
  table_id_t lastTableId;
  page_id_t lastPageFreeSpacePageId;
  page_id_t lastGamPageId;

  DatabaseHeader();
  DatabaseHeader(const table_number_t &numberOfTables,
                 const page_id_t &lastPageFreeSpacePageId,
                 const page_id_t &lastGamPageId);
  DatabaseHeader(const DatabaseHeader &dbHeader);
  DatabaseHeader &operator=(const DatabaseHeader &dbHeader);
} DatabaseHeader;

class Database {
  DatabaseHeader header;
  std::string name;
  string filename;
  string fileExtension;
  vector<StorageTypes::Table *> tables;

protected:
    static void MergeRows(StorageTypes::Row& row, const vector<StorageTypes::Row>& selectedRows, const vector<column_index_t>& selectedColumnIndices, const StorageTypes::Table *secondTable);

    void WriteHeaderToFile() const;

    static bool IsSystemPage(const page_id_t &pageId);

    static Constants::byte GetObjectSizeToCategory(const row_size_t &size);

    bool AllocateNewExtent( Pages::PageFreeSpacePage **pageFreeSpacePage,
                            page_id_t *lowerLimit, 
                            page_id_t *newPageId,
                            extent_id_t *newExtentId, 
                            const table_id_t &tableId);

    [[nodiscard]] const StorageTypes::Table *GetTable(const table_id_t &tableId) const;

    AdditionalDataTypes::ResultStatus InsertRowToClusteredIndex(
        const table_id_t& tableId, 
        StorageTypes::Row *row, 
        page_id_t* rowPageId,
        int* rowIndex);

    AdditionalDataTypes::ResultStatus InsertRowToNonClusteredIndex(
        const table_id_t& tableId,
        const StorageTypes::Row *row,
        const int& nonClusteredIndexId,
        const vector<column_index_t>& indexedColumns,
        const Indexing::BPlusTreeNonClusteredData& data);

    void InsertRowToHeapTable(
        const StorageTypes::Table &table,
        vector<extent_id_t> &allocatedExtents,
        extent_id_t &lastExtentIndex,
        StorageTypes::Row *row,
        page_id_t* rowPageId,
        int* rowIndex);

    void UpdateNonClusteredData(const StorageTypes::Table& table, Pages::Page* nextLeafPage, const page_id_t& nextLeafPageId) const;

    static void InsertRowToPage( Pages::PageFreeSpacePage *pageFreeSpacePage,
                                 Pages::Page *page, StorageTypes::Row *row,
                                 const int &indexPosition);

    void InsertRowToNonEmptyNode( Indexing::Node *node,
                                  const StorageTypes::Table &table,
                                  StorageTypes::Row *row, 
                                  const Indexing::Key &key,
                                  const int &indexPosition);



    void UpdateTableIndexes(const table_id_t& tableId, Indexing::Node*& node, const int& nonClusteredIndexId) const;

    [[nodiscard]] Pages::PageFreeSpacePage* GetAssociatedPfsPage(const page_id_t& pageId)const;


public:
    explicit Database(const string &dbName, const bool& isServerInitialization = false);

    explicit Database(const std::string& dbName, const vector<Headers::sysTable>& tables);

    ~Database();

    [[nodiscard]] static Indexing::Key CreateKey(const vector<column_index_t>& indexedColumns, const StorageTypes::Row* row);

    static page_id_t GetGamAssociatedPage(const page_id_t &pageId);

    static page_id_t GetPfsAssociatedPage(const page_id_t &pageId);

    static page_id_t CalculateSystemPageOffset(const page_id_t &pageId);

    StorageTypes::Table *CreateTable(
      const string &tableName,
      const string &schemaName,
      const table_id_t &tableId,
      const vector<StorageTypes::Column *> &columns,
      const vector<column_index_t> *clusteredKeyIndexes = nullptr,
      const vector<vector<column_index_t>> *nonClusteredIndexes = nullptr);

    void CreateTable(const Headers::TableHeader& masterDbHeader, const StorageTypes::TableHeader &tableHeader);

    void CreateTable(const Headers::sysTable& sysHeader, const StorageTypes::TableHeader &tableHeader);

    [[nodiscard]] StorageTypes::Table *OpenTable(const string& schemaName, const string &tableName) const;

    [[nodiscard]] StorageTypes::Table *OpenTable(const table_id_t& tableId) const;

    void DeleteTable(const string& tableName);

    void DeleteDatabase() const;

    AdditionalDataTypes::ResultStatus InsertRowToPage(const table_id_t& tableId, vector<extent_id_t> &allocatedExtents, extent_id_t &lastExtentIndex, StorageTypes::Row *row);

    void UpdateTableRows(const table_id_t &tableId, const vector<StorageTypes::Block*> &updateBlocks, const vector<Field> *conditions);

    void DeleteTableRows(const table_id_t& tableId, const QueryPipeline::Statements::Expression* expression)const;

    void TruncateTable(const table_id_t& tableId);

    Pages::Page *CreateDataPage(const table_id_t &tableId);

    Pages::LargeDataPage *CreateLargeDataPage(const table_id_t &tableId);

    Pages::LargeDataPage *GetTableLastLargeDataPage(const table_id_t &tableId, const page_size_t &minObjectSize);

    Pages::LargeDataPage *GetLargeDataPage(const page_id_t &pageId, const table_id_t &tableId);

    Pages::IndexPage *CreateIndexPage(const table_id_t &tableId, const page_id_t& treeId = 0);

    void SetPageMetaDataToPfs(const Pages::Page *page)const;

    [[nodiscard]] string GetFileName() const;

    static page_id_t CalculateSystemPageOffsetByExtentId(const extent_id_t &extentId);

    static extent_id_t CalculateExtentIdByPageId(const page_id_t &pageId);

    [[nodiscard]] Pages::Page *FindOrAllocateNextDataPage(  Pages::PageFreeSpacePage *&pageFreeSpacePage,
                                                            const page_id_t &pageId,
                                                            const page_id_t &extentFirstPageId,
                                                            const extent_id_t &extentId,
                                                            const StorageTypes::Table &table,
                                                            extent_id_t *nextExtentId);

    [[nodiscard]] Pages::IndexPage* FindOrAllocateNextIndexPage(  const table_id_t& tableId
                                                                , const page_id_t &indexPageId
                                                                , const int& nodeSize
                                                                , const int& nonClusteredIndexId = -1
                                                                , const bool& findPageDifferentFromCurrent = false);

    void SplitPage( Indexing::Node*& firstNode,
                Indexing::Node*& secondNode,
                const int &branchingFactor,
                const table_id_t& tableId);

    void SplitNodeFromIndexPage(const table_id_t& tableId, Indexing::Node*& node, const int& nonClusteredIndexId = -1);

    void UpdateNodeConnections(Indexing::Node*& node);

    void UpdateNodeConnections(Indexing::Node*& node, const Indexing::NodeHeader& newNodeHeader);

    void UpdateNodeConnectionsOnDelete(Indexing::Node*& node, Indexing::Node* deletedNode, const Indexing::NodeHeader& newNodeHeader);

    static void JoinTables(vector<StorageTypes::Row>& selectedRows, StorageTypes::Table* firstTable, StorageTypes::Table*, const vector<column_index_t>& secondTableSelectedColumnIndices, const vector<JoinField>& conditions);

    static void JoinTables(vector<StorageTypes::Row>& firstTableRows, StorageTypes::Table* secondTable, const vector<column_index_t>& selectedColumnIndices, const vector<JoinField>& conditions);
  };

void CreateDatabase(const string &dbName);

void PrintRows(const vector<StorageTypes::Row> &rows);

void PrintRows(const vector<StorageTypes::Row*> &rows);
}; // namespace DatabaseEngine