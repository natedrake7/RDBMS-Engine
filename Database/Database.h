#pragma once
#include "Constants.h"
#include "../AdditionalLibraries/AdditionalDataTypes/Field/Field.h"
#include <string>
#include <vector>

#include "B+Tree/BPlusTree.h"
#include "Column/Column.h"

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
  struct TableFullHeader;
  class Block;
  class Column;
  class Row;
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
  header_literal_t databaseNameSize;
  string databaseName;
  table_id_t lastTableId;
  page_id_t lastPageFreeSpacePageId;
  page_id_t lastGamPageId;

  DatabaseHeader();
  DatabaseHeader(const string &databaseName,
                 const table_number_t &numberOfTables,
                 const page_id_t &lastPageFreeSpacePageId,
                 const page_id_t &lastGamPageId);
  DatabaseHeader(const DatabaseHeader &dbHeader);
  DatabaseHeader &operator=(const DatabaseHeader &dbHeader);
} DatabaseHeader;

class Database {
  DatabaseHeader header;
  string filename;
  string fileExtension;
  vector<StorageTypes::Table *> tables;

protected:
    void ValidateTableCreation(StorageTypes::Table *table) const;

    void WriteHeaderToFile() const;

    static bool IsSystemPage(const page_id_t &pageId);

    static Constants::byte GetObjectSizeToCategory(const row_size_t &size);

    bool AllocateNewExtent( Pages::PageFreeSpacePage **pageFreeSpacePage,
                            page_id_t *lowerLimit, 
                            page_id_t *newPageId,
                            extent_id_t *newExtentId, 
                            const table_id_t &tableId);

    [[nodiscard]] const StorageTypes::Table *GetTable(const table_id_t &tableId) const;

    void InsertRowToClusteredIndex( const table_id_t& tableId, 
                                    StorageTypes::Row *row, 
                                    page_id_t* rowPageId,
                                    int* rowIndex);

    void InsertRowToNonClusteredIndex(  const table_id_t& tableId,
                                        const StorageTypes::Row *row,
                                        const int& nonClusteredIndexId,
                                        const vector<column_index_t>& indexedColumns,
                                        const Indexing::BPlusTreeNonClusteredData& data);

    void InsertRowToHeapTable(  const StorageTypes::Table &table,
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


    static void UpdateNodeConnections(Indexing::Node*& node, const Indexing::NodeHeader& newNodeHeader);

    void UpdateTableIndexes(const table_id_t& tableId, Indexing::Node*& node, const int& nonClusteredIndexId) const;

    [[nodiscard]] static Pages::PageFreeSpacePage* GetAssociatedPfsPage(const page_id_t& pageId);

    [[nodiscard]] static Indexing::Key CreateKey(const vector<column_index_t>& indexedColumns, const StorageTypes::Row* row);

public:
    explicit Database(const string &dbName);

    ~Database();

    static page_id_t GetGamAssociatedPage(const page_id_t &pageId);

    static page_id_t GetPfsAssociatedPage(const page_id_t &pageId);

    static page_id_t CalculateSystemPageOffset(const page_id_t &pageId);

    StorageTypes::Table *CreateTable( const string &tableName, 
                                      const vector<StorageTypes::Column *> &columns, 
                                      const vector<column_index_t> *clusteredKeyIndexes = nullptr, 
                                      const vector<vector<column_index_t>> *nonClusteredIndexes = nullptr);

    void CreateTable(const StorageTypes::TableFullHeader &tableMetaData);

    [[nodiscard]] StorageTypes::Table *OpenTable(const string &tableName) const;

    void DeleteTable(const string& tableName);

    void DeleteDatabase() const;

    void InsertRowToPage(const table_id_t& tableId, vector<extent_id_t> &allocatedExtents, extent_id_t &lastExtentIndex, StorageTypes::Row *row);

    void UpdateTableRows(const table_id_t &tableId, const vector<StorageTypes::Block*> &updateBlocks, const vector<Field> *conditions);

    void DeleteTableRows(const table_id_t& tableId, const vector<Field>* conditions);

    void TruncateTable(const table_id_t& tableId);
  
    Pages::Page *CreateDataPage(const table_id_t &tableId);

    Pages::LargeDataPage *CreateLargeDataPage(const table_id_t &tableId);

    Pages::LargeDataPage *GetTableLastLargeDataPage(const table_id_t &tableId, const page_size_t &minObjectSize);

    Pages::LargeDataPage *GetLargeDataPage(const page_id_t &pageId, const table_id_t &tableId);

    Pages::IndexPage *CreateIndexPage(const table_id_t &tableId, const page_id_t& treeId = 0);

    static void SetPageMetaDataToPfs(const Pages::Page *page);

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

    static void UpdateNodeConnections(Indexing::Node*& node);
};

void CreateDatabase(const string &dbName);

void UseDatabase(const string &dbName, Database **db);

void PrintRows(const vector<StorageTypes::Row> &rows);

void PrintRows(const vector<StorageTypes::Row*> &rows);
}; // namespace DatabaseEngine