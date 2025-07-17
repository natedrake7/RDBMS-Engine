#pragma once
#include "Constants.h"
#include <string>
#include <vector>
#include "../AdditionalLibraries/AdditionalDataTypes/JoinField/JoinField.h"
#include "../Server/Server.h"
#include "B+Tree/BPlusTree.h"
#include "Column/Column.h"
#include "Logger/Logger.h"
#include "Table/Table.h"
#include "Pages/OverflowPage/OverflowPage.h"

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
  string systemFilename;

  Dictionary<int32_t, table_id_t> tableIdsDictionary;

  vector<StorageTypes::Table *> tables;

  Logging::Logger* logger;

protected:

    void PopulateFilenames(const std::string& dbName);

    static void MergeRows(StorageTypes::Row& row, const vector<StorageTypes::Row>& selectedRows, const vector<column_index_t>& selectedColumnIndices, const StorageTypes::Table *secondTable);

    void WriteHeaderToFile() const;

    static bool IsSystemPage(const page_id_t &pageId);


    bool AllocateNewExtent( Pages::PageFreeSpacePage **pageFreeSpacePage,
                            page_id_t *lowerLimit,
                            page_id_t *newPageId,
                            extent_id_t *newExtentId,
                            const table_id_t &tableId);

    [[nodiscard]] const StorageTypes::Table *GetTable(const table_id_t &tableId) const;

    void UpdateNonClusteredData(const StorageTypes::Table& table, Pages::Page* nextLeafPage, const page_id_t& nextLeafPageId) const;

public:
    explicit Database(const string &dbName, const bool& isServerInitialization = false);

    explicit Database(const std::string& dbName, const vector<Headers::sysTable>& tables);

    ~Database();

    std::vector<Logging::LogEntry> RecoverLogs()const;

    void InitializeLogger(const std::string& dbName);

    void LogCheckPoint(Logging::CheckPoint& checkPoint) const;

    [[nodiscard]] Constants::transaction_id_t StartLogTransaction()const;

    [[nodiscard]] Logging::CheckPoint LogRowInsert(
        StorageTypes::Row* row,
        const Constants::transaction_id_t& transactionId,
        const Constants::table_id_t& tableOrdinal)const;

    static string CreateDatabasePath(const std::string& dbName);

    [[nodiscard]] static Indexing::Key CreateKey(const vector<column_index_t>& indexedColumns, const StorageTypes::Row* row);

    [[nodiscard]] static Indexing::Key CreateKey(
        const vector<column_index_t>& indexedColumns,
        const StorageTypes::Row* row,
        const Headers::RowIdentifier& rowId);

    [[nodiscard]] static Pages::PageFreeSpacePage* GetAssociatedPfsPage(const string& filename, const page_id_t& pageId);

    static page_id_t GetGamAssociatedPage(const page_id_t &pageId);

    static page_id_t GetPfsAssociatedPage(const page_id_t &pageId);

    static page_id_t CalculateSystemPageOffset(const page_id_t &pageId);

    static Constants::byte GetObjectSizeToCategory(const row_size_t &size);

    StorageTypes::Table *CreateTable(
      const table_id_t &tableId,
      const int& ordinalPosition,
      const vector<StorageTypes::Column *> &columns,
      const Headers::Index *clusteredKeyIndexes = nullptr,
      const vector<Headers::Index> *nonClusteredIndexes = nullptr);

    void CreateTable(const Headers::TableHeader& masterDbHeader, const StorageTypes::TableHeader &tableHeader);

    void CreateTable(
      const Headers::sysTable& sysHeader,
      const StorageTypes::TableHeader &tableHeader,
      const Headers::Index& primaryKey,
      const int& ordinalPosition);

//    [[nodiscard]] StorageTypes::Table *OpenTable(const string& schemaName, const string &tableName) const;

    [[nodiscard]] StorageTypes::Table *OpenTable(const table_id_t& tableId) const;

    [[nodiscard]] StorageTypes::Table *OpenTableById(const table_id_t& tableId) const;

    void DeleteTable(const string& tableName);

    void DeleteDatabase() const;

    void TruncateTable(const table_id_t& tableId);

    Pages::OverflowPage *CreateOverflowPage(const table_id_t &tableId);

    Pages::Page *CreateDataPage(const table_id_t &tableId);

    Pages::LargeDataPage *CreateLargeDataPage(const table_id_t &tableId);

    [[nodiscard]] Pages::LargeDataPage *GetTableLastLargeDataPage(const table_id_t &tableId)const;

    [[nodiscard]] Pages::LargeDataPage *GetLargeDataPage(const page_id_t &pageId, const table_id_t &tableId)const;

    Pages::OverflowPage* GetLastOverflowPage(const table_id_t &tableId, const block_size_t& size);

    Pages::IndexPage *CreateIndexPage(const table_id_t &tableId, const page_id_t& treeId = 0);

    void SetPageMetaDataToPfs(const Pages::Page *page)const;

    [[nodiscard]] string GetFileName() const;

    [[nodiscard]] string GetSystemFilename() const;

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
                                                                , const int& nonClusteredIndexId = -1
                                                                , const bool& findPageDifferentFromCurrent = false);

    void GetIdentityColumns()const;

    void GetColumnsHeaders()const;

    void GetDefaultValues()const;

    void GetIndexes() const;

    void GetTableHeaders()const;

    static void JoinTables(vector<StorageTypes::Row>& selectedRows, StorageTypes::Table* firstTable, StorageTypes::Table*, const vector<column_index_t>& secondTableSelectedColumnIndices, const vector<JoinField>& conditions);

    static void JoinTables(vector<StorageTypes::Row>& firstTableRows, StorageTypes::Table* secondTable, const vector<column_index_t>& selectedColumnIndices, const vector<JoinField>& conditions);

    void UpdateMasterDatabase()const;
};

void CreateDatabase(const string &dbName);

void PrintRows(const vector<StorageTypes::Row> &rows);

void PrintRows(const vector<StorageTypes::Row*> &rows);

Database* UseSystemDatabase(const std::string& dbName, const vector<Headers::sysTable>& tables);

}; // namespace DatabaseEngine