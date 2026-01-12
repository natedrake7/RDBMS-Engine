#pragma once
#include "DatabaseConstants.h"
#include <string>
#include <vector>
#include "DataStorage/Column.h"
#include "Logger/Logger.h"
#include "Pages/IndexPage.h"
#include "DataStorage/Table.h"
#include "Pages/OverflowPage.h"
#include "Pages/PageGuard.h"

namespace Indexing {
  struct Node;
  class BTree;
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

namespace Pages {
  class Page;
  class LargeObjectPage;
  class PageFreeSpacePage;
  class IndexAllocationMapPage;
  class IndexPage;
} // namespace Pages

namespace DatabaseEngine {
struct DatabaseHeader {
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
};

class Database {
  DatabaseHeader header;
  std::string name;
  std::string filename;
  std::string fileExtension;
  std::string systemFilename;

  Dictionary<int32_t, table_id_t> tableIdsDictionary;

  vector<StorageTypes::Table *> tables;

  MultiThreading::ReadWriteMutex gamPageMutex;
  MultiThreading::ReadWriteMutex pfsPageMutex;

protected:

    void PopulateFilenames(const std::string& dbName);

    void WriteHeaderToFile() const;

    static bool IsSystemPage(const page_id_t &pageId);


    std::vector<extent_id_t> AllocateNewExtents(
      const int& pagesToAllocate,
      const table_id_t &tableId,
      page_id_t& lowerLimit
    );

    [[nodiscard]] const StorageTypes::Table *GetTable(const table_id_t &tableId) const;

    // [[nodiscard]] bool ValidateLogIntegrity(const Logging::LogEntry& logEntry) const;

    void ApplyRecoveryLog(const Logging::LogEntry& logEntry)const;

    static int CalculateExtentsToAllocate(const int& pagesToAllocate);

public:
    explicit Database(const string &dbName, const bool& isServerInitialization = false);

    explicit Database(const std::string& dbName, const vector<Headers::sysTable>& tables);

    ~Database();

    static std::vector<Logging::LogEntry> RecoverLogs();

    void EnterRecoveryMode()const;

    static void LogCheckPoint(Logging::CheckPoint& checkPoint);

    [[nodiscard]] static Logging::CheckPoint LogRowInsert(
        Pointer<StorageTypes::Row>& row,
        const transaction_id_t& transactionId,
        const table_id_t& tableOrdinal
    );

    static string CreateDatabasePath(const std::string& dbName);

    [[nodiscard]] static DataTypes::Indexing::Key CreateKey(
      const vector<column_index_t>& indexedColumns,
      const Pointer<StorageTypes::Row>& row
    );

    [[nodiscard]] static DataTypes::Indexing::Key CreateKey(
      const vector<column_index_t>& indexedColumns,
      const Pointer<StorageTypes::Row>& row,
      const Int& offSet
    );

    [[nodiscard]] static DataTypes::Indexing::Key CreateKey(
        const vector<column_index_t>& indexedColumns,
        const Pointer<StorageTypes::Row>& row,
        const Headers::RowIdentifier& rowId);

    [[nodiscard]] static Pages::PageGuard<Pages::PageFreeSpacePage> GetAssociatedPfsPage(const string& filename, const page_id_t& pageId);

    static page_id_t GetGamAssociatedPage(const page_id_t &pageId);

    static page_id_t GetPfsAssociatedPage(const page_id_t &pageId);

    static page_id_t CalculateSystemPageOffset(const page_id_t &pageId);

    static page_id_t CalculateNextGamPageId(const page_id_t &currentGamPageId);

    static byte_t GetObjectSizeToCategory(const row_size_t &size);

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
      const int& ordinalPosition
    );

    static void InferSchemaFromColumns(const std::vector<StorageTypes::Column*>& columns);

//    [[nodiscard]] StorageTypes::Table *OpenTable(const string& schemaName, const string &tableName) const;

    [[nodiscard]] StorageTypes::Table *OpenTable(const table_id_t& tableId) const;

    // [[nodiscard]] StorageTypes::Table *OpenTableById(const table_id_t& tableId) const;

    void DeleteTable(const string& tableName);

    void DeleteDatabase() const;

    void TruncateTable(const table_id_t& tableId);

    Pages::PageGuard<Pages::OverflowPage> CreateOverflowPage(const int& pagesToAllocate, const table_id_t &tableOrdinalPosition);

    Pages::PageGuard<Pages::Page> CreateDataPage(const table_id_t &tableId, const int& pagesToAllocate);

    Pages::PageGuard<Pages::LargeObjectPage> CreateLargeDataPage(const int& pagesToAllocate, const table_id_t &tableOrdinalPosition);

    [[nodiscard]] Pages::PageGuard<Pages::LargeObjectPage> GetTableLastLargeDataPage(const table_id_t &tableId)const;

    [[nodiscard]] Pages::PageGuard<Pages::LargeObjectPage> GetLargeDataPage(const page_id_t &pageId, const table_id_t &tableId)const;

    Pages::PageGuard<Pages::OverflowPage> GetLastOverflowPage(const table_id_t &tableId, const block_size_t& size);

    Pages::PageGuard<Pages::IndexPage> CreateIndexPage(
      const table_id_t &tableOrdinalPosition,
      const int& pageCount,
      const TreeType& treeType,
      const page_id_t& treeId = 0
    );

    [[nodiscard]] string GetFileName() const;

    [[nodiscard]] string GetSystemFilename() const;

    static page_id_t CalculateExtentFirstPageId(const extent_id_t &extentId);

    static page_id_t CalculateGamPageId(const extent_id_t &extentId);

    static extent_id_t CalculateExtentId(const page_id_t &pageId);

    [[nodiscard]] Pages::PageGuard<Pages::Page> FindOrAllocateNextDataPage(
      Pages::PageGuard<Pages::PageFreeSpacePage> &pageFreeSpacePage,
      const page_id_t &pageId,
      const page_id_t &extentFirstPageId,
      const StorageTypes::Table &table,
      const int& pageToAllocate
    );

    [[nodiscard]] Pages::PageGuard<Pages::IndexPage> FindOrAllocateNextIndexPage(
      StorageTypes::Table*& table,
      const page_id_t &indexPageId,
      const int& pagesToAllocate,
      const int& nonClusteredIndexId = -1
    );

    void GetIdentityColumns()const;

    void UpdateIdentityManagersIds()const;

    void GetColumnsHeaders()const;

    void GetDefaultValues()const;

    void GetIndexes() const;

    void GetTableHeaders()const;

    void UpdateMasterDatabase()const;

    const std::vector<StorageTypes::Table*>& GetTables() const;
};

void CreateDatabase(const string &dbName);

void PrintRows(const vector<StorageTypes::Row> &rows);

void PrintRows(const vector<StorageTypes::Row*> &rows);

Database* UseSystemDatabase(const std::string& dbName, const vector<Headers::sysTable>& tables);

}; // namespace DatabaseEngine