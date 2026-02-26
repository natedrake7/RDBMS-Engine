#pragma once
#include "DatabaseConstants.h"
#include <string>
#include <vector>
#include "DataStorage/Column.h"
#include "Logger/Logger.h"
#include "Pages/IndexPageView.h"
#include "Pages/LargeObjectView.h"
#include "Pages/OverflowPageView.h"
#include "Pages/PageFreeSpaceView.h"

namespace Indexing {
    class BTree;
} // namespace Indexing

namespace DatabaseEngine::StorageTypes {
    class Table;
    class Column;
    struct TableHeader;
} // namespace DatabaseEngine::StorageTypes

namespace DatabaseEngine {
    class ExecutionContext;
    struct ScanState;

    struct DatabaseHeader {
        table_number_t numberOfTables;
        table_id_t lastTableId;
        page_id_t lastPageFreeSpacePageId;
        page_id_t lastGamPageId;

        DatabaseHeader();
        DatabaseHeader(
            table_number_t numberOfTables,
            page_id_t lastPageFreeSpacePageId,
            page_id_t lastGamPageId
        );
        DatabaseHeader(const DatabaseHeader &dbHeader);
        DatabaseHeader &operator=(const DatabaseHeader &dbHeader);
    };

class Database {
    DatabaseHeader header;
    std::string name;
    std::string filename;
    std::string fileExtension;
    std::string systemFilename;

    Dictionary<Int, table_id_t> tableIdsDictionary;

    std::vector<StorageTypes::Table *> tables;

    MultiThreading::ReadWriteMutex gamPageMutex;
    MultiThreading::ReadWriteMutex pfsPageMutex;

protected:

    void PopulateFilenames(const std::string& dbName);

    void WriteHeaderToFile() const;

    static bool IsSystemPage(page_id_t pageId);


    std::vector<extent_id_t> AllocateNewExtents(
      Int pagesToAllocate,
      table_id_t tableId,
      page_id_t& lowerLimit
    );

    [[nodiscard]] const StorageTypes::Table *GetTable(table_id_t tableId) const;

    // [[nodiscard]] bool ValidateLogIntegrity(const Logging::LogEntry& logEntry) const;

    void ApplyRecoveryLog(const Logging::LogEntry& logEntry)const;

    static int CalculateExtentsToAllocate(Int pagesToAllocate);

public:
    explicit Database(const std::string &dbName, const bool& isServerInitialization = false);

    explicit Database(const std::string& dbName, const std::vector<Headers::sysTable>& tables);

    ~Database();

    static std::vector<Logging::LogEntry> RecoverLogs();

    void EnterRecoveryMode()const;

    static void LogCheckPoint(Logging::CheckPoint& checkPoint);

    [[nodiscard]] static Logging::CheckPoint LogRowInsert(
        const StorageTypes::InsertPayload& payload,
        transaction_id_t transactionId,
        table_id_t tableOrdinal
    );

    [[nodiscard]] static Logging::CheckPoint LogRowBatchInsert(
      std::vector<char>& buffer,
      transaction_id_t transactionId,
      table_id_t tableOrdinal
    );

    static std::string CreateDatabasePath(const std::string& dbName);

    [[nodiscard]] static DataTypes::Indexing::Key CreateKey(
      const std::vector<column_index_t>& indexedColumns,
      const StorageTypes::InsertPayload& payload
    );

    [[nodiscard]] static DataTypes::Indexing::Key CreateKey(
        const ExecutionContext& context,
        const std::vector<column_index_t>& indexedColumns,
        const Pages::RowReference& rowPtr,
        Int offSet
    );

    [[nodiscard]] static DataTypes::Indexing::Key CreateKey(
        const ExecutionContext& context,
        const std::vector<column_index_t>& indexedColumns,
        const Pages::RowReference& rowPtr,
        const DataTypes::RowIdentifier& rowId
    );

    [[nodiscard]] static Pages::PageFreeSpaceView GetAssociatedPfsPage(const std::string& filename, page_id_t pageId);

    static page_id_t GetGamAssociatedPage(page_id_t pageId);

    static page_id_t GetPfsAssociatedPage(page_id_t pageId);

    static page_id_t CalculateSystemPageOffset(page_id_t pageId);

    static page_id_t CalculateNextGamPageId(page_id_t currentGamPageId);

    static byte_t GetObjectSizeToCategory(const row_size_t &size);

    StorageTypes::Table *CreateTable(
      table_id_t tableId,
      Int ordinalPosition,
      const std::vector<StorageTypes::Column *> &columns,
      const Headers::Index *clusteredKeyIndexes = nullptr,
      const std::vector<Headers::Index> *nonClusteredIndexes = nullptr);

    void CreateTable(const Headers::TableHeader& masterDbHeader, const StorageTypes::TableHeader &tableHeader);

    void CreateTable(
      const Headers::sysTable& sysHeader,
      const StorageTypes::TableHeader &tableHeader,
      const Headers::Index& primaryKey,
      Int ordinalPosition
    );

    static void InferSchemaFromColumns(const std::vector<StorageTypes::Column*>& columns);

//    [[nodiscard]] StorageTypes::Table *OpenTable(const string& schemaName, const string &tableName) const;

    [[nodiscard]] StorageTypes::Table *OpenTable(table_id_t tableId) const;

    // [[nodiscard]] StorageTypes::Table *OpenTableById(table_id_t tableId) const;

    void DeleteTable(const std::string& tableName);

    void DeleteDatabase() const;

    void TruncateTable(table_id_t tableId) const;

    Pages::OverflowPageView CreateOverflowPage(Int pagesToAllocate, table_id_t tableOrdinalPosition);

    Pages::PageView CreateDataPage(table_id_t tableId, Int pagesToAllocate);

    Pages::LargeObjectView CreateLargeDataPage(Int pagesToAllocate, table_id_t tableOrdinalPosition);

    [[nodiscard]] Pages::LargeObjectView GetTableLastLargeDataPage(table_id_t tableId)const;

    [[nodiscard]] Pages::LargeObjectView GetLargeDataPage(page_id_t pageId, table_id_t tableId)const;

    Pages::OverflowPageView GetLastOverflowPage(table_id_t tableId, const block_size_t& size);

    Pages::IndexPageView CreateIndexPage(
      table_id_t tableOrdinalPosition,
      Int pageCount,
      TreeType treeType,
      page_id_t treeId = 0
    );

    [[nodiscard]] std::string GetFileName() const;

    [[nodiscard]] std::string GetSystemFilename() const;

    static page_id_t CalculateExtentFirstPageId(const extent_id_t &extentId);

    static page_id_t CalculateGamPageId(const extent_id_t &extentId);

    static extent_id_t CalculateExtentId(page_id_t pageId);

    [[nodiscard]] Pages::PageView FindOrAllocateNextDataPage(
      Pages::PageFreeSpaceView &pageFreeSpacePage,
      page_id_t pageId,
      page_id_t extentFirstPageId,
      const StorageTypes::Table &table,
      Int pageToAllocate
    );

    [[nodiscard]] Pages::IndexPageView FindOrAllocateNextIndexPage(
      StorageTypes::Table*& table,
      page_id_t indexPageId,
      Int pagesToAllocate,
      Int nonClusteredIndexId = -1
    );

    void GetIdentityColumns()const;

    void UpdateIdentityManagersIds()const;

    void GetColumnsHeaders()const;

    void GetDefaultValues()const;

    void GetIndexes() const;

    void GetTableHeaders()const;

    void UpdateMasterDatabase(const Memory::Allocator& allocator)const;

    const std::vector<StorageTypes::Table*>& GetTables() const;
};

void CreateDatabase(const std::string &dbName);

Database* UseSystemDatabase(const std::string& dbName, const std::vector<Headers::sysTable>& tables);

}; // namespace DatabaseEngine