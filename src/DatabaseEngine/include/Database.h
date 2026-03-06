#pragma once
#include "DatabaseConstants.h"
#include <vector>

#include  "Memory/PersistentAllocator.h"
#include "BufferPool/FileManager.h"
#include "DataStorage/Column.h"
#include "DataStructures/PolymorphicArray.h"
#include "Logger/Logger.h"
#include "Pages/IndexPageView.h"
#include "Pages/LargeObjectView.h"
#include "Pages/OverflowPageView.h"
#include "Pages/PageFreeSpaceView.h"


namespace Memory{
    class IAllocator;
}

namespace Indexing {
    class BTree;
}

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

class Database final{
    MultiThreading::ReadWriteMutex gamPageMutex;
    MultiThreading::ReadWriteMutex pfsPageMutex;

    // Dictionary<Int, table_id_t> tableIdsDictionary;

    Memory::PersistentAllocator _allocator;

    DataTypes::String name;
    DataTypes::String filename;
    DataTypes::String systemFilename;
    DataTypes::StringView fileExtension;

    DataStructures::PolymorphicArray<StorageTypes::Table*> _tables;

    DatabaseHeader header;

    DataTypes::StringView filenameView;
    DataTypes::StringView systemFilenameView;
    Storage::FileKey dataFileKey;
    Storage::FileKey systemFileKey;

    Int id;

    void PopulateFilenames(const ::Memory::IAllocator* tempAllocator, const DataTypes::String& dbName);

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

    void InitializeStaticData();

public:
    Database(
        const DataTypes::String& dbName,
        const bool& isServerInitialization = false
    );

    Database(
        const DataTypes::String& dbName,
        const std::vector<Headers::sysTable>& tables
    );

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

    [[nodiscard]] static Pages::PageFreeSpaceView GetAssociatedPfsPage(
        Storage::FileKey sysFileKey,
        const DataTypes::StringView& filenameView,
        page_id_t pageId
    );

    static page_id_t GetGamAssociatedPage(page_id_t pageId);
    static page_id_t GetPfsAssociatedPage(page_id_t pageId);
    static page_id_t CalculateSystemPageOffset(page_id_t pageId);
    static page_id_t CalculateNextGamPageId(page_id_t currentGamPageId);
    static byte_t GetObjectSizeToCategory(const row_size_t &size);

    StorageTypes::Table* CreateTable(
        table_id_t tableId,
        Int ordinalPosition
    );

    // StorageTypes::Table *CreateTable(
    //     table_id_t tableId,
    //     Int ordinalPosition,
    //     const std::vector<StorageTypes::Column *> &columns,
    //     const Headers::Index *clusteredKeyIndexes = nullptr,
    //     const std::vector<Headers::Index> *nonClusteredIndexes = nullptr
    // );

    void CreateTable(
        const Headers::TableHeader& masterDbHeader,
        const StorageTypes::TableHeader &tableHeader
    );

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

    void DeleteTable(const DataTypes::String& tableName);

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

    [[nodiscard]] DataTypes::StringView GetFileName() const;
    [[nodiscard]] DataTypes::StringView GetSystemFilename() const;
    [[nodiscard]] Storage::FileKey GetDataFileKey() const;
    [[nodiscard]] Storage::FileKey GetSystemFileKey() const;

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

    void UpdateMasterDatabase(const ::Memory::IAllocator* allocator)const;

    const DataStructures::Array<StorageTypes::Table*>& GetTables() const;
};

void CreateDatabase(Int databaseId, const DataTypes::String& dbName);
}; // namespace DatabaseEngine