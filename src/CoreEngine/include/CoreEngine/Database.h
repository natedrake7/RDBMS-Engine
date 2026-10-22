#pragma once
#include <CoreEngine/DatabaseConstants.h>
#include <vector>

#include <Systemic/DataStructures/PolymorphicArray.h>
#include  <CoreEngine/Memory/PersistentAllocator.h>
#include <CoreEngine/DataStorage/ExtentReservation.h>
#include <CoreEngine/Logger/Logger.h>
#include <CoreEngine/Pages/AllocationPageView.h>
#include <CoreEngine/Pages/GlobalAllocationPageView.h>
#include <CoreEngine/Pages/PageFreeSpaceView.h>
#include <CoreEngine/BufferPool/FileKey.h>

#include "Accessors/PageAccessor.h"
#include "Systemic/DataStructures/SortedDirectory.h"

namespace Memory{
    class IAllocator;
}

namespace Indexing {
    class BTree;
}

namespace CoreEngine::StorageTypes {
    class Table;
    class Column;
    struct TableHeader;
} // namespace DatabaseEngine::StorageTypes

namespace CoreEngine {
    struct SystemTable;

    namespace Directory
    {
        struct TableEntry;
        struct TableKey;
        struct SchemaEntry;
    }

    namespace Catalog
    {
        struct DatabaseHeader;
    }

    namespace Schemas
    {
        class TableSchema;
    }

    class ExecutionContext;
    struct ScanState;
    struct sysTable;

    struct DatabaseHeader {
        // UnsignedInt _formatVersion;       // StorageLayout::FILE_ORMAT_VERSION of the code that created the file
        page_id_t _lastPfsPageId;
        gam_index_t _lastGamIndex;

        DatabaseHeader();
        DatabaseHeader(
            page_id_t pfsPageId,
            gam_index_t gamIndex
        );

        // Throws when the file was written with another layout version
        void ValidateFormatVersion(const DataTypes::StringView& databaseName) const;
    };

class Database final{
    MultiThreading::Mutex gamPageMutex;
    MultiThreading::Mutex pfsPageMutex;

    Memory::PersistentAllocator _allocator;

    DataTypes::StringView _lowerName;

    MultiThreading::Mutex _directoryMutex;
    MultiThreading::Mutex _ddlMutex;
    DataStructures::SortedDirectory<std::string_view, Directory::SchemaEntry> _schemas;
    DataStructures::SortedDirectory<Directory::TableKey, Directory::TableEntry> _tables;

    DatabaseHeader header;

    Int id;
    UnsignedSmallInt _nextOrdinal;

    Accessors::PageAccessor _pageAccessor;

    static void PopulateFilenames(
        const ::Memory::IAllocator* tempAllocator,
        const DataTypes::StringView& dbName,
        DataTypes::String& outFile,
        DataTypes::String& outSysFile
    );

    void ApplyRecoveryLog(const Logging::LogEntry& logEntry)const;

    [[nodiscard]] static Int CalculateExtentsToAllocate(Int pagesToAllocate);

    Pages::GlobalAllocationPageView RollToNewGamPageNoLock();
    Pages::AllocationPageView FindOrRollToNewAllocationPage(
        const StorageTypes::Table* table,
        page_id_t currentAllocationPageId,
        gam_index_t gamIndex,
        page_id_t newAllocationPageId
    ) const;

public:
    Database(
        const ::Memory::IAllocator* allocator,
        Accessors::PageAccessor pageAccessor,
        Int databaseId,
        const DataTypes::StringView& dbName
    );

    ~Database();

    void Bootstrap(
        const ::Memory::IAllocator* allocator,
        const Catalog::DatabaseHeader& header
    );

    // The catalog database cannot read its own tables from the catalog: they come from SYSTEM_TABLES.
    // isNew: the files were just created, so every table starts with an empty physical header.
    void BootstrapSystem(
        const ::Memory::IAllocator* allocator,
        const DataTypes::StringView& name,
        bool isNew
    );

    void Destroy() const;

    static std::vector<Logging::LogEntry> RecoverLogs();

    void EnterRecoveryMode()const;

    static void LogCheckPoint(Logging::CheckPoint& checkPoint);

    [[nodiscard]] static Logging::CheckPoint LogRowInsert(
        const ExecutionContext& context,
        const StorageTypes::SerializedRow& payload,
        transaction_id_t transactionId,
        table_id_t tableOrdinal
    );

    [[nodiscard]] static Logging::CheckPoint LogRowBatchInsert(
      DataStructures::PolymorphicArray<char>& buffer,
      transaction_id_t transactionId,
      table_id_t tableOrdinal
    );

    [[nodiscard]]
    StorageTypes::Table* CreateTable(const Schemas::TableSchema* schema);

    [[nodiscard]]
    StorageTypes::Table* CreateTablePtr(
        const Schemas::TableSchema* schema,
        const StorageTypes::TableHeader& physicalHeader
    );

    [[nodiscard]] StorageTypes::Table *OpenTableByOrdinal(table_id_t tableId) const;

    void DeleteTable(const DataTypes::String& tableName);

    void DeleteDatabase() const;

    void TruncateTable(table_id_t tableId) const;

    void PublishSchema(
        StorageTypes::Table* table,
        const Schemas::TableSchema* newSchema
    );

    [[nodiscard]]
    StorageTypes::ExtentReservation ReserveExtents(
        const ::Memory::IAllocator* allocator,
        const StorageTypes::Table* table,
        Int requiredPages
    );

    template <typename TView>
    [[nodiscard]] TView LazyAllocateSinglePage(
        const ::Memory::IAllocator* allocator,
        const StorageTypes::Table* table
    );

    template<typename TVIew>
    [[nodiscard]]
    TVIew LazyAllocateTablePage(
        const ::Memory::IAllocator* allocator,
        const StorageTypes::Table* table
    );

    void UpdateSystemCatalog(const ::Memory::IAllocator* allocator)const;

    const DataStructures::SortedDirectory<Directory::TableKey, Directory::TableEntry>& GetTables() const{
        return this->_tables;
    }

    [[nodiscard]] UnsignedSmallInt ReserveOrdinalNoLock();

    [[nodiscard]] MultiThreading::Mutex* DDLMutex(){
        return &this->_ddlMutex;
    }

    [[nodiscard]] Accessors::PageAccessor GetPageAccessor() const{
        return this->_pageAccessor;
    }
};

Accessors::PageAccessor CreateDatabase(
    const ::Memory::IAllocator* allocator,
    const DataTypes::StringView& dbName
);

Accessors::PageAccessor UseDatabase(
    const ::Memory::IAllocator* allocator,
    const DataTypes::StringView& dbName
);
}; // namespace DatabaseEngine