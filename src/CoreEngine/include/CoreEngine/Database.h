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
    namespace Catalog
    {
        struct TableHeader;
    }

    namespace Schemas
    {
        class TableSchema;
    }

    class ExecutionContext;
    struct ScanState;
    struct sysTable;

    struct DatabaseHeader {
        table_number_t numberOfTables;
        table_id_t lastTableId;
        page_id_t lastPfsPageId;
        page_id_t lastGamPageId;

        DatabaseHeader();
        DatabaseHeader(
            table_number_t numberOfTables,
            page_id_t lastPfsPageId,
            page_id_t lastGamPageId
        );
    };

class Database final{
    MultiThreading::Mutex gamPageMutex;
    MultiThreading::Mutex pfsPageMutex;

    Memory::PersistentAllocator _allocator;

    DataTypes::String name;

    DataStructures::PolymorphicArray<StorageTypes::Table*> _tables;

    DatabaseHeader header;

    Storage::FileKey dataFileKey;
    Storage::FileKey systemFileKey;

    Int id;

    static void PopulateFilenames(
        const ::Memory::IAllocator* tempAllocator,
        const DataTypes::StringView& dbName,
        DataTypes::String& outFile,
        DataTypes::String& outSysFile
    );
    void CreateKeys();

    void WriteHeaderToFile() const;

    [[nodiscard]] const StorageTypes::Table *GetTable(table_id_t tableId) const;

    void ApplyRecoveryLog(const Logging::LogEntry& logEntry)const;

    [[nodiscard]] static Int CalculateExtentsToAllocate(Int pagesToAllocate);

    void InitializeStaticData();

    Pages::GlobalAllocationPageView RollToNewGamPageNoLock();
    Pages::AllocationPageView FindOrRollToNewAllocationPage(
        StorageTypes::Table* tablePtr,
        page_id_t currentAllocationPageId,
        page_id_t gamPageId,
        page_id_t newAllocationPageId
    ) const;

public:
    Database(
        const ::Memory::IAllocator* allocator,
        Int databaseId,
        const DataTypes::StringView& dbName
    );

    void Bootstrap(const ::Memory::IAllocator* allocator);

    void Destroy();

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

    [[nodiscard]] static Pages::PageFreeSpaceView GetAssociatedPfsPage(
        Storage::FileKey sysFileKey,
        page_id_t pageId
    );

    static page_id_t GetGamAssociatedPage(page_id_t pageId);
    static page_id_t GetPfsAssociatedPage(page_id_t pageId);
    static page_id_t CalculateNextGamPageId(page_id_t currentGamPageId);
    static byte_t GetObjectSizeToCategory(const row_size_t &size);

    [[nodiscard]]
    StorageTypes::Table* CreateTable(const Schemas::TableSchema* schema);

    [[nodiscard]]
    StorageTypes::Table* AttachTable(
        const Schemas::TableSchema* schema,
        const StorageTypes::TableHeader& physicalHeader
    );

    [[nodiscard]] StorageTypes::Table *OpenTable(table_id_t tableId) const;

    void DeleteTable(const DataTypes::String& tableName);

    void DeleteDatabase() const;

    void TruncateTable(table_id_t tableId) const;

    [[nodiscard]]
    StorageTypes::ExtentReservation ReserveExtents(
        const ::Memory::IAllocator* allocator,
        Int requiredPages,
        Int tableOrdinalPos
    );

    template <typename TView>
    [[nodiscard]] TView LazyAllocateSinglePage(
        const ::Memory::IAllocator* allocator,
        table_id_t ordinalPos
    );

    template<typename TVIew>
    [[nodiscard]]
    TVIew LazyAllocateTablePage(
        const ::Memory::IAllocator* allocator,
        table_id_t ordinalPos
    );

    [[nodiscard]] Storage::FileKey DataFileKey() const;
    [[nodiscard]] Storage::FileKey SystemFileKey() const;

    static page_id_t CalculateExtentFirstPageId(extent_id_t extentId);

    static page_id_t CalculateGamPageId(const extent_id_t &extentId);

    static extent_id_t CalculateExtentId(page_id_t pageId);

    void UpdateSystemCatalog(const ::Memory::IAllocator* allocator)const;

    const DataStructures::PolymorphicArray<StorageTypes::Table*>& GetTables() const;
};

void CreateDatabase(
    const ::Memory::IAllocator* allocator,
    Int databaseId,
    const DataTypes::StringView& dbName
);
}; // namespace DatabaseEngine