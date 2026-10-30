#include <CoreEngine/Database.h>

#include <CoreEngine/SystemDatabases/SystemCatalog.h>
#include <CoreEngine/SystemDatabases/CatalogSchema.h>

#include <stdexcept>
#include <vector>
#include <CoreEngine/DatabaseConstants.h>
#include <CoreEngine/DataStorage/Table.h>
#include <CoreEngine/BufferPool/StorageManager.h>
#include <Systemic/Guards/WriterGuard.h>
#include <CoreEngine/Logger/WriteAheadLogger.h>

#include <iostream>

#include <CoreEngine/Memory/PersistentAllocator.h>
#include <Systemic/Macros.h>

#include "CoreEngine/BufferPool/StorageLayout.h"

#if IS_GCC
    #include <cmath>
#endif

namespace CoreEngine{
    DatabaseHeader::DatabaseHeader()
        :   _lastPfsPageId(0), _lastGamIndex(StorageLayout::FIRST_GAM_INDEX),
            _formatVersion(StorageLayout::FILE_FORMAT_VERSION), _reserved(0){}

    DatabaseHeader::DatabaseHeader(
        const page_id_t pfsPageId,
        const gam_index_t gamIndex
    ):  _lastPfsPageId(pfsPageId), _lastGamIndex(gamIndex),
        _formatVersion(StorageLayout::FILE_FORMAT_VERSION), _reserved(0){}

    void DatabaseHeader::ValidateFormatVersion() const{
        if (this->_formatVersion != StorageLayout::FILE_FORMAT_VERSION)
            throw std::runtime_error("Database::ValidateFormatVersion: invalid format version");
    }

    void Database::PopulateFilenames(
        const ::Memory::IAllocator* tempAllocator,
        const DataTypes::StringView& dbName,
        DataTypes::String& outFile,
        DataTypes::String& outSysFile
    ){
        const auto path = DataTypes::String::Concat(tempAllocator, dbName, "/", dbName);

        outFile = DataTypes::String::Concat(tempAllocator, path, Constants::DATA_FILE_EXTENSION);
        outSysFile = DataTypes::String::Concat(tempAllocator, path, Constants::SYS_EXTENSION, Constants::DATA_FILE_EXTENSION);
    }

    void Database::ApplyRecoveryLog(const Logging::LogEntry &logEntry)const{
        if (!logEntry.ValidateIntegrity())
            return;

        if (Logging::RowAffectedOperationTypes.Contains(logEntry.operation)) {
            // auto* table = this->_tables[logEntry.tableOrdinalPosition];

            // auto row = logEntry.GetRow();

            if (logEntry.operation == Logging::OperationType::DeleteRow) {
                //handle row delete trickier, need to identify whether to use pk or not (heap delete)
                return;
            }

            // table->InsertRow(row, 1);
        }

        //Data structure affected changes from here down.

    }

    Pages::GlobalAllocationPageView Database::RollToNewGamPageNoLock(){
        const auto nextGamIndex = this->_header._lastGamIndex + 1;
        if (nextGamIndex > StorageLayout::MAX_GAM_INDEX)
            throw std::runtime_error("Database::RollToNewGamPageNoLock: data file reached its maximum size");

        auto gamPage = this->_pageAccessor.CreatePage<Pages::GlobalAllocationPageView>(StorageLayout::GamPageId(nextGamIndex), nextGamIndex);
        this->_header._lastGamIndex = nextGamIndex;
        return gamPage;
    }

    Pages::AllocationPageView Database::FindOrRollToNewAllocationPage(
        const StorageTypes::Table* table,
        page_id_t currentAllocationPageId,
        const gam_index_t gamIndex,
        const page_id_t newAllocationPageId
    ) const{
        if (currentAllocationPageId == INVALID_PAGE_ID){
            auto page = this->_pageAccessor.CreatePage<Pages::AllocationPageView>(
                newAllocationPageId,
                gamIndex
            );

            table->UpdateAllocationPageId(page.PageId());
            return page;
        }

        Pages::AllocationPageView prev, current;
        while (currentAllocationPageId != INVALID_PAGE_ID){
            current = this->_pageAccessor.GetPage<Pages::AllocationPageView>(
                currentAllocationPageId
            );
            if (current.GamIndex() == gamIndex)
                return current;

            currentAllocationPageId = current.NextPageId();
            prev = std::move(current);
        }

        current = this->_pageAccessor.CreatePage<Pages::AllocationPageView>(
                newAllocationPageId,
                gamIndex
        );

        {
            MultiThreading::WriterGuard prevGuard(&prev.Latch());
            prev.SetNextPageId(current.PageId());
        }

        return current;
    }

    Database::Database(
        const ::Memory::IAllocator* allocator,
        const Accessors::PageAccessor pageAccessor,
        const Int databaseId,
        const DataTypes::StringView& dbName
    ): _id(databaseId), _nextOrdinal(0), _pageAccessor(pageAccessor){
        DataTypes::String file,sysFile;
        Database::PopulateFilenames(allocator, dbName, file, sysFile);

        const auto headerPage = this->_pageAccessor.GetPage<Pages::HeaderPageView>(StorageLayout::HEADER_PAGE_ID);
        this->_header = headerPage.GetDatabaseHeader();
        this->_tables.SetAllocator(&this->_allocator);
        this->_schemas.SetAllocator(&this->_allocator);
    }

    Database::~Database() = default;

    void Database::Destroy() const{
        {
            const auto headerPage = this->_pageAccessor.GetPage<Pages::HeaderPageView>(StorageLayout::HEADER_PAGE_ID);
            headerPage.SetDatabaseHeader(this->_header);

            for (auto& [key, table] : this->_tables){
                headerPage.SetTableHeader(table->GetPhysicalHeader(), table->GetOrdinalPosition());
                table->Destroy();
                delete table;
            }
        }

        this->_pageAccessor.ReleasePageTables<true>();
        this->_allocator.Release();
    }

    std::vector<Logging::LogEntry> Database::RecoverLogs(){
        return {};
        // return this->writeAheadLogger->RecoverLogs(this->_tables);
    }

    void Database::EnterRecoveryMode()const{
        const auto logs = CoreEngine::Database::RecoverLogs();

        if (logs.empty()) {
            std::cout << "No logs to recover." << std::endl;
            return;
        }
        for (const auto& log : logs)
            this->ApplyRecoveryLog(log);
    }

    void Database::LogCheckPoint(Logging::CheckPoint &checkPoint) {
        static const auto& logger = Logging::WriteAheadLogger::Get();
        logger.LogCheckPoint(checkPoint);
    }

    Logging::CheckPoint Database::LogRowInsert(
        const ExecutionContext& context,
        const StorageTypes::SerializedRow& payload,
        const transaction_id_t transactionId,
        const table_id_t tableOrdinal
    ) {
        static auto& logger = Logging::WriteAheadLogger::Get();

        DataStructures::PolymorphicArray<char> buffer(context.GetAllocator(), payload.Size());
        std::memcpy(buffer.Data(), payload.Data(), payload.Size());

        const auto logEntry = logger.CreateLogEntry(
            transactionId,
            Logging::OperationType::InsertRow,
            tableOrdinal,
            buffer
        );

        return logger.Log(logEntry);
    }

    Logging::CheckPoint Database::LogRowBatchInsert(
        DataStructures::PolymorphicArray<char>& buffer,
        const transaction_id_t transactionId,
        const table_id_t tableOrdinal
    ){
        static auto& logger = Logging::WriteAheadLogger::Get();

        const auto logEntry = logger.CreateLogEntry(
            transactionId,
            Logging::OperationType::InsertRow,
            tableOrdinal,
            buffer
        );

        return logger.Log(logEntry);
    }

    StorageTypes::Table * Database::OpenTableByOrdinal(const table_id_t tableId) const{
        for (Int i  = 0;i < this->_tables.Size(); i++){
            const auto& entry = this->_tables[i];
            if (entry._table->GetOrdinalPosition() == tableId)
                return entry._table;
        }

        return nullptr;
    }

    // void DatabaseHeader::ValidateFormatVersion(const DataTypes::StringView& databaseName) const{
    //     if (this->_formatVersion == StorageLayout::FILE_FORMAT_VERSION)
    //         return;
    //
    //     throw std::runtime_error(
    //         "Database '" + std::string(databaseName.Data(), databaseName.Size())
    //         + "' was written with file format version " + std::to_string(this->_formatVersion)
    //         + ", this build reads version " + std::to_string(StorageLayout::FILE_FORMAT_VERSION)
    //         + ". Recreate the database."
    //     );
    // }

    void Database::DeleteTable(const DataTypes::String& tableName){
        const StorageTypes::Table* table = nullptr;
        if (table == nullptr)
            return;

        const auto& tableHeader = table->GetPhysicalHeader();

        // const auto extentId = Database::CalculateExtentId(tableHeader._allocationPageId);

        // const auto indexAllocationMapPage = Storage::StorageManager::Get().GetPage<Pages::AllocationPageView>(this->filename, tableHeader.indexAllocationMapPageId, table);
        //
        // if (indexAllocationMapPage.Get() == nullptr)
        // {
        //     this->_tables.erase(it);
        //     delete table;
        //     return;
        // }

        // const page_id_t globalAllocationMapPageId = Database::GetGamAssociatedPage(tableHeader._allocationPageId);
        //
        // const auto globalAllocationMapPage = Storage::StorageManager::Get().GetPage<Pages::GlobalAllocationPageView>(
        //     this->dataFileKey,
        //     globalAllocationMapPageId
        // );

        std::vector<extent_id_t> allocatedExtents;
        // indexAllocationMapPage.GetAllocatedExtents(&allocatedExtents);

        //deallocate pages as well
        for (const auto& extentId : allocatedExtents)
        {

        }
        //deletes all rows
        // table->Delete(nullptr);
    }

    void Database::DeleteDatabase() const{
        // if (remove(this->filename.Data()) != 0)
        //     throw std::runtime_error("Database " + std::string(this->filename.Data(), this->filename.Size()) + " could not be deleted");
    }

    void Database::TruncateTable(const table_id_t  tableId) const{
        // auto* table = this->_tables[tableId];

        // auto indexAllocationMapPageId = table->GetHeader()._allocationPageId;

        // while (indexAllocationMapPageId != INVALID_PAGE_ID)
        // {
        //     const auto iamExtentId = Database::CalculateExtentId(indexAllocationMapPageId);
        //
        //     const auto tableMapPage = Storage::StorageManager::Get().GetPage<Pages::AllocationPageView>(this->filename, indexAllocationMapPageId, table);
        //
        //     std::vector<extent_id_t> allocatedExtents;
        //     tableMapPage.GetAllocatedExtents(&allocatedExtents);
        //
        //     const page_id_t globalAllocationMapPageId = Database::GetGamAssociatedPage(indexAllocationMapPageId);
        //
        //     auto gamPage = Storage::StorageManager::Get().GetPage<Pages::GlobalAllocationPageView>(this->filename, globalAllocationMapPageId);
        //
        //     indexAllocationMapPageId = tableMapPage.NextPageId();
        //
        //     for (const auto& extentId : allocatedExtents)
        //     {
        //         const page_id_t extentFirstPageId = Database::CalculateExtentFirstPageId(extentId);
        //
        //         for (page_id_t pageId = extentFirstPageId; pageId < extentFirstPageId + EXTENT_SIZE; pageId++)
        //         {
        //             const page_id_t pageFreeSpacePageId = Database::GetPfsAssociatedPage(pageId);
        //
        //             auto pageFreeSpacePage = Storage::StorageManager::Get().GetPage<Pages::PageFreeSpaceView>(this->systemFilename, pageFreeSpacePageId);
        //
        //             pageFreeSpacePage.SetPageFreed(pageId);
        //         }
        //
        //         gamPage.DeallocateExtent(extentId);
        //     }
        // }

        // table->UpdateAllocationPageId(INVALID_PAGE_ID);
    }

    void Database::PublishSchema(
        StorageTypes::Table* table,
        const Schemas::TableSchema* newSchema
    ){
        const auto* oldSchema = table->Schema();
        const auto oldKey = table->Key();

        const Directory::TableKey newKey(newSchema->_schemaId, newSchema->_lowerName);

        {
            MultiThreading::WriterGuard guard(&this->_directoryMutex);

            if (oldKey == newKey)
                this->_tables.ReKey(oldKey, newKey);     // same position: just repoint the string_view
            else{
                this->_tables.Remove(oldKey);            // RENAME / schema move: changes sort position
                this->_tables.Insert(Directory::TableEntry(table, newKey));
            }

            table->ReplaceSchema(newSchema);
        }

        Schemas::TableSchema::Destroy(oldSchema);

    }

    StorageTypes::ExtentReservation Database::ReserveExtents(
        const ::Memory::IAllocator* allocator,
        const StorageTypes::Table* table,
        const Int requiredPages
    ){
        const auto extentsToAllocate =  StorageLayout::ExtentsToAllocate(requiredPages);

        //pre account for required allocation page
        DataStructures::PolymorphicArray<StorageTypes::ExtentSegment> segmentsRuns(allocator, extentsToAllocate);

        // Step 1: Allocate extents from GAM page
        {
            DataStructures::PolymorphicArray<StorageTypes::ExtentSegment> localRuns(allocator, extentsToAllocate);
            auto currentAllocationPageId = table->GetAllocationPageId();

            MultiThreading::WriterGuard gamLock(&this->gamPageMutex);

            auto gamPage = this->_pageAccessor.GetPage<Pages::GlobalAllocationPageView>(StorageLayout::GamPageId(this->_header._lastGamIndex));

            // Handle GAM page overflow - allocate extents across multiple GAM pages if needed
            bool newAllocationPageRequired = currentAllocationPageId == INVALID_PAGE_ID;
            UnsignedInt remainingExtents = extentsToAllocate + newAllocationPageRequired;

            auto recounted = false;
            while (remainingExtents > 0) {
                if (gamPage.IsFull()){
                    gamPage = this->RollToNewGamPageNoLock();
                    remainingExtents += 1;
                    newAllocationPageRequired = true;
                }

                MultiThreading::WriterGuard gamPageLock(&gamPage.Latch());

                // Allocate one extent from current GAM page
                const auto extentsAllocated = gamPage.ReserveExtentsNoLock(localRuns, remainingExtents);
                remainingExtents-= extentsAllocated;

                if (extentsAllocated == 0){
                    if (recounted)
                        throw std::runtime_error("Database::ReserveExtents: GAM reports free extents but no free run exists");

                    gamPage.RecountFreeExtentsNoLock();
                    continue;
                }
                recounted = false;

                {
                    page_id_t newAllocationPageId = INVALID_PAGE_ID;
                    if (newAllocationPageRequired){
                        auto* localRunsFront = localRuns.Front();
                        newAllocationPageId = localRunsFront->_firstExtentId++ * StorageLayout::EXTENT_SIZE;
                        localRunsFront->_count--;
                        if (localRunsFront->_count == 0)
                            localRuns.Remove(0);
                    }

                    auto allocPage = this->FindOrRollToNewAllocationPage(
                        table,
                        currentAllocationPageId,
                        gamPage.GamIndex(),
                        newAllocationPageId
                    );
                    currentAllocationPageId = table->GetAllocationPageId();
                    newAllocationPageRequired = false;

                    if (!localRuns.Empty()){
                        MultiThreading::WriterGuard allocPageLock(&allocPage.Latch());
                        allocPage.ReserveExtentsNoLock(localRuns);
                    }
                }

                for (const auto& localRun : localRuns)
                    segmentsRuns.Push(localRun);

                localRuns.Clear();
            }

            if (segmentsRuns.Empty())
                throw std::runtime_error("Failed to allocate any extents");
        }

        // Step 4: Ensure PFS pages exist for all allocated extents
        {
            MultiThreading::WriterGuard pfsLock(&this->pfsPageMutex);

            for (const auto& [_firstExtentId, _count] : segmentsRuns) {
                for (auto extentId = _firstExtentId; extentId < _firstExtentId + _count; extentId++){
                    const auto firstPageId = StorageLayout::ExtentFirstPageId(extentId);

                    // Check each page in the extent
                    for (page_id_t pageId = firstPageId; pageId < firstPageId + StorageLayout::EXTENT_SIZE; pageId++) {
                        const auto pfsPageId = StorageLayout::PfsPageId(pageId);

                        if (pfsPageId > this->_header._lastPfsPageId) {
                            this->_pageAccessor.CreatePage<Pages::PageFreeSpaceView>(pfsPageId);
                            this->_header._lastPfsPageId = pfsPageId;
                        }
                    }
                }
            }
        }

        return StorageTypes::ExtentReservation(segmentsRuns, this, table);
    }

    void Database::UpdateSystemCatalog(const ::Memory::IAllocator* allocator)const{
        for(const auto& [key, table]: this->_tables)
            table->UpdateSystemCatalog(allocator);
    }

    UnsignedSmallInt Database::ReserveOrdinalNoLock(){
        return this->_nextOrdinal++;
    }

    Accessors::PageAccessor CreateDatabase(
        const ::Memory::IAllocator* allocator,
        const DataTypes::StringView& dbName
    ){
        static auto& storageManager = Storage::StorageManager::Get();

        const auto pageAccessor = storageManager.ReserveDbPageTables();

        const auto path = DataTypes::String::Concat(allocator, dbName, "/", dbName);
        const auto dataPath = DataTypes::String::Concat(allocator, path, Constants::DATA_FILE_EXTENSION);
        pageAccessor.OpenDataFile(DataTypes::StringView::ViewOf(dataPath), ::Storage::FileOpenMode::CreateNew);

        const auto sysDbPath = DataTypes::String::Concat(allocator, path, Constants::SYS_EXTENSION, Constants::DATA_FILE_EXTENSION);
        pageAccessor.OpenSystemFile(DataTypes::StringView::ViewOf(sysDbPath), ::Storage::FileOpenMode::CreateNew);

        // First interval of the system file: GAM #0 at page 1, then the PFS page covering data page 0 at page 2
        pageAccessor.CreatePage<Pages::GlobalAllocationPageView>(
            StorageLayout::GamPageId(StorageLayout::FIRST_GAM_INDEX),
            StorageLayout::FIRST_GAM_INDEX
        );

        pageAccessor.CreatePage<Pages::PageFreeSpaceView>(
            StorageLayout::FIRST_PFS_PAGE_ID
        );

        const auto headerPage = pageAccessor.CreatePage<Pages::HeaderPageView>(StorageLayout::HEADER_PAGE_ID);
        headerPage.SetDatabaseHeader(DatabaseHeader(StorageLayout::FIRST_PFS_PAGE_ID, StorageLayout::FIRST_GAM_INDEX));

        return pageAccessor;
    }

    Accessors::PageAccessor UseDatabase(
        const ::Memory::IAllocator* allocator,
        const DataTypes::StringView& dbName
    ){
        static auto& storageManager = Storage::StorageManager::Get();

        const auto pageAccessor = storageManager.ReserveDbPageTables();

        const auto path = DataTypes::String::Concat(allocator, dbName, "/", dbName);
        const auto dataPath = DataTypes::String::Concat(allocator, path, Constants::DATA_FILE_EXTENSION);
        pageAccessor.OpenDataFile(DataTypes::StringView::ViewOf(dataPath), ::Storage::FileOpenMode::OpenExisting);

        const auto sysDbPath = DataTypes::String::Concat(allocator, path, Constants::SYS_EXTENSION, Constants::DATA_FILE_EXTENSION);
        pageAccessor.OpenSystemFile(DataTypes::StringView::ViewOf(sysDbPath), ::Storage::FileOpenMode::OpenExisting);

        return pageAccessor;
    }
}
