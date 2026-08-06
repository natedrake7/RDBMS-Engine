#include "../include/Database.h"

#include "../include/SystemDatabases/SystemCatalog.h"

#include <stdexcept>
#include <vector>
#include "../include/DatabaseConstants.h"
#include "../include/DataStorage/Table.h"
#include "../include/DataStorage/Column.h"
#include "../include/BufferPool/StorageManager.h"
#include "../../Systemic/include/Guards/WriterGuard.h"
#include "../include/Logger/WriteAheadLogger.h"

#include <iostream>

#include "Managers/GlobalMemoryManager.h"
#include "Memory/Allocator.h"
#include "Memory/PersistentAllocator.h"

namespace CoreEngine{
    void Database::WriteHeaderToFile() const{
        const auto headerPage = Storage::StorageManager::Get().GetPage<Pages::HeaderPageView>(
            this->systemFileKey,
            Constants::HEADER_PAGE_ID
        );
        headerPage.SetDatabaseHeader(this->header);
    }

    page_id_t Database::GetPfsAssociatedPage(const page_id_t pageId) {
        const page_id_t pfsIndex = pageId / Constants::PAGE_FREE_SPACE_SIZE;
        if (pfsIndex == 0) return 1;

        // Count of GAM pages that precede PFS #pfsIndex in the sys file:
        // GAM #g precedes PFS #pfsIndex when g*GAM_COVERAGE < pfsIndex*PFS_COVERAGE,
        // plus 1 because GAM#0 (at sys page 2) always precedes PFS#1 and above.
        const page_id_t gamsPreceding =
            (static_cast<page_id_t>(pfsIndex) * Constants::PAGE_FREE_SPACE_SIZE)
            / Constants::GAM_NUMBER_OF_PAGES + 1;

        return 1 + pfsIndex + gamsPreceding;
    }

    page_id_t Database::GetGamAssociatedPage(const page_id_t pageId) {
        const page_id_t gamIndex = pageId / Constants::GAM_NUMBER_OF_PAGES;

        // PFS pages that precede GAM #gamIndex in the sys file:
        // PFS #0 always precedes GAM#0 → +1
        // For GAM #g, additional PFS pages = floor(g * GAM_COVERAGE / PFS_COVERAGE)
        const page_id_t pfsPreceding =
            (static_cast<page_id_t>(gamIndex) * Constants::GAM_NUMBER_OF_PAGES)
            / Constants::PAGE_FREE_SPACE_SIZE + 1;

        // Header(1) + PFS pages before it + prior GAM pages
        return 1 + pfsPreceding + gamIndex;
    }

    void Database::PopulateFilenames(
        const ::Memory::IAllocator* tempAllocator,
        const DataTypes::String& dbName,
        DataTypes::String& outFile,
        DataTypes::String& outSysFile
    ){
        const auto path = DataTypes::String::Concat(tempAllocator, dbName, "/", dbName);

        outFile = DataTypes::String::Concat(tempAllocator, path, Constants::DATA_FILE_EXTENSION);
        outSysFile = DataTypes::String::Concat(tempAllocator, path, Constants::SYS_EXTENSION, Constants::DATA_FILE_EXTENSION);
    }

    void Database::CreateKeys(){
        this->dataFileKey = Storage::FileKey::Create(this->id, Storage::FileType::Data);
        this->systemFileKey = Storage::FileKey::Create(this->id, Storage::FileType::System);
    }

    void Database::ApplyRecoveryLog(const Logging::LogEntry &logEntry)const{
        if (!logEntry.ValidateIntegrity())
            return;

        if (Logging::RowAffectedOperationTypes.Contains(logEntry.operation)) {
            auto* table = this->_tables[logEntry.tableOrdinalPosition];

            // auto row = logEntry.GetRow();

            if (logEntry.operation == Logging::OperationType::DeleteRow) {
                //handle row delete trickier, need to identify whether to use pk or not (heap delete)
                return;
            }

            // table->InsertRow(row, 1);
        }

        //Data structure affected changes from here down.

    }

    Int Database::CalculateExtentsToAllocate(const Int pagesToAllocate) {
        return static_cast<Int>(std::ceil(static_cast<float>(pagesToAllocate) / static_cast<float>(Constants::EXTENT_SIZE)));
    }

    void Database::InitializeStaticData(){
        this->dataFileKey = Storage::FileKey::Create(this->id, Storage::FileType::Data);
        this->systemFileKey = Storage::FileKey::Create(this->id, Storage::FileType::System);
    }

    Pages::GlobalAllocationPageView Database::RollToNewGamPageNoLock(){
        this->header.lastGamPageId = Database::CalculateNextGamPageId(this->header.lastGamPageId);
        auto gamPage = Storage::StorageManager::Get().GetPage<Pages::GlobalAllocationPageView>(
            this->systemFileKey,
            this->header.lastGamPageId
        );
        return gamPage;
    }

    Pages::AllocationPageView Database::FindOrRollToNewAllocationPage(
        StorageTypes::Table* tablePtr,
        page_id_t currentAllocationPageId,
        const page_id_t gamPageId,
        const page_id_t newAllocationPageId
    ) const{
        if (currentAllocationPageId == INVALID_PAGE_ID){
            auto page = Storage::StorageManager::Get().CreateAllocationPage(
                this->dataFileKey,
                newAllocationPageId,
                gamPageId
            );

            tablePtr->UpdateAllocationPageId(page.PageId());
            return page;
        }

        Pages::AllocationPageView prev, current;
        while (currentAllocationPageId != INVALID_PAGE_ID){
            current = Storage::StorageManager::Get().GetPage<Pages::AllocationPageView>(
                this->dataFileKey,
                currentAllocationPageId
            );
            if (current.GamPageId() == gamPageId)
                return current;

            currentAllocationPageId = current.NextPageId();
            prev = std::move(current);
        }

        current = Storage::StorageManager::Get().CreateAllocationPage(
            this->dataFileKey,
            newAllocationPageId,
            gamPageId
        );

        {
            MultiThreading::WriterGuard prevGuard(&prev.Latch());
            prev.SetNextPageId(current.PageId());
        }

        return current;
    }

    Database::Database(
        const ::Memory::IAllocator* allocator,
        const Int databaseId,
        const DataTypes::String& dbName,
        const bool& isServerInitialization
    ) {
        this->id = databaseId;
        DataTypes::String file,sysFile;
        Database::PopulateFilenames(allocator, dbName, file, sysFile);
        this->CreateKeys();

        Storage::StorageManager::Get().OpenFile(this->dataFileKey, DataTypes::StringView::ViewOf(file));
        Storage::StorageManager::Get().OpenFile(this->systemFileKey, DataTypes::StringView::ViewOf(sysFile));

        const auto headerPage = Storage::StorageManager::Get().GetPage<Pages::HeaderPageView>(this->systemFileKey, Constants::HEADER_PAGE_ID);

        this->header = *headerPage.GetDatabaseHeaderPtr();
        this->_tables.SetAllocator(&this->_allocator);

        if (isServerInitialization) return;

        static auto& catalog = SystemCatalog::Get();

        //query get from masterDb
        const auto masterDbData = catalog.SelectTables(allocator, DataTypes::StringView::ViewOf(dbName));

        if (this->header.numberOfTables != masterDbData.Size()) return;

        for (int i = 0;i < masterDbData.Size(); i++)
            this->CreateTable(masterDbData[i], *headerPage.GetTableHeaderPtr(i));
    }

    Database::Database(
        const ::Memory::IAllocator* allocator,
        const Int databaseId,
        const DataTypes::String& dbName,
        const std::vector<Headers::sysTable>& tables
    ){
        this->id = databaseId;
        DataTypes::String file,sysFile;
        Database::PopulateFilenames(allocator, dbName, file, sysFile);
        this->CreateKeys();

        Storage::StorageManager::Get().OpenFile(this->dataFileKey, DataTypes::StringView::ViewOf(file));
        Storage::StorageManager::Get().OpenFile(this->systemFileKey, DataTypes::StringView::ViewOf(sysFile));

        const auto headerPage = Storage::StorageManager::Get().GetPage<Pages::HeaderPageView>(this->systemFileKey, Constants::HEADER_PAGE_ID);

        this->header = *headerPage.GetDatabaseHeaderPtr();
        this->_tables.SetAllocator(&this->_allocator);

        for (int i = 0; i < tables.size(); i++) {
            HashSet primaryKeysSet(tables[i].primaryKey);
            Headers::Index index;

            Int counter = 0;
            for(int j = 0;j < tables[i].columns.size(); j++){
                const auto& column = tables[i].columns[j];

                if(primaryKeysSet.Contains(column.name))
                    index.columns[counter++] = j;
            }

            this->CreateTable(tables[i], *headerPage.GetTableHeaderPtr(i), index, i);
        }
    }

    void Database::Destroy(){
        // save db header;
        const auto headerPage = Storage::StorageManager::Get().GetPage<Pages::HeaderPageView>(this->systemFileKey, Constants::HEADER_PAGE_ID);
        headerPage.SetDatabaseHeader(this->header);

        for (const auto* dbTable : this->_tables){
            headerPage.SetTableHeader(dbTable->GetHeader(), dbTable->GetOrdinalPosition());
            dbTable->Destroy();
        }

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
        Logging::WriteAheadLogger::Get().LogCheckPoint(checkPoint);
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

    page_id_t Database::CalculateNextGamPageId(const page_id_t currentGamPageId) {
        return currentGamPageId + Constants::NEXT_GAM_PAGE_ID_OFFSET;
    }

    byte_t Database::GetObjectSizeToCategory(const row_size_t &size){
        // Direct mapping to 7levels (0-7)
        const auto freeSpacePercentage = static_cast<float>(size) / Constants::PAGE_SIZE;
        return static_cast<byte_t>(freeSpacePercentage * 7);
    }

    StorageTypes::Table* Database::CreateTable(
        const table_id_t tableId,
        const Int ordinalPosition
    ){
        auto* table = this->_allocator.Allocate<StorageTypes::Table>(tableId, ordinalPosition, this);
        this->_tables.Push(table);
        this->header.numberOfTables++;
        this->header.lastTableId = tableId;
        return table;
    }

    void Database::CreateTable(const Headers::TableHeader& masterDbHeader, const StorageTypes::TableHeader &tableHeader){
        static auto& catalog = SystemCatalog::Get();

        auto* table = this->_allocator.Allocate<StorageTypes::Table>(masterDbHeader, tableHeader, this);

        const Memory::Allocator allocator;
        const auto masterDbColumns = catalog.SelectColumns(&allocator, masterDbHeader.id);
        for (const auto & masterDbColumn : masterDbColumns) {
            if (masterDbColumn.isSystem)
                continue;

            auto* column = this->_allocator.Allocate<StorageTypes::Column>(masterDbColumn, table);
            table->AddColumn(column);
        }

        //TODO
        //maybe add in a single function
        table->RetrieveColumnHeadersFromCatalog(&allocator);
        table->RetrieveIdentityColumnsFromCatalog(&allocator);
        table->RetrieveIndexesFromCatalog(&allocator);
        table->RetrieveDefaultValuesFromCatalog(&allocator);

        this->_tables.Push(table);
    }

    void Database::CreateTable(
        const Headers::sysTable &sysHeader,
        const StorageTypes::TableHeader &tableHeader,
        const Headers::Index& primaryKey,
        const Int ordinalPosition
    ){
        auto* table = this->_allocator.Allocate<StorageTypes::Table>(sysHeader, tableHeader, primaryKey, this, ordinalPosition);
        this->_tables.Push(table);
    }

    void Database::InferSchemaFromColumns(const std::vector<StorageTypes::Column*>& columns){
        HashSet<std::string> schemaNamesSet;


    }

    page_id_t Database::CalculateExtentFirstPageId(const extent_id_t extentId){
        return extentId * Constants::EXTENT_SIZE;
    }

    page_id_t Database::CalculateGamPageId(const extent_id_t &extentId) {
        return static_cast<page_id_t>(std::ceil(static_cast<float>(extentId) / static_cast<float>(Constants::GAM_PAGE_SIZE)) + 2);
    }

    extent_id_t Database::CalculateExtentId(const page_id_t pageId){
        return pageId / 8;
    }

    StorageTypes::Table * Database::OpenTable(const table_id_t tableId) const{
        return this->_tables[tableId];
    }

    DatabaseHeader::DatabaseHeader()
        :   numberOfTables(0), lastTableId(0),
            lastPfsPageId(0), lastGamPageId(0){}

    DatabaseHeader::DatabaseHeader(
        const table_number_t numberOfTables,
        const page_id_t lastPfsPageId,
        const page_id_t lastGamPageId
    ):  numberOfTables(numberOfTables), lastTableId(0),
        lastPfsPageId(lastPfsPageId), lastGamPageId(lastGamPageId){}

    const StorageTypes::Table *Database::GetTable(const table_id_t tableId) const{
        if (tableId >= this->_tables.Size())
            throw std::out_of_range("No table with ID: " + std::to_string(tableId) + " exists");
        return this->_tables[tableId];
    }

    Pages::PageFreeSpaceView Database::GetAssociatedPfsPage(
        const Storage::FileKey sysFileKey,
        const page_id_t pageId
    ){
        const auto pageFreeSpacePageId = Database::GetPfsAssociatedPage(pageId);
        return Storage::StorageManager::Get().GetPage<Pages::PageFreeSpaceView>(sysFileKey, pageFreeSpacePageId);
    }

    void Database::DeleteTable(const DataTypes::String& tableName){
        const StorageTypes::Table* table = nullptr;
        if (table == nullptr)
            return;

        const auto& tableHeader = table->GetHeader();

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
        auto* table = this->_tables[tableId];

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

        table->UpdateAllocationPageId(INVALID_PAGE_ID);
    }

    StorageTypes::ExtentReservation Database::ReserveExtents(
        const ::Memory::IAllocator* allocator,
        const Int requiredPages,
        const Int tableOrdinalPos
    ){
        const auto extentsToAllocate =  Database::CalculateExtentsToAllocate(requiredPages);
        auto* table = this->_tables[tableOrdinalPos];

        //pre account for required allocation page
        DataStructures::PolymorphicArray<StorageTypes::ExtentSegment> segmentsRuns(allocator, extentsToAllocate);

        // Step 1: Allocate extents from GAM page
        {
            DataStructures::PolymorphicArray<StorageTypes::ExtentSegment> localRuns(allocator, extentsToAllocate);
            const page_id_t currentAllocationPageId = table->GetAllocationPageId();

            MultiThreading::WriterGuard gamLock(&this->gamPageMutex);

            auto gamPage = Storage::StorageManager::Get().GetPage<Pages::GlobalAllocationPageView>(
                this->systemFileKey,
                this->header.lastGamPageId
            );

            // Handle GAM page overflow - allocate extents across multiple GAM pages if needed
            bool newAllocationPageRequired = currentAllocationPageId == INVALID_PAGE_ID;
            UnsignedInt remainingExtents = extentsToAllocate + newAllocationPageRequired;
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

                if (extentsAllocated == 0)
                    continue;

                {
                    page_id_t newAllocationPageId = INVALID_PAGE_ID;
                    if (newAllocationPageRequired){
                        auto* localRunsFront = localRuns.Front();
                        newAllocationPageId = localRunsFront->_firstExtentId++ * Constants::EXTENT_SIZE;
                        localRunsFront->_count--;
                    }

                    auto allocPage = this->FindOrRollToNewAllocationPage(
                        table,
                        currentAllocationPageId,
                        gamPage.PageId(),
                        newAllocationPageId
                    );

                    MultiThreading::WriterGuard allocPageLock(&allocPage.Latch());
                    allocPage.ReserveExtentsNoLock(localRuns);
                }

                for (const auto& localRun : localRuns)
                    segmentsRuns.Push(localRun);

                localRuns.Clear();
            }

            if (segmentsRuns.Empty())
                throw std::runtime_error("Failed to allocate any extents");

            newAllocationPageRequired = false;
        }

        // Step 4: Ensure PFS pages exist for all allocated extents
        {
            MultiThreading::WriterGuard pfsLock(&this->pfsPageMutex);

            for (const auto& [_firstExtentId, _count] : segmentsRuns) {
                for (auto extentId = _firstExtentId; extentId < _firstExtentId + _count; extentId++){
                    const auto firstPageId = Database::CalculateExtentFirstPageId(extentId);

                    // Check each page in the extent
                    for (page_id_t pageId = firstPageId; pageId < firstPageId + Constants::EXTENT_SIZE; pageId++) {
                        const auto pfsPageId = Database::GetPfsAssociatedPage(pageId);

                        if (pfsPageId > this->header.lastPfsPageId) {
                            Storage::StorageManager::Get().CreatePageFreeSpacePage(
                                this->systemFileKey,
                                pfsPageId
                            );
                            this->header.lastPfsPageId = pfsPageId;
                        }
                    }
                }
            }
        }

        return StorageTypes::ExtentReservation(segmentsRuns, this, tableOrdinalPos);
    }

    Storage::FileKey Database::DataFileKey() const{ return this->dataFileKey; }

    Storage::FileKey Database::SystemFileKey() const{ return this->systemFileKey; }

    void Database::GetIdentityColumns(const ::Memory::IAllocator* allocator)const{
        for(const auto& table: this->_tables)
            table->RetrieveIdentityColumnsFromCatalog(allocator);
    }

    void Database::UpdateIdentityManagersIds(const ::Memory::IAllocator* allocator)const{
        for(const auto& table: this->_tables)
            table->UpdateCatalogIdentityColumns(allocator);
    }

    void Database::GetColumnsHeaders(const ::Memory::IAllocator* allocator) const{
        for (const auto& table : this->_tables)
            table->RetrieveColumnHeadersFromCatalog(allocator);
    }

    void Database::GetDefaultValues(const ::Memory::IAllocator* allocator) const{
        for (const auto& table : this->_tables)
            table->RetrieveDefaultValuesFromCatalog(allocator);
    }

    void Database::GetIndexes(const ::Memory::IAllocator* allocator) const{
        for (const auto& table : this->_tables)
            table->RetrieveIndexesFromCatalog(allocator);
    }

    void Database::GetTableHeaders() const{
    }

    void Database::UpdateMasterDatabase(const ::Memory::IAllocator* allocator)const{
        for(const auto& table: this->_tables)
            table->UpdateSystemCatalog(allocator);
    }

    const DataStructures::PolymorphicArray<StorageTypes::Table*>&  Database::GetTables() const{ return this->_tables; }

    void CreateDatabase(const Int databaseId, const DataTypes::String& dbName){
        static auto& storageManager = Storage::StorageManager::Get();

        const auto path = dbName.ConcatInPlace("/", dbName);
        const auto dataKey = Storage::FileKey::Create(databaseId, Storage::FileType::Data);

        storageManager.CreateFile(dataKey, DataTypes::StringView::ViewOf(path), Constants::DATA_FILE_EXTENSION);


        const auto sysDbName = DataTypes::String::Concat(path.GetAllocator(), path, Constants::SYS_EXTENSION);
        const auto sysKey = Storage::FileKey::Create(databaseId, Storage::FileType::System);

        storageManager.CreateFile(sysKey, DataTypes::StringView::ViewOf(sysDbName), Constants::DATA_FILE_EXTENSION);

        static constexpr page_id_t FIRST_PFS_PAGE_ID = 1;
        static constexpr page_id_t FIRST_GAM_PAGE_ID = 2;

        storageManager.CreateGlobalAllocationMapPage(sysKey, FIRST_GAM_PAGE_ID);
        storageManager.CreatePageFreeSpacePage(sysKey, FIRST_PFS_PAGE_ID);

        const auto headerPage = storageManager.CreateHeaderPage(sysKey);
        headerPage.SetDatabaseHeader(DatabaseHeader(0, FIRST_PFS_PAGE_ID, FIRST_GAM_PAGE_ID));
    }
}

