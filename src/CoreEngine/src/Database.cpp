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

#include <cmath>
#include <iostream>

#include "Managers/GlobalMemoryManager.h"
#include "Memory/Allocator.h"
#include "Memory/PersistentAllocator.h"

namespace CoreEngine{
    void Database::WriteHeaderToFile() const
    {
        const auto metaDataPage = Storage::StorageManager::Get().GetHeaderPage(this->systemFileKey, this->systemFilenameView);
        metaDataPage.SetDatabaseHeader(this->header);
    }

    page_id_t Database::GetPfsAssociatedPage(const page_id_t pageId) {
        const page_id_t numGAMPagesBefore = pageId / Constants::GAM_NUMBER_OF_PAGES;
        const page_id_t pfsIndex = pageId / Constants::PAGE_FREE_SPACE_SIZE;
        constexpr page_id_t firstPfsPageId = 1;

        return firstPfsPageId + pfsIndex + numGAMPagesBefore;
    }

    page_id_t Database::GetGamAssociatedPage(const page_id_t pageId) {
        const auto numOfGamPages = (pageId / Constants::GAM_NUMBER_OF_PAGES) + 2;

        const auto numOfPfsPages = (pageId / Constants::PAGE_FREE_SPACE_SIZE);

        return numOfGamPages > 2 ? numOfPfsPages + numOfGamPages + 1 : numOfPfsPages + numOfGamPages;
    }

//    page_id_t Database::GetPfsAssociatedPage(const page_id_t pageId) {
//
//      //TODO find how to track the pages correctly
//      uint32_t numGamPages = (pageId / GAM_NUMBER_OF_PAGES) + 1;
//      uint32_t numPfsPages = (pageId / PAGE_FREE_SPACE_SIZE) + 1;
//
//      // Convert to logical data-only page ID
//      uint32_t logicalDataPageId = pageId - numGamPages - numPfsPages;
//
//      // Find which PFS page covers this logical data page
//      uint32_t pfsIndex = logicalDataPageId / PAGE_FREE_SPACE_SIZE;
//
//      // Now convert back to physical pageId of that PFS page
//      // +1 is often where the first PFS page starts (adjust to your system)
//      page_id_t pfsPageId = (pfsIndex * PAGE_FREE_SPACE_SIZE) + numPfsPages + numGamPages - 1;
//
//      if(pageId == 8088 || pageId == 8092)
//      {
//        cout << "hello";
//      }
//
//      return pfsPageId;
//    }
//
//    page_id_t Database::GetGamAssociatedPage(const page_id_t pageId) {
//      uint32_t numGamPages = pageId / GAM_NUMBER_OF_PAGES + 1;
//      uint32_t numPfsPages = pageId / PAGE_FREE_SPACE_SIZE + 1;
//
//      uint32_t logicalDataPageId = pageId - numGamPages - numPfsPages;
//
//      uint32_t gamIndex = logicalDataPageId / GAM_NUMBER_OF_PAGES;
//
//      page_id_t gamPageId = gamIndex * GAM_NUMBER_OF_PAGES + 2;
//
//      return gamPageId;
//    }

    void Database::PopulateFilenames(const ::Memory::IAllocator* tempAllocator, const DataTypes::String& dbName){
        const auto path = DataTypes::String::Concat(tempAllocator, dbName, "/", dbName);

        const auto tempFilename = DataTypes::String::Concat(tempAllocator, path, Constants::DATA_FILE_EXTENSION);
        const auto tempSysFilename = DataTypes::String::Concat(tempAllocator, path, Constants::SYS_EXTENSION, Constants::DATA_FILE_EXTENSION);

        this->filename = DataTypes::String(tempFilename, &this->_allocator);
        this->systemFilename = DataTypes::String(tempSysFilename, &this->_allocator);
        this->name = DataTypes::String(dbName, &this->_allocator);

        this->fileExtension = Constants::DATA_FILE_EXTENSION;
        this->systemFilenameView = this->systemFilename.ToView();
        this->filenameView = this->filename.ToView();
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

    int Database::CalculateExtentsToAllocate(const Int pagesToAllocate) {
        return static_cast<int>(std::ceil(static_cast<float>(pagesToAllocate) / static_cast<float>(Constants::EXTENT_SIZE)));
    }

    void Database::InitializeStaticData(){
        this->filenameView = this->filename.ToView();
        this->systemFilenameView = this->systemFilename.ToView();
        this->dataFileKey = Storage::FileKey::Create(this->id, Storage::FileType::Data);
        this->systemFileKey = Storage::FileKey::Create(this->id, Storage::FileType::System);
    }

    Database::Database(
        const ::Memory::IAllocator* allocator,
        const Int databaseId,
        const DataTypes::String& dbName,
        const bool& isServerInitialization
    ) {
        this->id = databaseId;
        this->PopulateFilenames(allocator, dbName);
        this->CreateKeys();

        const auto headerPage = Storage::StorageManager::Get().GetHeaderPage(
            this->systemFileKey,
            this->systemFilenameView
        );

        this->header = *headerPage.GetDatabaseHeaderPtr();
        this->_tables.SetAllocator(&this->_allocator);

        if (isServerInitialization) return;

        static auto& catalog = SystemCatalog::Get();

        //query get from masterDb
        const auto masterDbData = catalog.SelectTables(allocator, this->name.ToView());
        const auto& headerPageTables = headerPage.GetTableHeaders();

        if (headerPageTables.size() != masterDbData.Size()) return;

        for (int i = 0;i < masterDbData.Size(); i++)
            this->CreateTable(masterDbData[i], headerPageTables[i]);
    }

    Database::Database(
        const ::Memory::IAllocator* allocator,
        const Int databaseId,
        const DataTypes::String& dbName,
        const std::vector<Headers::sysTable>& tables
    ){
        this->id = databaseId;
        this->PopulateFilenames(allocator, dbName);
        this->CreateKeys();

        const auto headerPage = Storage::StorageManager::Get().GetHeaderPage(this->systemFileKey, this->systemFilenameView);

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

            this->CreateTable(tables[i], headerPage.GetTableHeader(i), index, i);
        }
    }

    Database::~Database(){
        // save db header;
        auto headerPage = Storage::StorageManager::Get().GetHeaderPage(
                this->systemFileKey,
                this->systemFilenameView
        );
        headerPage.SetDatabaseHeader(this->header);

        for (const auto* dbTable : this->_tables){
            headerPage.SetTableHeader(dbTable->GetHeader());
            dbTable->Destroy();
        }

        headerPage.WriteTableHeadersToDisk();
        this->_allocator.Reset();
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
        const StorageTypes::InsertPayload& payload,
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

    page_id_t Database::CalculateSystemPageOffset(const page_id_t pageId)
    {
        page_id_t pfsPages = pageId / Constants::PAGE_FREE_SPACE_SIZE + 1;

        //        if (pfsPages == 0)
        //            pfsPages = 1;

        page_id_t gamPages = pageId / Constants::GAM_NUMBER_OF_PAGES + 1;

        //        if (gamPages == 0)
        //            gamPages = 1;

        return pageId ;//+ pfsPages + gamPages + 1;
    }

    page_id_t Database::CalculateNextGamPageId(const page_id_t currentGamPageId) {
        return currentGamPageId + Constants::NEXT_GAM_PAGE_ID_OFFSET;
    }

    byte_t Database::GetObjectSizeToCategory(const row_size_t &size)
    {
        const float freeSpacePercentage = static_cast<float>(size) / Constants::PAGE_SIZE;

        // Direct mapping to 7levels (0-7)
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

    // StorageTypes::Table *Database::CreateTable(
    //     const table_id_t tableId,
    //     const Int ordinalPosition,
    //     const std::vector<StorageTypes::Column *> &columns,
    //     const Headers::Index *clusteredKeyIndexes,
    //     const std::vector<Headers::Index> *nonClusteredIndexes
    // ){
    //     // auto* table = this->_allocator.Allocate<StorageTypes::Table>(
    //     //     tableId,
    //     //     ordinalPosition,
    //     //     columns,
    //     //     this,
    //     //     clusteredKeyIndexes,
    //     //     nonClusteredIndexes
    //     // );
    //     // this->_tables.Push(table);
    //     // this->header.numberOfTables = this->_tables.Size();
    //     // return table;
    // }

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

    page_id_t Database::CalculateExtentFirstPageId(const extent_id_t &extentId){
        return extentId * Constants::EXTENT_SIZE;
    }

    page_id_t Database::CalculateGamPageId(const extent_id_t &extentId) {
        return static_cast<page_id_t>(std::ceil(static_cast<float>(extentId) / static_cast<float>(Constants::GAM_PAGE_SIZE)) + 2);
    }

    extent_id_t Database::CalculateExtentId(const page_id_t pageId){
        return pageId / 8;
    }

    //    Table *Database::OpenTable(const string& schemaName, const string &tableName) const
//    {
//        for (const auto &table : this->_tables)
//        {
//            if (table->GetTableName() == tableName
//                && table->GetSchema() == schemaName)
//                return table;
//        }
//
//        return nullptr;
//    }

    StorageTypes::Table * Database::OpenTable(const table_id_t tableId) const{
        return this->_tables[tableId];
    }

    // StorageTypes::Table * Database::OpenTableById(const table_id_t tableId) const{
    //     return this->_tables.At(this->tableIdsDictionary.Get(tableId));
    // }

    void Database::DeleteTable(const DataTypes::String& tableName)
    {
        const StorageTypes::Table* table = nullptr;
        std::vector<StorageTypes::Table*>::iterator it;

//         for (it = this->_tables.begin(); it != this->_tables.end(); it++)
//         {
// //            if ((*it)->GetTableName() == tableName)
// //            {
// //                table = *it;
// //                break;
// //            }
//         }

        if (table == nullptr)
            return;

        const auto& tableHeader = table->GetHeader();

        const auto extentId = Database::CalculateExtentId(tableHeader.allocationPageId);

        // const auto indexAllocationMapPage = Storage::StorageManager::Get().GetAllocationPage(this->filename, tableHeader.indexAllocationMapPageId, table);
        //
        // if (indexAllocationMapPage.Get() == nullptr)
        // {
        //     this->_tables.erase(it);
        //     delete table;
        //     return;
        // }

        const page_id_t globalAllocationMapPageId = Database::GetGamAssociatedPage(tableHeader.allocationPageId);

        const auto globalAllocationMapPage = Storage::StorageManager::Get().GetGlobalAllocationMapPage(
            this->dataFileKey,
            this->filenameView,
            globalAllocationMapPageId
        );

        std::vector<extent_id_t> allocatedExtents;
        // indexAllocationMapPage.GetAllocatedExtents(&allocatedExtents);

        //deallocate pages as well
        for (const auto& extentId : allocatedExtents)
        {

        }
        //deletes all rows
        // table->Delete(nullptr);
    }

    void CreateDatabase(const Int databaseId, const DataTypes::String& dbName){
        const auto path = dbName.ConcatInPlace("/", dbName);
        const auto pathView = path.ToView();

        const auto dataKey = Storage::FileKey::Create(databaseId, Storage::FileType::Data);

        Storage::StorageManager::Get().CreateFile(dataKey, pathView, Constants::DATA_FILE_EXTENSION);

        const auto sysDbName = path.Concat(Constants::SYS_EXTENSION);
        const auto sysDbNameView = sysDbName.ToView();

        const auto sysKey = Storage::FileKey::Create(databaseId, Storage::FileType::System);

        Storage::StorageManager::Get().CreateFile(sysKey, sysDbNameView, Constants::DATA_FILE_EXTENSION);

        static constexpr page_id_t firstGamPageId = 2;
        static constexpr page_id_t firstPfsPageId = 1;

        const auto sysDbFileName = sysDbName.Concat(Constants::DATA_FILE_EXTENSION);
        Storage::StorageManager::Get().CreateGlobalAllocationMapPage(sysKey, sysDbNameView, firstGamPageId);
        Storage::StorageManager::Get().CreatePageFreeSpacePage(sysKey, sysDbNameView, firstPfsPageId);

        const auto headerPage = Storage::StorageManager::Get().CreateHeaderPage(sysKey, sysDbNameView);
        headerPage.SetDatabaseHeader(DatabaseHeader(0, firstPfsPageId, firstGamPageId));
    }

    void Database::DeleteDatabase() const{
        if (remove(this->filename.Data()) != 0)
            throw std::runtime_error("Database " + std::string(this->filename.Data(), this->filename.Size()) + " could not be deleted");
    }

    Pages::PageView Database::FindOrAllocateNextDataPage(
        const ::Memory::IAllocator* allocator,
        Pages::PageFreeSpaceView &pageFreeSpacePage,
        const page_id_t pageId,
        const page_id_t extentFirstPageId,
        const StorageTypes::Table &table,
        const Int pageToAllocate
    ){
        Pages::PageView page;
        bool pageAllocated = false;
        if (pageId < extentFirstPageId + Constants::EXTENT_SIZE - 1){
            for (page_id_t nextLeafPageId = pageId + 1; nextLeafPageId < extentFirstPageId + Constants::EXTENT_SIZE; nextLeafPageId++){
                if (pageFreeSpacePage.GetPageSizeCategory(nextLeafPageId) == 0)
                    continue;

                page = Storage::StorageManager::Get().GetPage(
                    this->dataFileKey,
                    this->filenameView,
                    nextLeafPageId,
                    &table
                );

                if (page.PageSize() == 0)
                    break;

                pageAllocated = true;
            }
        }

        if (!pageAllocated || page.PageSize() > 0){
            page = this->CreateDataPage(allocator, table.GetTableId(), pageToAllocate);
            pageFreeSpacePage = Database::GetAssociatedPfsPage(this->systemFileKey, this->systemFilenameView, page.PageId());
        }

        return page;
    }

    Pages::PageFreeSpaceView Database::GetAssociatedPfsPage(
        const Storage::FileKey sysFileKey,
        const DataTypes::StringView& filenameView,
        const page_id_t pageId
    ){
        const auto pageFreeSpacePageId = Database::GetPfsAssociatedPage(pageId);
        return Storage::StorageManager::Get().GetPageFreeSpacePage(sysFileKey, filenameView, pageFreeSpacePageId);
    }

    void Database::TruncateTable(const table_id_t  tableId) const{
        auto* table = this->_tables[tableId];

        auto indexAllocationMapPageId = table->GetHeader().allocationPageId;

        // while (indexAllocationMapPageId != INVALID_PAGE_ID)
        // {
        //     const auto iamExtentId = Database::CalculateExtentId(indexAllocationMapPageId);
        //
        //     const auto tableMapPage = Storage::StorageManager::Get().GetAllocationPage(this->filename, indexAllocationMapPageId, table);
        //
        //     std::vector<extent_id_t> allocatedExtents;
        //     tableMapPage.GetAllocatedExtents(&allocatedExtents);
        //
        //     const page_id_t globalAllocationMapPageId = Database::GetGamAssociatedPage(indexAllocationMapPageId);
        //
        //     auto gamPage = Storage::StorageManager::Get().GetGlobalAllocationMapPage(this->filename, globalAllocationMapPageId);
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
        //             auto pageFreeSpacePage = Storage::StorageManager::Get().GetPageFreeSpacePage(this->systemFilename, pageFreeSpacePageId);
        //
        //             pageFreeSpacePage.SetPageFreed(pageId);
        //         }
        //
        //         gamPage.DeallocateExtent(extentId);
        //     }
        // }

        table->UpdateIndexAllocationMapPageId(INVALID_PAGE_ID);
    }

    Pages::OverflowPageView Database::CreateOverflowPage(
        const ::Memory::IAllocator* allocator,
        const Int pagesToAllocate,
        const table_id_t tableOrdinalPosition
    ){
        page_id_t lowerLimit = 0;

        const auto extentsToAllocate =  static_cast<int>(std::ceil(static_cast<float>(pagesToAllocate) / Constants::EXTENT_SIZE));

        const auto extents = this->AllocateNewExtents(
            allocator, extentsToAllocate,
            tableOrdinalPosition, lowerLimit
        );

        Pages::OverflowPageView firstPage;
        for (const auto& extentId : extents) {
            const auto firstExtentPageId = Database::CalculateExtentFirstPageId(extentId);

            for (page_id_t pageId = firstExtentPageId; pageId < firstExtentPageId + Constants::EXTENT_SIZE; pageId++){

                if (pageId == lowerLimit)
                    continue;

                auto pageFreeSpacePage = Database::GetAssociatedPfsPage(this->systemFileKey, this->systemFilenameView, pageId);

                auto overflowPage = Storage::StorageManager::Get().CreateOverflowPage(this->dataFileKey, this->filenameView, pageId);

                MultiThreading::WriterGuard lock(&pageFreeSpacePage.Latch());
                MultiThreading::WriterGuard overflowLock(&overflowPage.Latch());

                // pageFreeSpacePage.SetPageMetaData(overflowPage.Get());

                if (!firstPage.IsValid())
                    firstPage = std::move(overflowPage);
            }
        }

        return firstPage;
    }

    Pages::PageView Database::CreateDataPage(
        const ::Memory::IAllocator* allocator,
        const table_id_t tableId,
        const Int pagesToAllocate
    ) {
        page_id_t lowerLimit = INVALID_PAGE_ID;

        const auto extents = this->AllocateNewExtents(
            allocator, pagesToAllocate,
            tableId, lowerLimit
        );

        Pages::PageView page;
        bool pageAllocated = false;

        bool firstExtent = true;
        for (const auto& extentId : extents) {
            const auto firstExtentPageId = (lowerLimit != INVALID_PAGE_ID) && firstExtent
                            ? lowerLimit + 1
                            : Database::CalculateExtentFirstPageId(extentId);

            firstExtent = false;
            for (page_id_t pageId = firstExtentPageId; pageId < firstExtentPageId + Constants::EXTENT_SIZE; pageId++){

                auto pageFreeSpacePage = Database::GetAssociatedPfsPage(this->systemFileKey, this->systemFilenameView, pageId);

                auto dataPage = Storage::StorageManager::Get().CreatePage(this->dataFileKey, this->filenameView, this->_tables[tableId], pageId);

                MultiThreading::WriterGuard lock(&pageFreeSpacePage.Latch());
                MultiThreading::WriterGuard dataPageLock(&dataPage.Latch());
                pageFreeSpacePage.SetPageMetaData(&dataPage);

                if (!pageAllocated){
                    page = std::move(dataPage);
                    pageAllocated = true;
                }
            }
        }

        return page;
    }

    Pages::LargeObjectView Database::CreateLargeDataPage(
        const ::Memory::IAllocator* allocator,
        const Int pagesToAllocate,
        const table_id_t tableOrdinalPosition
    ){
        page_id_t lowerLimit = 0;

        const auto extents = this->AllocateNewExtents(
            allocator, pagesToAllocate,
            tableOrdinalPosition, lowerLimit
        );

        Pages::LargeObjectView page;
        for (const auto& extentId : extents) {
            const auto firstExtentPageId = Database::CalculateExtentFirstPageId(extentId);

            for (page_id_t pageId = firstExtentPageId; pageId < firstExtentPageId + Constants::EXTENT_SIZE; pageId++){

                auto pageFreeSpacePage = Database::GetAssociatedPfsPage(this->systemFileKey, this->systemFilenameView, pageId);

                auto dataPage = Storage::StorageManager::Get().CreateLargeDataPage(
                    this->dataFileKey,
                    this->filenameView,
                    pageId
                );

                MultiThreading::WriterGuard lock(&pageFreeSpacePage.Latch());
                MultiThreading::WriterGuard dataPageLock(&dataPage.Latch());
                pageFreeSpacePage.SetPageMetaData(&dataPage);

                if (!page.IsValid())
                    page = std::move(dataPage);
            }
        }

        return page;
    }

    Pages::IndexPageView Database::CreateIndexPage(
        const ::Memory::IAllocator* allocator,
        const table_id_t tableOrdinalPosition,
        const Int pageCount,
        const Constants::TreeType treeType,
        const page_id_t treeId
    ){
        page_id_t lowerLimit = 0;

        const auto extentsToAllocate =  static_cast<int>(std::ceil(static_cast<float>(pageCount) / Constants::EXTENT_SIZE));

        const auto extents = this->AllocateNewExtents(
            allocator, extentsToAllocate,
            tableOrdinalPosition, lowerLimit
        );

        const auto& table = this->_tables[tableOrdinalPosition];
        const auto indexedColumnDatatypes = table->GetColumnTypeByTreeId(treeType);

        Pages::IndexPageView page;
        bool pageAssigned = false;
        for (const auto& extentId : extents) {
            const auto firstExtentPageId = Database::CalculateExtentFirstPageId(extentId);

            for (page_id_t pageId = firstExtentPageId; pageId < firstExtentPageId + Constants::EXTENT_SIZE; pageId++){

                if (pageId == lowerLimit)
                    continue;

                auto indexPage = Storage::StorageManager::Get().CreateIndexPage(this->dataFileKey, this->filenameView, table, pageId);
                auto pageFreeSpacePage = Database::GetAssociatedPfsPage(this->systemFileKey, this->systemFilenameView, pageId);

                MultiThreading::WriterGuard lock(&pageFreeSpacePage.Latch());
                MultiThreading::WriterGuard indexPageLock(&indexPage.Latch());

                pageFreeSpacePage.SetPageMetaData(&indexPage);

                const auto parentPageId = treeId == INVALID_PAGE_ID
                    ? page.PageId()
                    : treeId;

                indexPage.SetTreeId(parentPageId);
                indexPage.SetTreeType(treeType);
                indexPage.SetKeyTypes(indexedColumnDatatypes);
                indexPage.SetSubKeys(indexedColumnDatatypes.Size());

                if (!pageAssigned){
                    page = std::move(indexPage);
                    pageAssigned = true;
                }
            }
        }

        return page;
    }

    DataStructures::PolymorphicArray<extent_id_t> Database::AllocateNewExtents(
        const ::Memory::IAllocator* allocator,
        const Int pagesToAllocate,
        const table_id_t tableId,
        page_id_t& lowerLimit
    ) {
        const auto extentsToAllocate =  Database::CalculateExtentsToAllocate(pagesToAllocate);

        DataStructures::PolymorphicArray<extent_id_t> allocatedExtents(allocator, extentsToAllocate);

        const auto* table = this->_tables[tableId];
        const page_id_t indexAllocationMapPageId = table->GetHeader().allocationPageId;
        bool newGamPageCreated = false;

        // Step 1: Allocate extents from GAM page
        page_id_t initialGamPageId = 0;
        page_id_t newPageId = 0;
        extent_id_t newExtentId = 0;
        {
            auto gamPage = Storage::StorageManager::Get().GetGlobalAllocationMapPage(
                this->systemFileKey,
                this->systemFilenameView,
                this->header.lastGamPageId
            );

            MultiThreading::WriterGuard gamLock(&this->gamPageMutex);
            initialGamPageId = this->header.lastGamPageId;

            // Handle GAM page overflow - allocate extents across multiple GAM pages if needed
            int remainingExtents = extentsToAllocate;

            while (remainingExtents > 0) {
                if (gamPage.IsFull()) {
                    newGamPageCreated = true;

                    const auto nextGamPageId = Database::CalculateNextGamPageId(gamPage.PageId());

                    gamPage = Storage::StorageManager::Get().CreateGlobalAllocationMapPage(
                        this->systemFileKey,
                        this->systemFilenameView,
                        nextGamPageId
                    );

                    this->header.lastGamPageId = nextGamPageId;
                }

                MultiThreading::WriterGuard gamPageLock(&gamPage.Latch());

                // Allocate one extent from current GAM page
                const auto extentsAllocated = gamPage.AllocateExtentsNoLock(allocatedExtents, remainingExtents);

                if (extentsAllocated == 0)
                    continue;

                remainingExtents-= extentsAllocated;
            }

            if (allocatedExtents.Empty())
                throw std::runtime_error("Failed to allocate any extents");

            // Set output parameters based on first allocated extent
            newExtentId = allocatedExtents[0];
            newPageId = Database::CalculateExtentFirstPageId(newExtentId);
        }

        // Step 2: Set up or update IAM page
        {
            Pages::AllocationPageView tableMapPage;

            const bool isFirstExtent = indexAllocationMapPageId == INVALID_PAGE_ID;

            if (isFirstExtent || newGamPageCreated) {
                // First extent for this table - create new IAM page
                tableMapPage = Storage::StorageManager::Get().CreateAllocationPage(
                    this->dataFileKey,
                    this->filenameView,
                    tableId,
                    newPageId,
                    newExtentId
                );

                lowerLimit = newPageId;

                if (newGamPageCreated && !isFirstExtent) {
                    const auto previousIamPage = Storage::StorageManager::Get().GetAllocationPage(
                        this->dataFileKey,
                        this->filenameView,
                        indexAllocationMapPageId,
                        table
                    );

                    // Update GAM for the IAM page itself
                    MultiThreading::WriterGuard gamLock(&previousIamPage.Latch());
                    previousIamPage.SetNextPageId(tableMapPage.PageId());
                }

                // Update PFS for the IAM page itself
                {
                    const auto pageFreeSpacePage = Database::GetAssociatedPfsPage(
                        this->systemFileKey,
                        this->systemFilenameView,
                        tableMapPage.PageId()
                    );

                    MultiThreading::WriterGuard pageIdLock(&pageFreeSpacePage.Latch());
                    pageFreeSpacePage.SetPageMetaData(&tableMapPage);
                }

                // Update table header with new IAM page ID
                this->_tables[tableId]->UpdateIndexAllocationMapPageId(newPageId);
            }
            else {
                // Table already has IAM page - get it
                tableMapPage = Storage::StorageManager::Get().GetAllocationPage(
                    this->dataFileKey,
                    this->filenameView,
                    indexAllocationMapPageId,
                    table
                );
            }

            // Record all allocated extents in IAM page
            MultiThreading::WriterGuard tableMapLock(&tableMapPage.Latch());
            extent_id_t lastNotRecordedExtent = 0;
            const auto gamPageId = initialGamPageId;

            while (lastNotRecordedExtent != INVALID_EXTENT_ID) {
                //todo handle multiple iams
                lastNotRecordedExtent = tableMapPage.SetExtentsAllocated(allocatedExtents, gamPageId);
            }
        }

        // Step 4: Ensure PFS pages exist for all allocated extents
        {
            MultiThreading::WriterGuard pfsLock(&this->pfsPageMutex);

            for (const auto& extentId : allocatedExtents) {
                const page_id_t extentFirstPageId = Database::CalculateExtentFirstPageId(extentId);

                // Check each page in the extent
                for (page_id_t pageId = extentFirstPageId; pageId < extentFirstPageId + Constants::EXTENT_SIZE; pageId++) {
                    const page_id_t pfsPageId = Database::GetPfsAssociatedPage(pageId);

                    if (pfsPageId > this->header.lastPageFreeSpacePageId) {
                        Storage::StorageManager::Get().CreatePageFreeSpacePage(
                            this->systemFileKey,
                            this->systemFilenameView,
                            pfsPageId
                        );
                        this->header.lastPageFreeSpacePageId = pfsPageId;
                    }
                }
            }
        }

        return allocatedExtents;
    }

    const StorageTypes::Table *Database::GetTable(const table_id_t tableId) const
    {
        if (tableId >= this->_tables.Size())
            throw std::out_of_range("No table with ID: " + std::to_string(tableId) + " exists");

        return this->_tables[tableId];
    }

    Pages::LargeObjectView Database::GetTableLastLargeDataPage(
        const ::Memory::IAllocator* allocator,
        const table_id_t tableId
    )const{
        if (tableId >= this->_tables.Size())
            return Pages::LargeObjectView();

        const auto* table = this->_tables[tableId];

        const auto& tableMapPageId = table->GetHeader().allocationPageId;

        if(tableMapPageId == INVALID_PAGE_ID)
            return Pages::LargeObjectView();

        const auto iamExtentId = Database::CalculateExtentId(tableMapPageId);

        const auto tableMapPage = Storage::StorageManager::Get().GetAllocationPage(
            this->dataFileKey,
            this->filenameView,
            tableMapPageId,
            table
        );

        DataStructures::PolymorphicArray<extent_id_t> allocatedExtents(allocator);
        tableMapPage.GetAllocatedExtents(&allocatedExtents);

        for (const auto &extentId : allocatedExtents)
        {
            const page_id_t firstExtentPageId = Database::CalculateExtentFirstPageId(extentId);

            for (page_id_t pageId = firstExtentPageId; pageId < firstExtentPageId + Constants::EXTENT_SIZE; pageId++)
            {
                const page_id_t correspondingPfsPageId = Database::GetPfsAssociatedPage(pageId);

                const auto pageFreeSpace = Storage::StorageManager::Get().GetPageFreeSpacePage(
                    this->systemFileKey,
                    this->systemFilenameView,
                    correspondingPfsPageId
                );

                if (pageFreeSpace.GetPageType(pageId) != Constants::PageType::LOB)
                    break;

                auto lastLargeDataPage = Storage::StorageManager::Get().GetLargeDataPage(
                    this->dataFileKey,
                    this->filenameView,
                    pageId,
                    this->_tables[tableId]
                );

                if (lastLargeDataPage.PageSize() == 0)
                    return lastLargeDataPage;
            }
        }

        return {};
    }

    Pages::OverflowPageView Database::GetLastOverflowPage(
        const ::Memory::IAllocator* allocator,
        const table_id_t tableId,
        const block_size_t& size
    ){
        if (tableId >= this->_tables.Size())
            return {};

        const auto& table = this->_tables[tableId];

        const auto& allocationPageId = table->GetHeader().allocationPageId;

        if(allocationPageId == INVALID_PAGE_ID)
            return {};

        const auto iamExtentId = Database::CalculateExtentId(allocationPageId);

        const auto tableMapPage = Storage::StorageManager::Get().GetAllocationPage(
            this->dataFileKey,
            this->filenameView,
            allocationPageId,
            table
        );

        DataStructures::PolymorphicArray<extent_id_t> allocatedExtents(allocator);
        tableMapPage.GetAllocatedExtents(&allocatedExtents);
        for (const auto extentId : allocatedExtents)
        {
            const page_id_t firstExtentPageId = Database::CalculateExtentFirstPageId(extentId);

            for (page_id_t pageId = firstExtentPageId; pageId < firstExtentPageId + Constants::EXTENT_SIZE; pageId++)
            {
                const page_id_t correspondingPfsPageId = Database::GetPfsAssociatedPage(pageId);

                const auto pageFreeSpace = Storage::StorageManager::Get().GetPageFreeSpacePage(
                    this->systemFileKey,
                    this->systemFilenameView,
                    correspondingPfsPageId
                );

                if (pageFreeSpace.GetPageType(pageId) != Constants::PageType::OVERFLOWTYPE)
                    break;

                const auto categorySize = Database::GetObjectSizeToCategory(size);

                if(pageFreeSpace.GetPageSizeCategory(pageId) <= categorySize)
                    continue;

                auto lastOverflowPage = Storage::StorageManager::Get().GetOverflowPage(
                    this->dataFileKey,
                    this->filenameView,
                    pageId,
                    table
                );

                if (lastOverflowPage.BytesLeft() >= size)
                    return lastOverflowPage;
            }
        }

        return this->CreateOverflowPage(allocator, 1, tableId);
    }

    Pages::LargeObjectView Database::GetLargeDataPage(const page_id_t pageId, const table_id_t tableId)const
    {
        const auto extentId = Database::CalculateExtentId(pageId);

        return Storage::StorageManager::Get().GetLargeDataPage(
            this->dataFileKey,
            this->filenameView,
            pageId,
            this->_tables[tableId]
        );

//        if (tableId >= this->_tables.size())
//            return nullptr;
//
//        const page_id_t tableMapPageId = this->_tables[tableId]->GetTableHeader().indexAllocationMapPageId;
//
//        const IndexAllocationMapPage *tableMapPage = StorageManager::Get().GetIndexAllocationMapPage(this->filename, tableMapPageId);
//
//        vector<extent_id_t> allocatedExtents;
//        tableMapPage->GetAllocatedExtents(&allocatedExtents);
//
//        extent_id_t associatedExtentId = 0;
//        bool extentFound = false;
//        for (const auto &extentId : allocatedExtents)
//        {
//            const page_id_t firstExtentPageId = Database::CalculateSystemPageOffsetByExtentId(extentId);
//
//            if (pageId >= firstExtentPageId && pageId < firstExtentPageId + EXTENT_SIZE)
//            {
//                associatedExtentId = extentId;
//                extentFound = true;
//                break;
//            }
//        }
//
//        return (extentFound)
//                   ? StorageManager::Get().GetLargeDataPage(this->filename, pageId, associatedExtentId, this->_tables[tableId])
//                   : nullptr;
    }

    DataTypes::StringView Database::GetFileName() const { return this->filenameView; }

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

    const DataStructures::Array<StorageTypes::Table*>&  Database::GetTables() const{ return this->_tables; }

    DataTypes::StringView Database::GetSystemFilename() const{ return this->systemFilenameView; }

    Storage::FileKey Database::GetDataFileKey() const{ return this->dataFileKey; }

    Storage::FileKey Database::GetSystemFileKey() const{ return this->systemFileKey; }

    DatabaseHeader::DatabaseHeader(){
        this->numberOfTables = 0;
        this->lastTableId = 0;
        this->lastPageFreeSpacePageId = 0;
        this->lastGamPageId = 0;
    }

    DatabaseHeader::DatabaseHeader(const table_number_t numberOfTables, const page_id_t lastPageFreeSpacePageId, const page_id_t lastGamPageId){
        this->numberOfTables = numberOfTables;
        this->lastTableId = 0;
        this->lastPageFreeSpacePageId = lastPageFreeSpacePageId;
        this->lastGamPageId = lastGamPageId;
    }
}

