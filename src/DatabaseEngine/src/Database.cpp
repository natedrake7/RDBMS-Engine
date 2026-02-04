#include "../include/Database.h"

#include "../include/SystemDatabases/SystemCatalog.h"

#include <cstdint>
#include <stdexcept>
#include <vector>
#include "../include/DatabaseConstants.h"
#include "../include/DataStorage/Table.h"
#include "../include/DataStorage/Column.h"
#include "../include/BufferPool/StorageManager.h"
#include "../../Server/include/Server.h"
#include "../../Systemic/include/Guards/WriterGuard.h"
#include "../include/Logger/WriteAheadLogger.h"

#include <cmath>
#include <iostream>

namespace DatabaseEngine
{
    void Database::WriteHeaderToFile() const
    {
        const auto metaDataPage = Storage::StorageManager::Get().GetHeaderPage(this->systemFilename);
        metaDataPage.SetDatabaseHeader(this->header);
    }

    bool Database::IsSystemPage(const page_id_t pageId) { return pageId == 0 || pageId == 1 || pageId == 2 || pageId % PAGE_FREE_SPACE_SIZE == 1 || pageId % GAM_NUMBER_OF_PAGES == 2; }

    page_id_t Database::GetPfsAssociatedPage(const page_id_t pageId) {
        const auto numOfGamPages = (pageId / GAM_NUMBER_OF_PAGES);

        const auto numOfPfsPages = (pageId / PAGE_FREE_SPACE_SIZE) + 1;

        return numOfPfsPages > 1 ? numOfPfsPages + numOfGamPages + 1 : numOfPfsPages + numOfGamPages;
    }

    page_id_t Database::GetGamAssociatedPage(const page_id_t pageId) {
        const auto numOfGamPages = (pageId / GAM_NUMBER_OF_PAGES) + 2;

        const auto numOfPfsPages = (pageId / PAGE_FREE_SPACE_SIZE);

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

    page_id_t Database::CalculateSystemPageOffset(const page_id_t pageId)
    {
        page_id_t pfsPages = pageId / PAGE_FREE_SPACE_SIZE + 1;

//        if (pfsPages == 0)
//            pfsPages = 1;

        page_id_t gamPages = pageId / GAM_NUMBER_OF_PAGES + 1;

//        if (gamPages == 0)
//            gamPages = 1;

        return pageId ;//+ pfsPages + gamPages + 1;
    }

    page_id_t Database::CalculateNextGamPageId(const page_id_t currentGamPageId) {
        return currentGamPageId + NEXT_GAM_PAGE_ID_OFFSET;
    }

    byte_t Database::GetObjectSizeToCategory(const row_size_t &size)
    {
      const float freeSpacePercentage = static_cast<float>(size) / PAGE_SIZE;

      // Direct mapping to 7levels (0-7)
      return static_cast<byte_t>(freeSpacePercentage * 7);
    }

    page_id_t Database::CalculateExtentFirstPageId(const extent_id_t &extentId){
        return extentId * EXTENT_SIZE;
    }

    page_id_t Database::CalculateGamPageId(const extent_id_t &extentId) {
        return static_cast<page_id_t>(std::ceil(static_cast<float>(extentId) / static_cast<float>(GAM_PAGE_SIZE)) + 2);
    }

    extent_id_t Database::CalculateExtentId(const page_id_t pageId){
        return pageId / 8;
    }

    std::string Database::CreateDatabasePath(const std::string & dbName){ return dbName + "/" + dbName; }

    void Database::PopulateFilenames(const std::string& dbName){
        const auto& path = Database::CreateDatabasePath(dbName);

        this->filename = path + ".db";
        this->fileExtension = ".db";
        this->name = dbName;
        this->systemFilename = path + "_sys" + ".db";
    }

    Database::Database(const std::string &dbName, const std::vector<Headers::sysTable>& tables){
        this->PopulateFilenames(dbName);

        const auto headerPage = Storage::StorageManager::Get().GetHeaderPage(this->systemFilename);

        this->header = *headerPage.GetDatabaseHeaderPtr();

        for (int i = 0; i < tables.size(); i++) {
          HashSet<std::string> primaryKeysSet(tables[i].primaryKey);
          Headers::Index index;

          for(int j = 0;j < tables[i].columns.size(); j++){
              const auto& column = tables[i].columns[j];

              if(primaryKeysSet.Contains(column.name))
                index.columns.push_back(j);
          }

          this->CreateTable(tables[i], headerPage.GetTableHeader(i), index, i);
        }
    }

    Database::Database(const std::string &dbName, const bool& isServerInitialization) {
        static auto& catalog = SystemCatalog::Get();

        this->PopulateFilenames(dbName);

        const auto headerPage = Storage::StorageManager::Get().GetHeaderPage(this->systemFilename);

        this->header = *headerPage.GetDatabaseHeaderPtr();

        if (isServerInitialization)
            return;

        //query get from masterDb
        const auto& masterDbData = catalog.SelectTables(dbName);
        const auto& headerPageTables = headerPage.GetTableHeaders();

        if (headerPageTables.size() != masterDbData.size())
            return;

        for (int i = 0;i < masterDbData.size(); i++)
            this->CreateTable(masterDbData[i], headerPageTables[i]);
    }

    Database::~Database()
    {
        // save db header;
        this->WriteHeaderToFile();

        for (const auto &dbTable : this->tables)
            delete dbTable;
    }

    std::vector<Logging::LogEntry> Database::RecoverLogs(){
        return {};
        // return this->writeAheadLogger->RecoverLogs(this->tables);
    }

    void Database::EnterRecoveryMode()const{
        const auto logs = DatabaseEngine::Database::RecoverLogs();

        if (logs.empty()) {
            std::cout << "No logs to recover." << std::endl;
            return;
        }
        for (const auto& log : logs)
            this->ApplyRecoveryLog(log);
    }

    void Database::ApplyRecoveryLog(const Logging::LogEntry &logEntry)const{
        if (!logEntry.ValidateIntegrity())
            return;

        if (Logging::RowAffectedOperationTypes.Contains(logEntry.operation)) {
            auto* table = this->tables.at(logEntry.tableOrdinalPosition);

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
        return static_cast<int>(std::ceil(static_cast<float>(pagesToAllocate) / static_cast<float>(EXTENT_SIZE)));
    }

    void Database::LogCheckPoint(Logging::CheckPoint &checkPoint) {
        Logging::WriteAheadLogger::Get().LogCheckPoint(checkPoint);
    }

    Logging::CheckPoint Database::LogRowInsert(
        const StorageTypes::InsertPayload& payload,
        const transaction_id_t transactionId,
        const table_id_t tableOrdinal
    ) {
        static auto& logger = Logging::WriteAheadLogger::Get();

        std::vector<char> buffer;
        buffer.resize(payload.Size());
        std::memcpy(buffer.data(), payload.Data(), payload.Size());

        const auto logEntry = logger.CreateLogEntry(
            transactionId,
            Logging::OperationType::InsertRow,
            tableOrdinal,
            buffer
        );

        return logger.Log(logEntry);
    }

    Logging::CheckPoint Database::LogRowBatchInsert(
        std::vector<char>& buffer,
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

    StorageTypes::Table *Database::CreateTable(
        const table_id_t tableId,
        const Int ordinalPosition,
        const std::vector<StorageTypes::Column *> &columns,
        const Headers::Index *clusteredKeyIndexes,
        const std::vector<Headers::Index> *nonClusteredIndexes)
    {
        auto *table = new StorageTypes::Table(tableId, ordinalPosition, columns, this, clusteredKeyIndexes, nonClusteredIndexes);

        this->tables.push_back(table);
        this->header.numberOfTables = this->tables.size();

        this->tableIdsDictionary.Add(tableId, ordinalPosition);

        return table;
    }

    void Database::CreateTable(const Headers::TableHeader& masterDbHeader, const StorageTypes::TableHeader &tableHeader){
        static auto& catalog = SystemCatalog::Get();

        auto *table = new StorageTypes::Table(masterDbHeader, tableHeader, this);

        const auto& masterDbColumns = catalog.SelectColumns(masterDbHeader.id);

        for (const auto & masterDbColumn : masterDbColumns) {
            if (masterDbColumn.isSystem)
                continue;

            table->AddColumn(new StorageTypes::Column(masterDbColumn, table));
        }

        //TODO
        //maybe add in a single function
        table->RetrieveColumnHeadersFromCatalog();
        table->RetrieveIdentityColumnsFromCatalog();
        table->RetrieveIndexesFromCatalog();
        table->RetrieveDefaultValuesFromCatalog();

        this->tables.push_back(table);
    }

    void Database::CreateTable(const Headers::sysTable &sysHeader, const StorageTypes::TableHeader &tableHeader, const Headers::Index& primaryKey, const Int ordinalPosition){
        this->tables.push_back(new StorageTypes::Table(sysHeader, tableHeader, primaryKey, this, ordinalPosition));
    }

    void Database::InferSchemaFromColumns(const std::vector<StorageTypes::Column*>& columns){
        HashSet<std::string> schemaNamesSet;


    }

    //    Table *Database::OpenTable(const string& schemaName, const string &tableName) const
//    {
//        for (const auto &table : this->tables)
//        {
//            if (table->GetTableName() == tableName
//                && table->GetSchema() == schemaName)
//                return table;
//        }
//
//        return nullptr;
//    }

    StorageTypes::Table * Database::OpenTable(const table_id_t tableId) const{
        return this->tables.at(tableId);
    }

    // StorageTypes::Table * Database::OpenTableById(const table_id_t tableId) const{
    //     return this->tables.at(this->tableIdsDictionary.Get(tableId));
    // }

    void Database::DeleteTable(const std::string& tableName)
    {
        const StorageTypes::Table* table = nullptr;
        std::vector<StorageTypes::Table*>::iterator it;

        for (it = this->tables.begin(); it != this->tables.end(); it++)
        {
//            if ((*it)->GetTableName() == tableName)
//            {
//                table = *it;
//                break;
//            }
        }

        if (table == nullptr)
            return;

        const auto& tableHeader = table->GetHeader();

        const auto extentId = Database::CalculateExtentId(tableHeader.indexAllocationMapPageId);

        // const auto indexAllocationMapPage = Storage::StorageManager::Get().GetAllocationPage(this->filename, tableHeader.indexAllocationMapPageId, table);
        //
        // if (indexAllocationMapPage.Get() == nullptr)
        // {
        //     this->tables.erase(it);
        //     delete table;
        //     return;
        // }

        const page_id_t globalAllocationMapPageId = Database::GetGamAssociatedPage(tableHeader.indexAllocationMapPageId);

        const auto globalAllocationMapPage = Storage::StorageManager::Get().GetGlobalAllocationMapPage(this->filename, globalAllocationMapPageId);

        std::vector<extent_id_t> allocatedExtents;
        // indexAllocationMapPage.GetAllocatedExtents(&allocatedExtents);

        //deallocate pages as well
        for (const auto& extentId : allocatedExtents)
        {

        }
        //deletes all rows
        // table->Delete(nullptr);
    }

    void CreateDatabase(const std::string &dbName){
        const auto& path = Database::CreateDatabasePath(dbName);

        Storage::StorageManager::Get().CreateFile(path, ".db");

        const auto sysDbName = path + "_sys";

        Storage::StorageManager::Get().CreateFile(sysDbName, ".db");

        constexpr page_id_t firstGamePageId = 2;
        constexpr page_id_t firstPfsPageId = 1;
        
        Storage::StorageManager::Get().CreateGlobalAllocationMapPage(sysDbName + ".db", firstGamePageId);
        Storage::StorageManager::Get().CreatePageFreeSpacePage(sysDbName + ".db", firstPfsPageId);

        const auto headerPage = Storage::StorageManager::Get().CreateHeaderPage(sysDbName + ".db");

        headerPage.SetDatabaseHeader(DatabaseHeader(0, firstPfsPageId, firstGamePageId));
    }

    Database* UseSystemDatabase(const std::string & dbName, const std::vector<Headers::sysTable> & tables){
      return nullptr;
    }

    void Database::DeleteDatabase() const
    {
        const std::string path = this->filename;

        if (remove(path.c_str()) != 0)
            throw std::runtime_error("Database " + this->filename + " could not be deleted");
    }

    Pages::PageView Database::FindOrAllocateNextDataPage(
        Pages::PageFreeSpaceView &pageFreeSpacePage,
        const page_id_t pageId,
        const page_id_t extentFirstPageId,
        const StorageTypes::Table &table,
        const Int pageToAllocate
    )
    {
        Pages::PageView page;
        bool pageAllocated = false;
        if (pageId < extentFirstPageId + EXTENT_SIZE - 1){
            for (page_id_t nextLeafPageId = pageId + 1; nextLeafPageId < extentFirstPageId + EXTENT_SIZE; nextLeafPageId++){
                if (pageFreeSpacePage.GetPageSizeCategory(nextLeafPageId) == 0)
                    continue;

                page = Storage::StorageManager::Get().GetPage(this->filename, nextLeafPageId, &table);

                if (page.PageSize() == 0)
                    break;

                pageAllocated = true;
            }
        }

        if (!pageAllocated || page.PageSize() > 0){
            page = this->CreateDataPage(table.GetTableId(), pageToAllocate);
            pageFreeSpacePage = Database::GetAssociatedPfsPage(this->filename, page.PageId());
        }

        return page;
    }

    Pages::PageFreeSpaceView Database::GetAssociatedPfsPage(const std::string& filename, const page_id_t  pageId)
    {
        const page_id_t pageFreeSpacePageId = Database::GetPfsAssociatedPage(pageId);

        return Storage::StorageManager::Get().GetPageFreeSpacePage(filename, pageFreeSpacePageId);
    }

    void Database::TruncateTable(const table_id_t  tableId) const{
        auto* table = this->tables.at(tableId);

        auto indexAllocationMapPageId = table->GetHeader().indexAllocationMapPageId;

        while (indexAllocationMapPageId != INVALID_PAGE_ID)
        {
            const auto iamExtentId = Database::CalculateExtentId(indexAllocationMapPageId);

            const auto tableMapPage = Storage::StorageManager::Get().GetAllocationPage(this->filename, indexAllocationMapPageId, table);

            std::vector<extent_id_t> allocatedExtents;
            tableMapPage.GetAllocatedExtents(&allocatedExtents);

            const page_id_t globalAllocationMapPageId = Database::GetGamAssociatedPage(indexAllocationMapPageId);

            auto gamPage = Storage::StorageManager::Get().GetGlobalAllocationMapPage(this->filename, globalAllocationMapPageId);

            indexAllocationMapPageId = tableMapPage.NextPageId();

            for (const auto& extentId : allocatedExtents)
            {
                const page_id_t extentFirstPageId = Database::CalculateExtentFirstPageId(extentId);

                for (page_id_t pageId = extentFirstPageId; pageId < extentFirstPageId + EXTENT_SIZE; pageId++)
                {
                    const page_id_t pageFreeSpacePageId = Database::GetPfsAssociatedPage(pageId);
                
                    auto pageFreeSpacePage = Storage::StorageManager::Get().GetPageFreeSpacePage(this->systemFilename, pageFreeSpacePageId);

                    pageFreeSpacePage.SetPageFreed(pageId);
                }

                gamPage.DeallocateExtent(extentId);
            }
        }

        table->UpdateIndexAllocationMapPageId(INVALID_PAGE_ID);
    }

    Pages::OverflowPageView Database::CreateOverflowPage(const Int pagesToAllocate, const table_id_t tableOrdinalPosition){
        page_id_t lowerLimit = 0;

        const auto extentsToAllocate =  static_cast<int>(std::ceil(static_cast<float>(pagesToAllocate) / EXTENT_SIZE));

        const auto extents = this->AllocateNewExtents(extentsToAllocate, tableOrdinalPosition, lowerLimit);

        Pages::OverflowPageView firstPage;
        for (const auto& extentId : extents) {
            const auto firstExtentPageId = Database::CalculateExtentFirstPageId(extentId);

            for (page_id_t pageId = firstExtentPageId; pageId < firstExtentPageId + EXTENT_SIZE; pageId++){

                if (pageId == lowerLimit)
                    continue;

                auto pageFreeSpacePage = Database::GetAssociatedPfsPage(this->systemFilename, pageId);

                auto overflowPage = Storage::StorageManager::Get().CreateOverflowPage(this->filename, pageId);

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
        const table_id_t tableId,
        const Int pagesToAllocate
    ) {
        page_id_t lowerLimit = INVALID_PAGE_ID;

        const auto extents = this->AllocateNewExtents(pagesToAllocate, tableId, lowerLimit);

        Pages::PageView page;
        bool pageAllocated = false;

        bool firstExtent = true;
        for (const auto& extentId : extents) {
            const auto firstExtentPageId = (lowerLimit != INVALID_PAGE_ID) && firstExtent
                            ? lowerLimit + 1
                            : Database::CalculateExtentFirstPageId(extentId);

            firstExtent = false;
            for (page_id_t pageId = firstExtentPageId; pageId < firstExtentPageId + EXTENT_SIZE; pageId++){

                auto pageFreeSpacePage = Database::GetAssociatedPfsPage(this->systemFilename, pageId);

                auto dataPage = Storage::StorageManager::Get().CreatePage(this->filename, this->tables[tableId], pageId);

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

    Pages::LargeObjectView Database::CreateLargeDataPage(const Int pagesToAllocate, const table_id_t tableOrdinalPosition){
        page_id_t lowerLimit = 0;

        const auto extents = this->AllocateNewExtents(pagesToAllocate, tableOrdinalPosition, lowerLimit);

        Pages::LargeObjectView page;
        for (const auto& extentId : extents) {
            const auto firstExtentPageId = Database::CalculateExtentFirstPageId(extentId);

            for (page_id_t pageId = firstExtentPageId; pageId < firstExtentPageId + EXTENT_SIZE; pageId++){

                auto pageFreeSpacePage = Database::GetAssociatedPfsPage(this->systemFilename, pageId);

                auto dataPage = Storage::StorageManager::Get().CreateLargeDataPage(this->filename, pageId);

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
        const table_id_t tableOrdinalPosition,
        const Int pageCount,
        const TreeType treeType,
        const page_id_t treeId
    ){
        page_id_t lowerLimit = 0;

        const auto extentsToAllocate =  static_cast<int>(std::ceil(static_cast<float>(pageCount) / EXTENT_SIZE));

        const auto extents = this->AllocateNewExtents(extentsToAllocate, tableOrdinalPosition, lowerLimit);

        const auto& table = this->tables.at(tableOrdinalPosition);
        const auto indexedColumnDatatypes = table->GetColumnTypeByTreeId(treeType);

        Pages::IndexPageView page;
        bool pageAssigned = false;
        for (const auto& extentId : extents) {
            const auto firstExtentPageId = Database::CalculateExtentFirstPageId(extentId);

            for (page_id_t pageId = firstExtentPageId; pageId < firstExtentPageId + EXTENT_SIZE; pageId++){

                if (pageId == lowerLimit)
                    continue;

                auto indexPage = Storage::StorageManager::Get().CreateIndexPage(this->filename, table, pageId);
                auto pageFreeSpacePage = Database::GetAssociatedPfsPage(this->systemFilename, pageId);

                MultiThreading::WriterGuard lock(&pageFreeSpacePage.Latch());
                MultiThreading::WriterGuard indexPageLock(&indexPage.Latch());

                pageFreeSpacePage.SetPageMetaData(&indexPage);

                if (!pageAssigned){
                    page = std::move(indexPage);
                    pageAssigned = true;
                }

                const auto parentPageId = treeId == INVALID_PAGE_ID ? page.PageId() : treeId;

                indexPage.SetTreeId(parentPageId);
                indexPage.SetTreeType(treeType);
                indexPage.SetKeyTypes(indexedColumnDatatypes);
                indexPage.SetSubKeys(indexedColumnDatatypes.size());
            }
        }

        return page;
    }

    std::vector<extent_id_t> Database::AllocateNewExtents(
        const Int pagesToAllocate,
        const table_id_t tableId,
        page_id_t& lowerLimit
    ) {
        const auto extentsToAllocate =  Database::CalculateExtentsToAllocate(pagesToAllocate);

        std::vector<extent_id_t> allocatedExtents;
        allocatedExtents.reserve(extentsToAllocate);

        const auto* table = this->tables[tableId];
        const page_id_t indexAllocationMapPageId = table->GetHeader().indexAllocationMapPageId;
        bool newGamPageCreated = false;

        // Step 1: Allocate extents from GAM page
        page_id_t initialGamPageId = 0;
        page_id_t newPageId = 0;
        extent_id_t newExtentId = 0;
        {
            auto gamPage = Storage::StorageManager::Get().GetGlobalAllocationMapPage(this->systemFilename, this->header.lastGamPageId);

            MultiThreading::WriterGuard gamLock(&this->gamPageMutex);
            initialGamPageId = this->header.lastGamPageId;

            // Handle GAM page overflow - allocate extents across multiple GAM pages if needed
            int remainingExtents = extentsToAllocate;

            while (remainingExtents > 0) {
                if (gamPage.IsFull()) {
                    newGamPageCreated = true;

                    const auto nextGamPageId = Database::CalculateNextGamPageId(gamPage.PageId());

                    gamPage = Storage::StorageManager::Get().CreateGlobalAllocationMapPage(this->systemFilename, nextGamPageId);

                    this->header.lastGamPageId = nextGamPageId;
                }

                MultiThreading::WriterGuard gamPageLock(&gamPage.Latch());

                // Allocate one extent from current GAM page
                const auto extentsAllocated = gamPage.AllocateExtentsNoLock(allocatedExtents, remainingExtents);

                if (extentsAllocated == 0)
                    continue;

                remainingExtents-= extentsAllocated;
            }

            if (allocatedExtents.empty())
                throw std::runtime_error("Failed to allocate any extents");

            // Set output parameters based on first allocated extent
            newExtentId = allocatedExtents.front();
            newPageId = Database::CalculateExtentFirstPageId(newExtentId);
        }

        // Step 2: Set up or update IAM page
        {
            Pages::AllocationPageView tableMapPage;

            const bool isFirstExtent = indexAllocationMapPageId == INVALID_PAGE_ID;

            if (isFirstExtent || newGamPageCreated) {
                // First extent for this table - create new IAM page
                tableMapPage = Storage::StorageManager::Get().CreateAllocationPage(
                    this->filename,
                    tableId,
                    newPageId,
                    newExtentId
                );

                lowerLimit = newPageId;

                if (newGamPageCreated && !isFirstExtent) {
                    const auto previousIamPage = Storage::StorageManager::Get().GetAllocationPage(
                        this->filename,
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
                        this->systemFilename,
                        tableMapPage.PageId()
                    );

                    MultiThreading::WriterGuard pageIdLock(&pageFreeSpacePage.Latch());
                    pageFreeSpacePage.SetPageMetaData(&tableMapPage);
                }

                // Update table header with new IAM page ID
                this->tables[tableId]->UpdateIndexAllocationMapPageId(newPageId);
            }
            else {
                // Table already has IAM page - get it
                tableMapPage = Storage::StorageManager::Get().GetAllocationPage(
                    this->filename,
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
                for (page_id_t pageId = extentFirstPageId; pageId < extentFirstPageId + EXTENT_SIZE; pageId++) {
                    const page_id_t pfsPageId = Database::GetPfsAssociatedPage(pageId);

                    if (pfsPageId > this->header.lastPageFreeSpacePageId) {
                        Storage::StorageManager::Get().CreatePageFreeSpacePage(this->systemFilename, pfsPageId);
                        this->header.lastPageFreeSpacePageId = pfsPageId;
                    }
                }
            }
        }

        return allocatedExtents;
    }

    const StorageTypes::Table *Database::GetTable(const table_id_t tableId) const
    {
        if (tableId >= this->tables.size())
            throw std::out_of_range("No table with ID: " + std::to_string(tableId) + " exists");

        return this->tables[tableId];
    }

    Pages::LargeObjectView Database::GetTableLastLargeDataPage(const table_id_t tableId)const
    {
        if (tableId >= this->tables.size())
            return Pages::LargeObjectView();

        const auto* table = this->tables[tableId];

        const auto& tableMapPageId = table->GetHeader().indexAllocationMapPageId;

        if(tableMapPageId == INVALID_PAGE_ID)
            return Pages::LargeObjectView();

        const auto iamExtentId = Database::CalculateExtentId(tableMapPageId);

        const auto tableMapPage = Storage::StorageManager::Get().GetAllocationPage(this->filename, tableMapPageId, table);

        std::vector<extent_id_t> allocatedExtents;
        tableMapPage.GetAllocatedExtents(&allocatedExtents);

        for (const auto &extentId : allocatedExtents)
        {
            const page_id_t firstExtentPageId = Database::CalculateExtentFirstPageId(extentId);

            for (page_id_t pageId = firstExtentPageId; pageId < firstExtentPageId + EXTENT_SIZE; pageId++)
            {
                const page_id_t correspondingPfsPageId = Database::GetPfsAssociatedPage(pageId);

                const auto pageFreeSpace = Storage::StorageManager::Get().GetPageFreeSpacePage(this->filename, correspondingPfsPageId);

                if (pageFreeSpace.GetPageType(pageId) != PageType::LOB)
                    break;

                auto lastLargeDataPage = Storage::StorageManager::Get().GetLargeDataPage(this->filename, pageId, this->tables[tableId]);

                if (lastLargeDataPage.PageSize() == 0)
                    return lastLargeDataPage;
            }
        }

        return {};
    }

    Pages::OverflowPageView Database::GetLastOverflowPage(const table_id_t  tableId, const block_size_t& size){
        if (tableId >= this->tables.size())
            return {};

        const auto& table = this->tables[tableId];

        const auto& tableMapPageId = table->GetHeader().indexAllocationMapPageId;

        if(tableMapPageId == INVALID_PAGE_ID)
            return {};

        const auto iamExtentId = Database::CalculateExtentId(tableMapPageId);

        const auto tableMapPage = Storage::StorageManager::Get().GetAllocationPage(this->filename, tableMapPageId, table);

        std::vector<extent_id_t> allocatedExtents;
        tableMapPage.GetAllocatedExtents(&allocatedExtents);

        for (const auto &extentId : allocatedExtents)
        {
            const page_id_t firstExtentPageId = Database::CalculateExtentFirstPageId(extentId);

            for (page_id_t pageId = firstExtentPageId; pageId < firstExtentPageId + EXTENT_SIZE; pageId++)
            {
                const page_id_t correspondingPfsPageId = Database::GetPfsAssociatedPage(pageId);

                const auto pageFreeSpace = Storage::StorageManager::Get().GetPageFreeSpacePage(this->systemFilename, correspondingPfsPageId);

                if (pageFreeSpace.GetPageType(pageId) != PageType::OVERFLOWTYPE)
                    break;

                const auto categorySize = Database::GetObjectSizeToCategory(size);

                if(pageFreeSpace.GetPageSizeCategory(pageId) <= categorySize)
                    continue;

                auto lastOverflowPage = Storage::StorageManager::Get().GetOverflowPage(this->filename, pageId, table);

                if (lastOverflowPage.BytesLeft() >= size)
                    return lastOverflowPage;
            }
        }

        return this->CreateOverflowPage(1, tableId);
    }

    Pages::LargeObjectView Database::GetLargeDataPage(const page_id_t pageId, const table_id_t tableId)const
    {
        const auto extentId = Database::CalculateExtentId(pageId);

        return Storage::StorageManager::Get().GetLargeDataPage(this->filename, pageId, this->tables[tableId]);

//        if (tableId >= this->tables.size())
//            return nullptr;
//
//        const page_id_t tableMapPageId = this->tables[tableId]->GetTableHeader().indexAllocationMapPageId;
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
//                   ? StorageManager::Get().GetLargeDataPage(this->filename, pageId, associatedExtentId, this->tables[tableId])
//                   : nullptr;
    }

    std::string Database::GetFileName() const { return this->filename; }

    void Database::GetIdentityColumns()const{
            for(const auto& table: this->tables)
                table->RetrieveIdentityColumnsFromCatalog();
    }

    void Database::UpdateIdentityManagersIds()const{
        for(const auto& table: this->tables)
            table->UpdateCatalogIdentityColumns();
    }

    void Database::GetColumnsHeaders() const{
            for (const auto& table : this->tables)
                table->RetrieveColumnHeadersFromCatalog();
    }

    void Database::GetDefaultValues() const{
            for (const auto& table : this->tables)
                table->RetrieveDefaultValuesFromCatalog();
    }

    void Database::GetIndexes() const{
            for (const auto& table : this->tables)
                table->RetrieveIndexesFromCatalog();
    }

    void Database::GetTableHeaders() const{
    }

    void Database::UpdateMasterDatabase()const{
            for(const auto& table: this->tables)
                table->UpdateSystemCatalog();
    }

    const std::vector<StorageTypes::Table *> & Database::GetTables() const{ return this->tables; }

    std::string Database::GetSystemFilename() const{ return this->systemFilename; }

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

    DatabaseHeader::DatabaseHeader(const DatabaseHeader &dbHeader){
        this->numberOfTables = dbHeader.numberOfTables;
        this->lastTableId = dbHeader.lastTableId;
        this->lastPageFreeSpacePageId = dbHeader.lastPageFreeSpacePageId;
        this->lastGamPageId = dbHeader.lastGamPageId;
    }

    DatabaseHeader &DatabaseHeader::operator=(const DatabaseHeader &dbHeader){
        if (&dbHeader == this)
            return *this;

        this->numberOfTables = dbHeader.numberOfTables;
        this->lastTableId = dbHeader.lastTableId;
        this->lastGamPageId = dbHeader.lastGamPageId;
        this->lastPageFreeSpacePageId = dbHeader.lastPageFreeSpacePageId;

        return *this;
    }
}

