#include "../include/Database.h"
#include <cstdint>
#include <stdexcept>
#include <vector>
#include "../include/Pages/HeaderPage.h"
#include "../include/Pages/GlobalAllocationMapPage.h"
#include "../include/Pages/PageFreeSpacePage.h"
#include "../include/Pages/IndexAllocationMapPage.h"
#include "../include/Pages/IndexPage.h"
#include "../include/Pages/LargeObjectPage.h"
#include "../include/Constants.h"
#include "../include/DataStorage/Table.h"
#include "../include/DataStorage/Column.h"
#include "../include/DataStorage/Row.h"
#include "../include/BufferPool/StorageManager.h"
#include "../../Server/include/Server.h"
#include "../../Systemic/include/Guards/WriterGuard.h"
#include "../include/Logger/WriteAheadLogger.h"

#include <iostream>

using namespace Pages;
using namespace DatabaseEngine::StorageTypes;
using namespace Storage;
using namespace Indexing;
using namespace std;
using namespace ByteMaps;

namespace DatabaseEngine
{
    void Database::WriteHeaderToFile() const
    {
        auto metaDataPage = StorageManager::Get().GetHeaderPage(this->systemFilename);

        metaDataPage->SetDbHeader(this->header);
    }

    bool Database::IsSystemPage(const page_id_t &pageId) { return pageId == 0 || pageId == 1 || pageId == 2 || pageId % PAGE_FREE_SPACE_SIZE == 1 || pageId % GAM_NUMBER_OF_PAGES == 2; }

    page_id_t Database::GetPfsAssociatedPage(const page_id_t &pageId) {
        const auto numOfGamPages = (pageId / GAM_NUMBER_OF_PAGES);

        const auto numOfPfsPages = (pageId / PAGE_FREE_SPACE_SIZE) + 1;

        return numOfPfsPages > 1 ? numOfPfsPages + numOfGamPages + 1 : numOfPfsPages + numOfGamPages;
    }

    page_id_t Database::GetGamAssociatedPage(const page_id_t &pageId) {
        const auto numOfGamPages = (pageId / GAM_NUMBER_OF_PAGES) + 2;

        const auto numOfPfsPages = (pageId / PAGE_FREE_SPACE_SIZE);

        return numOfGamPages > 2 ? numOfPfsPages + numOfGamPages + 1 : numOfPfsPages + numOfGamPages;
    }

//    page_id_t Database::GetPfsAssociatedPage(const page_id_t &pageId) {
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
//    page_id_t Database::GetGamAssociatedPage(const page_id_t &pageId) {
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

    page_id_t Database::CalculateSystemPageOffset(const page_id_t &pageId)
    {
        page_id_t pfsPages = pageId / PAGE_FREE_SPACE_SIZE + 1;

//        if (pfsPages == 0)
//            pfsPages = 1;

        page_id_t gamPages = pageId / GAM_NUMBER_OF_PAGES + 1;

//        if (gamPages == 0)
//            gamPages = 1;

        return pageId ;//+ pfsPages + gamPages + 1;
    }

    page_id_t Database::CalculateNextGamPageId(const page_id_t &currentGamPageId) {
        return currentGamPageId + NEXT_GAM_PAGE_ID_OFFSET;
    }

    Constants::byte Database::GetObjectSizeToCategory(const row_size_t &size)
    {
      const float freeSpacePercentage = static_cast<float>(size) / PAGE_SIZE;

      // Direct mapping to 7levels (0-7)
      return static_cast<Constants::byte>(freeSpacePercentage * 7);
    }

    page_id_t Database::CalculateFirstPageIdByExtentId(const extent_id_t &extentId)
    {
        const page_id_t pageId = extentId * EXTENT_SIZE;
        //
        // const page_id_t pfsPages = pageId / PAGE_FREE_SPACE_SIZE + 1;
        //
        // const page_id_t gamPages = pageId / GAM_NUMBER_OF_PAGES + 1;

        return pageId; //+ pfsPages + gamPages + 1;
    }

    extent_id_t Database::CalculateExtentIdByPageId(const page_id_t &pageId)
    {
        const page_id_t pfsPages = pageId / PAGE_FREE_SPACE_SIZE + 1;

        const page_id_t gamPages = pageId / GAM_NUMBER_OF_PAGES + 1;

        return pageId / 8;//(pageId - ( pfsPages + gamPages + 1 )) / 8 ;
    }

    string Database::CreateDatabasePath(const string & dbName){ return dbName + "/" + dbName; }

    void Database::PopulateFilenames(const std::string& dbName){
        const auto& path = Database::CreateDatabasePath(dbName);

        this->filename = path + ".db";
        this->fileExtension = ".db";
        this->name = dbName;
        this->systemFilename = path + "_sys" + ".db";
    }

    Database::Database(const string &dbName, const vector<Headers::sysTable>& tables){
        this->PopulateFilenames(dbName);

        const auto headerPage = StorageManager::Get().GetHeaderPage(this->systemFilename);

        this->header = *headerPage->GetDatabaseHeader();

        const auto& headerPageTables = headerPage->GetTablesFullHeaders();

        for (int i = 0; i < tables.size(); i++) {
          HashSet<string> primaryKeysSet(tables[i].primaryKey);
          Headers::Index index;

          for(int j = 0;j < tables[i].columns.size(); j++){
              const auto& column = tables[i].columns[j];

              if(primaryKeysSet.Contains(column.name))
                index.columns.push_back(j);
          }

          this->CreateTable(tables[i], headerPageTables[i], index, i);
        }
    }

    Database::Database(const string &dbName, const bool& isServerInitialization) {
        static auto& server = Server::ServerInstance::Get();

        this->PopulateFilenames(dbName);

        const auto headerPage = StorageManager::Get().GetHeaderPage(this->systemFilename);

        this->header = *headerPage->GetDatabaseHeader();

        if (isServerInitialization)
            return;

        //query get from masterDb
        const auto& masterDbData = server.SelectTables(dbName);
        const auto& headerPageTables = headerPage->GetTablesFullHeaders();

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
        const auto logs = this->RecoverLogs();

        if (logs.empty()) {
            std::cout << "No logs to recover." << std::endl;
            return;
        }
        std::vector<extent_id_t> allocatedExtents; //since multiple rows might be inserted should be
        extent_id_t startingExtentIndex = 0;

        for (const auto& log : logs)
            this->ApplyRecoveryLog(log, allocatedExtents, startingExtentIndex);
    }

    void Database::ApplyRecoveryLog(
        const Logging::LogEntry &logEntry,
        std::vector<extent_id_t>& allocatedExtents,
        extent_id_t& startingExtentIndex
    )const{
        if (!logEntry.ValidateIntegrity())
            return;

        if (Logging::RowAffectedOperationTypes.Contains(logEntry.operation)) {
            auto* table = this->tables.at(logEntry.tableOrdinalPosition);

            auto* row = logEntry.GetRow();

            if (logEntry.operation == Logging::OperationType::DeleteRow) {
                //handle row delete trickier, need to identify whether to use pk or not (heap delete)
                return;
            }

            table->InsertRow(row, allocatedExtents, startingExtentIndex);
        }

        //Data structure affected changes from here down.

    }

    void Database::LogCheckPoint(Logging::CheckPoint &checkPoint) {
        Logging::WriteAheadLogger::Get().LogCheckPoint(checkPoint);
    }

    Logging::CheckPoint Database::LogRowInsert(
        StorageTypes::Row *row,
        const Constants::transaction_id_t& transactionId,
        const Constants::table_id_t& tableOrdinal
    ) {
        auto& logger =  Logging::WriteAheadLogger::Get();

        const auto logEntry = logger.CreateLogEntry(
            transactionId,
            Logging::OperationType::InsertRow,
            tableOrdinal,
            new LoggingStructures::RowInsertBody(row)
        );

        return logger.Log(logEntry);
    }

    Table *Database::CreateTable(
        const table_id_t &tableId,
        const int& ordinalPosition,
        const vector<StorageTypes::Column *> &columns,
        const Headers::Index *clusteredKeyIndexes,
        const vector<Headers::Index> *nonClusteredIndexes)
    {
        auto *table = new Table(tableId, ordinalPosition, columns, this, clusteredKeyIndexes, nonClusteredIndexes);

        this->tables.push_back(table);
        this->header.numberOfTables = this->tables.size();

        this->tableIdsDictionary.Add(tableId, ordinalPosition);

        return table;
    }

    void Database::CreateTable(const Headers::TableHeader& masterDbHeader, const TableHeader &tableHeader){
        auto *table = new Table(masterDbHeader, tableHeader, this);

        const auto& masterDbColumns = Server::ServerInstance::Get().SelectColumns(masterDbHeader.id);

        for (const auto & masterDbColumn : masterDbColumns) {
            if (masterDbColumn.isSystem)
                continue;

            table->AddColumn(new Column(masterDbColumn, table));
        }

        //TODO
        //maybe add in a single function
        table->GetColumnsHeaders();
        table->GetIdentityColumns();
        table->GetIndexes();
        table->GetDefaultValuesHeaders();
        table->GetStatistics();

        this->tables.push_back(table);
    }

    void Database::CreateTable(const Headers::sysTable &sysHeader, const TableHeader &tableHeader, const Headers::Index& primaryKey, const int& ordinalPosition)
    {
        this->tables.push_back(new Table(sysHeader, tableHeader, primaryKey, this, ordinalPosition));
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

    StorageTypes::Table * Database::OpenTable(const table_id_t &tableId) const{
        return this->tables.at(tableId);
    }

    StorageTypes::Table * Database::OpenTableById(const table_id_t &tableId) const{
        return this->tables.at(this->tableIdsDictionary.Get(tableId));
    }

    void Database::DeleteTable(const string& tableName)
    {
        const Table* table = nullptr;
        vector<Table*>::iterator it;

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

        const auto extentId = Database::CalculateExtentIdByPageId(tableHeader.indexAllocationMapPageId);

        const auto indexAllocationMapPage = StorageManager::Get().GetIndexAllocationMapPage(this->filename, tableHeader.indexAllocationMapPageId, table);

        if (indexAllocationMapPage.Get() == nullptr)
        {
            this->tables.erase(it);
            delete table;
            return;
        }

        const page_id_t globalAllocationMapPageId = Database::GetGamAssociatedPage(tableHeader.indexAllocationMapPageId);

        const auto globalAllocationMapPage = StorageManager::Get().GetGlobalAllocationMapPage(this->filename, globalAllocationMapPageId);

        vector<extent_id_t> allocatedExtents;
        indexAllocationMapPage->GetAllocatedExtents(&allocatedExtents);

        //deallocate pages as well
        for (const auto& extentId : allocatedExtents)
        {

        }
        //deletes all rows
        // table->Delete(nullptr);
    }

    void CreateDatabase(const string &dbName)
    {
        const auto& path = Database::CreateDatabasePath(dbName);

        StorageManager::Get().CreateFile(path, ".db");

        const auto sysDbName = path + "_sys";

        StorageManager::Get().CreateFile(sysDbName, ".db");

        constexpr page_id_t firstGamePageId = 2;
        constexpr page_id_t firstPfsPageId = 1;
        
        StorageManager::Get().CreateGlobalAllocationMapPage(sysDbName + ".db", firstGamePageId);
        StorageManager::Get().CreatePageFreeSpacePage(sysDbName + ".db", firstPfsPageId);

        auto headerPage = StorageManager::Get().CreateHeaderPage(sysDbName + ".db");

        headerPage->SetDbHeader(DatabaseHeader(0, firstPfsPageId, firstGamePageId));
    }

    void PrintRows(const vector<Row> &rows)
    {
        uint16_t rowCount = 0;
        for (const auto &row : rows)
        {
            row.PrintRow();
            rowCount++;
        }

        cout << "Rows printed: " << rowCount << endl;
    }

    void PrintRows(const vector<Row*> &rows)
    {
        uint16_t rowCount = 0;
        for (const auto &row : rows)
        {
            row->PrintRow();
            rowCount++;
        }

        cout << "Rows printed: " << rowCount << endl;
    }

    Database* UseSystemDatabase(const string & dbName, const vector<Headers::sysTable> & tables){
      return nullptr;
    }

    void Database::DeleteDatabase() const
    {
        const string path = this->filename;

        if (remove(path.c_str()) != 0)
            throw runtime_error("Database " + this->filename + " could not be deleted");
    }

    Pages::PageGuard<Page> Database::FindOrAllocateNextDataPage(Pages::PageGuard<Pages::PageFreeSpacePage> &pageFreeSpacePage, const page_id_t &pageId, const page_id_t &extentFirstPageId, const extent_id_t &extentId, const Table &table, extent_id_t *nextExtentId)
    {
        Pages::PageGuard<Pages::Page> page;
        if (pageId < extentFirstPageId + EXTENT_SIZE - 1)
        {
            for (page_id_t nextLeafPageId = pageId + 1; nextLeafPageId < extentFirstPageId + EXTENT_SIZE; nextLeafPageId++)
            {
                const auto &pageSizeCategory = pageFreeSpacePage->GetPageSizeCategory(nextLeafPageId);

                if (pageSizeCategory == 0)
                    continue;

                page = StorageManager::Get().GetPage(this->filename, nextLeafPageId, &table);

                if (page->GetPageSize() == 0)
                    break;
            }
        }

        if (page.Get() == nullptr || page->GetPageSize() > 0)
        {
            page = this->CreateDataPage(table.GetTableId());

            pageFreeSpacePage = Database::GetAssociatedPfsPage(this->filename, page->GetPageId());
        }

        return page;
    }

    Pages::PageGuard<PageFreeSpacePage> Database::GetAssociatedPfsPage(const string& filename, const page_id_t & pageId)
    {
        const page_id_t pageFreeSpacePageId = Database::GetPfsAssociatedPage(pageId);

        return StorageManager::Get().GetPageFreeSpacePage(filename, pageFreeSpacePageId);
    }

    void Database::TruncateTable(const table_id_t & tableId)
    {
        Table *table = this->tables.at(tableId);

        auto indexAllocationMapPageId = table->GetHeader().indexAllocationMapPageId;

        while (indexAllocationMapPageId != INVALID_PAGE_ID)
        {
            const auto iamExtentId = Database::CalculateExtentIdByPageId(indexAllocationMapPageId);

            const auto tableMapPage = StorageManager::Get().GetIndexAllocationMapPage(this->filename, indexAllocationMapPageId, table);

            vector<extent_id_t> allocatedExtents;
            tableMapPage->GetAllocatedExtents(&allocatedExtents);

            const page_id_t globalAllocationMapPageId = Database::GetGamAssociatedPage(indexAllocationMapPageId);

            auto gamPage = StorageManager::Get().GetGlobalAllocationMapPage(this->filename, globalAllocationMapPageId);

            indexAllocationMapPageId = tableMapPage->GetNextPageId();

            for (const auto& extentId : allocatedExtents)
            {
                const page_id_t extentFirstPageId = Database::CalculateFirstPageIdByExtentId(extentId);

                for (page_id_t pageId = extentFirstPageId; pageId < extentFirstPageId + EXTENT_SIZE; pageId++)
                {
                    const page_id_t pageFreeSpacePageId = Database::GetPfsAssociatedPage(pageId);
                
                    auto pageFreeSpacePage = StorageManager::Get().GetPageFreeSpacePage(this->systemFilename, pageFreeSpacePageId);

                    pageFreeSpacePage->SetPageFreed(pageId);
                }

                gamPage->DeallocateExtent(extentId);
            }
        }

        table->UpdateIndexAllocationMapPageId(INVALID_PAGE_ID);
    }

    Pages::PageGuard<Pages::OverflowPage> Database::CreateOverflowPage(const table_id_t &tableId)
    {
        extent_id_t newExtentId = 0;
        page_id_t lowerLimit = 0, newPageId = 0;

        this->AllocateNewExtent(lowerLimit, newPageId, newExtentId, tableId);

        for (page_id_t pageId = lowerLimit; pageId < newPageId + EXTENT_SIZE; pageId++){
            auto pageFreeSpacePage = Database::GetAssociatedPfsPage(this->systemFilename, pageId);

            MultiThreading::WriterGuard lock(&pageFreeSpacePage->GetLatch());
            pageFreeSpacePage->SetPageMetaData(StorageManager::Get().CreateOverflowPage(this->filename, pageId).Get());
        }

        return StorageManager::Get().GetOverflowPage(this->filename, lowerLimit, this->tables[tableId]);
    }

    Pages::PageGuard<Page> Database::CreateDataPage(const table_id_t &tableId)
    {
        extent_id_t newExtentId = 0;
        page_id_t lowerLimit = 0, newPageId = 0;

        this->AllocateNewExtent(lowerLimit, newPageId, newExtentId, tableId);

        for (page_id_t pageId = lowerLimit; pageId < newPageId + EXTENT_SIZE; pageId++){

            auto pageFreeSpacePage = Database::GetAssociatedPfsPage(this->systemFilename, pageId);

            MultiThreading::WriterGuard lock(&pageFreeSpacePage->GetLatch());
            pageFreeSpacePage->SetPageMetaData(StorageManager::Get().CreatePage(this->filename, pageId).Get());
        }

        return StorageManager::Get().GetPage(this->filename, lowerLimit, this->tables[tableId]);
    }

    Pages::PageGuard<LargeObjectPage> Database::CreateLargeDataPage(const table_id_t &tableId)
    {
        extent_id_t newExtentId = 0;
        page_id_t lowerLimit = 0, newPageId = 0;

        this->AllocateNewExtent(lowerLimit, newPageId, newExtentId, tableId);

        for (page_id_t pageId = lowerLimit; pageId < newPageId + EXTENT_SIZE; pageId++){
            auto pageFreeSpacePage = Database::GetAssociatedPfsPage(this->systemFilename, pageId);

            MultiThreading::WriterGuard lock(&pageFreeSpacePage->GetLatch());

            pageFreeSpacePage->SetPageMetaData(StorageManager::Get().CreateLargeDataPage(this->filename, pageId).Get());
        }

        return StorageManager::Get().GetLargeDataPage(this->filename, lowerLimit, this->tables[tableId]);
    }

    Pages::PageGuard<IndexPage> Database::CreateIndexPage(
        const StorageTypes::Table* table,
        const table_id_t &tableOrdinalPosition,
        const page_id_t& treeId
    )
    {
        extent_id_t newExtentId = 0;
        page_id_t lowerLimit = 0, newPageId = 0;

        this->AllocateNewExtent(lowerLimit, newPageId, newExtentId, tableOrdinalPosition);

        for (page_id_t pageId = lowerLimit; pageId < newPageId + EXTENT_SIZE; pageId++)
        {
            auto indexPage = StorageManager::Get().CreateIndexPage(this->filename, pageId);
            indexPage->SetTreeId(treeId);

            auto pageFreeSpacePage = Database::GetAssociatedPfsPage(this->systemFilename, pageId);

            MultiThreading::WriterGuard lock(&pageFreeSpacePage->GetLatch());
            pageFreeSpacePage->SetPageMetaData(indexPage.Get());
        }

        return StorageManager::Get().GetIndexPage(this->filename, lowerLimit, table);
    }

    bool Database::AllocateNewExtent(page_id_t& lowerLimit, page_id_t& newPageId, extent_id_t& newExtentId, const table_id_t &tableId)
    {
        bool isFirstExtent = false;
        {
            const auto* table = this->tables[tableId];
            const Constants::page_id_t indexAllocationMapPageId = table->GetHeader().indexAllocationMapPageId;
            MultiThreading::WriterGuard gamLock(&this->gamPageMutex);

            auto gamPage = StorageManager::Get().GetGlobalAllocationMapPage(this->systemFilename, this->header.lastGamPageId);

            if (gamPage->IsFull())
            {
                auto nextGamPage = StorageManager::Get().CreateGlobalAllocationMapPage(this->systemFilename, Database::CalculateNextGamPageId(gamPage->GetPageId()));

                newExtentId = gamPage->AllocateExtent();
                newPageId = Database::CalculateFirstPageIdByExtentId(newExtentId);

                auto previousTableMapPage = StorageManager::Get().GetIndexAllocationMapPage(this->filename, indexAllocationMapPageId, table);
                previousTableMapPage->SetNextPageId(newPageId);

                auto nextTableMapPage = StorageManager::Get().CreateIndexAllocationMapPage(this->filename, tableId, newPageId, newExtentId);
                this->tables[tableId]->UpdateIndexAllocationMapPageId(newPageId);

                auto pageFreeSpacePage = Database::GetAssociatedPfsPage(this->systemFilename, nextTableMapPage->GetPageId());

                MultiThreading::WriterGuard pageIdLock(&pageFreeSpacePage->GetLatch());

                pageFreeSpacePage->SetPageMetaData(nextTableMapPage.Get());
            }
            else {
                newExtentId = gamPage->AllocateExtent();
                newPageId = Database::CalculateFirstPageIdByExtentId(newExtentId);
            }

            Pages::PageGuard<Pages::IndexAllocationMapPage> tableMapPage;

            isFirstExtent = indexAllocationMapPageId == Constants::INVALID_PAGE_ID;

            if (isFirstExtent) {
                tableMapPage = StorageManager::Get().CreateIndexAllocationMapPage(this->filename, tableId, newPageId, newExtentId);

                {
                    auto pageFreeSpacePage = Database::GetAssociatedPfsPage(this->systemFilename, tableMapPage->GetPageId());

                    MultiThreading::WriterGuard pageIdLock(&pageFreeSpacePage->GetLatch());
                    pageFreeSpacePage->SetPageMetaData(tableMapPage.Get());

                }

                this->tables[tableId]->UpdateIndexAllocationMapPageId(newPageId);
            }
            else
                tableMapPage = StorageManager::Get().GetIndexAllocationMapPage(this->filename, indexAllocationMapPageId, table);

            MultiThreading::WriterGuard tableMapLock(&tableMapPage->GetLatch());

            tableMapPage->SetAllocatedExtent(newExtentId, gamPage.Get());
        }

        lowerLimit = (isFirstExtent)
                          ? newPageId + 1
                          : newPageId;

        const auto pfsPageId = Database::GetPfsAssociatedPage(newPageId);

        {
            MultiThreading::WriterGuard pfsLock(&this->pfsPageMutex);

            if (pfsPageId > this->header.lastPageFreeSpacePageId) {
                Storage::StorageManager::Get().CreatePageFreeSpacePage(this->systemFilename, pfsPageId);
                this->header.lastPageFreeSpacePageId = pfsPageId;
            }
        }

      return true;
    }

    const Table *Database::GetTable(const table_id_t &tableId) const
    {
        if (tableId >= this->tables.size())
            throw out_of_range("No table with ID: " + to_string(tableId) + " exists");

        return this->tables[tableId];
    }

    Pages::PageGuard<LargeObjectPage> Database::GetTableLastLargeDataPage(const table_id_t &tableId)const
    {
        if (tableId >= this->tables.size())
            return Pages::PageGuard<LargeObjectPage>();

        const auto* table = this->tables[tableId];

        const auto& tableMapPageId = table->GetHeader().indexAllocationMapPageId;

        if(tableMapPageId == INVALID_PAGE_ID)
            return Pages::PageGuard<LargeObjectPage>();

        const auto iamExtentId = Database::CalculateExtentIdByPageId(tableMapPageId);

        const auto tableMapPage = StorageManager::Get().GetIndexAllocationMapPage(this->filename, tableMapPageId, table);

        vector<extent_id_t> allocatedExtents;
        tableMapPage->GetAllocatedExtents(&allocatedExtents);

        for (const auto &extentId : allocatedExtents)
        {
            const page_id_t firstExtentPageId = Database::CalculateFirstPageIdByExtentId(extentId);

            for (page_id_t pageId = firstExtentPageId; pageId < firstExtentPageId + EXTENT_SIZE; pageId++)
            {
                const page_id_t correspondingPfsPageId = Database::GetPfsAssociatedPage(pageId);

                const auto pageFreeSpace = StorageManager::Get().GetPageFreeSpacePage(this->filename, correspondingPfsPageId);

                if (pageFreeSpace->GetPageType(pageId) != PageType::LOB)
                    break;

                auto lastLargeDataPage = StorageManager::Get().GetLargeDataPage(this->filename, pageId, this->tables[tableId]);

                if (lastLargeDataPage->GetPageSize() == 0)
                    return lastLargeDataPage;
            }
        }

        return {};
    }

    Pages::PageGuard<Pages::OverflowPage> Database::GetLastOverflowPage(const table_id_t & tableId, const block_size_t& size){
        if (tableId >= this->tables.size())
            return {};

        const auto& table = this->tables[tableId];

        const auto& tableMapPageId = table->GetHeader().indexAllocationMapPageId;

        if(tableMapPageId == INVALID_PAGE_ID)
            return {};

        const auto iamExtentId = Database::CalculateExtentIdByPageId(tableMapPageId);

        const auto tableMapPage = StorageManager::Get().GetIndexAllocationMapPage(this->filename, tableMapPageId, table);

        vector<extent_id_t> allocatedExtents;
        tableMapPage->GetAllocatedExtents(&allocatedExtents);

        for (const auto &extentId : allocatedExtents)
        {
            const page_id_t firstExtentPageId = Database::CalculateFirstPageIdByExtentId(extentId);

            for (page_id_t pageId = firstExtentPageId; pageId < firstExtentPageId + EXTENT_SIZE; pageId++)
            {
                const page_id_t correspondingPfsPageId = Database::GetPfsAssociatedPage(pageId);

                const auto pageFreeSpace = StorageManager::Get().GetPageFreeSpacePage(this->systemFilename, correspondingPfsPageId);

                if (pageFreeSpace->GetPageType(pageId) != PageType::OVERFLOW)
                    break;

                const auto categorySize = Database::GetObjectSizeToCategory(size);

                if(pageFreeSpace->GetPageSizeCategory(pageId) <= categorySize)
                    continue;

                auto lastOverflowPage = StorageManager::Get().GetOverflowPage(this->filename, pageId, table);

                if (lastOverflowPage->GetBytesLeft() >= size)
                    return lastOverflowPage;
            }
        }

        return this->CreateOverflowPage(tableId);
    }



    Pages::PageGuard<LargeObjectPage> Database::GetLargeDataPage(const page_id_t &pageId, const table_id_t &tableId)const
    {
        const auto extentId = Database::CalculateExtentIdByPageId(pageId);

        return StorageManager::Get().GetLargeDataPage(this->filename, pageId, this->tables[tableId]);

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

    void Database::SetPageMetaDataToPfs(const Page *page)const
    {
        const page_id_t correspondingPfsPageId = Database::GetPfsAssociatedPage(page->GetPageId());

       auto pageFreeSpacePage = StorageManager::Get().GetPageFreeSpacePage(this->filename, correspondingPfsPageId);

        pageFreeSpacePage->SetPageMetaData(page);
    }

    string Database::GetFileName() const { return this->filename; }

    void Database::GetIdentityColumns()const{
            for(const auto& table: this->tables)
                table->GetIdentityColumns();
    }

    void Database::UpdateIdentityManagersIds()const{
        for(const auto& table: this->tables)
            table->UpdateIdentityManagersIds();
    }

    void Database::GetColumnsHeaders() const{
            for (const auto& table : this->tables)
                table->GetColumnsHeaders();
    }

    void Database::GetDefaultValues() const{
            for (const auto& table : this->tables)
                table->GetDefaultValuesHeaders();
    }

    void Database::GetIndexes() const{
            for (const auto& table : this->tables)
                table->GetIndexes();
    }

    void Database::GetTableHeaders() const{
    }

    void Database::UpdateMasterDatabase()const{
            for(const auto& table: this->tables)
                table->UpdateMasterDatabase();
    }

    string Database::GetSystemFilename() const{ return this->systemFilename;}

    DatabaseHeader::DatabaseHeader()
    {
        this->numberOfTables = 0;
        this->lastTableId = 0;
        this->lastPageFreeSpacePageId = 0;
        this->lastGamPageId = 0;
    }

    DatabaseHeader::DatabaseHeader(const table_number_t &numberOfTables, const page_id_t &lastPageFreeSpacePageId, const page_id_t &lastGamPageId)
    {
        this->numberOfTables = 0;
        this->lastTableId = 0;
        this->lastPageFreeSpacePageId = lastPageFreeSpacePageId;
        this->lastGamPageId = lastGamPageId;
    }

    DatabaseHeader::DatabaseHeader(const DatabaseHeader &dbHeader)
    {
        this->numberOfTables = dbHeader.numberOfTables;
        this->lastTableId = dbHeader.lastTableId;
        this->lastPageFreeSpacePageId = dbHeader.lastPageFreeSpacePageId;
        this->lastGamPageId = dbHeader.lastGamPageId;
    }

    DatabaseHeader &DatabaseHeader::operator=(const DatabaseHeader &dbHeader)
    {
        if (&dbHeader == this)
            return *this;

        this->numberOfTables = dbHeader.numberOfTables;
        this->lastTableId = dbHeader.lastTableId;
        this->lastGamPageId = dbHeader.lastGamPageId;
        this->lastPageFreeSpacePageId = dbHeader.lastPageFreeSpacePageId;

        return *this;
    }
}