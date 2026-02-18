#include "../../include/SystemDatabases/VersionDatabase.h"

#include "../../include/BufferPool/StorageManager.h"
#include "../../../Systemic/include/Guards/ReaderGuard.h"
#include "../../../Systemic/include/Guards/WriterGuard.h"

#include <nlohmann/json.hpp>

namespace DatabaseEngine {
    VersionDatabase& VersionDatabase::Get(){
        static VersionDatabase instance;
        return instance;
    }

    void VersionDatabase::Initialize(const std::string_view configPath){
        this->ReadConfiguration(configPath);
        this->PopulateFilenames();

        if (!this->VersionDatabaseExists())
            DatabaseEngine::CreateDatabase(this->name);

        this->lastUsedPageId = INVALID_PAGE_ID;
        const auto headerPage = Storage::StorageManager::Get().GetHeaderPage(this->systemFilename);

        this->header = *headerPage.GetDatabaseHeaderPtr();
    }

    VersionDatabase::VersionDatabase(){
        this->lastUsedPageId = INVALID_PAGE_ID;
    }

    VersionDatabase::~VersionDatabase() = default;

    void VersionDatabase::ReadConfiguration(const std::string_view configPath){
        std::ifstream file(configPath.data());

        if (!file.is_open())
            throw std::runtime_error("System Tables file: " + std::string(configPath.data()) + " could not be opened");

        nlohmann::json jsonFile;

        try {
            file >> jsonFile;
        }
        catch (std::exception &e){
            throw std::runtime_error(e.what());
        }

        this->name = jsonFile.at("version_db_name");
        this->filename = jsonFile.at("version_db_path");
    }

    bool VersionDatabase::VersionDatabaseExists() const{
        return std::filesystem::exists(this->filename);
    }

    std::string VersionDatabase::CreateDatabasePath(const std::string & dbName){ return dbName + "/" + dbName; }

    void VersionDatabase::PopulateFilenames(){
        const auto& path = Database::CreateDatabasePath(this->name);

        this->filename = path + ".db";
        this->fileExtension = ".db";
        this->systemFilename = path + "_sys" + ".db";
    }

    void VersionDatabase::WriteHeaderToFile() const{
        const auto headerPage = Storage::StorageManager::Get().GetHeaderPage(this->systemFilename);
        headerPage.SetDatabaseHeader(this->header);
    }

    bool VersionDatabase::AllocateNewExtent(page_id_t& newPageId, extent_id_t& newExtentId){
        {
            const MultiThreading::WriterGuard gamLock(&this->gamPageMutex);

            auto gamPage = Storage::StorageManager::Get().GetGlobalAllocationMapPage(this->systemFilename, this->header.lastGamPageId);

            if (gamPage.IsFull()){
                const auto nextGamPageId = Database::CalculateNextGamPageId(this->header.lastGamPageId);

                gamPage = Storage::StorageManager::Get().CreateGlobalAllocationMapPage(this->systemFilename, nextGamPageId);

                this->header.lastGamPageId = gamPage.PageId();
            }

            std::vector<extent_id_t> extents;

            MultiThreading::WriterGuard gamPageLock(&gamPage.Latch());
            // Step 3: allocate an extent from the current (or new) GAM page
            const auto allocatedExtentsCount = gamPage.AllocateExtentsNoLock(extents, 1);

            newExtentId = extents.front();
            newPageId   = Database::CalculateExtentFirstPageId(newExtentId);
        }

        // Step 4: ensure PFS page exists
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

    Pages::PageView VersionDatabase::TryGetLastUndoPage(
        const StorageTypes::Table *table,
        const row_size_t size
    ) {
        page_id_t pageId;

        {
            MultiThreading::ReaderGuard lock(&this->lastUsedPageMutex);

            if (this->lastUsedPageId == INVALID_PAGE_ID)
                return {};

            pageId = this->lastUsedPageId;
        }

        auto lastUsedPage = Storage::StorageManager::Get().GetPage(this->filename, pageId, table);

        MultiThreading::ReaderGuard lastUsedPageLatch(&lastUsedPage.Latch());

        if (lastUsedPage.BytesLeft() >= size)
            return lastUsedPage;

        return {};
    }

    Pages::PageView VersionDatabase::CreateUndoPage(){
        extent_id_t newExtentId = 0;
        page_id_t newPageId = 0;

        this->AllocateNewExtent(newPageId, newExtentId);

        {
            MultiThreading::WriterGuard pageIdLock(&this->lastUsedPageMutex);
            this->lastUsedPageId = newPageId;
        }

        for (page_id_t pageId = newPageId; pageId < newPageId + EXTENT_SIZE; pageId++){
            auto pageFreeSpacePage = Database::GetAssociatedPfsPage(this->systemFilename, pageId);

            MultiThreading::WriterGuard lock(&pageFreeSpacePage.Latch());

            auto undoPage = Storage::StorageManager::Get().CreatePage(this->filename, nullptr, pageId);

            pageFreeSpacePage.SetPageMetaData(&undoPage);
        }

        return Storage::StorageManager::Get().GetPage(this->filename, newPageId, nullptr);
    }

    Pages::PageView VersionDatabase::GetLastUndoPage(
        const StorageTypes::Table* table,
        const row_size_t size
    ) {
        const auto gamPage = Storage::StorageManager::Get().GetGlobalAllocationMapPage(this->systemFilename, this->header.lastGamPageId);

        auto cachedPage = this->TryGetLastUndoPage(table, size);
        if (cachedPage.IsValid())
            return cachedPage;

        for (const auto &extentId : gamPage.GetAllocatedExtents()){
            const page_id_t firstExtentPageId = Database::CalculateExtentFirstPageId(extentId);

            for (page_id_t pageId = firstExtentPageId; pageId < firstExtentPageId + EXTENT_SIZE; pageId++){
                {
                    const page_id_t correspondingPfsPageId = Database::GetPfsAssociatedPage(pageId);
                    const auto pageFreeSpace = Storage::StorageManager::Get().GetPageFreeSpacePage(this->systemFilename, correspondingPfsPageId);

                    MultiThreading::ReaderGuard pfsLock(&pageFreeSpace.Latch());

                    const auto categorySize = Database::GetObjectSizeToCategory(size);

                    if(pageFreeSpace.GetPageSizeCategory(pageId) <= categorySize)
                        continue;
                }

                auto undoPage = Storage::StorageManager::Get().GetPage(this->filename, pageId, table);

                MultiThreading::ReaderGuard undoLatch(&undoPage.Latch());

                if (undoPage.BytesLeft() >= size) {
                    {
                     MultiThreading::WriterGuard lock(&this->lastUsedPageMutex);
                     this->lastUsedPageId = undoPage.PageId();
                    }

                    return undoPage;
                }
            }
        }

        return this->CreateUndoPage();
    }

    Errors::RuntimeStatus VersionDatabase::InsertRow(
        const Pages::RawRowReference& rowRef,
        StorageTypes::RowVersionPointer& rowPointer,
        const StorageTypes::Table* table
    ){
        Errors::RuntimeStatus status;
        const auto payload = StorageTypes::InsertPayload::FromRowPtr(rowRef);
        const auto page = this->GetLastUndoPage(table, payload.Size());

        MultiThreading::WriterGuard lock(&page.Latch());

        const auto indexPosition = page.InsertRow(payload);

        rowPointer.pageId = page.PageId();
        rowPointer.offset = indexPosition;

        return {};
    }

    Pages::RowReference VersionDatabase::RetrieveRowReference(
        const Snapshot& snapshot,
        const StorageTypes::RowVersionPointer &rowPointer,
        const StorageTypes::Table *table
    )const {
        {
            const auto page = Storage::StorageManager::Get().GetPage(this->filename, rowPointer.pageId, table);

            MultiThreading::ReaderGuard lock(&page.Latch());

            return page.PeekRow(rowPointer.offset, 0);
        }

        return {};
    }

    std::vector<extent_id_t> VersionDatabase::GetAllocatedExtents(const extent_id_t startingExtentId) const {
        const auto gamPage = Storage::StorageManager::Get().GetGlobalAllocationMapPage(this->systemFilename, this->header.lastGamPageId);
        return gamPage.GetAllocatedExtents(startingExtentId);
    }

    extent_id_t VersionDatabase::CleanupVersionedData(const transaction_id_t transactionId, const extent_id_t startingExtentId)const {
        const auto extents = this->GetAllocatedExtents(startingExtentId);

        for (const auto &extentId : extents){
            const auto firstExtentPageId = Database::CalculateExtentFirstPageId(extentId * EXTENT_SIZE);

            bool isExtentEmpty = true;
            for (page_id_t pageId = firstExtentPageId; pageId < firstExtentPageId + EXTENT_SIZE; pageId++){
                auto pfsPage = Database::GetAssociatedPfsPage(this->systemFilename, pageId);

                {
                    MultiThreading::ReaderGuard pfsReaderLock(&pfsPage.Latch());

                    if (!pfsPage.IsPageAllocated(pageId))
                        continue;
                }

                auto page = Storage::StorageManager::Get().GetPage(this->filename, pageId, nullptr);
                MultiThreading::WriterGuard pageLatch(&page.Latch());

                for (int i = 0; i < page.PageSize(); i++) {
                    const auto rowPtr = page.PeekRow(i, 0);
                    rowPtr.lazyState->header = page.PeekRowHeader(i, 0);

                    if (transactionId == FIRST_TRANSACTION_ID
                        || rowPtr.lazyState->header.version.createdTransactionId <= transactionId) {
                        page.Delete(i);
                        i--;
                    }
                }

                if (page.PageSize() == 0) {
                    MultiThreading::WriterGuard pfsWriterLock(&pfsPage.Latch());
                    pfsPage.SetPageFreed(pageId);

                    continue;
                }

                isExtentEmpty = false;
            }

            if (isExtentEmpty) {
                auto gamPage = Storage::StorageManager::Get().GetGlobalAllocationMapPage(this->systemFilename, this->header.lastGamPageId);

                MultiThreading::WriterGuard gamLock(&gamPage.Latch());

                gamPage.DeallocateExtent(extentId);
            }
        }

        return extents.empty() ? 0 : extents.back() + 1;
    }
}