#include "../../include/SystemDatabases/VersionDatabase.h"

#include <fstream>

#include "../../include/BufferPool/StorageManager.h"
#include "../../../Systemic/include/Guards/ReaderGuard.h"
#include "../../../Systemic/include/Guards/WriterGuard.h"

#include <nlohmann/json.hpp>

#include "Contexts/ExecutionContext.h"
#include "../../include/Extensions/StringExtensions.h"
#include "DataStorage/SerializedRow.h"

namespace CoreEngine {
    VersionDatabase& VersionDatabase::Get(){
        static VersionDatabase instance;
        return instance;
    }

    void VersionDatabase::Initialize(
        const ExecutionContext& baseContext,
        const DataTypes::StringView& configPath
    ){
        const auto [dbName, dbPath] = this->ReadConfiguration(baseContext.GetAllocator(), configPath);
        this->PopulateFilenames(baseContext.GetAllocator(), dbName);
        this->CreateKeys();

        if (!this->VersionDatabaseExists(dbName.ToView()))
            CoreEngine::CreateDatabase(Constants::VERSION_DATABASE_ID, dbName);

        Storage::StorageManager::Get().OpenFile(this->dataFileKey, this->filenameView);
        Storage::StorageManager::Get().OpenFile(this->systemFileKey, this->systemFilenameView);

        this->lastUsedPageId = INVALID_PAGE_ID;
        const auto headerPage = Storage::StorageManager::Get().GetPage<Pages::HeaderPageView>(this->systemFileKey, Constants::HEADER_PAGE_ID);
        this->header = *headerPage.GetDatabaseHeaderPtr();
    }

    VersionDatabase::VersionDatabase(){
        this->lastUsedPageId = INVALID_PAGE_ID;
    }

    VersionDatabase::~VersionDatabase(){
        this->_allocator.Release();
    }

    std::tuple<DataTypes::String, DataTypes::String> VersionDatabase::ReadConfiguration(
        const ::Memory::IAllocator* allocator,
        const DataTypes::StringView& configPath
    ){
        std::ifstream file(configPath.Data());

        if (!file.is_open())
            throw std::runtime_error("System Tables file: " + std::string(configPath.Data(), configPath.Size()) + " could not be opened");

        nlohmann::json jsonFile;

        try {
            file >> jsonFile;
        }
        catch (std::exception &e){
            throw std::runtime_error(e.what());
        }

        DataTypes::String sysDbName(allocator);
        DataTypes::String sysDbPath(allocator);

        jsonFile.at("version_db_name").get_to(sysDbName);
        jsonFile.at("version_db_path").get_to(sysDbPath);

        return std::make_tuple(std::move(sysDbName), std::move(sysDbPath));
    }

    bool VersionDatabase::VersionDatabaseExists(const DataTypes::StringView& path){
        return Storage::FileManager::FileExists(path);
    }

    void VersionDatabase::CreateKeys(){
        this->dataFileKey = Storage::FileKey(Constants::VERSION_DATABASE_ID, Storage::FileType::Data);
        this->systemFileKey = Storage::FileKey(Constants::VERSION_DATABASE_ID, Storage::FileType::System);
    }

    void VersionDatabase::PopulateFilenames(
        const ::Memory::IAllocator* allocator,
        const DataTypes::String& dbName
    ){
        const auto path = DataTypes::String::Concat(allocator, dbName, "/", dbName);
        this->filename = DataTypes::String::Concat(&this->_allocator, path, Constants::DATA_FILE_EXTENSION);
        this->systemFilename = DataTypes::String::Concat(&this->_allocator, path, Constants::SYS_EXTENSION, Constants::DATA_FILE_EXTENSION);

        this->filenameView = this->filename.ToView();
        this->systemFilenameView = this->systemFilename.ToView();
    }

    void VersionDatabase::WriteHeaderToFile() const{
        const auto headerPage = Storage::StorageManager::Get().GetPage<Pages::HeaderPageView>(this->systemFileKey, Constants::HEADER_PAGE_ID);
        headerPage.SetDatabaseHeader(this->header);
    }

    bool VersionDatabase::AllocateNewExtent(
        const ::Memory::IAllocator* allocator,
        page_id_t& newPageId,
        extent_id_t& newExtentId
    ){
        // {
        //     const MultiThreading::WriterGuard gamLock(&this->gamPageMutex);
        //
        //     auto gamPage = Storage::StorageManager::Get().GetPage<Pages::GlobalAllocationPageView>(
        //         this->systemFileKey,
        //         this->header.lastGamPageId
        //     );
        //
        //     if (gamPage.IsFull()){
        //         const auto nextGamPageId = Database::CalculateNextGamPageId(this->header.lastGamPageId);
        //
        //         gamPage = Storage::StorageManager::Get().CreateGlobalAllocationMapPage(
        //             this->systemFileKey,
        //             nextGamPageId
        //         );
        //
        //         this->header.lastGamPageId = gamPage.PageId();
        //     }
        //
        //     DataStructures::PolymorphicArray<extent_id_t> extents(allocator);
        //     MultiThreading::WriterGuard gamPageLock(&gamPage.Latch());
        //     // Step 3: allocate an extent from the current (or new) GAM page
        //     const auto allocatedExtentsCount = gamPage.ReserveExtentsNoLock(extents, 1);
        //
        //     newExtentId = extents[0];
        //     newPageId   = Database::CalculateExtentFirstPageId(newExtentId);
        // }
        //
        // // Step 4: ensure PFS page exists
        // const auto pfsPageId = Database::GetPfsAssociatedPage(newPageId);
        //
        // {
        //     MultiThreading::WriterGuard pfsLock(&this->pfsPageMutex);
        //
        //     if (pfsPageId > this->header.lastPageFreeSpacePageId) {
        //         Storage::StorageManager::Get().CreatePageFreeSpacePage(
        //             this->systemFileKey,
        //             pfsPageId
        //         );
        //         this->header.lastPageFreeSpacePageId = pfsPageId;
        //     }
        // }

        return true;
    }

    Pages::PageView VersionDatabase::TryGetLastUndoPage(const row_size_t size) {
        page_id_t pageId;

        {
            MultiThreading::ReaderGuard lock(&this->lastUsedPageMutex);

            if (this->lastUsedPageId == INVALID_PAGE_ID)
                return {};

            pageId = this->lastUsedPageId;
        }

        auto lastUsedPage = Storage::StorageManager::Get().GetPage<Pages::PageView>(
            this->dataFileKey,
            pageId
        );

        MultiThreading::ReaderGuard lastUsedPageLatch(&lastUsedPage.Latch());

        if (lastUsedPage.BytesLeft() >= size)
            return lastUsedPage;

        return {};
    }

    Pages::PageView VersionDatabase::CreateUndoPage(const ::Memory::IAllocator* allocator){
        extent_id_t newExtentId = 0;
        page_id_t newPageId = 0;

        this->AllocateNewExtent(allocator, newPageId, newExtentId);

        {
            MultiThreading::WriterGuard pageIdLock(&this->lastUsedPageMutex);
            this->lastUsedPageId = newPageId;
        }

        for (page_id_t pageId = newPageId; pageId < newPageId + Constants::EXTENT_SIZE; pageId++){
            auto pageFreeSpacePage = Database::GetAssociatedPfsPage(
                this->systemFileKey,
                pageId
            );

            MultiThreading::WriterGuard lock(&pageFreeSpacePage.Latch());

            auto undoPage = Storage::StorageManager::Get().CreatePage(
                this->dataFileKey,
                pageId
            );

            pageFreeSpacePage.SetPageMetaData(&undoPage);
        }

        return Storage::StorageManager::Get().GetPage<Pages::PageView>(
            this->dataFileKey,
            newPageId
        );
    }

    Pages::PageView VersionDatabase::GetLastUndoPage(
        const ::Memory::IAllocator* allocator,
        const row_size_t size
    ) {
        const auto gamPage = Storage::StorageManager::Get().GetPage<Pages::GlobalAllocationPageView>(
            this->systemFileKey,
            this->header.lastGamPageId
        );

        auto cachedPage = this->TryGetLastUndoPage(size);
        if (cachedPage.IsValid())
            return cachedPage;

        for (const auto extentId : gamPage.GetAllocatedExtents(allocator)){
            const page_id_t firstExtentPageId = Database::CalculateExtentFirstPageId(extentId);

            for (page_id_t pageId = firstExtentPageId; pageId < firstExtentPageId + Constants::EXTENT_SIZE; pageId++){
                {
                    const page_id_t correspondingPfsPageId = Database::GetPfsAssociatedPage(pageId);
                    const auto pageFreeSpace = Storage::StorageManager::Get().GetPage<Pages::PageFreeSpaceView>(
                        this->systemFileKey,
                        correspondingPfsPageId
                    );

                    MultiThreading::ReaderGuard pfsLock(&pageFreeSpace.Latch());

                    const auto categorySize = Database::GetObjectSizeToCategory(size);

                    if(pageFreeSpace.GetPageSizeCategory(pageId) <= categorySize)
                        continue;
                }

                auto undoPage = Storage::StorageManager::Get().GetPage<Pages::PageView>(
                    this->dataFileKey,
                    pageId
                );

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

        return this->CreateUndoPage(allocator);
    }

    StorageTypes::RID VersionDatabase::InsertRow(
        const ::Memory::IAllocator* allocator,
        const Pages::RawRowReference& rowRef
    ){
        const auto page = this->GetLastUndoPage(allocator, rowRef.size);

        MultiThreading::WriterGuard lock(&page.Latch());

        const auto payload = StorageTypes::SerializedRow::FromRowPtr(rowRef);
        const auto indexPosition = page.InsertRow(payload);

        this->numberOfPendingVersions.fetch_add(1, std::memory_order_relaxed);

        return StorageTypes::RID(page.PageId(), indexPosition);
    }

     bool VersionDatabase::RetrieveVersionedRID(
        const Snapshot& snapshot,
        const StorageTypes::RowHeader* rowHeader,
        StorageTypes::RID* outRID
    )const {
        while (true){
            const auto page = Storage::StorageManager::Get().GetPage<Pages::PageView>(
                this->dataFileKey,
                rowHeader->_versionRID._pageId
            );

            MultiThreading::ReaderGuard lock(&page.Latch());

            if (page.IsRowVisible(snapshot, rowHeader->_versionRID._index)){
                *outRID = StorageTypes::RID(page.PageId(), rowHeader->_versionRID._index, CoreEngine::StorageTypes::RID::Version);
                return true;
            }

            rowHeader = page.PeekRowHeader(rowHeader->_versionRID._index);
            if (!rowHeader->HasOldVersion())
                return false;
        }
    }

    std::vector<extent_id_t> VersionDatabase::GetAllocatedExtents(const extent_id_t startingExtentId) const {
        const auto gamPage = Storage::StorageManager::Get().GetPage<Pages::GlobalAllocationPageView>(
            this->systemFileKey,
            this->header.lastGamPageId
        );

        // return gamPage.GetAllocatedExtents(startingExtentId);
        return std::vector<extent_id_t>();
        // return gamPage.GetAllocatedExtents(startingExtentId);
    }

    extent_id_t VersionDatabase::CleanupVersionedData(
        const transaction_id_t transactionId,
        const extent_id_t startingExtentId
    )const {
        static auto& storageManager = Storage::StorageManager::Get();
        const auto extents = this->GetAllocatedExtents(startingExtentId);

        for (const auto extentId : extents){
            const auto firstExtentPageId = Database::CalculateExtentFirstPageId(extentId * Constants::EXTENT_SIZE);

            bool isExtentEmpty = true;
            for (page_id_t pageId = firstExtentPageId; pageId < firstExtentPageId + Constants::EXTENT_SIZE; pageId++){
                auto pfsPage = Database::GetAssociatedPfsPage(
                    this->systemFileKey,
                    pageId
                );

                {
                    MultiThreading::ReaderGuard pfsReaderLock(&pfsPage.Latch());

                    if (!pfsPage.IsPageAllocated(pageId))
                        continue;
                }

                auto page = storageManager.GetPage<Pages::PageView>(
                    this->dataFileKey,
                    pageId
                );

                MultiThreading::WriterGuard pageLatch(&page.Latch());

                for (int i = 0; i < page.PageSize(); i++) {
                    const auto rowHeader = page.PeekRowHeader(i);

                    if (
                        transactionId == FIRST_TRANSACTION_ID
                        || rowHeader->_createdTransactionId <= transactionId
                    ) {
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
                auto gamPage = storageManager.GetPage<Pages::GlobalAllocationPageView>(
                    this->systemFileKey,
                    this->header.lastGamPageId
                );

                MultiThreading::WriterGuard gamLock(&gamPage.Latch());

                gamPage.DeallocateExtentNoLock(extentId);
            }
        }

        return extents.empty() ? 0 : extents.back() + 1;
    }

    bool VersionDatabase::HasPendingVersions() const{
        return this->numberOfPendingVersions.load(std::memory_order_relaxed) > VersionDatabase::PENDING_VERSIONS_THRESHOLD;
    }
}
