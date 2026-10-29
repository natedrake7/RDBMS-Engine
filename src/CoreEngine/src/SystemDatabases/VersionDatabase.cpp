#include <CoreEngine/SystemDatabases/VersionDatabase.h>

#include <fstream>

#include <CoreEngine/BufferPool/StorageManager.h>
#include <Systemic/Guards/ReaderGuard.h>
#include <Systemic/Guards/WriterGuard.h>

#include <CoreEngine/Contexts/ExecutionContext.h>
#include <CoreEngine/DataStorage/Row/SerializedRow.h>

#include "CoreEngine/BufferPool/StorageLayout.h"

namespace CoreEngine {
    VersionDatabase& VersionDatabase::Get(){
        static VersionDatabase instance;
        return instance;
    }

    void VersionDatabase::Initialize(const ExecutionContext& baseContext){
        constexpr auto sysDbName = DataTypes::StringView("versionDb");
        constexpr auto sysDbPath = DataTypes::StringView("versionDb/versionDb.data");

        this->PopulateFilenames(baseContext.GetAllocator(), sysDbName);

        this->_pageAccessor = !CoreEngine::VersionDatabase::VersionDatabaseExists(sysDbPath)
            ? CoreEngine::CreateDatabase(baseContext.GetAllocator(), sysDbName)
            : UseDatabase(baseContext.GetAllocator(), sysDbName);

        this->lastUsedPageId = INVALID_PAGE_ID;
        const auto headerPage = this->_pageAccessor.GetPage<Pages::HeaderPageView>(StorageLayout::HEADER_PAGE_ID);
        this->header = headerPage.GetDatabaseHeader();
    }

    VersionDatabase::VersionDatabase(): _pageAccessor(){
        this->lastUsedPageId = INVALID_PAGE_ID;
    }

    VersionDatabase::~VersionDatabase(){
        this->_allocator.Release();
    }

    bool VersionDatabase::VersionDatabaseExists(const DataTypes::StringView& path){
        return std::filesystem::exists(path.ToPath());
    }

    void VersionDatabase::PopulateFilenames(
        const ::Memory::IAllocator* allocator,
        const DataTypes::StringView& dbName
    ){
        const auto path = DataTypes::String::Concat(allocator, dbName, "/", dbName);
        this->filename = DataTypes::String::Concat(&this->_allocator, path, Constants::DATA_FILE_EXTENSION);
        this->systemFilename = DataTypes::String::Concat(&this->_allocator, path, Constants::SYS_EXTENSION, Constants::DATA_FILE_EXTENSION);

        this->filenameView = DataTypes::StringView::ViewOf(this->filename);
        this->systemFilenameView = DataTypes::StringView::ViewOf(this->systemFilename);
    }

    void VersionDatabase::WriteHeaderToFile() const{
        const auto headerPage = this->_pageAccessor.GetPage<Pages::HeaderPageView>(StorageLayout::HEADER_PAGE_ID);
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
        //         StorageLayout::GamPageId(this->header._lastGamIndex)
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

    bool VersionDatabase::TryGetLastUndoPage(const row_size_t size, Pages::PageView* outPage) const{
        const auto pageId = this->lastUsedPageId.load(std::memory_order_relaxed);
        if (pageId == INVALID_PAGE_ID)
            return false;

        auto page = this->_pageAccessor.GetPage<Pages::PageView>(pageId);

        MultiThreading::ReaderGuard lastUsedPageLatch(&page.Latch());

        if (page.BytesLeft() >= size){
            *outPage = std::move(page);
            return true;

        }

        return false;
    }

    Pages::PageView VersionDatabase::CreateUndoPage(const ::Memory::IAllocator* allocator){
        extent_id_t newExtentId = 0;
        page_id_t newPageId = 0;

        this->AllocateNewExtent(allocator, newPageId, newExtentId);

        this->lastUsedPageId.store(newPageId, std::memory_order_release);

        for (page_id_t pageId = newPageId; pageId < newPageId + StorageLayout::EXTENT_SIZE; pageId++){
            const auto pfs = this->_pageAccessor.GetAssociatedPfsPage(pageId);

            MultiThreading::WriterGuard lock(&pfs.Latch());
            auto page = this->_pageAccessor.CreatePage<Pages::PageView>(pageId);

            pfs.SetPageMetaData(&page);

            return page;
        }

        std::unreachable();
    }

    Pages::PageView VersionDatabase::GetLastUndoPage(
        const ::Memory::IAllocator* allocator,
        const row_size_t size
    ) {
        const auto gamPage = this->_pageAccessor.GetPage<Pages::GlobalAllocationPageView>(StorageLayout::GamPageId(this->header._lastGamIndex));

        Pages::PageView cachedPage;
        if (this->TryGetLastUndoPage(size, &cachedPage))
            return cachedPage;

        for (const auto extentId : gamPage.GetAllocatedExtents(allocator)){
            const page_id_t firstExtentPageId = StorageLayout::ExtentFirstPageId(extentId);

            for (page_id_t pageId = firstExtentPageId; pageId < firstExtentPageId + StorageLayout::EXTENT_SIZE; pageId++){
                {
                    const auto pfs = this->_pageAccessor.GetAssociatedPfsPage(pageId);

                    MultiThreading::ReaderGuard pfsLock(&pfs.Latch());

                    const auto categorySize = Pages::PageFreeSpaceView::ObjectSizeToCategory(size);

                    if(pfs.GetPageSizeCategory(pageId) <= categorySize)
                        continue;
                }

                auto page = this->_pageAccessor.GetPage<Pages::PageView>(pageId);

                MultiThreading::ReaderGuard undoLatch(&page.Latch());

                if (page.BytesLeft() >= size) {
                    this->lastUsedPageId.store(page.PageId(), std::memory_order_release);
                    return page;
                }
            }
        }

        return this->CreateUndoPage(allocator);
    }

    StorageTypes::RID VersionDatabase::InsertRow(
        const ::Memory::IAllocator* allocator,
        const Pages::RawRowReference& rowRef
    ){
        // const auto page = this->GetLastUndoPage(allocator, rowRef.size);
        //
        // MultiThreading::WriterGuard lock(&page.Latch());
        //
        // const auto payload = StorageTypes::SerializedRow::FromRowPtr(rowRef);
        // const auto indexPosition = page.InsertRow(payload);
        //
        // this->numberOfPendingVersions.fetch_add(1, std::memory_order_relaxed);
        //
        // return StorageTypes::RID(page.PageId(), indexPosition);
        return StorageTypes::RID(0, 0);
    }

     bool VersionDatabase::RetrieveVersionedRID(
        const Snapshot& snapshot,
        const StorageTypes::RowHeader* rowHeader,
        StorageTypes::RID* outRID
    )const {
        while (true){
            const auto page = this->_pageAccessor.GetPage<Pages::PageView>(rowHeader->_versionRID._pageId);

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
        // const auto gamPage = Storage::StorageManager::Get().GetPage<Pages::GlobalAllocationPageView>(
        //     this->systemFileKey,
        //     StorageLayout::GamPageId(this->header._lastGamIndex)
        // );

        // return gamPage.GetAllocatedExtents(startingExtentId);
        return std::vector<extent_id_t>();
        // return gamPage.GetAllocatedExtents(startingExtentId);
    }

    extent_id_t VersionDatabase::CleanupVersionedData(
        const transaction_id_t transactionId,
        const extent_id_t startingExtentId
    )const {
        const auto extents = this->GetAllocatedExtents(startingExtentId);

        for (const auto extentId : extents){
            const auto firstExtentPageId = StorageLayout::ExtentFirstPageId(extentId * StorageLayout::EXTENT_SIZE);

            bool isExtentEmpty = true;
            for (page_id_t pageId = firstExtentPageId; pageId < firstExtentPageId + StorageLayout::EXTENT_SIZE; pageId++){
                auto pfs = this->_pageAccessor.GetAssociatedPfsPage(pageId);

                {
                    MultiThreading::ReaderGuard pfsReaderLock(&pfs.Latch());

                    if (!pfs.IsPageAllocated(pageId))
                        continue;
                }

                auto page = this->_pageAccessor.GetPage<Pages::PageView>(pageId);

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
                    MultiThreading::WriterGuard pfsWriterLock(&pfs.Latch());
                    pfs.SetPageFreed(pageId);

                    continue;
                }

                isExtentEmpty = false;
            }

            if (isExtentEmpty) {
                auto gamPage = this->_pageAccessor.GetPage<Pages::GlobalAllocationPageView>(StorageLayout::GamPageId(this->header._lastGamIndex));

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
