#include "../include/Database.h"

#include "../include/SystemDatabases/SystemCatalog.h"

#include "../include/DatabaseConstants.h"
#include "../include/DataStorage/Table.h"
#include "../include/BufferPool/StorageManager.h"
#include "../../Systemic/include/Guards/WriterGuard.h"

#include "Guards/ReaderGuard.h"
#include "Managers/GlobalMemoryManager.h"

namespace CoreEngine{
    template <typename TView>
    TView Database::LazyAllocateSinglePage(const ::Memory::IAllocator* allocator, const table_id_t ordinalPos){
        auto extentReservation = this->ReserveExtents(allocator, 1, ordinalPos);
        auto page = extentReservation.Next<TView>();

        {
            const auto pfs = Database::GetAssociatedPfsPage(this->systemFileKey, page.PageId());
            MultiThreading::WriterGuard lock(&pfs.Latch());
            pfs.SetPageMetaData(&page);
        }

        return page;
    }

    template Pages::PageView Database::LazyAllocateSinglePage<Pages::PageView>(const ::Memory::IAllocator* allocator, table_id_t ordinalPos);
    template Pages::IndexPageView Database::LazyAllocateSinglePage<Pages::IndexPageView>(const ::Memory::IAllocator* allocator, table_id_t ordinalPos);
    template Pages::LargeObjectView Database::LazyAllocateSinglePage<Pages::LargeObjectView>(const ::Memory::IAllocator* allocator, table_id_t ordinalPos);
    template Pages::OverflowPageView Database::LazyAllocateSinglePage<Pages::OverflowPageView>(const ::Memory::IAllocator* allocator, table_id_t ordinalPos);

    template<typename TVIew>
    TVIew Database::LazyAllocateTablePage(
        const ::Memory::IAllocator* allocator,
        const table_id_t ordinalPos
    ){
        const auto allocationPageId = this->_tables[ordinalPos]->GetAllocationPageId();

        TVIew page;
        if (allocationPageId == INVALID_PAGE_ID)
            return this->LazyAllocateSinglePage<TVIew>(allocator, ordinalPos);

        constexpr auto PAGE_TYPE = Storage::StorageManager::DeducePageType<TVIew>();
        const auto allocationPage = Storage::StorageManager::Get().GetPage<Pages::AllocationPageView>(
            this->dataFileKey,
            allocationPageId
        );

        DataStructures::PolymorphicArray<extent_id_t> allocatedExtents(allocator);
        allocationPage.GetAllocatedExtents(&allocatedExtents);

        for (const auto extentId : allocatedExtents){
            const auto firstExtentPageId = Database::CalculateExtentFirstPageId(extentId);
            for (page_id_t pageId = firstExtentPageId; pageId < firstExtentPageId + Constants::EXTENT_SIZE; pageId++){
                const auto pfs = Database::GetAssociatedPfsPage(this->systemFileKey, pageId);

                {
                    MultiThreading::ReaderGuard lock(&pfs.Latch());
                    if (pfs.GetPageType(pageId) != PAGE_TYPE)
                        break;

                    if (pfs.IsPageAllocated(pageId))
                        continue;
                }

                {
                    MultiThreading::WriterGuard lock(&pfs.Latch());
                    if (pfs.IsPageAllocated(pageId))
                        continue;

                    page = Storage::StorageManager::Get().CreatePage<TVIew>(this->dataFileKey, pageId);
                    pfs.SetPageMetaData(&page);
                }
            }
        }

        return this->LazyAllocateSinglePage<TVIew>(allocator, ordinalPos);
    }


    template Pages::PageView Database::LazyAllocateTablePage<Pages::PageView>(const ::Memory::IAllocator* allocator, table_id_t ordinalPos);
    template Pages::IndexPageView Database::LazyAllocateTablePage<Pages::IndexPageView>(const ::Memory::IAllocator* allocator, table_id_t ordinalPos);
    template Pages::LargeObjectView Database::LazyAllocateTablePage<Pages::LargeObjectView>(const ::Memory::IAllocator* allocator, table_id_t ordinalPos);
    template Pages::OverflowPageView Database::LazyAllocateTablePage<Pages::OverflowPageView>(const ::Memory::IAllocator* allocator, table_id_t ordinalPos);
}