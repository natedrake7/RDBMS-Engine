#pragma once
#include <CoreEngine/Database.h>

#include <CoreEngine/DataStorage/Table.h>
#include <Systemic/Guards/WriterGuard.h>

#include <CoreEngine/Accessors/PageAccessor.h>
#include <CoreEngine/DataStorage/ExtentReservation.Templates.h>

namespace CoreEngine{
    static_assert(StorageLayout::PAGE_FREE_SPACE_SIZE % StorageLayout::EXTENT_SIZE == 0);

    template <PageLayout::PageViewType TView, typename... Args>
    TView Database::LazyAllocateSinglePage(
        const ::Memory::IAllocator* allocator,
        const StorageTypes::Table* table,
        Args&&... args
    ){
        auto extentReservation = this->ReserveExtents(allocator, table, 1);
        return extentReservation.Next<TView>(std::forward<Args>(args)...);
    }

    template<PageLayout::PageViewType TView, typename... Args>
    TView Database::LazyAllocateTablePage(
        const ::Memory::IAllocator* allocator,
        const StorageTypes::Table* table,
        Args&&... args
    ){
        const auto allocationPageId = table->GetAllocationPageId();

        if (allocationPageId == INVALID_PAGE_ID)
            return this->LazyAllocateSinglePage<TView>(allocator, table, std::forward<Args>(args)...);

        const auto allocationPage = this->_pageAccessor.GetPage<Pages::AllocationPageView>(allocationPageId);

        DataStructures::PolymorphicArray<extent_id_t> allocatedExtents(allocator);
        allocationPage.GetAllocatedExtents(&allocatedExtents);

        for (const auto extentId : allocatedExtents){
            const auto firstExtentPageId = StorageLayout::ExtentFirstPageId(extentId);
            for (page_id_t pageId = firstExtentPageId; pageId < firstExtentPageId + StorageLayout::EXTENT_SIZE; pageId++){
                const auto pfs = this->_pageAccessor.GetAssociatedPfsPage(pageId);

                MultiThreading::WriterGuard lock(&pfs.Latch());
                if (pfs.IsPageAllocated(pageId))
                    continue;

                auto page = this->_pageAccessor.CreatePage<TView>(pageId, std::forward<Args>(args)...);
                pfs.SetPageMetaData(&page);
                return page;
            }
        }

        return this->LazyAllocateSinglePage<TView>(allocator, table, std::forward<Args>(args)...);
    }
}
