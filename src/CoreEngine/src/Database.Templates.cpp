#include <CoreEngine/Database.h>

#include <CoreEngine/SystemDatabases/SystemCatalog.h>

#include <CoreEngine/DataStorage/Table.h>
#include <Systemic/Guards/WriterGuard.h>

#include <CoreEngine/Managers/GlobalMemoryManager.h>

#include "CoreEngine/Pages/OverflowPageView.h"
#include "CoreEngine/Pages/LargeObjects/LobDataView.h"
#include "CoreEngine/Pages/LargeObjects/LobIndexView.h"
#include "CoreEngine/Pages/LargeObjects/LobRootView.h"
#include <CoreEngine/BufferPool/StorageManager.Templates.h>

namespace CoreEngine{
    static_assert(StorageLayout::PAGE_FREE_SPACE_SIZE % StorageLayout::EXTENT_SIZE == 0);

    template <typename TView>
    TView Database::LazyAllocateSinglePage(
        const ::Memory::IAllocator* allocator,
        const StorageTypes::Table* table
    ){
        auto extentReservation = this->ReserveExtents(allocator, table, 1);
        return extentReservation.Next<TView>();
    }

    template Pages::PageView Database::LazyAllocateSinglePage<Pages::PageView>(const ::Memory::IAllocator* allocator, const StorageTypes::Table* table);
    template Pages::IndexPageView Database::LazyAllocateSinglePage<Pages::IndexPageView>(const ::Memory::IAllocator* allocator, const StorageTypes::Table* table);
    template Pages::LobDataView Database::LazyAllocateSinglePage<Pages::LobDataView>(const ::Memory::IAllocator* allocator, const StorageTypes::Table* table);
    template Pages::OverflowPageView Database::LazyAllocateSinglePage<Pages::OverflowPageView>(const ::Memory::IAllocator* allocator, const StorageTypes::Table* table);

    template<typename TVIew>
    TVIew Database::LazyAllocateTablePage(
        const ::Memory::IAllocator* allocator,
        const StorageTypes::Table* table
    ){
        const auto allocationPageId = table->GetAllocationPageId();

        if (allocationPageId == INVALID_PAGE_ID)
            return this->LazyAllocateSinglePage<TVIew>(allocator, table);

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

                auto page = this->_pageAccessor.GetPage<TVIew>(pageId);
                pfs.SetPageMetaData(&page);
                return page;
            }
        }

        return this->LazyAllocateSinglePage<TVIew>(allocator, table);
    }


    template Pages::PageView Database::LazyAllocateTablePage<Pages::PageView>(const ::Memory::IAllocator* allocator, const StorageTypes::Table* table);
    template Pages::IndexPageView Database::LazyAllocateTablePage<Pages::IndexPageView>(const ::Memory::IAllocator* allocator, const StorageTypes::Table* table);
    template Pages::OverflowPageView Database::LazyAllocateTablePage<Pages::OverflowPageView>(const ::Memory::IAllocator* allocator, const StorageTypes::Table* table);

    template Pages::LobRootView Database::LazyAllocateTablePage<Pages::LobRootView>(const ::Memory::IAllocator* allocator, const StorageTypes::Table* table);
    template Pages::LobIndexView Database::LazyAllocateTablePage<Pages::LobIndexView>(const ::Memory::IAllocator* allocator, const StorageTypes::Table* table);
    template Pages::LobDataView Database::LazyAllocateTablePage<Pages::LobDataView>(const ::Memory::IAllocator* allocator, const StorageTypes::Table* table);
}
