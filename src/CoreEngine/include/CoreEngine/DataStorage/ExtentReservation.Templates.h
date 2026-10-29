#pragma once
#include <CoreEngine/DataStorage/ExtentReservation.h>
#include <CoreEngine/Database.Templates.h>

namespace CoreEngine::StorageTypes{
    template <PageLayout::PageViewType TView, typename ... Args>
    TView ExtentReservation::Next(Args&&... args){
        if (this->_segmentIndex == this->_segments.Size()){
            return this->_db->LazyAllocateTablePage<TView>(
                this->_segments.GetAllocator(),
                this->_table,
                std::forward<Args>(args)...
            );
        }

        const auto& [_firstExtentId, _count] = this->_segments[this->_segmentIndex];

        const page_id_t pageId = (_firstExtentId + this->_segmentOffset) * StorageLayout::EXTENT_SIZE + this->_extentIndex++;

        if (this->_extentIndex == StorageLayout::EXTENT_SIZE){
            this->_segmentOffset++;
            this->_extentIndex = 0;
        }

        if (this->_segmentOffset == _count){
            this->_segmentIndex++;
            this->_segmentOffset = 0;
        }

        auto page = this->_pageAccessor.CreatePage<TView>(pageId, std::forward<Args>(args)...);

        {
            const auto pfs = this->_pageAccessor.GetAssociatedPfsPage(pageId);
            MultiThreading::WriterGuard lock(&pfs.Latch());
            pfs.SetPageMetaData(&page);
        }

        return page;
    }
}