#include "../../include/Pages/GlobalAllocationPageView.h"
#include "Guards/ReaderGuard.h"
#include "Pages/AllocationPageView.h"
#include "Pages/Additional/Frame.h"

namespace Pages{
    bool GlobalAllocationPageView::GetBit(const std::size_t bitIndex) const noexcept{
        const std::size_t byteIndex = bitIndex >> 3;                 // / 8
        const auto mask = static_cast<UnsignedTinyInt>(1u << (bitIndex & 7u));
        return (this->framePtr->data[byteIndex] & mask) != 0;
    }

    void GlobalAllocationPageView::SetBit(const std::size_t bitIndex) const noexcept{
        const std::size_t byteIndex = bitIndex >> 3;
        const auto mask = static_cast<UnsignedTinyInt>(1u << (bitIndex & 7u));
        const auto byte = static_cast<UnsignedTinyInt>(this->framePtr->data[byteIndex] | mask);
        this->framePtr->data[byteIndex] = static_cast<char>(byte);
    }

    void GlobalAllocationPageView::ClearBit(const std::size_t bitIndex) const noexcept{
        const std::size_t byteIndex = bitIndex >> 3;
        const auto mask = static_cast<UnsignedTinyInt>(1u << (bitIndex & 7u));
        auto byte = this->framePtr->data[byteIndex];
        byte = static_cast<UnsignedTinyInt>(byte & static_cast<UnsignedTinyInt>(~mask));
        this->framePtr->data[byteIndex] = static_cast<char>(byte);
    }

    GlobalAllocationPageView::GlobalAllocationPageView(Frame* frame) : PageView(frame) {
        this->lastAllocatedExtentId = 0;
        this->type = PageType::GAM;
    }

    int GlobalAllocationPageView::AllocateExtentsNoLock(std::vector<extent_id_t>& extents, const Int numberOfExtents){
        int allocatedExtents = 0;

        for (extent_id_t extentId = this->lastAllocatedExtentId; extentId < EXTENT_BIT_MAP_SIZE; extentId++){
            if (allocatedExtents == numberOfExtents)
                break;

            if (!this->GetBit(extentId))
                continue;

            this->lastAllocatedExtentId = extentId;
            this->ClearBit(extentId);
            this->framePtr->isDirty = true;

            allocatedExtents++;

            extents.push_back(AllocationPageView::CalculatePageIdOffsetByGamPageId(this->headerPtr->pageId) + extentId);
        }

        return allocatedExtents;
    }

    void GlobalAllocationPageView::DeallocateExtent(const extent_id_t extentId) const{
        this->SetBit(extentId);
        this->framePtr->isDirty = true;
    }

    bool GlobalAllocationPageView::IsFull() const{
        return !this->GetBit(EXTENT_BIT_MAP_SIZE - 1);
    }

    std::vector<extent_id_t> GlobalAllocationPageView::GetAllocatedExtents(const extent_id_t startingIndex) const{
        std::vector<extent_id_t> allocatedExtents;

        if(startingIndex >= EXTENT_BIT_MAP_SIZE)
            return allocatedExtents;

        MultiThreading::ReaderGuard lock(&this->framePtr->latch);

        allocatedExtents.reserve(EXTENT_BIT_MAP_SIZE - startingIndex);

        for (extent_id_t id = startingIndex; id < EXTENT_BIT_MAP_SIZE; id++){
            if (this->GetBit(id))
                continue;

            allocatedExtents.push_back(AllocationPageView::CalculatePageIdOffsetByGamPageId(this->headerPtr->pageId) + id);
        }

        return allocatedExtents;
    }
}
