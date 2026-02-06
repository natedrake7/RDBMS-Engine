#include "../../include/Pages/AllocationPageView.h"

#include "Database.h"
#include "Guards/ReaderGuard.h"
#include "Pages/Additional/Frame.h"

namespace Pages{
    size_t AllocationPageView::GetByteIndex(const extent_id_t extentId) const noexcept{
        return this->initialOffset + (extentId >> 3);
    }

    bool AllocationPageView::GetBit(const std::size_t bitIndex) const noexcept{
        const auto mask = static_cast<UnsignedTinyInt>(1u << (bitIndex & 7u));
        return (this->framePtr->data[this->GetByteIndex(bitIndex)] & mask) != 0;
    }

    void AllocationPageView::SetBit(const std::size_t bitIndex) const noexcept{
        const std::size_t byteIndex = this->GetByteIndex(bitIndex);
        const auto mask = static_cast<UnsignedTinyInt>(1u << (bitIndex & 7u));
        const auto byte = static_cast<UnsignedTinyInt>(this->framePtr->data[byteIndex] | mask);
        this->framePtr->data[byteIndex] = static_cast<char>(byte);
    }

    void AllocationPageView::ClearBit(const std::size_t bitIndex) const noexcept{
        const std::size_t byteIndex = this->GetByteIndex(bitIndex);
        const auto mask = static_cast<UnsignedTinyInt>(1u << (bitIndex & 7u));
        auto byte = this->framePtr->data[byteIndex];
        byte = static_cast<UnsignedTinyInt>(byte & static_cast<UnsignedTinyInt>(~mask));
        this->framePtr->data[byteIndex] = static_cast<char>(byte);
    }

    AllocationPageView::AllocationPageView() : PageView() {
        this->lastAllocatedExtentId = 0;
        this->initialOffset = PAGE_HEADER_SIZE + ALLOCATION_PAGE_ADDITIONAL_HEADER_SIZE;
    }

    AllocationPageView::AllocationPageView(Frame* framePtr) : PageView(framePtr) {
        this->lastAllocatedExtentId = 0;
        this->initialOffset = PAGE_HEADER_SIZE + ALLOCATION_PAGE_ADDITIONAL_HEADER_SIZE;
    }

    AllocationPageView::AllocationPageView(AllocationPageView&& other) noexcept{
        this->framePtr = other.framePtr;
        this->lastAllocatedExtentId = other.lastAllocatedExtentId;
        this->initialOffset = other.initialOffset;

        other.framePtr = nullptr;
    }

    AllocationPageView& AllocationPageView::operator=(AllocationPageView&& other) noexcept{
        if (this == &other)
            return *this;

        this->framePtr = other.framePtr;
        this->lastAllocatedExtentId = other.lastAllocatedExtentId;
        this->initialOffset = other.initialOffset;

        other.framePtr = nullptr;
        return *this;
    }

    extent_id_t AllocationPageView::SetExtentsAllocated(
        const std::vector<extent_id_t>& extentIds,
        const page_id_t globalAllocationMapPageId
    ){
        for (const auto& extentId : extentIds){
            const extent_id_t bitMapId = extentId - AllocationPageView::CalculatePageIdOffsetByGamPageId(globalAllocationMapPageId);

            if (bitMapId >= EXTENT_BIT_MAP_SIZE)
                return extentId;

            this->SetBit(bitMapId);
            this->lastAllocatedExtentId = bitMapId;
            this->framePtr->isDirty = true;
        }

        return INVALID_EXTENT_ID;
    }

    void AllocationPageView::SetDeallocatedExtent(const extent_id_t extentId) const{
        this->ClearBit(extentId);
        this->framePtr->isDirty = true;
    }

    void AllocationPageView::GetAllocatedExtents(std::vector<extent_id_t>* allocatedExtents) const{
        this->GetAllocatedExtents(allocatedExtents, 0);
    }

    void AllocationPageView::GetAllocatedExtents(
        std::vector<extent_id_t>* allocatedExtents,
        const extent_id_t startingExtentIndex
    ) const{
        allocatedExtents->clear();

        const page_id_t globalAllocationMapPageId = DatabaseEngine::Database::GetGamAssociatedPage(this->framePtr->headerPtr->pageId);
        const page_id_t offSet = AllocationPageView::CalculatePageIdOffsetByGamPageId(globalAllocationMapPageId);

        if(startingExtentIndex >= EXTENT_BIT_MAP_SIZE)
            return;

        MultiThreading::ReaderGuard lock(&this->framePtr->latch);
        for (extent_id_t id = startingExtentIndex; id < this->lastAllocatedExtentId; id++){
            if (this->GetBit(id))
                allocatedExtents->push_back(offSet + id);
        }
    }

    void AllocationPageView::SetNextPageId(const page_id_t nextPageId) const{
        this->framePtr->additionalHeader.allocationHeaderPtr->nextPageId = nextPageId;
    }

    page_id_t AllocationPageView::NextPageId() const{
        return this->framePtr->additionalHeader.allocationHeaderPtr->nextPageId;
    }

    page_id_t AllocationPageView::CalculatePageIdOffsetByGamPageId(const page_id_t globalAllocationMapPageId) {
        return (globalAllocationMapPageId - 2) * GAM_PAGE_SIZE;
    }
}
