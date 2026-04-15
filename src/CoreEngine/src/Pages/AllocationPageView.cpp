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
        this->initialOffset = Constants::PAGE_HEADER_SIZE + Constants::ALLOCATION_PAGE_ADDITIONAL_HEADER_SIZE;
    }

    AllocationPageView::AllocationPageView(Frame* framePtr) : PageView(framePtr) {
        this->initialOffset = Constants::PAGE_HEADER_SIZE + Constants::ALLOCATION_PAGE_ADDITIONAL_HEADER_SIZE;
    }

    AllocationPageView::AllocationPageView(AllocationPageView&& other) noexcept{
        this->framePtr = other.framePtr;
        this->initialOffset = other.initialOffset;

        other.framePtr = nullptr;
    }

    AllocationPageView& AllocationPageView::operator=(AllocationPageView&& other) noexcept{
        if (this == &other)
            return *this;

        this->framePtr = other.framePtr;
        this->initialOffset = other.initialOffset;

        other.framePtr = nullptr;
        return *this;
    }

    extent_id_t AllocationPageView::SetExtentsAllocated(
        const std::vector<extent_id_t>& extentIds,
        const page_id_t globalAllocationMapPageId
    ) const{
        for (const auto extentId : extentIds){
            const extent_id_t bitMapId = extentId - AllocationPageView::CalculatePageIdOffsetByGamPageId(globalAllocationMapPageId);

            if (bitMapId >= Constants::EXTENT_BIT_MAP_SIZE)
                return extentId;

            this->SetBit(bitMapId);
            this->framePtr->additionalHeader.allocationHeaderPtr->lastAllocatedExtentId = extentId;
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
        const page_id_t globalAllocationMapPageId = CoreEngine::Database::GetGamAssociatedPage(this->framePtr->headerPtr->pageId);
        const page_id_t offSet = AllocationPageView::CalculatePageIdOffsetByGamPageId(globalAllocationMapPageId);

        if(startingExtentIndex >= Constants::EXTENT_BIT_MAP_SIZE)
            return;

        MultiThreading::ReaderGuard lock(&this->framePtr->latch);
        for (extent_id_t id = startingExtentIndex - offSet; id < this->framePtr->additionalHeader.allocationHeaderPtr->lastAllocatedExtentId; id++){
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
        return (globalAllocationMapPageId - 2) * Constants::GAM_PAGE_SIZE;
    }
}
