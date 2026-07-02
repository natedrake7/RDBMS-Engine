#include "../../include/Pages/AllocationPageView.h"
#include "../../../Systemic/include/DataStructures/PolymorphicArray.h"

#include "Database.h"
#include "Guards/ReaderGuard.h"
#include "Pages/Additional/Frame.h"

namespace Pages{
    IndexAllocationPageAdditionalHeader* AllocationPageView::GetAdditionalHeader() const{
        return reinterpret_cast<IndexAllocationPageAdditionalHeader*>(this->_frame->_data + Constants::PAGE_HEADER_SIZE);
    }

    AllocationPageView::AllocationPageView(){
        this->initialOffset = Constants::PAGE_HEADER_SIZE + Constants::ALLOCATION_PAGE_ADDITIONAL_HEADER_SIZE;
    }

    AllocationPageView::AllocationPageView(Frame* framePtr) : PageView(framePtr) {
        this->initialOffset = Constants::PAGE_HEADER_SIZE + Constants::ALLOCATION_PAGE_ADDITIONAL_HEADER_SIZE;
    }

    AllocationPageView::AllocationPageView(AllocationPageView&& other) noexcept{
        this->_frame = other._frame;
        this->initialOffset = other.initialOffset;

        other._frame = nullptr;
    }

    AllocationPageView& AllocationPageView::operator=(AllocationPageView&& other) noexcept{
        if (this == &other)
            return *this;

        this->_frame = other._frame;
        this->initialOffset = other.initialOffset;

        other._frame = nullptr;
        return *this;
    }

    extent_id_t AllocationPageView::SetExtentsAllocatedNoLock(
        const DataStructures::PolymorphicArray<extent_id_t>& extentIds,
        const page_id_t globalAllocationMapPageId
    ) const{
        const auto base = AllocationPageView::CalculateExtentIdOffsetByGamPageId(globalAllocationMapPageId);
        extent_id_t lastExtentId = 0;
        for (const auto extentId : extentIds){
            const extent_id_t bitMapId = extentId - base;

            if (bitMapId >= Constants::EXTENT_BIT_MAP_SIZE){
                lastExtentId = extentId;
                break;
            }

            this->SetBit(bitMapId);
            this->_frame->isDirty = true;
        }

        

        return INVALID_EXTENT_ID;
    }

    void AllocationPageView::SetDeallocatedExtent(const extent_id_t extentId) const{
        this->ClearBit(extentId);
        this->_frame->isDirty = true;
    }

    void AllocationPageView::GetAllocatedExtents(DataStructures::PolymorphicArray<extent_id_t>* allocatedExtents) const{
        this->GetAllocatedExtents(allocatedExtents, 0);
    }

    void AllocationPageView::GetAllocatedExtents(
        DataStructures::PolymorphicArray<extent_id_t>* allocatedExtents,
        const extent_id_t startingExtentIndex
    ) const{
        allocatedExtents->Clear();
        const page_id_t globalAllocationMapPageId = CoreEngine::Database::GetGamAssociatedPage(this->_frame->Header()->pageId);
        const page_id_t offSet = AllocationPageView::CalculateExtentIdOffsetByGamPageId(globalAllocationMapPageId);

        if(startingExtentIndex >= Constants::EXTENT_BIT_MAP_SIZE)
            return;

        MultiThreading::ReaderGuard lock(&this->_frame->latch);
        for (extent_id_t id = startingExtentIndex - offSet; id < this->GetAdditionalHeader()->lastAllocatedExtentId; id++){
            if (this->GetBit(id))
                allocatedExtents->Push(offSet + id);
        }
    }

    void AllocationPageView::SetNextPageId(const page_id_t nextPageId) const{
        this->GetAdditionalHeader()->nextPageId = nextPageId;
    }

    page_id_t AllocationPageView::NextPageId() const{
        return this->GetAdditionalHeader()->nextPageId;
    }

    extent_id_t AllocationPageView::CalculateExtentIdOffsetByGamPageId(const page_id_t gamPageId) {
        return (gamPageId - 2) * Constants::GAM_PAGE_SIZE;
    }
}
