#include <CoreEngine/Pages/AllocationPageView.h>
#include <Systemic/DataStructures/PolymorphicArray.h>

#include <CoreEngine/Database.h>
#include <Systemic/Guards/ReaderGuard.h>
#include <CoreEngine/Pages/Additional/Frame.h>

#include <CoreEngine/BufferPool/StorageLayout.h>

namespace Pages{
    AllocationPageView::AllocationPageView(){
        this->initialOffset = CoreEngine::StorageLayout::PAGE_HEADER_SIZE + sizeof(AllocationPageAdditionalHeader);
    }

    AllocationPageView::AllocationPageView(Frame* framePtr) : PageView(framePtr) {
        this->initialOffset = CoreEngine::StorageLayout::PAGE_HEADER_SIZE + sizeof(AllocationPageAdditionalHeader);
    }

    void AllocationPageView::ReserveExtentsNoLock(
        DataStructures::PolymorphicArray<CoreEngine::StorageTypes::ExtentSegment>& segments
    ) const{
        const auto base = CoreEngine::StorageLayout::GamBaseExtent(this->GamIndex());
        for (const auto& [_extentId, _count] : segments){
            const extent_id_t bitId = _extentId - base;

            for (extent_id_t i = bitId; i < bitId + _count; i++){
                this->SetBit(i);
            }
        }

        const auto* back = segments.Back();
        this->GetAdditionalHeader()->_lastAllocatedExtentId = back->_firstExtentId + back->_count - 1;
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
        const auto base = CoreEngine::StorageLayout::GamBaseExtent(this->GamIndex());
        const auto lastExtentId = this->GetAdditionalHeader()->_lastAllocatedExtentId;

        if(startingExtentIndex >= CoreEngine::StorageLayout::GAM_BIT_MAP_SIZE)
            return;

        MultiThreading::ReaderGuard lock(&this->_frame->_latch);
        for (extent_id_t id = startingExtentIndex - base; id <= lastExtentId; id++){
            if (this->GetBit(id))
                allocatedExtents->Push(base + id);
        }
    }
}
