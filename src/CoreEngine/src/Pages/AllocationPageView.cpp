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
        if (segments.Empty())
            return;

        auto* header = this->GetAdditionalHeader();

        const auto base = CoreEngine::StorageLayout::GamBaseExtent(this->GamIndex());
        auto lastBit = header->_lastAllocatedExtentId;
        for (const auto& [_extentId, _count] : segments){
            const extent_id_t bitId = _extentId - base;

            for (extent_id_t i = bitId; i < bitId + _count; i++)
                this->SetBit(i);

            lastBit = Math::Max(lastBit, bitId + _count - 1);
        }

        header->_lastAllocatedExtentId = lastBit;
        this->_frame->MarkDirty();
    }

    void AllocationPageView::SetDeallocatedExtent(const extent_id_t extentId) const{
        this->ClearBit(extentId);
        this->_frame->MarkDirty();
    }

    void AllocationPageView::GetAllocatedExtents(DataStructures::PolymorphicArray<extent_id_t>* allocatedExtents) const{
        this->GetAllocatedExtents(allocatedExtents, 0);
    }

    void AllocationPageView::GetAllocatedExtents(
        DataStructures::PolymorphicArray<extent_id_t>* allocatedExtents,
        const extent_id_t startingExtentId
    ) const{
        allocatedExtents->Clear();
        const auto base = CoreEngine::StorageLayout::GamBaseExtent(this->GamIndex());
        const auto lastBit = this->GetAdditionalHeader()->_lastAllocatedExtentId;
        const auto firstBit = startingExtentId > base
            ? startingExtentId - base
            : 0;

        if(firstBit >= CoreEngine::StorageLayout::GAM_BIT_MAP_SIZE)
            return;

        MultiThreading::ReaderGuard lock(&this->_frame->_latch);
        for (extent_id_t bit = firstBit; bit <= lastBit; bit++){
            if (this->GetBit(bit))
                allocatedExtents->Push(base + bit);
        }
    }
}
