#include "../../include/Pages/GlobalAllocationPageView.h"

#include "DataStorage/ExtentReservation.h"
#include "Pages/AllocationPageView.h"
#include "Pages/Additional/Frame.h"

namespace Pages{
    GlobalAllocationPageAdditionalHeader* GlobalAllocationPageView::GetAdditionalHeader() const{
        return reinterpret_cast<GlobalAllocationPageAdditionalHeader*>(this->_frame->_data + Constants::PAGE_HEADER_SIZE);
    }

    void GlobalAllocationPageView::SetBits(const UnsignedInt startIndex, const UnsignedInt numberOfBits) const{
        for (Int i = startIndex; i < startIndex + numberOfBits; i++)
            this->SetBit(i);
    }

    GlobalAllocationPageView::GlobalAllocationPageView(Frame* frame) : PageView(frame){
        this->initialOffset = Constants::GAM_METADATA_SIZE;
    }

    GlobalAllocationPageView::GlobalAllocationPageView(GlobalAllocationPageView&& other) noexcept{
        this->_frame = other._frame;
        other._frame = nullptr;
    }

    GlobalAllocationPageView& GlobalAllocationPageView::operator=(GlobalAllocationPageView&& other) noexcept{
        if (this == &other)
            return *this;

        this->_frame = other._frame;
        this->initialOffset = other.initialOffset;

        other._frame = nullptr;
        return *this;
    }

    extent_id_t GlobalAllocationPageView::FindContiguousExtentsNoLock(const extent_id_t startingIndex, const Int numberOfExtents) const{
        Int length = 0;

        extent_id_t startingBit = startingIndex;
        extent_id_t currentBit = startingIndex;

        const auto step = [&](const extent_id_t bitIndex) -> bool{
            if (this->GetBit(bitIndex)){
                length = 0;
                startingBit = bitIndex + 1;
            }
             else if (++length == numberOfExtents)
                return true;

            return false;
        };

        for (; currentBit < Constants::EXTENT_BIT_MAP_SIZE && (currentBit & 63) != 0; currentBit++){
            if (step(currentBit))
                return startingBit;
        }

        for (; currentBit + 64 <= Constants::EXTENT_BIT_MAP_SIZE; currentBit += 64){
            uint64_t word;
            std::memcpy(&word, this->_frame->_data + this->initialOffset + (currentBit >> 3), sizeof(word));

            if (word == 0x00){
                length += 64;
                if (length >= numberOfExtents)
                    return startingBit;
            }
            else if (word == ~0x00){
                length = 0;
                startingBit = currentBit + 64;
            }
            else{
                for (extent_id_t bit = 0; bit < 64; bit++){
                    if (step(currentBit + bit))
                        return startingBit;
                }
            }
        }

        for (; currentBit < Constants::EXTENT_BIT_MAP_SIZE; currentBit++){
            if (step(currentBit))
                return startingBit;
        }

        return INVALID_EXTENT_ID;
    }

    Int GlobalAllocationPageView::ReserveExtentsNoLock(
        DataStructures::PolymorphicArray<CoreEngine::StorageTypes::ExtentSegment>& extents,
        const Int neededExtents
    ) const{
        auto* additionalHeader = this->GetAdditionalHeader();
        if (additionalHeader->_freeExtentCount == 0 || neededExtents <= 0)
            return 0;

        //fast path:
        const auto base = AllocationPageView::CalculateExtentIdOffsetByGamPageId(this->_frame->Header()->pageId);
        UnsignedInt remaining = neededExtents;
        const auto tailAvailable = static_cast<UnsignedInt>(Constants::EXTENT_BIT_MAP_SIZE - additionalHeader->_appendExtentId);
        if (tailAvailable > 0){
            const auto bitsToTake = Math::Min(remaining, tailAvailable);
            this->SetBits(additionalHeader->_appendExtentId, bitsToTake);

            extents.Push(CoreEngine::StorageTypes::ExtentSegment(base + additionalHeader->_appendExtentId, bitsToTake));

            additionalHeader->_appendExtentId += bitsToTake;
            remaining -= bitsToTake;
        }

        if (remaining > 0){

        }

        const auto bitsReserved = neededExtents - remaining;
        additionalHeader->_freeExtentCount -= bitsReserved;
        this->_frame->isDirty = true;

        return bitsReserved;
    }

    Int GlobalAllocationPageView::AllocateFragmentedExtentsNoLock(
        DataStructures::PolymorphicArray<extent_id_t>& extents,
        const Int numberOfExtents
    ) const{
        Int allocatedExtents = 0;

        auto* additionalHeader = this->GetAdditionalHeader();

        const auto base = AllocationPageView::CalculateExtentIdOffsetByGamPageId(this->_frame->Header()->pageId);
        for (extent_id_t extentId = additionalHeader->_firstFreeExtentId; extentId < Constants::EXTENT_BIT_MAP_SIZE; extentId++){
            if (allocatedExtents == numberOfExtents)
                break;

            if (this->GetBit(extentId))
                continue;

            this->SetBit(extentId);
            this->_frame->isDirty = true;

            allocatedExtents++;

            extents.Push(base + extentId);
        }

        if (allocatedExtents > 0){
            // Fragmented consumes every free extent from _firstFreeExtentId up to the last one it
            // touched, so [0, lastLocal] is now fully allocated: first free is lastLocal + 1.
            const extent_id_t lastLocal = *extents.Back() - base;
            additionalHeader->_firstFreeExtentId = lastLocal + 1;
            if (lastLocal + 1 > additionalHeader->_appendExtentId)
                additionalHeader->_appendExtentId = lastLocal + 1;
            additionalHeader->_freeExtentCount -= allocatedExtents;
        }

        return allocatedExtents;
    }

    bool GlobalAllocationPageView::TryAllocateContiguousExtentsNoLock(
        DataStructures::PolymorphicArray<extent_id_t>& extents,
        const Int numberOfExtents
    ) const{
        auto* additionalHeader = this->GetAdditionalHeader();

        if (additionalHeader->_freeExtentCount < numberOfExtents)
            return false;

        const auto startingBit = this->FindContiguousExtentsNoLock(additionalHeader->_firstFreeExtentId, numberOfExtents);
        if (startingBit == INVALID_EXTENT_ID)
            return false;

        const auto base = AllocationPageView::CalculateExtentIdOffsetByGamPageId(this->_frame->Header()->pageId);
        for (Int i = 0; i < numberOfExtents; i++){
            this->SetBit(startingBit + i);
            extents.Push(base + startingBit + i);
        }

        const extent_id_t runEnd = startingBit + numberOfExtents;

        // If we consumed the leading hole, [0, runEnd) is now fully allocated.
        if (startingBit == additionalHeader->_firstFreeExtentId)
            additionalHeader->_firstFreeExtentId = runEnd;

        // Only advance the tail frontier if the run reached into it.
        if (runEnd > additionalHeader->_appendExtentId)
            additionalHeader->_appendExtentId = runEnd;

        additionalHeader->_freeExtentCount -= numberOfExtents;
        this->_frame->isDirty = true;
        return true;
    }

    DataStructures::PolymorphicArray<extent_id_t> GlobalAllocationPageView::GetAllocatedExtents(const ::Memory::IAllocator* allocator) const{
        return DataStructures::PolymorphicArray<extent_id_t>(allocator);
    }

    void GlobalAllocationPageView::DeallocateExtentNoLock(const extent_id_t extentId) const{
        const auto bitIndex = extentId - AllocationPageView::CalculateExtentIdOffsetByGamPageId(this->_frame->Header()->pageId);
        this->ClearBit(bitIndex);
        this->_frame->isDirty = true;

        auto* additionalHeader = this->GetAdditionalHeader();
        if (bitIndex == additionalHeader->_appendExtentId - 1)
            additionalHeader->_appendExtentId--;

        additionalHeader->_firstFreeExtentId = Math::Min(bitIndex, additionalHeader->_firstFreeExtentId);
        additionalHeader->_freeExtentCount++;
    }

    bool GlobalAllocationPageView::IsFull() const{
        return this->GetAdditionalHeader()->_freeExtentCount == 0;
    }
}
