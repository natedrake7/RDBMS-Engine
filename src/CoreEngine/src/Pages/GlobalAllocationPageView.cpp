#include "../../include/Pages/GlobalAllocationPageView.h"

#include "DataStorage/ExtentReservation.h"
#include "Pages/AllocationPageView.h"
#include "Pages/Additional/Frame.h"

namespace Pages{
    GlobalAllocationPageAdditionalHeader* GlobalAllocationPageView::GetAdditionalHeader() const{
        return reinterpret_cast<GlobalAllocationPageAdditionalHeader*>(this->_frame->_data + Constants::PAGE_HEADER_SIZE);
    }

    void GlobalAllocationPageView::SetBits(const UnsignedInt startIndex, const UnsignedInt numberOfBits) const{
        for (auto i = startIndex; i < startIndex + numberOfBits; i++)
            this->SetBit(i);
    }

    void GlobalAllocationPageView::CollectRunsNoLock(
        DataStructures::PolymorphicArray<CoreEngine::StorageTypes::ExtentSegment>& segments,
        const extent_id_t from,
        const extent_id_t to
    ) const{
        Int length = 0;
        extent_id_t start = from;
        const auto append = [&]{
            if (length > 0)
                segments.Push(
                    CoreEngine::StorageTypes::ExtentSegment(
                        start,
                        length
                    )
                );
            length = 0;
        };

        extent_id_t bit = from;
        while (bit <= to){
            if ((bit & 63) == 0 && bit + 64 <= to){
                uint64_t word;
                std::memcpy(&word, this->_frame->_data + this->initialOffset + (bit >> 3), sizeof(word));
                if (word == 0x00){
                    if (length == 0)
                        start = bit;
                    length += 64;
                }
                else if (word == ~0x00)
                    append();

                bit += 64;
                continue;
            }

            if (this->GetBit(bit))
                append();
            else{
                if (length == 0)
                    start = bit;
                length++;
            }
            bit++;
        }

        append();
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

    UnsignedInt GlobalAllocationPageView::ReserveExtentsNoLock(
        DataStructures::PolymorphicArray<CoreEngine::StorageTypes::ExtentSegment>& segmentsRuns,
        const UnsignedInt neededExtents
    ) const{
        auto* additionalHeader = this->GetAdditionalHeader();
        if (additionalHeader->_freeExtentCount == 0 || neededExtents <= 0)
            return 0;

        //fast path:
        const auto base = AllocationPageView::CalculateExtentIdOffsetByGamPageId(this->_frame->Header()->pageId);
        UnsignedInt remaining = neededExtents;
        const auto tailAvailable = static_cast<UnsignedInt>(Constants::EXTENT_BIT_MAP_SIZE - additionalHeader->_appendBitId);
        if (tailAvailable > 0){
            const auto bitsToTake = Math::Min(remaining, tailAvailable);
            this->SetBits(additionalHeader->_appendBitId, bitsToTake);

            segmentsRuns.Push(CoreEngine::StorageTypes::ExtentSegment(base + additionalHeader->_appendBitId, bitsToTake));

            additionalHeader->_appendBitId += bitsToTake;
            remaining -= bitsToTake;
        }

        if (remaining > 0){
            DataStructures::PolymorphicArray<CoreEngine::StorageTypes::ExtentSegment> fragments(segmentsRuns.GetAllocator());
            this->CollectRunsNoLock(
                fragments,
                additionalHeader->_firstFreeBitId,
                additionalHeader->_appendBitId - 1
            );
            fragments.Sort<DataStructures::ArraySortType::DESC>();

            for (const auto& [_firstBitId, _count] : fragments){
                if (remaining == 0)
                    break;

                const auto bitsToTake = Math::Min(remaining, _count);
                this->SetBits(_firstBitId, bitsToTake);

                segmentsRuns.Push(CoreEngine::StorageTypes::ExtentSegment(base + _firstBitId, bitsToTake));
                remaining -= bitsToTake;
            }
        }

        const auto bitsReserved = neededExtents - remaining;
        additionalHeader->_freeExtentCount -= bitsReserved;
        this->_frame->isDirty = true;

        return bitsReserved;
    }

    DataStructures::PolymorphicArray<extent_id_t> GlobalAllocationPageView::GetAllocatedExtents(const ::Memory::IAllocator* allocator) const{
        return DataStructures::PolymorphicArray<extent_id_t>(allocator);
    }

    void GlobalAllocationPageView::DeallocateExtentNoLock(const extent_id_t extentId) const{
        const auto bitIndex = extentId - AllocationPageView::CalculateExtentIdOffsetByGamPageId(this->_frame->Header()->pageId);
        this->ClearBit(bitIndex);
        this->_frame->isDirty = true;

        auto* additionalHeader = this->GetAdditionalHeader();
        if (bitIndex == additionalHeader->_appendBitId - 1)
            additionalHeader->_appendBitId--;

        additionalHeader->_firstFreeBitId = Math::Min(bitIndex, additionalHeader->_firstFreeBitId);
        additionalHeader->_freeExtentCount++;
    }

    bool GlobalAllocationPageView::IsFull() const{
        return this->GetAdditionalHeader()->_freeExtentCount == 0;
    }
}
