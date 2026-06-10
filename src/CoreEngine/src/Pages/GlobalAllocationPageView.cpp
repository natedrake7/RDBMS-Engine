#include "../../include/Pages/GlobalAllocationPageView.h"
#include "Guards/ReaderGuard.h"
#include "Pages/AllocationPageView.h"
#include "Pages/Additional/Frame.h"

namespace Pages{
    size_t GlobalAllocationPageView::GetBitIndex(const extent_id_t extentId) const noexcept{
        return this->initialOffset + (extentId >> 3);
    }

    bool GlobalAllocationPageView::GetBit(const std::size_t bitIndex) const noexcept{
        const auto mask = static_cast<UnsignedTinyInt>(1u << (bitIndex & 7u));
        return (this->_frame->_data[this->GetBitIndex(bitIndex)] & mask) != 0;
    }

    void GlobalAllocationPageView::SetBit(const std::size_t bitIndex) const noexcept{
        const std::size_t byteIndex = this->GetBitIndex(bitIndex);
        const auto mask = static_cast<UnsignedTinyInt>(1u << (bitIndex & 7u));
        const auto byte = static_cast<UnsignedTinyInt>(this->_frame->_data[byteIndex] | mask);
        this->_frame->_data[byteIndex] = static_cast<char>(byte);
    }

    void GlobalAllocationPageView::ClearBit(const std::size_t bitIndex) const noexcept{
        const std::size_t byteIndex = this->GetBitIndex(bitIndex);
        const auto mask = static_cast<UnsignedTinyInt>(1u << (bitIndex & 7u));
        auto byte = this->_frame->_data[byteIndex];
        byte = static_cast<UnsignedTinyInt>(byte & static_cast<UnsignedTinyInt>(~mask));
        this->_frame->_data[byteIndex] = static_cast<char>(byte);
    }

    GlobalAllocationPageView::GlobalAllocationPageView(Frame* frame) : PageView(frame) {}

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

    int GlobalAllocationPageView::AllocateExtentsNoLock(DataStructures::PolymorphicArray<extent_id_t>& extents, const Int numberOfExtents) const{
        int allocatedExtents = 0;

        for (extent_id_t extentId = 0; extentId < Constants::EXTENT_BIT_MAP_SIZE; extentId++){
            if (allocatedExtents == numberOfExtents)
                break;

            if (this->GetBit(extentId))
                continue;

            this->SetBit(extentId);
            this->_frame->isDirty = true;

            allocatedExtents++;

            extents.Push(AllocationPageView::CalculatePageIdOffsetByGamPageId(this->_frame->Header()->pageId) + extentId);
        }

        return allocatedExtents;
    }

    void GlobalAllocationPageView::DeallocateExtent(const extent_id_t extentId) const{
        this->ClearBit(extentId);
        this->_frame->isDirty = true;
    }

    bool GlobalAllocationPageView::IsFull() const{
        return this->GetBit(Constants::EXTENT_BIT_MAP_SIZE - 1);
    }

    std::vector<extent_id_t> GlobalAllocationPageView::GetAllocatedExtents(const extent_id_t startingIndex) const{
        std::vector<extent_id_t> allocatedExtents;

        if(startingIndex >= Constants::EXTENT_BIT_MAP_SIZE)
            return allocatedExtents;

        MultiThreading::ReaderGuard lock(&this->_frame->latch);

        allocatedExtents.reserve(Constants::EXTENT_BIT_MAP_SIZE - startingIndex);

        for (extent_id_t id = startingIndex; id < Constants::EXTENT_BIT_MAP_SIZE; id++){
            if (!this->GetBit(id))
                continue;

            allocatedExtents.push_back(AllocationPageView::CalculatePageIdOffsetByGamPageId(this->_frame->Header()->pageId) + id);
        }

        return allocatedExtents;
    }
}
