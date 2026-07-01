#pragma once
#include "PageView.h"

namespace Pages{
    struct GlobalAllocationPageAdditionalHeader{
        UnsignedInt _freeExtentCount;
        extent_id_t _firstFreeExtentId;
        extent_id_t _appendExtentId;
        byte_t _reserved[Constants::GAM_HEADER_RESERVED_SPACE];
    };

    class GlobalAllocationPageView final : public PageView{
        [[nodiscard]] GlobalAllocationPageAdditionalHeader* GetAdditionalHeader() const;

        public:
            explicit GlobalAllocationPageView(Frame* frame);

            GlobalAllocationPageView(GlobalAllocationPageView&& other) noexcept;
            GlobalAllocationPageView& operator=(GlobalAllocationPageView&& other) noexcept;

            [[nodiscard]] extent_id_t FindContiguousExtentsNoLock(extent_id_t startingIndex, Int numberOfExtents) const;

            Int AllocateExtentsNoLock(DataStructures::PolymorphicArray<extent_id_t>& extents, Int numberOfExtents) const;
            Int AllocateFragmentedExtentsNoLock(DataStructures::PolymorphicArray<extent_id_t>& extents, Int numberOfExtents) const;
            bool TryAllocateContiguousExtentsNoLock(DataStructures::PolymorphicArray<extent_id_t>& extents, Int numberOfExtents) const;

            [[nodiscard]] DataStructures::PolymorphicArray<extent_id_t> GetAllocatedExtents(const ::Memory::IAllocator* allocator) const;

            void DeallocateExtentNoLock(extent_id_t extentId) const;
            [[nodiscard]] bool IsFull() const;
    };
}
