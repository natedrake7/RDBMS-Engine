#pragma once
#include "PageView.h"

namespace Pages{
    class GlobalAllocationPageView final : public PageView{
        public:
            explicit GlobalAllocationPageView(Frame* frame);

            GlobalAllocationPageView(GlobalAllocationPageView&& other) noexcept;
            GlobalAllocationPageView& operator=(GlobalAllocationPageView&& other) noexcept;

            int AllocateExtentsNoLock(DataStructures::PolymorphicArray<extent_id_t>& extents, Int numberOfExtents) const;
            void DeallocateExtent(extent_id_t extentId) const;
            [[nodiscard]] bool IsFull() const;
            [[nodiscard]] std::vector<extent_id_t> GetAllocatedExtents(extent_id_t startingIndex = 0) const;
    };
}
