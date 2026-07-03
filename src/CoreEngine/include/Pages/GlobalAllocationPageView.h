#pragma once
#include "PageView.h"

namespace CoreEngine::StorageTypes
{
    struct ExtentSegment;
}

namespace Pages{
    struct GlobalAllocationPageAdditionalHeader{
        UnsignedInt _freeExtentCount;
        extent_id_t _firstFreeBitId;
        extent_id_t _appendBitId;
        byte_t _reserved[Constants::GAM_HEADER_RESERVED_SPACE];
    };

    class GlobalAllocationPageView final : public PageView{
        [[nodiscard]] GlobalAllocationPageAdditionalHeader* GetAdditionalHeader() const;

        void SetBits(UnsignedInt startIndex, UnsignedInt numberOfBits) const;
        void CollectRunsNoLock(
            DataStructures::PolymorphicArray<CoreEngine::StorageTypes::ExtentSegment>& segments,
            extent_id_t from,
            extent_id_t to
        ) const;

        public:
            explicit GlobalAllocationPageView(Frame* frame);

            GlobalAllocationPageView(GlobalAllocationPageView&& other) noexcept;
            GlobalAllocationPageView& operator=(GlobalAllocationPageView&& other) noexcept;

            [[nodiscard]] UnsignedInt ReserveExtentsNoLock(
                DataStructures::PolymorphicArray<CoreEngine::StorageTypes::ExtentSegment>& segmentsRuns,
                Int neededExtents
            ) const;

            [[nodiscard]] DataStructures::PolymorphicArray<extent_id_t> GetAllocatedExtents(const ::Memory::IAllocator* allocator) const;

            void DeallocateExtentNoLock(extent_id_t extentId) const;
            [[nodiscard]] bool IsFull() const;
    };
}
