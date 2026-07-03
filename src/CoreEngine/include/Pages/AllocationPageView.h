#pragma once
#include "PageView.h"

namespace CoreEngine::StorageTypes
{
    struct ExtentSegment;
}

namespace DataStructures{
    template
    class PolymorphicArray<extent_id_t>;
}

namespace Pages{
    struct IndexAllocationPageAdditionalHeader {
        page_id_t gamPageId;
        extent_id_t lastAllocatedExtentId;
        page_id_t nextPageId;

        IndexAllocationPageAdditionalHeader(
            const page_id_t gamPageId,
            const extent_id_t extentId,
            const page_id_t nextPageId
        ): gamPageId(gamPageId), lastAllocatedExtentId(extentId), nextPageId(nextPageId){}
    };

    class AllocationPageView final : public PageView{
        [[nodiscard]] IndexAllocationPageAdditionalHeader* GetAdditionalHeader() const;

        public:
            AllocationPageView();
            explicit AllocationPageView(Frame* framePtr);
            AllocationPageView(AllocationPageView&& other) noexcept;
            AllocationPageView& operator=(AllocationPageView&& other) noexcept;

            [[nodiscard]] extent_id_t SetExtentsAllocatedNoLock(
                const DataStructures::PolymorphicArray<extent_id_t>& extentIds,
                page_id_t globalAllocationMapPageId
            ) const;

            void ReserveExtentsNoLock(DataStructures::PolymorphicArray<CoreEngine::StorageTypes::ExtentSegment>& segments) const;
            void SetDeallocatedExtent(extent_id_t extentId) const;
            void GetAllocatedExtents(DataStructures::PolymorphicArray<extent_id_t>* allocatedExtents) const;
            void GetAllocatedExtents(DataStructures::PolymorphicArray<extent_id_t>* allocatedExtents, extent_id_t startingExtentIndex) const;
            void SetNextPageId(page_id_t nextPageId) const;
            [[nodiscard]] page_id_t NextPageId() const;
            [[nodiscard]] page_id_t GamPageId() const;
            static extent_id_t CalculateExtentIdOffsetByGamPageId(page_id_t gamPageId);
    };
}
