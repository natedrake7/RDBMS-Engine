#pragma once
#include "PageView.h"

namespace DataStructures{
    template
    class PolymorphicArray<extent_id_t>;
}

namespace Pages{
    struct IndexAllocationPageAdditionalHeader {
        extent_id_t lastAllocatedExtentId;
        page_id_t nextPageId;

        IndexAllocationPageAdditionalHeader();
        IndexAllocationPageAdditionalHeader(extent_id_t extentId, page_id_t nextPageId);
        ~IndexAllocationPageAdditionalHeader();
    };

    class AllocationPageView final : public PageView{
        [[nodiscard]] size_t GetByteIndex(extent_id_t extentId) const noexcept;
        [[nodiscard]] inline bool GetBit(std::size_t bitIndex) const noexcept;
        inline void SetBit(std::size_t bitIndex) const noexcept;
        inline void ClearBit(std::size_t bitIndex) const noexcept;
        [[nodiscard]] IndexAllocationPageAdditionalHeader* GetAdditionalHeader() const;

        public:
            AllocationPageView();
            explicit AllocationPageView(Frame* framePtr);
            AllocationPageView(AllocationPageView&& other) noexcept;
            AllocationPageView& operator=(AllocationPageView&& other) noexcept;

            [[nodiscard]] extent_id_t SetExtentsAllocated(
                const DataStructures::PolymorphicArray<extent_id_t>& extentIds,
                page_id_t globalAllocationMapPageId
            ) const;
            void SetDeallocatedExtent(extent_id_t extentId) const;
            void GetAllocatedExtents(DataStructures::PolymorphicArray<extent_id_t>* allocatedExtents) const;
            void GetAllocatedExtents(DataStructures::PolymorphicArray<extent_id_t>* allocatedExtents, extent_id_t startingExtentIndex) const;
            void SetNextPageId(page_id_t nextPageId) const;
            [[nodiscard]] page_id_t NextPageId() const;
            static page_id_t CalculatePageIdOffsetByGamPageId(page_id_t globalAllocationMapPageId);
    };
}
