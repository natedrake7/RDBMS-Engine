#pragma once
#include "PageView.h"

namespace Pages{
    struct IndexAllocationPageAdditionalHeader {
        table_id_t tableId;
        extent_id_t startingExtentId;
        page_id_t nextPageId;

        IndexAllocationPageAdditionalHeader();
        IndexAllocationPageAdditionalHeader(table_id_t tableId, extent_id_t extentId, page_id_t nextPageId);
        ~IndexAllocationPageAdditionalHeader();
    };

    class AllocationPageView final : public PageView{
        IndexAllocationPageAdditionalHeader* additionalHeaderPtr;
        extent_id_t lastAllocatedExtentId;

        [[nodiscard]] inline bool GetBit(std::size_t bitIndex) const noexcept;
        inline void SetBit(std::size_t bitIndex) const noexcept;
        inline void ClearBit(std::size_t bitIndex) const noexcept;

        public:
            AllocationPageView();
            explicit AllocationPageView(Frame* framePtr);

            extent_id_t SetExtentsAllocated(
                const std::vector<extent_id_t>& extentIds,
                page_id_t globalAllocationMapPageId
            );
            void SetDeallocatedExtent(extent_id_t extentId) const;
            void GetAllocatedExtents(std::vector<extent_id_t>* allocatedExtents) const;
            void GetAllocatedExtents(std::vector<extent_id_t>* allocatedExtents, extent_id_t startingExtentIndex) const;
            void SetNextPageId(page_id_t nextPageId) const;
            [[nodiscard]] page_id_t NextPageId() const;
            static page_id_t CalculatePageIdOffsetByGamPageId(page_id_t globalAllocationMapPageId);
    };
}
