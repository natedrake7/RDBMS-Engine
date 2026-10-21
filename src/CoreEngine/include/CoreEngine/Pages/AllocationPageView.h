#pragma once
#include <CoreEngine/Pages/PageView.h>

namespace CoreEngine::StorageTypes
{
    struct ExtentSegment;
}

namespace DataStructures{
    template
    class PolymorphicArray<extent_id_t>;
}

namespace CoreEngine::Pages{
    struct AllocationPageAdditionalHeader {
        gam_index_t _gamIndex;
        extent_id_t _lastAllocatedExtentId;
        page_id_t _nextPageId;

        AllocationPageAdditionalHeader(
            const gam_index_t gamIndex,
            const extent_id_t extentId,
            const page_id_t nextPageId
        ): _gamIndex(gamIndex), _lastAllocatedExtentId(extentId), _nextPageId(nextPageId){}
    };

    class AllocationPageView final : public PageView{
        [[nodiscard]] inline AllocationPageAdditionalHeader* GetAdditionalHeader() const{
            return reinterpret_cast<AllocationPageAdditionalHeader*>(this->_frame->_data + CoreEngine::StorageLayout::PAGE_HEADER_SIZE);
        }

        public:
            static constexpr auto TYPE = PageLayout::PageType::ALLOCATION;
            static constexpr auto IS_SYSTEM = false;

            static void Initialize(const Frame* frame, const gam_index_t gamIndex){
                auto* header = reinterpret_cast<AllocationPageAdditionalHeader*>(frame->_data + StorageLayout::PAGE_HEADER_SIZE);
                header->_nextPageId = INVALID_PAGE_ID;
                header->_lastAllocatedExtentId = 0;
                header->_gamIndex = gamIndex;
            }

            AllocationPageView();
            explicit AllocationPageView(Frame* framePtr);
            AllocationPageView(AllocationPageView&& other) noexcept = default;
            AllocationPageView& operator=(AllocationPageView&& other) noexcept = default;

            void ReserveExtentsNoLock(DataStructures::PolymorphicArray<CoreEngine::StorageTypes::ExtentSegment>& segments) const;
            void SetDeallocatedExtent(extent_id_t extentId) const;
            void GetAllocatedExtents(DataStructures::PolymorphicArray<extent_id_t>* allocatedExtents) const;
            void GetAllocatedExtents(DataStructures::PolymorphicArray<extent_id_t>* allocatedExtents, extent_id_t startingExtentId) const;
            void SetNextPageId(const page_id_t nextPageId) const{
                this->GetAdditionalHeader()->_nextPageId = nextPageId;
            }

            [[nodiscard]] page_id_t NextPageId() const{
                return this->GetAdditionalHeader()->_nextPageId;
            }

            [[nodiscard]] gam_index_t GamIndex() const{
                return this->GetAdditionalHeader()->_gamIndex;
            }
    };
}
