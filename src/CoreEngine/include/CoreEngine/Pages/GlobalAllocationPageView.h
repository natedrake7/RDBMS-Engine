#pragma once
#include <CoreEngine/Pages/PageView.h>
#include <CoreEngine/Pages/Additional/Frame.h>

namespace CoreEngine::StorageTypes{
    struct ExtentSegment;
}

namespace CoreEngine::Pages{
    struct GlobalAllocationPageAdditionalHeader{
        gam_index_t _gamIndex;
        UnsignedInt _freeExtentCount;
        extent_id_t _firstFreeBitId;
        extent_id_t _appendBitId;
        byte_t _reserved[CoreEngine::StorageLayout::GAM_HEADER_RESERVED_SPACE];
    };
    static_assert(sizeof(GlobalAllocationPageAdditionalHeader) == CoreEngine::StorageLayout::GAM_HEADER_SIZE);
    static_assert(CoreEngine::StorageLayout::GAM_METADATA_SIZE + CoreEngine::StorageLayout::GAM_BIT_MAP_SIZE / 8 == CoreEngine::StorageLayout::PAGE_SIZE);

    class GlobalAllocationPageView final: public PageView{
        [[nodiscard]] inline GlobalAllocationPageAdditionalHeader* GetAdditionalHeader() const{
            return reinterpret_cast<GlobalAllocationPageAdditionalHeader*>(this->_frame->_data + CoreEngine::StorageLayout::PAGE_HEADER_SIZE);
        }

        void SetBits(UnsignedInt startIndex, UnsignedInt numberOfBits) const;
        void CollectRunsNoLock(
            DataStructures::PolymorphicArray<CoreEngine::StorageTypes::ExtentSegment>& segments,
            extent_id_t from,
            extent_id_t to
        ) const;

        public:
            explicit GlobalAllocationPageView(Frame* frame);

            GlobalAllocationPageView(GlobalAllocationPageView&& other) noexcept = default;
            GlobalAllocationPageView& operator=(GlobalAllocationPageView&& other) noexcept = default;

            [[nodiscard]] UnsignedInt ReserveExtentsNoLock(
                DataStructures::PolymorphicArray<CoreEngine::StorageTypes::ExtentSegment>& segmentsRuns,
                UnsignedInt neededExtents
            ) const;

            [[nodiscard]] DataStructures::PolymorphicArray<extent_id_t> GetAllocatedExtents(const ::Memory::IAllocator* allocator) const;

            void DeallocateExtentNoLock(extent_id_t extentId) const;
            [[nodiscard]] bool IsFull() const;

            [[nodiscard]] gam_index_t GamIndex() const{
                return this->GetAdditionalHeader()->_gamIndex;
            }

            void RecountFreeExtentsNoLock()const;
    };
}
