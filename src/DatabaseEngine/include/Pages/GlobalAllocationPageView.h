#pragma once
#include "PageView.h"

namespace Pages{
    class GlobalAllocationPageView final : public PageView{
        extent_id_t lastAllocatedExtentId;

            [[nodiscard]] inline bool GetBit(std::size_t bitIndex) const noexcept;
            inline void SetBit(std::size_t bitIndex) const noexcept;
            inline void ClearBit(std::size_t bitIndex) const noexcept;

        public:
            explicit GlobalAllocationPageView(Frame* frame);
            int AllocateExtentsNoLock(std::vector<extent_id_t>& extents, Int numberOfExtents);
            void DeallocateExtent(extent_id_t extentId) const;
            // void WriteToDisk(std::fstream *filePtr) override;
            // void ReadFromDisk(
            //     const std::vector<char>& data,
            //     const DatabaseEngine::StorageTypes::Table* table,
            //     page_offset_t& offSet,
            //     std::fstream* filePtr
            // ) override;
            [[nodiscard]] bool IsFull() const;
            [[nodiscard]] std::vector<extent_id_t> GetAllocatedExtents(extent_id_t startingIndex = 0) const;
    };
}
