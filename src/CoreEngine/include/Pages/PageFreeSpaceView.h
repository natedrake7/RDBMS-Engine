#pragma once
#include "PageView.h"

namespace Pages{
    class PageFreeSpaceView final : public PageView{

        static constexpr byte_t ALLOCATION_MASK  = 0x80;  // bit 7        (1000 0000)
        static constexpr byte_t TYPE_MASK        = 0x78;  // bits 3-6     (0111 1000)
        static constexpr byte_t SIZE_MASK        = 0x07;  // bits 0-2     (0000 0111)

        static constexpr Int ALLOCATION_SHIFT    = 7;
        static constexpr Int TYPE_SHIFT          = 3;
        static constexpr Int SIZE_SHIFT          = 0;

        static constexpr byte_t ALLOCATION_SINGLE_BIT_MASK = 0x01;  // used after shifting for IsPageAllocated

        [[nodiscard]] inline page_offset_t GetOffset(page_id_t pageId) const;
        [[nodiscard]] inline byte_t* GetByte(page_id_t pageId) const;

        public:
            explicit PageFreeSpaceView(Frame* frame);

            PageFreeSpaceView(PageFreeSpaceView&& other) noexcept;
            PageFreeSpaceView& operator=(PageFreeSpaceView&& other) noexcept;

            [[nodiscard]] bool IsPageAllocated(page_id_t pageId) const;
            [[nodiscard]] Constants::PageType GetPageType(page_id_t pageId) const;
            [[nodiscard]] byte_t GetPageSizeCategory(page_id_t pageId) const;

            void SetPageMetaData(const PageView* page) const;
            void SetPageFreed(page_id_t pageId)const;
            void SetPageAllocated(page_id_t pageId)const;
            void SetPageAllocationStatus(page_id_t pageId, page_size_t bytesLeft) const;
            void SetPageType(page_id_t pageId, Constants::PageType pageType) const;
            [[nodiscard]] bool IsFull() const;
    };
}
