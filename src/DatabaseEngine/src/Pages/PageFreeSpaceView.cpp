#include "../../include/Pages/PageFreeSpaceView.h"

#include "Pages/Additional/Frame.h"

namespace Pages{
    PageFreeSpaceView::PageFreeSpaceView(Frame* frame) : PageView(frame){}

    PageFreeSpaceView::PageFreeSpaceView(PageFreeSpaceView&& other) noexcept{
        this->framePtr = other.framePtr;
        this->initialOffset = other.initialOffset;
        other.framePtr = nullptr;
    }

    PageFreeSpaceView& PageFreeSpaceView::operator=(PageFreeSpaceView&& other) noexcept{
        if (this == &other)
            return *this;

        this->framePtr = other.framePtr;
        this->initialOffset = other.initialOffset;
        other.framePtr = nullptr;

        return *this;
    }

    bool PageFreeSpaceView::IsPageAllocated(const page_id_t pageId) const{
        const auto byte = this->framePtr->data + this->initialOffset + pageId;
        return PackedByte::ExtractBits<bool>(*byte, 0, ALLOCATION_MASK);
    }

    PageType PageFreeSpaceView::GetPageType(const page_id_t pageId) const{
        const auto byte = this->framePtr->data + this->initialOffset + pageId;
        return PackedByte::ExtractBits<PageType>(*byte, TYPE_SHIFT, TYPE_MASK);
    }

    byte_t PageFreeSpaceView::GetPageSizeCategory(const page_id_t pageId) const{
        const auto byte = this->framePtr->data + this->initialOffset + pageId;
        return PackedByte::ExtractBits<byte_t>(*byte, 0, SIZE_MASK);
    }

    void PageFreeSpaceView::SetPageMetaData(const PageView* page) const{
        const auto* header = page->GetHeader();

        this->SetPageAllocated(header->pageId);
        this->SetPageType(header->pageId, page->GetPageType());
        this->SetPageAllocationStatus(header->pageId, header->bytesLeft);

        this->framePtr->isDirty = true;
    }

    void PageFreeSpaceView::SetPageFreed(const page_id_t pageId) const{
        const auto byte = this->framePtr->data + this->initialOffset + pageId;
        PackedByte::SetBit(*byte, 0, false);
    }

    void PageFreeSpaceView::SetPageAllocated(const page_id_t pageId) const{
        const auto byte = this->framePtr->data + this->initialOffset + pageId;
        PackedByte::SetBit(*byte, 0, true);
    }

    void PageFreeSpaceView::SetPageAllocationStatus(const page_id_t pageId, const page_size_t bytesLeft) const{
        const auto pageAllocationStatus = static_cast<byte_t>(bytesLeft * 7 / Constants::PAGE_SIZE);
        const auto byte = this->framePtr->data + this->initialOffset + pageId;

        PackedByte::SetBits(*byte, pageAllocationStatus, 0, ALLOCATION_MASK);
    }

    void PageFreeSpaceView::SetPageType(const page_id_t pageId, PageType pageType) const{
        const auto byte = this->framePtr->data + this->initialOffset + pageId;
        PackedByte::SetBits(*byte, static_cast<byte_t>(pageType), TYPE_SHIFT, TYPE_MASK);
    }
}
