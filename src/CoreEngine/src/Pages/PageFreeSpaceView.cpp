#include <CoreEngine/Pages/PageFreeSpaceView.h>

#include <cassert>
#include <CoreEngine/Pages/Additional/Frame.h>

namespace Pages{
    page_offset_t PageFreeSpaceView::GetOffset(const page_id_t pageId) const{
        return this->initialOffset + pageId % Constants::PAGE_FREE_SPACE_SIZE;
    }

    byte_t* PageFreeSpaceView::GetByte(const page_id_t pageId) const{
        return this->_frame->_data + this->GetOffset(pageId);
    }

    PageFreeSpaceView::PageFreeSpaceView(Frame* frame) : PageView(frame){}

    bool PageFreeSpaceView::IsPageAllocated(const page_id_t pageId) const{
        return PackedByte::ExtractBits<bool, ALLOCATION_SHIFT, ALLOCATION_SINGLE_BIT_MASK>(*this->GetByte(pageId));
    }

    Constants::PageType PageFreeSpaceView::GetPageType(const page_id_t pageId) const{
        return PackedByte::ExtractBits<Constants::PageType, TYPE_SHIFT, TYPE_VALUE_MASK>(*this->GetByte(pageId));
    }

    byte_t PageFreeSpaceView::GetPageSizeCategory(const page_id_t pageId) const{
        return PackedByte::ExtractBits<byte_t, SIZE_SHIFT, SIZE_MASK>(*this->GetByte(pageId));
    }

    void PageFreeSpaceView::SetPageMetaData(const PageView* page) const{
        const auto* header = page->GetHeader();
        this->SetPageAllocated(header->pageId);
        this->SetPageType(header->pageId, page->GetPageType());
        this->SetPageAllocationStatus(header->pageId, header->bytesLeft);

        const auto type = this->GetPageType(header->pageId);
        const auto pageType = page->GetPageType();
        assert(pageType == type);

        this->_frame->isDirty = true;
    }

    void PageFreeSpaceView::SetPageFreed(const page_id_t pageId) const{
        PackedByte::SetBit<ALLOCATION_SHIFT, false>(this->GetByte(pageId));
    }

    void PageFreeSpaceView::SetPageAllocated(const page_id_t pageId) const{
        PackedByte::SetBit<ALLOCATION_SHIFT, true>(this->GetByte(pageId));
    }

    void PageFreeSpaceView::SetPageAllocationStatus(const page_id_t pageId, const page_size_t bytesLeft) const{
        const auto pageAllocationStatus = static_cast<byte_t>(bytesLeft * 7 / Constants::PAGE_SIZE);
        PackedByte::SetBits<SIZE_SHIFT, SIZE_MASK>(this->GetByte(pageId), pageAllocationStatus);
    }

    void PageFreeSpaceView::SetPageType(const page_id_t pageId, Constants::PageType pageType) const{
        PackedByte::SetBits<TYPE_SHIFT, TYPE_VALUE_MASK>(this->GetByte(pageId), static_cast<byte_t>(pageType));
    }

    bool PageFreeSpaceView::IsAllocatedAs(const page_id_t pageId, const Constants::PageType pageType) const{
        return this->IsPageAllocated(pageId) && this->GetPageType(pageId) == pageType;
    }
}
