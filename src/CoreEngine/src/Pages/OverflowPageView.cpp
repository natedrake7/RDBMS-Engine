#include <CoreEngine/Pages/OverflowPageView.h>

namespace CoreEngine::Pages{
    OverflowPageView::OverflowPageView() : PageView() {}

    OverflowPageView::OverflowPageView(Frame* frame) : PageView(frame) {}

    OverflowPtr OverflowPageView::Insert(const object_t* data, const Int size) const{
        const auto offset = this->NewInsertOffset();

        std::memcpy(this->_frame->_data + offset, data, size);

        const auto newSlot = SlotDirectory(
            offset, 0,
            size, 0, SlotDirectory::SLOT_USED
        );
        this->InsertNewSlot(newSlot);

        auto* header = this->_frame->Header();

        header->size++;
        header->ConsumeBytes(size + SlotDirectory::SIZE);
        this->_frame->_isDirty = true;

        return OverflowPtr(header->pageId, header->size - 1);
    }

    const object_t* OverflowPageView::Get(const Int index, Int& outSize) const{
        const auto slot = this->GetSlotDirectory(index);
        outSize = slot.Size();
        return this->_frame->_data + slot.AbsoluteDataOffset();
    }
}
