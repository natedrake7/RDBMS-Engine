#include "../../../include/Pages/LargeObjects/LobDataView.h"

#include <cassert>

#include "Pages/Additional/Frame.h"

namespace Pages{
    LobDataView::LobDataView(Frame* framePtr) : PageView(framePtr){}

    void LobDataView::Initialize(const page_id_t rootPageId, const UnsignedInt pageIndex) const{
        auto* additionalHeader = this->GetAdditionalHeader();

        additionalHeader->_rootPageId = rootPageId;
        additionalHeader->_pageIndex = pageIndex;

        this->_frame->isDirty = true;
    }

    void LobDataView::WriteInline(
        const UnsignedInt offset,
        const object_t* data,
        const UnsignedInt size
    ) const{
        assert(offset + size <= LobLayout::DATA_CAPACITY);
        std::memcpy(this->Payload() + offset, data, size);

        this->_frame->isDirty = true;
    }
}
