#include "../../../include/Pages/LargeObjects/LobIndexView.h"

#include <assert.h>

#include "Pages/Additional/Frame.h"

namespace Pages{
    LobIndexView::LobIndexView(Frame* framePtr)
        : PageView(framePtr){}

    void LobIndexView::Initialize(const page_id_t rootPageId) const{
        auto* additionalHeader = this->GetAdditionalHeader();

        additionalHeader->_rootPageId = rootPageId;
        additionalHeader->_childCount = 0;

        this->_frame->isDirty = true;
    }

    page_id_t LobIndexView::Child(const UnsignedInt slot) const{
        const auto* header = this->GetAdditionalHeader();
        assert(slot < header->_childCount);
        page_id_t value;
        std::memcpy(&value, this->Payload() + slot * sizeof(page_id_t), sizeof(page_id_t));
        return value;
    }

    void LobIndexView::AppendChild(const page_id_t child) const{
        auto* header = this->GetAdditionalHeader();

        assert(header->_childCount < LobLayout::INDEX_FANOUT);
        std::memcpy(this->Payload() + header->_childCount * sizeof(page_id_t), &child, sizeof(page_id_t));
        header->_childCount++;
        this->_frame->isDirty = true;
    }

    void LobIndexView::BulkAppendChildren(const page_id_t* children, const UnsignedInt count) const{
        auto* header = this->GetAdditionalHeader();

        assert(header->_childCount + count <= LobLayout::INDEX_FANOUT);
        std::memcpy(this->Payload() + header->_childCount * sizeof(page_id_t), children, count * sizeof(page_id_t));
        header->_childCount += count;
        this->_frame->isDirty = true;
    }

    bool LobIndexView::IsConsistent() const{
        const auto* header = this->GetAdditionalHeader();
        return header->_rootPageId != INVALID_PAGE_ID
            && header->_childCount <= LobLayout::INDEX_FANOUT;
    }
}
