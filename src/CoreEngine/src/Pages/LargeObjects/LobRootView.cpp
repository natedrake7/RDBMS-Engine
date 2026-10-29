#include <CoreEngine/Pages/LargeObjects/LobRootView.h>

#include <cassert>

#include <CoreEngine/Pages/Additional/Frame.h>

namespace CoreEngine::Pages{
    LobRootView::LobRootView(Frame* framePtr)
        : PageView(framePtr){}

    void LobRootView::WriteInline(
        const UnsignedInt offset,
        const object_t* data,
        const UnsignedInt size
    ) const{
        assert(this->Level() == 0 && offset + size <= LobLayout::INLINE_CAPACITY);

        std::memcpy(this->Payload() + offset, data, size);
        this->_frame->MarkDirty();
    }

    page_id_t LobRootView::Child(const UnsignedInt slot) const{
        const auto* header = this->GetAdditionalHeader();
        assert(header->_level > 0 && slot < header->_childCount);
        page_id_t value;
        std::memcpy(&value, this->Payload() + slot * sizeof(page_id_t), sizeof(page_id_t));
        return value;
    }

    void LobRootView::AppendChild(const page_id_t child) const{
        auto* header = this->GetAdditionalHeader();

        assert(header->_level > 0 && header->_childCount < LobLayout::ROOT_FANOUT);
        std::memcpy(this->Payload() + header->_childCount * sizeof(page_id_t), &child, sizeof(page_id_t));
        header->_childCount++;
        this->_frame->MarkDirty();
    }

    void LobRootView::BulkAppendChildren(const page_id_t* children, const UnsignedInt count) const{
        if (count == 0)
            return;

        auto* header = this->GetAdditionalHeader();

        assert(header->_level > 0 && header->_childCount + count <= LobLayout::ROOT_FANOUT);
        std::memcpy(this->Payload() + header->_childCount * sizeof(page_id_t), children, count * sizeof(page_id_t));
        header->_childCount += count;
        this->_frame->MarkDirty();
    }

    bool LobRootView::IsConsistent() const{
        const auto* header = this->GetAdditionalHeader();
        return header->_totalLength <= LobLayout::MAX_LENGTH
            && header->_level == LobLayout::LevelFor(header->_totalLength);
    }
}
