#include <CoreEngine/Pages/LargeObjects/LobDataView.h>

#include <cassert>

#include <CoreEngine/Pages/Additional/Frame.h>

namespace CoreEngine::Pages{
    LobDataView::LobDataView(Frame* framePtr) : PageView(framePtr){}
    
    void LobDataView::WriteInline(
        const UnsignedInt offset,
        const object_t* data,
        const UnsignedInt size
    ) const{
        assert(offset + size <= LobLayout::DATA_CAPACITY);
        std::memcpy(this->Payload() + offset, data, size);

        this->_frame->MarkDirty();
    }
}
