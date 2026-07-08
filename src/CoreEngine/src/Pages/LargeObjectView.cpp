#include "../../include/Pages/LargeObjectView.h"

#include "Pages/Additional/Frame.h"

namespace Pages{
    LargeObjectView::LargeObjectView() : PageView(){
        this->initialOffset = Constants::LARGE_OBJECT_METADATA_SIZE;
    }

    LargeObjectView::LargeObjectView(Frame* framePtr) : PageView(framePtr){
        this->initialOffset = Constants::LARGE_OBJECT_METADATA_SIZE;
    }

    LargeObjectView::LargeObjectView(LargeObjectView&& other) noexcept{
        this->_frame = other._frame;
        this->initialOffset = other.initialOffset;
        other._frame = nullptr;
    }

    LargeObjectView& LargeObjectView::operator=(LargeObjectView&& other) noexcept{
        if (this == &other)
            return *this;

        this->_frame = other._frame;
        this->initialOffset = other.initialOffset;
        other._frame = nullptr;

        return *this;
    }

    page_size_t LargeObjectView::GetObjectSize() const{
        return *reinterpret_cast<page_size_t*>(this->_frame->_data + Constants::PAGE_HEADER_SIZE + sizeof(page_id_t));
    }

    page_id_t LargeObjectView::GetNextPageId() const{
        return *reinterpret_cast<page_id_t*>(this->_frame->_data + Constants::PAGE_HEADER_SIZE);
    }

    void LargeObjectView::SetObjectSize(const page_size_t size) const{
        auto* objectSizePtr = reinterpret_cast<page_size_t*>(this->_frame->_data + Constants::PAGE_HEADER_SIZE + sizeof(page_id_t));
        *objectSizePtr = size;
        this->_frame->isDirty = true;
    }

    void LargeObjectView::SetNextPageId(const page_id_t nextPageId) const{
        auto* nextPageIdPtr = reinterpret_cast<page_id_t*>(this->_frame->_data + Constants::PAGE_HEADER_SIZE);
        *nextPageIdPtr = nextPageId;
        this->_frame->isDirty = true;
    }

    void LargeObjectView::SetData(const object_t* object, const page_size_t size) const{
        this->SetNextPageId(INVALID_PAGE_ID);
        this->SetObjectSize(size);
        std::memcpy(this->_frame->_data + Constants::LARGE_OBJECT_METADATA_SIZE, object, size);
    }
}
