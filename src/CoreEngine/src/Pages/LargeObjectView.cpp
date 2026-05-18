#include "../../include/Pages/LargeObjectView.h"

#include "Pages/Additional/Frame.h"

namespace Pages{
    LargeObjectView::LargeObjectView() : PageView(){
        this->objectSizePtr = nullptr;
        this->nextPageIdPtr = nullptr;
        this->initialOffset = Constants::LARGE_OBJECT_METADATA_SIZE;
    }

    LargeObjectView::LargeObjectView(Frame* framePtr) : PageView(framePtr){
        this->objectSizePtr = reinterpret_cast<page_size_t*>(this->_frame->_data + Constants::PAGE_HEADER_SIZE);
        this->nextPageIdPtr = reinterpret_cast<page_id_t*>(this->_frame->_data + Constants::PAGE_HEADER_SIZE + sizeof(page_size_t));
        this->initialOffset = Constants::LARGE_OBJECT_METADATA_SIZE;
    }

    LargeObjectView::LargeObjectView(LargeObjectView&& other) noexcept{
        this->_frame = other._frame;
        this->objectSizePtr = other.objectSizePtr;
        this->nextPageIdPtr = other.nextPageIdPtr;
        this->initialOffset = other.initialOffset;

        other._frame = nullptr;
        other.objectSizePtr = nullptr;
        other.nextPageIdPtr = nullptr;
    }

    LargeObjectView& LargeObjectView::operator=(LargeObjectView&& other) noexcept{
        if (this == &other)
            return *this;

        this->_frame = other._frame;
        this->objectSizePtr = other.objectSizePtr;
        this->nextPageIdPtr = other.nextPageIdPtr;
        this->initialOffset = other.initialOffset;

        other._frame = nullptr;
        other.objectSizePtr = nullptr;
        other.nextPageIdPtr = nullptr;

        return *this;
    }

    page_size_t LargeObjectView::GetObjectSize() const{
        return *this->objectSizePtr;
    }

    page_id_t LargeObjectView::GetNextPageId() const{
        return *this->nextPageIdPtr;
    }

    void LargeObjectView::SetObjectSize(const page_size_t size) const{
        *this->objectSizePtr = size;
        this->_frame->isDirty = true;
    }

    void LargeObjectView::SetNextPageId(const page_id_t nextPageId) const{
        *this->nextPageIdPtr = nextPageId;
        this->_frame->isDirty = true;
    }

    void LargeObjectView::SetData(const object_t* object, const page_size_t size) const{
        *this->nextPageIdPtr = INVALID_PAGE_ID;
        *this->objectSizePtr = size;
        std::memcpy(this->_frame->_data + Constants::LARGE_OBJECT_METADATA_SIZE, object, size);
    }
}
