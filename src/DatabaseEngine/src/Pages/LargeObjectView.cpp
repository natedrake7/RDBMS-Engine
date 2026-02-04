#include "../../include/Pages/LargeObjectView.h"

#include "Pages/Additional/Frame.h"

namespace Pages{
    LargeObjectView::LargeObjectView() : PageView(){
        this->objectSizePtr = nullptr;
        this->nextPageIdPtr = nullptr;
        this->type = PageType::LOB;
    }

    LargeObjectView::LargeObjectView(Frame* framePtr) : PageView(framePtr){
        this->objectSizePtr = reinterpret_cast<page_size_t*>(this->framePtr->data + PAGE_HEADER_SIZE);
        this->nextPageIdPtr = reinterpret_cast<page_id_t*>(this->framePtr->data + PAGE_HEADER_SIZE + sizeof(page_size_t));

        this->type = PageType::LOB;
    }

    LargeObjectView::LargeObjectView(LargeObjectView&& other) noexcept{
        this->framePtr = other.framePtr;
        this->headerPtr = other.headerPtr;
        this->objectSizePtr = other.objectSizePtr;
        this->nextPageIdPtr = other.nextPageIdPtr;
        this->type = other.type;

        other.framePtr = nullptr;
        other.headerPtr = nullptr;
        other.objectSizePtr = nullptr;
        other.nextPageIdPtr = nullptr;
    }

    LargeObjectView& LargeObjectView::operator=(LargeObjectView&& other) noexcept{
        if (this == &other)
            return *this;

        this->framePtr = other.framePtr;
        this->headerPtr = other.headerPtr;
        this->objectSizePtr = other.objectSizePtr;
        this->nextPageIdPtr = other.nextPageIdPtr;
        this->type = other.type;

        other.framePtr = nullptr;
        other.headerPtr = nullptr;
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
        this->framePtr->isDirty = true;
    }

    void LargeObjectView::SetNextPageId(const page_id_t nextPageId) const{
        *this->nextPageIdPtr = nextPageId;
        this->framePtr->isDirty = true;
    }

    void LargeObjectView::SetData(const object_t* object, const page_size_t size) const{
        std::memcpy(this->framePtr->data + LARGE_OBJECT_METADATA_SIZE, object, size);
    }
}
