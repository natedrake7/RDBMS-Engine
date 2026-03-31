#include "../../../include/Pages/Additional/RowReference.h"
#include "Pages/PageView.h"
#include "Pages/Additional/Frame.h"

namespace Pages{
    RowLazyState::RowLazyState(const Memory::IAllocator* allocator)
        : dataOffset(0), isHeaderInitialized(false), joinedRows(allocator){}

   RowReference::RowReference(){
        this->pageView = nullptr;
        this->indexPosition = 0;
        this->keySize = 0;
        this->lazyState = nullptr;
    }

    RowReference::RowReference(
        Frame* framePtr,
        const ::Memory::IAllocator* allocator,
        const Int indexPosition,
        const Int offset
    ){
        this->pageView = allocator->Allocate<PageView>(framePtr);
        this->indexPosition = indexPosition;
        this->keySize = offset;

        this->lazyState = allocator->Allocate<RowLazyState>(allocator);
        this->lazyState->dataOffset = 0;
        this->lazyState->isHeaderInitialized = false;
    }

    // RowReference::RowReference(const RowReference& other){
    //     this->pageView = PageView(other.pageView.GetFrame());
    //     this->indexPosition = other.indexPosition;
    //     this->keySize = other.keySize;
    //     this->lazyState = nullptr;
    //
    //     if (other.lazyState != nullptr){
    //         this->lazyState = new RowLazyState();
    //          this->lazyState->header = other.lazyState->header;
    //          this->lazyState->dataOffset = other.lazyState->dataOffset;
    //          this->lazyState->isHeaderInitialized = other.lazyState->isHeaderInitialized;
    //          this->lazyState->sizes = other.lazyState->sizes;
    //          // this->lazyState->cache = other.lazyState->cache;
    //    }
    // }
    //
    // RowReference& RowReference::operator=(const RowReference& other){
    //     if (this == &other)
    //         return *this;
    //
    //     this->pageView = PageView(other.pageView.GetFrame());
    //     this->indexPosition = other.indexPosition;
    //     this->keySize = other.keySize;
    //     this->lazyState = nullptr;
    //
    //     if (other.lazyState != nullptr){
    //         this->lazyState = new RowLazyState();
    //         // this->lazyState->header = other.lazyState->header;
    //         this->lazyState->dataOffset = other.lazyState->dataOffset;
    //         this->lazyState->isHeaderInitialized = other.lazyState->isHeaderInitialized;
    //         this->lazyState->sizes = other.lazyState->sizes;
    //         // this->lazyState->cache = other.lazyState->cache;
    //     }
    //
    //     return *this;
    // }

    RowReference::RowReference(RowReference&& other) noexcept{
        this->pageView = other.pageView;
        this->indexPosition = other.indexPosition;
        this->keySize = other.keySize;
        this->lazyState = other.lazyState;

        other.indexPosition = 0;
        other.keySize = 0;
        other.lazyState = nullptr;
        other.pageView = nullptr;
    }

    RowReference& RowReference::operator=(RowReference&& other) noexcept{
        if (this == &other)
            return *this;

        this->pageView = other.pageView;
        this->indexPosition = other.indexPosition;
        this->keySize = other.keySize;
        this->lazyState = other.lazyState;

        other.indexPosition = 0;
        other.keySize = 0;
        other.lazyState = nullptr;
        other.pageView = nullptr;

        return *this;
    }

    RowReference::~RowReference() = default;
    // {
    //     // if (this->lazyState != nullptr)
    //     //     delete this->lazyState;
    // }

    QueryResult RowReference::Materialize(const Memory::IAllocator* allocator)const{
        return this->pageView->MaterializeRow(allocator, this->indexPosition, this->keySize);
    }

    Value RowReference::PartialMaterialize(const Memory::IAllocator* allocator, const column_index_t columnIndex) const{
        return this->pageView->PartialMaterializeRow(allocator, this, columnIndex);
    }

    Int RowReference::Size() const{
       const auto pageSlot = this->pageView->GetSlotDirectory(this->indexPosition);
       return pageSlot.GetSize();
    }

    void RowReference::Join(const RowReference& other) const{
       // this->lazyState->joinedRows.Push(other);
    }
}
