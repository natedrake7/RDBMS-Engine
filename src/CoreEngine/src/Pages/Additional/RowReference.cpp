#include "../../../include/Pages/Additional/RowReference.h"
#include "Pages/PageView.h"
#include "Pages/Additional/Frame.h"

namespace Pages{
    RowLazyState::RowLazyState(const Memory::IAllocator* allocator)
        :   joinedRows(allocator), sizes(allocator),
            dataOffset(0),
            isHeaderInitialized(false){}

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
        this->lazyState->numberOfColumns = framePtr->table->GetNumberOfColumns();
    }

    //shares lazy state as it points to the same row
    RowReference& RowReference::operator=(const RowReference& other){
        if (this == &other)
            return *this;

        this->pageView = other.pageView;
        this->indexPosition = other.indexPosition;
        this->keySize = other.keySize;
        this->lazyState = other.lazyState;
        return *this;
    }

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

    void RowReference::Join(RowReference& other) const{
       this->lazyState->joinedRows.Push(std::move(other));
    }

    void RowReference::Join(const RowReference& other) const{
        this->lazyState->joinedRows.Push(other);
    }
}
