#include "../../../include/Pages/Additional/RowView.h"
#include "Pages/PageView.h"
#include "Pages/Additional/Frame.h"

namespace Pages{
    RowPhysicalLazyState::RowPhysicalLazyState(const Memory::IAllocator* allocator, Frame* framePtr)
        :   sizes(allocator), pageView(framePtr),
            dataOffset(0), isHeaderInitialized(false){}

    RowLazyState::RowLazyState(const Memory::IAllocator* allocator)
        :   joinedRows(allocator){}

    RowLazyState::RowLazyState(const RowLazyState& other)
        :   joinedRows(other.joinedRows){}

    RowView::RowView()
       :    physicalState(nullptr), logicalState(nullptr),
            numberOfColumns(0), indexPosition(0),
            keySize(0){}

   RowView::RowView(const Int numberOfColumns)
       :    physicalState(nullptr), logicalState(nullptr),
            numberOfColumns(numberOfColumns),
            indexPosition(0), keySize(0){
   }

   RowView::RowView(
        Frame* framePtr,
        const ::Memory::IAllocator* allocator,
        const Int indexPosition,
        const Int offset
    ){
        this->numberOfColumns = framePtr->table->GetNumberOfColumns();
        this->indexPosition = indexPosition;
        this->keySize = offset;

        this->logicalState = allocator->Allocate<RowLazyState>(allocator);
        this->physicalState = allocator->Allocate<RowPhysicalLazyState>(allocator, framePtr);
    }

    RowView* RowView::NullReference(
        const ::Memory::IAllocator* allocator,
        const Int numberOfColumns
    ){
        return allocator->Allocate<RowView>(numberOfColumns);
    }

    RowView* RowView::Copy(
        const RowView* other,
        const Memory::IAllocator* allocator
    ){
        auto* copy = allocator->Allocate<RowView>();
        copy->numberOfColumns = other->numberOfColumns;
        copy->indexPosition = other->indexPosition;
        copy->keySize = other->keySize;
        copy->logicalState = allocator->Allocate<RowLazyState>(*other->logicalState);
        copy->physicalState = other->physicalState;
        return copy;
    }

    RowView::RowView(const RowView& other){
        this->numberOfColumns = other.numberOfColumns;
        this->indexPosition = other.indexPosition;
        this->keySize = other.keySize;
        this->logicalState = other.logicalState;

        auto* allocator = other.logicalState->joinedRows.GetAllocator();
        this->logicalState = allocator->Allocate<RowLazyState>(*other.logicalState);

        this->physicalState = other.physicalState;
    }

    RowView& RowView::operator=(const RowView& other){
        if (this == &other)
            return *this;

        this->numberOfColumns = other.numberOfColumns;
        this->indexPosition = other.indexPosition;
        this->keySize = other.keySize;

        auto* allocator = other.logicalState->joinedRows.GetAllocator();
        this->logicalState = allocator->Allocate<RowLazyState>(*other.logicalState);
        this->physicalState = other.physicalState;
        return *this;
    }

    //shares lazy state as it points to the same row
    RowView::RowView(RowView&& other) noexcept{
        this->numberOfColumns = other.numberOfColumns;
        this->indexPosition = other.indexPosition;
        this->keySize = other.keySize;
        this->logicalState = other.logicalState;
        this->physicalState = other.physicalState;

        other.numberOfColumns = 0;
        other.indexPosition = 0;
        other.keySize = 0;
        other.logicalState = nullptr;
        other.physicalState = nullptr;
    }

    RowView& RowView::operator=(RowView&& other) noexcept{
        if (this == &other)
            return *this;

        this->indexPosition = other.indexPosition;
        this->keySize = other.keySize;
        this->logicalState = other.logicalState;
        this->numberOfColumns = other.numberOfColumns;
        this->physicalState = other.physicalState;

        other.numberOfColumns = 0;
        other.indexPosition = 0;
        other.keySize = 0;
        other.logicalState = nullptr;
        other.physicalState = nullptr;

        return *this;
    }

    RowView::~RowView() = default;

    QueryResult RowView::Materialize(const Memory::IAllocator* allocator)const{
        return this->physicalState->pageView.MaterializeRow(allocator, this->indexPosition, this->keySize);
    }

    Value RowView::PartialMaterialize(const Memory::IAllocator* allocator, const column_index_t columnIndex) const{
        return (this->physicalState == nullptr)
            ? Value::Null(allocator)
            : this->physicalState->pageView.PartialMaterializeRow(allocator, this, columnIndex);
    }

    Int RowView::Size() const{
        const auto pageSlot = this->physicalState->pageView.GetSlotDirectory(this->indexPosition);
        return pageSlot.GetSize();
    }

    void RowView::Join(const RowView* other) const{
        this->logicalState->joinedRows.Push(other);
    }
}
