#include "../include/Pages/PageView.h"

#include <algorithm>

#include "DataStorage/Table.h"
#include "Pages/Additional/Frame.h"
#include "Pages/Additional/RawRowReference.h"
#include "Pages/Additional/RowReference.h"

namespace Pages{
    PageHeader::PageHeader(){
        this->pageId = INVALID_PAGE_ID;
        this->size = 0;
        this->bytesLeft = static_cast<page_size_t>(Constants::PAGE_SIZE - Constants::PAGE_HEADER_SIZE);
    }

    PageHeader::~PageHeader() = default;

    void PageView::SetFileName(const DataTypes::StringView& otherFilename) const{
        this->framePtr->filename = otherFilename;
    }

    void PageView::SetPageId(const page_id_t pageId) const{
        this->framePtr->headerPtr->pageId = pageId;
    }

    page_offset_t PageView::NewInsertOffset() const{
        return Constants::PAGE_SIZE - this->framePtr->headerPtr->bytesLeft; //- this->framePtr->headerPtr->size * SlotDirectory::Size;
    }

    Int PageView::SlotDirectoryOffSet(const Int indexPosition){
        return Constants::PAGE_SIZE - (indexPosition + 1) * SlotDirectory::Size;
    }

    Int PageView::SlotDirectoriesToMoveOffSet(const Int indexPosition, const Int slotToMove){
        return Constants::PAGE_SIZE - (indexPosition + slotToMove) * SlotDirectory::Size;
    }

    void PageView::UpdateSlotDirectory(
        const SlotDirectory slotDirectory,
        const Int indexPosition
    ) const{
        std::memcpy(
            this->framePtr->data + Pages::PageView::SlotDirectoryOffSet(indexPosition),
            &slotDirectory,
            SlotDirectory::Size
        );
    }

    bool PageView::IndexOutOfBounds(const Int indexPosition) const{
        return indexPosition >= this->framePtr->headerPtr->size;
    }

    void PageView::AdjustSlotDirectories(const Int indexPosition, const page_offset_t& offset, const Int slotSize) const{
        const auto slotsToMove = this->framePtr->headerPtr->size - indexPosition;
        const auto slotBytesToMove = slotsToMove * SlotDirectory::Size;
        const auto srcOffset = Pages::PageView::SlotDirectoriesToMoveOffSet(indexPosition, slotsToMove);
        const auto dstOffset = srcOffset - SlotDirectory::Size;

        std::memmove(
            this->framePtr->data + dstOffset,
            this->framePtr->data + srcOffset,
            slotBytesToMove
        );

        const auto slot = SlotDirectory(offset, slotSize, SlotDirectory::SLOT_USED);
        this->UpdateSlotDirectory(slot, indexPosition);
    }

    Int PageView::RawDataSize() const{
        return this->framePtr->type == Constants::PageType::INDEX
                   ? Constants::INDEX_PAGE_DEFAULT_SIZE
                   : Constants::PAGE_SIZE_WITHOUT_HEADER;
    }

    void PageView::InsertFirstRow(const CoreEngine::StorageTypes::InsertPayload& payload) const{
        const auto rowSize = payload.Size();

        std::memcpy(this->framePtr->data + this->initialOffset, payload.Data(), rowSize);

        const auto newSlot = SlotDirectory(0, rowSize, SlotDirectory::SLOT_USED);
        this->InsertNewSlot(newSlot);

        this->framePtr->headerPtr->size++;
        this->framePtr->isDirty = true;
        this->framePtr->headerPtr->bytesLeft -= (rowSize + SlotDirectory::Size);
    }

    bool PageView::IsIndexPage() const{
        return this->framePtr->type == Constants::PageType::INDEX;
    }

    PageView::PageView(){
        this->framePtr = nullptr;
        this->initialOffset = Constants::PAGE_HEADER_SIZE;
    }

    PageView::PageView(Frame* framePtr){
        this->framePtr = framePtr;
        this->framePtr->pinCount.fetch_add(1, std::memory_order_relaxed);
        this->initialOffset = Constants::PAGE_HEADER_SIZE;
    }

    PageView& PageView::operator=(PageView&& other) noexcept{
        if (this == &other)
            return *this;

        this->framePtr = other.framePtr;
        other.framePtr = nullptr;
        return *this;
    }

    PageView::PageView(PageView&& other) noexcept{
        this->framePtr = other.framePtr;
        other.framePtr = nullptr;
    }

    PageView::~PageView(){
        if (this->IsValid())
            this->framePtr->pinCount.fetch_sub(1, std::memory_order_relaxed);
    }

    PageHeader* PageView::GetHeader() const{
        return this->framePtr->headerPtr;
    }

    CoreEngine::StorageTypes::RowHeader PageView::PeekRowHeader(const Int indexPosition, const Int offSet) const{
        auto rowHeader = CoreEngine::StorageTypes::RowHeader();
        const auto slot = this->GetSlotDirectory(indexPosition);

        page_offset_t offSetCopy = offSet == 0 ? slot.GetOffset() : offSet;
        std::memcpy(&rowHeader.version, this->framePtr->data + offSet, Constants::ROW_VERSION_HEADER_SIZE);
        offSetCopy += Constants::ROW_VERSION_HEADER_SIZE;

        rowHeader.nullBitMap.GetDataFromFile(this->framePtr->data, offSetCopy);
        rowHeader.largeObjectBitMap.GetDataFromFile(this->framePtr->data, offSetCopy);
        rowHeader.overflowBitMap.GetDataFromFile(this->framePtr->data, offSetCopy);

        return rowHeader;
    }

    RowReference PageView::PeekRow(
        const ::Memory::IAllocator* allocator,
        const Int indexPosition,
        const Int offSet
    ) const{
        return RowReference(this->framePtr, allocator, indexPosition, offSet);
    }

    SlotDirectory PageView::GetSlotDirectory(const Int indexPosition) const{
        SlotDirectory slot;
        std::memcpy(
            &slot,
            this->framePtr->data + Pages::PageView::SlotDirectoryOffSet(indexPosition),
            SlotDirectory::Size
        );
        return slot;
    }

    void PageView::InsertNewSlot(const SlotDirectory slotDirectory) const{
        std::memcpy(
            this->framePtr->data + Pages::PageView::SlotDirectoryOffSet(this->framePtr->headerPtr->size),
            &slotDirectory,
            SlotDirectory::Size
        );
    }

    void PageView::Defragment(const ::Memory::IAllocator* allocator) const{
        auto* header = this->framePtr->headerPtr;

        if (header->size <= 1) return;

        DataStructures::PolymorphicArray<SlotDirectoryDefragment> defragmentationSlots(allocator, header->size);
        for (Int i = 0; i < header->size; i++){
            const auto slot = this->GetSlotDirectory(i);
            defragmentationSlots.Push(SlotDirectoryDefragment(slot, i));
        }

        std::ranges::sort(defragmentationSlots, SlotDirectoryDefragment::OrderAscendingByOffSet);

        page_offset_t offset = this->initialOffset;
        for (Int i = 0;i < header->size;i++){
            auto slot = defragmentationSlots[i];

            if (slot.slotDirectory.GetOffset() == offset){
                offset += slot.slotDirectory.GetSize();
                continue;
            }

            std::memmove(
                this->framePtr->data + offset,
                this->framePtr->data + slot.slotDirectory.GetOffset(),
                slot.slotDirectory.GetSize()
            );

            slot.slotDirectory.SetOffset(offset);
            this->UpdateSlotDirectory(slot.slotDirectory, slot.indexPosition);
            offset += slot.slotDirectory.GetSize();
        }

        const auto usedBytes = offset + (header->size * SlotDirectory::Size);
        header->bytesLeft = Constants::PAGE_SIZE - usedBytes;
        this->framePtr->isDirty = true;
    }

    void PageView::Resize(const Int size) const{
        for (int i = size; i < this->framePtr->headerPtr->size; i++){
            const auto slot = this->GetSlotDirectory(i);
            this->framePtr->headerPtr->bytesLeft += slot.GetSize() + SlotDirectory::Size;
        }

        this->framePtr->headerPtr->size = size;
        this->framePtr->isDirty = true;
    }

    void PageView::DistributeFromPage(
        const ::Memory::IAllocator* allocator,
        const PageView* donorPage,
        const Int slotToMoveFrom,
        const Int donorNewSize
    ) const{
        const auto* leftData = donorPage->GetData();
        const page_offset_t startOffset = this->NewInsertOffset();
        page_offset_t offset = startOffset;

        for (Int index = slotToMoveFrom; index < donorPage->PageSize(); index++){
            const auto leftSlot = donorPage->GetSlotDirectory(index);
            std::memcpy(this->framePtr->data + offset, leftData + leftSlot.GetOffset(), leftSlot.GetSize());

            const auto rightSlot = SlotDirectory(offset, leftSlot.GetSize(), SlotDirectory::SLOT_USED);
            this->InsertNewSlot(rightSlot);

            offset += leftSlot.GetSize();
            this->framePtr->headerPtr->size++;
        }

        this->framePtr->headerPtr->bytesLeft -= this->framePtr->headerPtr->size * SlotDirectory::Size + (offset - startOffset);;
        this->framePtr->isDirty = true;

        donorPage->Resize(donorNewSize);
        donorPage->Defragment(allocator);
    }

    void PageView::DistributeFromBeginningOfPage(
        const ::Memory::IAllocator* allocator,
        const PageView* donorPage,
        const Int numberOfSlotsToMove,
        const Int donorResizeVariant
    ) const{
        const auto* leftData = donorPage->GetData();
        page_offset_t offset = this->NewInsertOffset();

        for (Int index = 0; index < numberOfSlotsToMove; index++){
            const auto leftSlot = donorPage->GetSlotDirectory(index);
            std::memcpy(this->framePtr->data + offset, leftData + leftSlot.GetOffset(), leftSlot.GetSize());

            const auto rightSlot = SlotDirectory(offset, leftSlot.GetSize(), SlotDirectory::SLOT_USED);
            this->InsertNewSlot(rightSlot);

            offset += leftSlot.GetSize();
            this->framePtr->headerPtr->size++;
            this->framePtr->headerPtr->bytesLeft -= leftSlot.GetSize() + SlotDirectory::Size;
        }

        this->framePtr->isDirty = true;

        donorPage->Resize(donorResizeVariant);
        donorPage->Defragment(allocator);
    }

    void PageView::DistributeSingleSlotFromPage(PageView* donorPage, Int donorIndexPosition, Int donorResizeVariant)
    {
    }

    Int PageView::InsertRow(const CoreEngine::StorageTypes::InsertPayload& payload) const{
        const auto rowSize = payload.Size();

        auto nextOffset = this->NewInsertOffset();
        const auto offSetCopy = nextOffset;

        std::memcpy(this->framePtr->data + nextOffset, payload.Data(), rowSize);
        nextOffset += rowSize;

        const auto newSlot = SlotDirectory(offSetCopy, rowSize, SlotDirectory::SLOT_USED);
        this->InsertNewSlot(newSlot);

        this->framePtr->headerPtr->size++;
        this->framePtr->isDirty = true;
        this->framePtr->headerPtr->bytesLeft -= (rowSize + SlotDirectory::Size);

        return this->framePtr->headerPtr->size - 1;
    }

    void PageView::InsertRow(const CoreEngine::StorageTypes::InsertPayload& payload, const Int indexPosition) const{
        if (indexPosition >= this->framePtr->headerPtr->size){
            const auto _ = this->InsertRow(payload);
            return;
        }

        const auto nextOffset = this->NewInsertOffset();
        const auto offSetCopy = nextOffset;

        const auto rowSize = payload.Size();

        std::memcpy(this->framePtr->data + nextOffset, payload.Data(), rowSize);

        this->AdjustSlotDirectories(indexPosition, offSetCopy, rowSize);

        this->framePtr->headerPtr->bytesLeft -= (rowSize + SlotDirectory::Size);
        this->framePtr->headerPtr->size++;
        this->framePtr->isDirty = true;
    }

    bool PageView::UpdateRow(
        const ::Memory::IAllocator* allocator,
        const CoreEngine::StorageTypes::InsertPayload& payload,
        const RowReference& rowPtr
    ) const{
        if (this->IndexOutOfBounds(rowPtr.indexPosition))
            throw std::out_of_range("Page::UpdateRow: Index position is out of bounds.");

        const auto slot = this->GetSlotDirectory(rowPtr.indexPosition);

        const auto rowOffset = rowPtr.keySize + slot.GetOffset();
        // const auto key = this->GetKey(rowOffset);

        const auto previousRowSize = slot.GetSize() - rowPtr.keySize;
        const auto size = payload.Size();
        const auto totalSize = rowPtr.keySize + size;
        //if new row size is less than or equal to previous row size, update in place
        if (size <= previousRowSize){
            std::memcpy(this->framePtr->data + rowOffset, payload.Data(), size);
            // this->SerializeRow(rowHeader, row, offSet);
            this->framePtr->headerPtr->bytesLeft -= totalSize;
            this->framePtr->isDirty = true;
            return true;
        }

        //insert new row at the end
        auto nextOffset = this->NewInsertOffset();
        auto offSetCopy = nextOffset;

        //if next row cant fit in the remaining space, we need to compact the page
        //keep slot offset and set its size to 0 so defragmentation does nothing as it is last on the offset
        if (this->framePtr->headerPtr->bytesLeft < totalSize){
            this->UpdateSlotDirectory(SlotDirectory(nextOffset, 0, SlotDirectory::SLOT_DEAD), rowPtr.indexPosition);
            this->Defragment(allocator);

            //even if after the defragment row cant fit, throw exception
            if (this->framePtr->headerPtr->bytesLeft < totalSize)
                return false;

            nextOffset = this->NewInsertOffset();
            offSetCopy = nextOffset;
        }

        // key.Serialize(this->framePtr->data, nextOffset);
        // this->SerializeRow(rowHeader, row, nextOffset);
        // Use memmove instead of memcpy when copying within the same buffer to handle potential overlap
        if (this->IsIndexPage()){
            std::memmove(this->framePtr->data + nextOffset, this->framePtr->data + slot.GetOffset(), rowPtr.keySize);
            nextOffset += rowPtr.keySize;
        }

        // Use memcpy for payload since it's from a different buffer (no overlap possible)
        std::memcpy(this->framePtr->data + nextOffset, payload.Data(), size);

        //update slot directory
        const auto newSlot = SlotDirectory(offSetCopy, totalSize, SlotDirectory::SLOT_USED);
        this->UpdateSlotDirectory(newSlot, rowPtr.indexPosition);

        //update bytes
        //Decrease by total size even though the previous offset is freed, as it becomes fragmented and no row can be inserted
        //unless pages gets defragmented
        this->framePtr->headerPtr->bytesLeft -= totalSize;
        this->framePtr->isDirty = true;
        return true;
    }

    void PageView::SetForwardPointer(const Int indexPosition, const DataTypes::RowIdentifier& rowId) const{
        const auto slot = this->GetSlotDirectory(indexPosition);

        const auto offSet = slot.GetOffset();

        std::memcpy(this->framePtr->data + offSet, &rowId, ROW_ID_SIZE);

        this->UpdateSlotDirectory(SlotDirectory(offSet, ROW_ID_SIZE, SlotDirectory::SLOT_FORWARDED), indexPosition);
    }

    void PageView::Delete(const Int indexPosition) const{
        auto slot = this->GetSlotDirectory(indexPosition);
        slot.SetFlag(SlotDirectory::SLOT_DEAD);
    }

    page_id_t PageView::PageId() const{
        return this->framePtr->headerPtr->pageId;
    }

    page_size_t PageView::PageSize() const{
        return this->framePtr->headerPtr->size;
    }

    page_size_t PageView::BytesLeft() const{
        return this->framePtr->headerPtr->bytesLeft;
    }

    object_t* PageView::GetData() const{
        return this->framePtr->data;
    }

    Frame* PageView::GetFrame() const{
        return this->framePtr;
    }

    MultiThreading::ReadWriteMutex& PageView::Latch() const{
        return this->framePtr->latch;
    }

    void PageView::InitializeRowReferenceCache(const RowReference* rowPtr)const{
        const auto slot = this->GetSlotDirectory(rowPtr->indexPosition);

        page_offset_t offSet = rowPtr->keySize + slot.GetOffset() + Constants::ROW_VERSION_HEADER_SIZE;
        const auto numberOfColumns = rowPtr->lazyState->numberOfColumns;

        rowPtr->lazyState->isHeaderInitialized = true;

        const auto bitmapsSize = ByteMaps::BitMap::HeapSize(numberOfColumns);
        rowPtr->lazyState->header.nullBitMap = ByteMaps::BitMap::FromExistingData(this->framePtr->data + offSet, numberOfColumns);
        offSet += bitmapsSize;
        rowPtr->lazyState->header.largeObjectBitMap = ByteMaps::BitMap::FromExistingData(this->framePtr->data + offSet, numberOfColumns);
        offSet += bitmapsSize;
        rowPtr->lazyState->header.overflowBitMap = ByteMaps::BitMap::FromExistingData(this->framePtr->data + offSet, numberOfColumns);
        offSet += bitmapsSize;
        rowPtr->lazyState->sizes.Resize(numberOfColumns);

        for (int i = 0; i < numberOfColumns; i++){
            if (rowPtr->lazyState->header.nullBitMap.Get(i))
                continue;

            std::memcpy(&rowPtr->lazyState->sizes[i], this->framePtr->data + offSet, sizeof(block_size_t));
            offSet += sizeof(block_size_t);
        }

        rowPtr->lazyState->dataOffset = offSet;
    }

    QueryResult PageView::MaterializeRow(
        const Memory::IAllocator* allocator,
        const Int indexPosition,
        const Int keySize
    ) const{
        const auto slot = this->GetSlotDirectory(indexPosition);

        const auto& columns = this->framePtr->table->GetColumns();
        const auto columnsSize = columns.Size();

        page_offset_t offSet = keySize + slot.GetOffset();

        auto rowHeader = CoreEngine::StorageTypes::RowHeader();

        offSet += Constants::ROW_VERSION_HEADER_SIZE;

        const auto bitmapsSize = ByteMaps::BitMap::HeapSize(columnsSize);

        rowHeader.nullBitMap = ByteMaps::BitMap::FromExistingData(this->framePtr->data + offSet, columnsSize);
        offSet += bitmapsSize;
        rowHeader.largeObjectBitMap = ByteMaps::BitMap::FromExistingData(this->framePtr->data + offSet, columnsSize);
        offSet += bitmapsSize;
        rowHeader.overflowBitMap = ByteMaps::BitMap::FromExistingData(this->framePtr->data + offSet, columnsSize);
        offSet += bitmapsSize;

        // rowHeader.nullBitMap.GetDataFromFile(this->framePtr->data, offSet, columnsSize);
        // rowHeader.largeObjectBitMap.GetDataFromFile(this->framePtr->data, offSet, columnsSize);
        // rowHeader.overflowBitMap.GetDataFromFile(this->framePtr->data, offSet, columnsSize);

        auto result = QueryResult(allocator);

        block_size_t sizes[columnsSize];

        for (int i = 0;i < columnsSize; i++){
            if (rowHeader.nullBitMap.Get(i))
                continue;

            std::memcpy(&sizes[i], this->framePtr->data + offSet, sizeof(block_size_t));
            offSet += sizeof(block_size_t);
        }

        for (int i = 0;i < columnsSize; i++){
            if (rowHeader.nullBitMap.Get(i)){
                auto value = Value::Null();
                result.AddColumn(value);
                continue;
            }

            auto value = Value(
                static_cast<const object_t*>(this->framePtr->data + offSet),
                sizes[i],
                columns[i]->Type(),
                allocator
            );
            offSet += sizes[i];

            result.AddColumn(value);
        }

        return result;
    }

    Value PageView::PartialMaterializeRow(
        const Memory::IAllocator* allocator,
        const RowReference* rowPtr,
        const column_index_t columnIndex
    ) const{
        const auto& columns = this->framePtr->table->GetColumns();
        if (!rowPtr->lazyState->isHeaderInitialized)
            this->InitializeRowReferenceCache(rowPtr);

        block_size_t offSet = rowPtr->lazyState->dataOffset;
        for (int i = 0; i < columnIndex; i++){
            if (rowPtr->lazyState->header.nullBitMap.Get(i))
                continue;

            offSet += rowPtr->lazyState->sizes[i];
        }

        if (rowPtr->lazyState->header.nullBitMap.Get(columnIndex))
            return Value::Null(allocator);

        return Value::FromExternalStorage(
            this->framePtr->data + offSet,
            rowPtr->lazyState->sizes[columnIndex],
            columns[columnIndex]->Type(),
            allocator
        );
    }

    RawRowReference PageView::RowRawData(const Int indexPosition, const Int offSet) const{
        const auto slot = this->GetSlotDirectory(indexPosition);
        return RawRowReference(this->framePtr->data + slot.GetOffset() + offSet, slot.GetSize() - offSet);
    }

    bool PageView::IsValid() const{
        return this->framePtr != nullptr;
    }

    Constants::PageType PageView::GetPageType() const{
        return this->framePtr->type;
    }

    void PageView::IncreasePinCount() const{
        this->framePtr->pinCount.fetch_add(1, std::memory_order_relaxed);
    }

    void PageView::DecreasePinCount() const{
        this->framePtr->pinCount.fetch_sub(1, std::memory_order_relaxed);
    }
}
