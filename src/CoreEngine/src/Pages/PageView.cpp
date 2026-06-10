#include "../include/Pages/PageView.h"

#include <algorithm>

#include "DataStorage/Table.h"
#include "Pages/Additional/Frame.h"
#include "Pages/Additional/RawRowReference.h"
#include "DataStorage/Row.h"

namespace Pages{
    PageHeader::PageHeader(){
        this->pageId = INVALID_PAGE_ID;
        this->size = 0;
        this->bytesLeft = static_cast<page_size_t>(Constants::PAGE_SIZE - Constants::PAGE_HEADER_SIZE);
    }

    void PageView::SetPageId(const page_id_t pageId) const{
        this->_frame->Header()->pageId = pageId;
    }

    page_offset_t PageView::NewInsertOffset() const{
        const auto* headerPtr = this->_frame->Header();
        return Constants::PAGE_SIZE
                - headerPtr->bytesLeft
                - headerPtr->size * SlotDirectory::SIZE;
    }

    Int PageView::SlotDirectoryOffSet(const Int indexPosition){
        return Constants::PAGE_SIZE - (indexPosition + 1) * SlotDirectory::SIZE;
    }

    Int PageView::SlotDirectoriesToMoveOffSet(const Int indexPosition, const Int slotToMove){
        return Constants::PAGE_SIZE - (indexPosition + slotToMove) * SlotDirectory::SIZE;
    }

    void PageView::UpdateSlotDirectory(
        const SlotDirectory slotDirectory,
        const Int indexPosition
    ) const{
        std::memcpy(
            this->_frame->_data + Pages::PageView::SlotDirectoryOffSet(indexPosition),
            &slotDirectory,
            SlotDirectory::SIZE
        );
    }

    bool PageView::IndexOutOfBounds(const Int indexPosition) const{
        return indexPosition >= this->_frame->Header()->size;
    }

    void PageView::AdjustSlotDirectories(
        const Int indexPosition,
        const page_offset_t offset,
        const row_size_t rowSize,
        const key_size_t keySize
    ) const{
        const auto slotsToMove = this->_frame->Header()->size - indexPosition;
        const auto slotBytesToMove = slotsToMove * SlotDirectory::SIZE;
        const auto srcOffset = Pages::PageView::SlotDirectoriesToMoveOffSet(indexPosition, slotsToMove);
        const auto dstOffset = srcOffset - SlotDirectory::SIZE;

        std::memmove(
            this->_frame->_data + dstOffset,
            this->_frame->_data + srcOffset,
            slotBytesToMove
        );

        const auto slot = SlotDirectory(offset, keySize, rowSize, keySize, SlotDirectory::SLOT_USED);
        this->UpdateSlotDirectory(slot, indexPosition);
    }

    Int PageView::RawDataSize() const{
        return this->_frame->type == Constants::PageType::INDEX
                   ? Constants::INDEX_PAGE_DEFAULT_SIZE
                   : Constants::PAGE_SIZE_WITHOUT_HEADER;
    }

    void PageView::InsertFirstRow(const CoreEngine::StorageTypes::InsertPayload& payload) const{
        const auto rowSize = payload.Size();
        const auto offSet = this->NewInsertOffset();

        std::memcpy(this->_frame->_data + offSet, payload.Data(), rowSize);

        const auto newSlot = SlotDirectory(offSet, 0, rowSize, 0, SlotDirectory::SLOT_USED);
        this->InsertNewSlot(newSlot);

        this->_frame->Header()->size++;
        this->_frame->isDirty = true;
        this->_frame->Header()->bytesLeft -= (rowSize + SlotDirectory::SIZE);
    }

    bool PageView::IsIndexPage() const{
        return this->_frame->type == Constants::PageType::INDEX;
    }

    PageView::PageView(){
        this->_frame = nullptr;
        this->initialOffset = Constants::PAGE_HEADER_SIZE;
    }

    PageView::PageView(Frame* framePtr){
        this->_frame = framePtr;
        this->_frame->pinCount.fetch_add(1, std::memory_order_relaxed);
        this->initialOffset = Constants::PAGE_HEADER_SIZE;
    }

    PageView& PageView::operator=(PageView&& other) noexcept{
        if (this == &other)
            return *this;

        if (this->IsValid())
            this->_frame->pinCount.fetch_sub(1, std::memory_order_relaxed);

        this->_frame = other._frame;
        other._frame = nullptr;
        return *this;
    }

    PageView::PageView(PageView&& other) noexcept{
        this->_frame = other._frame;
        other._frame = nullptr;
    }

    PageView::~PageView(){
        if (this->IsValid())
            this->_frame->pinCount.fetch_sub(1, std::memory_order_relaxed);
    }

    PageHeader* PageView::GetHeader() const{
        return this->_frame->Header();
    }

    CoreEngine::StorageTypes::RowHeader PageView::PeekRowHeader(const Int indexPosition) const{
        const auto slot = this->GetSlotDirectory(indexPosition);
        CoreEngine::StorageTypes::RowHeader rowHeader;
        std::memcpy(&rowHeader, this->_frame->_data + slot.AbsoluteDataOffset(), Constants::ROW_VERSION_HEADER_SIZE);
        return rowHeader;
    }

    SlotDirectory PageView::GetSlotDirectory(const Int indexPosition) const{
        SlotDirectory slot;
        std::memcpy(
            &slot,
            this->_frame->_data + Pages::PageView::SlotDirectoryOffSet(indexPosition),
            SlotDirectory::SIZE
        );
        return slot;
    }

    void PageView::InsertNewSlot(const SlotDirectory slotDirectory) const{
        std::memcpy(
            this->_frame->_data + Pages::PageView::SlotDirectoryOffSet(this->_frame->Header()->size),
            &slotDirectory,
            SlotDirectory::SIZE
        );
    }

    void PageView::Defragment(const ::Memory::IAllocator* allocator) const{
        auto* header = this->_frame->Header();

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

            if (slot.slotDirectory.Offset() == offset){
                offset += slot.slotDirectory.Size();
                continue;
            }

            std::memmove(
                this->_frame->_data + offset,
                this->_frame->_data + slot.slotDirectory.Offset(),
                slot.slotDirectory.Size()
            );

            slot.slotDirectory.SetOffset(offset);
            this->UpdateSlotDirectory(slot.slotDirectory, slot.indexPosition);
            offset += slot.slotDirectory.Size();
        }

        const auto usedBytes = offset + (header->size * SlotDirectory::SIZE);
        header->bytesLeft = Constants::PAGE_SIZE - usedBytes;
        this->_frame->isDirty = true;
    }

    void PageView::Resize(const Int size) const{
        for (int i = size; i < this->_frame->Header()->size; i++){
            const auto slot = this->GetSlotDirectory(i);
            this->_frame->Header()->bytesLeft += slot.Size() + SlotDirectory::SIZE;
        }

        this->_frame->Header()->size = size;
        this->_frame->isDirty = true;
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
            std::memcpy(this->_frame->_data + offset, leftData + leftSlot.Offset(), leftSlot.Size());

            const auto rightSlot = SlotDirectory(
                offset, leftSlot.DataOffset(),
                leftSlot.DataSize(), leftSlot.KeySize(),
                SlotDirectory::SLOT_USED
            );
            this->InsertNewSlot(rightSlot);

            offset += leftSlot.Size();
            this->_frame->Header()->size++;
        }

        this->_frame->Header()->bytesLeft -= this->_frame->Header()->size * SlotDirectory::SIZE + (offset - startOffset);;
        this->_frame->isDirty = true;

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
            const auto leftSize = leftSlot.Size();

            std::memcpy(this->_frame->_data + offset, leftData + leftSlot.Offset(), leftSize);

            const auto rightSlot = SlotDirectory(
                offset, leftSlot.DataOffset(), leftSlot.DataSize(),
                leftSlot.KeySize(), SlotDirectory::SLOT_USED
            );
            this->InsertNewSlot(rightSlot);

            offset += leftSize;
            this->_frame->Header()->size++;
            this->_frame->Header()->bytesLeft -= leftSize + SlotDirectory::SIZE;
        }

        this->_frame->isDirty = true;

        donorPage->Resize(donorResizeVariant);
        donorPage->Defragment(allocator);
    }

    Int PageView::InsertRow(const CoreEngine::StorageTypes::InsertPayload& payload) const{
        const auto rowSize = payload.Size();

        auto nextOffset = this->NewInsertOffset();


        std::memcpy(this->_frame->_data + nextOffset, payload.Data(), rowSize);
        nextOffset += rowSize;

        const auto newSlot = SlotDirectory(
            nextOffset, 0,
            rowSize, 0, SlotDirectory::SLOT_USED
        );
        this->InsertNewSlot(newSlot);

        this->_frame->Header()->size++;
        this->_frame->isDirty = true;
        this->_frame->Header()->bytesLeft -= (rowSize + SlotDirectory::SIZE);

        return this->_frame->Header()->size - 1;
    }

    void PageView::InsertRow(
        const CoreEngine::StorageTypes::InsertPayload& payload,
        const Int indexPosition
    ) const{
        if (indexPosition >= this->_frame->Header()->size){
            const auto _ = this->InsertRow(payload);
            return;
        }

        const auto nextOffset = this->NewInsertOffset();
        const auto offSetCopy = nextOffset;
        const auto rowSize = payload.Size();

        std::memcpy(this->_frame->_data + nextOffset, payload.Data(), rowSize);

        this->AdjustSlotDirectories(indexPosition, offSetCopy, rowSize, 0);

        this->_frame->Header()->bytesLeft -= (rowSize + SlotDirectory::SIZE);
        this->_frame->Header()->size++;
        this->_frame->isDirty = true;
    }

    bool PageView::UpdateRow(
        const ::Memory::IAllocator* allocator,
        const CoreEngine::StorageTypes::InsertPayload& payload,
        const page_offset_t indexPosition
    ) const{
        if (this->IndexOutOfBounds(indexPosition))
            throw std::out_of_range("Page::UpdateRow: Index position is out of bounds.");

        auto slot = this->GetSlotDirectory(indexPosition);
        const auto newSize = payload.Size();

        // In-place update: new data fits in existing slot
        if (newSize <= slot.DataSize()){
            const auto dataOffset = slot.AbsoluteDataOffset();
            std::memcpy(this->_frame->_data + dataOffset, payload.Data(), newSize);

            // Update slot to reflect new (possibly smaller) size
            const auto updatedSlot = SlotDirectory(
                slot.Offset(), slot.DataOffset(),
                newSize, slot.KeySize(), SlotDirectory::SLOT_USED
            );
            this->UpdateSlotDirectory(updatedSlot, indexPosition);

            this->_frame->Header()->bytesLeft += (slot.DataSize() - newSize);
            this->_frame->isDirty = true;
            return true;
        }

        //preserve key by copying it into memory
        auto* keyData = static_cast<object_t*>(allocator->AllocateRaw(slot.KeySize()));
        std::memcpy(keyData, this->_frame->_data + slot.Offset(), slot.KeySize());

        // Defragment if not enough space for new data
        const auto requiredSpace = newSize + slot.KeySize();
        if (this->_frame->Header()->bytesLeft < requiredSpace){
            // Out-of-place update: mark old slot as dead, write new data at end
            // Mark old slot dead so defragment can reclaim it
            slot.SetFlag(SlotDirectory::SLOT_DEAD);
            this->UpdateSlotDirectory(slot, indexPosition);

            // Reclaim the old slot's bytes so defragment has accurate bytesLeft
            this->_frame->Header()->bytesLeft += slot.Size();
            this->Defragment(allocator);

            if (this->_frame->Header()->bytesLeft < requiredSpace)
                return false;  // truly full even after defragment
        }

        auto nextOffset = this->NewInsertOffset();
        const auto slotOffset = nextOffset;  // slot points to start of key+data

        // For index pages: copy key from old location first
        if (slot.KeySize() > 0){
            // key was at slot.Offset() (before data), copy it to new location
            std::memcpy(this->_frame->_data + nextOffset, keyData, slot.KeySize());
            nextOffset += slot.KeySize();
        }

        // Write new row data
        std::memcpy(this->_frame->_data + nextOffset, payload.Data(), newSize);

        // Update slot directory to point to new location
        const auto newSlot = SlotDirectory(
            slotOffset,             // offset = start of key+data
            nextOffset - slotOffset,           // dataOffset = start of data (after key)
            newSize,                // dataSize
            slot.KeySize(),         // keySize preserved
            SlotDirectory::SLOT_USED
        );
        this->UpdateSlotDirectory(newSlot, indexPosition);

        this->_frame->Header()->bytesLeft -= requiredSpace;
        this->_frame->isDirty = true;
        return true;
    }

    void PageView::SetForwardPointer(const Int indexPosition, const CoreEngine::StorageTypes::RID& rowId) const{
        auto slot = this->GetSlotDirectory(indexPosition);
        const auto offSet = slot.AbsoluteDataOffset();

        std::memcpy(this->_frame->_data + offSet, &rowId, ROW_ID_SIZE);

        slot.SetFlag(SlotDirectory::SLOT_FORWARDED);
        slot.SetDataOffset(offSet);
        this->UpdateSlotDirectory(slot, indexPosition);
    }

    void PageView::Delete(const Int indexPosition) const{
        auto slot = this->GetSlotDirectory(indexPosition);
        slot.SetFlag(SlotDirectory::SLOT_DEAD);
    }

    page_id_t PageView::PageId() const{
        return this->_frame->Header()->pageId;
    }

    page_size_t PageView::PageSize() const{
        return this->_frame->Header()->size;
    }

    page_size_t PageView::BytesLeft() const{
        return this->_frame->Header()->bytesLeft;
    }

    object_t* PageView::GetData() const{
        return this->_frame->_data;
    }

    Frame* PageView::GetFrame() const{
        return this->_frame;
    }

    MultiThreading::Mutex& PageView::Latch() const{
        return this->_frame->latch;
    }

    QueryResult PageView::MaterializeRow(
        const Memory::IAllocator* allocator,
        const Int indexPosition
    ) const{
        const auto slot = this->GetSlotDirectory(indexPosition);

        const auto& columns = this->_frame->table->GetColumns();
        const auto columnsSize = columns.Size();

        auto result = QueryResult(allocator);

        const auto dataOffset = slot.AbsoluteDataOffset();
        const auto entryTableOffset = dataOffset + Constants::ROW_VERSION_HEADER_SIZE;
        const auto* rowDataPtr = this->_frame->_data + dataOffset;
        auto* entries = reinterpret_cast<const CoreEngine::StorageTypes::RowEntry*>(this->_frame->_data + entryTableOffset);

        for (int i = 0;i < columnsSize; i++){
            const auto type = entries[i].Type();
            if (CoreEngine::StorageTypes::RowEntry::IsNull(type)){
                auto value = Value::Null(allocator);
                result.AddColumn(value);
                continue;
            }

            auto entrySize = entries[i].Size();
            auto offSet = entries[i].Offset();

            auto value = Value(
                rowDataPtr + entries[i].Offset(),
                entries[i].Size(),
                columns[i]->Type(),
                allocator
            );
            result.AddColumn(value);
        }

        return result;
    }

    bool PageView::IsValid() const{
        return this->_frame != nullptr;
    }

    Constants::PageType PageView::GetPageType() const{
        return this->_frame->type;
    }

    bool PageView::Filter(const Frame* frame, const CoreEngine::StorageTypes::RID* rowId, Int columnIndex){
    }

    Value PageView::GetColumnAt(
        const ::Memory::IAllocator* allocator,
        const PageView* page,
        const CoreEngine::StorageTypes::RID* row,
        const Int columnIndex
    ){
        const auto slot = page->GetSlotDirectory(row->_index);
        const auto* frame = page->GetFrame();
        const auto* rowDataPtr = frame->_data + slot.AbsoluteDataOffset();

        const auto rowEntry = *reinterpret_cast<const CoreEngine::StorageTypes::RowEntry*>(
            rowDataPtr + Constants::ROW_VERSION_HEADER_SIZE + columnIndex * sizeof(CoreEngine::StorageTypes::RowEntry)
        );

        if (rowEntry.IsNull())
            return Value::Null();

        return Value::FromExternalStorage(
            rowDataPtr + rowEntry._offset,
            rowEntry.Size(),
            frame->table->GetColumns()[columnIndex]->Type(),
            allocator
        );
    }

    const object_t* PageView::GetColumnAt(const Int rowIndex, const Int columnIndex) const{
        const auto slot = this->GetSlotDirectory(rowIndex);
        const auto* frame = this->GetFrame();
        const auto* rowDataPtr = frame->_data + slot.AbsoluteDataOffset();

        const auto rowEntry = *reinterpret_cast<const CoreEngine::StorageTypes::RowEntry*>(
            rowDataPtr + Constants::ROW_VERSION_HEADER_SIZE + columnIndex * sizeof(CoreEngine::StorageTypes::RowEntry)
        );

        if (rowEntry.IsNull()) return nullptr;

        return rowDataPtr + rowEntry._offset;
    }

    const object_t* PageView::GetColumnAt(const Int rowIndex, const Int columnIndex, UnsignedSmallInt& outSize) const{
        const auto slot = this->GetSlotDirectory(rowIndex);
        const auto* frame = this->GetFrame();
        const auto* rowDataPtr = frame->_data + slot.AbsoluteDataOffset();

        const auto rowEntry = *reinterpret_cast<const CoreEngine::StorageTypes::RowEntry*>(
            rowDataPtr + Constants::ROW_VERSION_HEADER_SIZE + columnIndex * sizeof(CoreEngine::StorageTypes::RowEntry)
        );

        // TODO: OVERFLOWVAL / LOB entries store a pointer here, not the bytes themselves.
        //       Mirror the row path once it follows those pointers; for now read inline,
        //       consistent with the existing GetColumnAt / Value::FromExternalStorage path.
        if (rowEntry.IsNull()){
            outSize = 0;
            return nullptr;
        }

        outSize = rowEntry.Size();
        return rowDataPtr + rowEntry._offset;
    }

    RawRowReference PageView::RawRowData(const Int indexPosition) const{
        const auto slot = this->GetSlotDirectory(indexPosition);
        return RawRowReference(this->_frame->_data + slot.AbsoluteDataOffset(), slot.DataSize());
    }
}
