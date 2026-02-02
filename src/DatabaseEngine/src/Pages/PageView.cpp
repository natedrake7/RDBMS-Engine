#include "../include/Pages/PageView.h"

namespace Pages{
    void PageView::SetFileName(const std::string& otherFilename) const{
        this->framePtr->filename = otherFilename;
    }

    void PageView::SetPageId(const page_id_t pageId) const{
        this->headerPtr->pageId = pageId;
    }

    page_offset_t PageView::NewInsertOffset() const{
        const auto defaultSize = this->headerPtr->type == PageType::DATA
                             ? PAGE_SIZE_WITHOUT_HEADER
                             : INDEX_PAGE_DEFAULT_SIZE;

        return defaultSize - this->headerPtr->bytesLeft - this->headerPtr->size * SlotDirectory::Size;
    }

    Int PageView::SlotDirectoryOffSet(const Int indexPosition) const{
        const auto defaultSize = this->headerPtr->type == PageType::DATA
                             ? PAGE_SIZE_WITHOUT_HEADER
                             : INDEX_PAGE_DEFAULT_SIZE;

        return defaultSize - (indexPosition + 1) * SlotDirectory::Size;
    }

    Int PageView::SlotDirectoriesToMoveOffSet(const Int indexPosition, const Int slotToMove) const{
        const auto defaultSize = this->headerPtr->type == PageType::DATA
                             ? PAGE_SIZE_WITHOUT_HEADER
                             : INDEX_PAGE_DEFAULT_SIZE;

        return defaultSize - (indexPosition + slotToMove) * SlotDirectory::Size;
    }

    void PageView::UpdateSlotDirectory(
        const SlotDirectory slotDirectory,
        const Int indexPosition
    ) const{
        std::memcpy(
        this->framePtr->data + this->SlotDirectoryOffSet(indexPosition),
            &slotDirectory,
    SlotDirectory::Size
        );
    }

    bool PageView::IndexOutOfBounds(const Int indexPosition) const{
        return indexPosition >= this->headerPtr->size;
    }

    void PageView::AdjustSlotDirectories(const Int indexPosition, const page_offset_t& offset, const Int slotSize) const{
        const auto slotsToMove = this->headerPtr->size - indexPosition;
        const auto slotBytesToMove = slotsToMove * SlotDirectory::Size;
        const auto srcOffset = this->SlotDirectoriesToMoveOffSet(indexPosition, slotsToMove);
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
        return this->headerPtr->type == PageType::INDEX
                   ? INDEX_PAGE_DEFAULT_SIZE
                   : PAGE_SIZE_WITHOUT_HEADER;
    }

    void PageView::InsertFirstRow(const DatabaseEngine::StorageTypes::InsertPayload& payload) const{
        const auto rowSize = payload.Size();

        std::memcpy(this->framePtr->data, payload.Data(), rowSize);

        const auto newSlot = SlotDirectory(0, rowSize, SlotDirectory::SLOT_USED);
        this->InsertNewSlot(newSlot);

        this->headerPtr->size++;
        this->framePtr->isDirty = true;
        this->headerPtr->bytesLeft -= (rowSize + SlotDirectory::Size);
    }

    Int PageView::InsertRow(const DatabaseEngine::StorageTypes::InsertPayload& payload) const{
        if (this->headerPtr->size == 0){
            this->InsertFirstRow(payload);
            return 0;
        }

        const auto rowSize = payload.Size();

        auto nextOffset = this->NewInsertOffset();
        const auto offSetCopy = nextOffset;

        std::memcpy(this->framePtr->data + nextOffset, payload.Data(), rowSize);
        nextOffset += rowSize;

        const auto newSlot = SlotDirectory(offSetCopy, rowSize, SlotDirectory::SLOT_USED);
        this->InsertNewSlot(newSlot);

        this->headerPtr->size++;
        this->framePtr->isDirty = true;
        this->headerPtr->bytesLeft -= (rowSize + SlotDirectory::Size);

        return this->headerPtr->size - 1;
    }

    void PageView::InsertRow(const DatabaseEngine::StorageTypes::InsertPayload& payload, const Int indexPosition) const{
        if (indexPosition >= this->headerPtr->size){
            const auto _ = this->InsertRow(payload);
            return;
        }

        const auto nextOffset = this->NewInsertOffset();
        const auto offSetCopy = nextOffset;

        const auto rowSize = payload.Size();

        std::memcpy(this->framePtr->data + nextOffset, payload.Data(), rowSize);

        this->AdjustSlotDirectories(indexPosition, offSetCopy, rowSize);

        this->headerPtr->bytesLeft -= (rowSize + SlotDirectory::Size);
        this->headerPtr->size++;
        this->framePtr->isDirty = true;
    }

    bool PageView::UpdateRow(
        const DatabaseEngine::StorageTypes::InsertPayload& payload,
        const RowReference& rowPtr
    ){
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
            this->headerPtr->bytesLeft -= totalSize;
            this->framePtr->isDirty = true;
            return true;
        }

        //insert new row at the end
        auto nextOffset = this->NewInsertOffset();
        auto offSetCopy = nextOffset;

        //if next row cant fit in the remaining space, we need to compact the page
        //keep slot offset and set its size to 0 so defragmentation does nothing as it is last on the offset
        if (this->headerPtr->bytesLeft < totalSize){
            this->UpdateSlotDirectory(SlotDirectory(nextOffset, 0, SlotDirectory::SLOT_DEAD), rowPtr.indexPosition);
            this->Defragment();

            //even if after the defragment row cant fit, throw exception
            if (this->headerPtr->bytesLeft < totalSize)
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
        this->headerPtr->bytesLeft -= totalSize;
        this->framePtr->isDirty = true;
        return true;
    }

    void PageView::SetForwardPointer(const Int indexPosition, const DataTypes::RowIdentifier& rowId) const{
        const auto slot = this->GetSlotDirectory(indexPosition);

        const auto offSet = slot.GetOffset();

        std::memcpy(this->framePtr->data + offSet, &rowId, ROW_ID_SIZE);

        this->UpdateSlotDirectory(SlotDirectory(offSet, ROW_ID_SIZE, SlotDirectory::SLOT_FORWARDED), indexPosition);
    }

    bool PageView::IsIndexPage() const{
        return this->headerPtr->type == PageType::INDEX;
    }

    PageView::PageView(Frame* framePtr){
        this->framePtr = framePtr;
        this->framePtr->pinCount.fetch_add(1, std::memory_order_relaxed);
        this->headerPtr = reinterpret_cast<PageHeader*>(this->framePtr->data);
    }

    PageView::~PageView(){
        this->framePtr->pinCount.fetch_sub(1, std::memory_order_relaxed);
    }

    PageHeader* PageView::GetHeader() const{
        return this->headerPtr;
    }

    DatabaseEngine::StorageTypes::RowHeader PageView::PeekRowHeader(const Int indexPosition, const Int offSet) const{
        auto rowHeader = DatabaseEngine::StorageTypes::RowHeader();
        const auto slot = this->GetSlotDirectory(indexPosition);

        page_offset_t offSetCopy = offSet == 0 ? slot.GetOffset() : offSet;
        std::memcpy(&rowHeader.version, this->framePtr->data + offSet, Constants::ROW_VERSION_HEADER_SIZE);
        offSetCopy += Constants::ROW_VERSION_HEADER_SIZE;

        rowHeader.nullBitMap.GetDataFromFile(this->framePtr->data, offSetCopy);
        rowHeader.largeObjectBitMap.GetDataFromFile(this->framePtr->data, offSetCopy);
        rowHeader.overflowBitMap.GetDataFromFile(this->framePtr->data, offSetCopy);

        return rowHeader;
    }

    RowReference PageView::PeekRow(Int indexPosition, Int offSet){
        return RowReference();
        // return RowReference(this, indexPosition, offSet);
    }

    SlotDirectory PageView::GetSlotDirectory(const Int indexPosition) const{
        SlotDirectory slot;
        std::memcpy(
            &slot,
            this->framePtr->data + this->SlotDirectoryOffSet(indexPosition),
            SlotDirectory::Size
        );
        return slot;
    }

    void PageView::InsertNewSlot(const SlotDirectory slotDirectory) const{
        std::memcpy(
            this->framePtr->data + this->SlotDirectoryOffSet(this->headerPtr->size),
            &slotDirectory,
            SlotDirectory::Size
        );
    }

    void PageView::Defragment() const{
        auto* header = this->headerPtr;

        if (header->size <= 1)
            return;

        std::vector<SlotDirectoryDefragment> defragmentationSlots;
        defragmentationSlots.reserve(header->size);

        for (Int i = 0; i < header->size; i++){
            auto slot = this->GetSlotDirectory(i);
            defragmentationSlots.emplace_back(slot, i);
        }

        std::ranges::sort(defragmentationSlots, SlotDirectoryDefragment::OrderAscendingByOffSet);

        page_offset_t offset = 0;
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
        header->bytesLeft = this->RawDataSize() - usedBytes;
        this->framePtr->isDirty = true;
    }

    void PageView::Resize(const Int size) const{
        for (int i = size; i < this->headerPtr->size; i++){
            const auto slot = this->GetSlotDirectory(i);
            this->headerPtr->bytesLeft += slot.GetSize() + SlotDirectory::Size;
        }

        this->headerPtr->size = size;
        this->framePtr->isDirty = true;
    }
}
