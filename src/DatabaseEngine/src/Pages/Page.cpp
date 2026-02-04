#include "../../include/Pages/Page.h"

#include <cmath>
#include <iostream>
#include "../../include/Database.h"
#include "../../include/BufferPool/StorageManager.h"

namespace Pages{
    PageHeader::PageHeader(){
        this->pageId = INVALID_PAGE_ID;
        this->size = 0;
        this->bytesLeft = static_cast<page_size_t>(PAGE_SIZE - PAGE_HEADER_SIZE);
    }

    PageHeader::~PageHeader() = default;

    bool Page::IndexOutOfBounds(const Int indexPosition) const{
        return indexPosition >= this->header.size;
    }

    void Page::AdjustSlotDirectories(const Int indexPosition, const page_offset_t& offset, const Int slotSize) const{
        const auto slotsToMove = this->header.size - indexPosition;
        const auto slotBytesToMove = slotsToMove * SlotDirectory::Size;
        const auto srcOffset = this->SlotDirectoriesToMoveOffSet(indexPosition, slotsToMove);
        const auto dstOffset = srcOffset - SlotDirectory::Size;

        std::memmove(
            this->data + dstOffset,
            this->data + srcOffset,
            slotBytesToMove
        );

        const auto slot = SlotDirectory(offset, slotSize, SlotDirectory::SLOT_USED);
        this->UpdateSlotDirectory(slot, indexPosition);
    }

    DatabaseEngine::StorageTypes::RowHeader Page::PeekRowHeader(const Int indexPosition, const Int offSet) const{
        auto rowHeader = DatabaseEngine::StorageTypes::RowHeader();
        const auto slot = this->GetSlotDirectory(indexPosition);

        page_offset_t offSetCopy = offSet == 0 ? slot.GetOffset() : offSet;
        std::memcpy(&rowHeader.version, this->data + offSet, Constants::ROW_VERSION_HEADER_SIZE);
        offSetCopy += Constants::ROW_VERSION_HEADER_SIZE;

        rowHeader.nullBitMap.GetDataFromFile(this->data, offSetCopy);
        rowHeader.largeObjectBitMap.GetDataFromFile(this->data, offSetCopy);
        rowHeader.overflowBitMap.GetDataFromFile(this->data, offSetCopy);

        return rowHeader;
    }

    RowReference Page::PeekRow(const Int indexPosition, const Int offSet){
        return RowReference(this, indexPosition, offSet);
    }

    bool Page::IsIndexPage() const{
        return this->header.type == PageType::INDEX;
    }

    void Page::SerializeRow(
        const DatabaseEngine::StorageTypes::RowHeader& rowHeader,
        const QueryResult& row,
        page_offset_t& offSet
    ){
        std::memcpy(this->data + offSet, &rowHeader.version, Constants::ROW_VERSION_HEADER_SIZE);
        offSet += Constants::ROW_VERSION_HEADER_SIZE;

        rowHeader.nullBitMap.WriteDataToBuffer(this->data, offSet);
        rowHeader.largeObjectBitMap.WriteDataToBuffer(this->data, offSet);
        rowHeader.overflowBitMap.WriteDataToBuffer(this->data, offSet);

        const auto& rowData = row.Data();
        for (Int indexPosition = 0; indexPosition < rowData.size(); indexPosition++){
            if (rowHeader.nullBitMap.Get(indexPosition))
                continue;

            const auto& value = rowData[indexPosition];
            const auto blockSize = value.Size();
            std::memcpy(this->data + offSet, &blockSize, sizeof(block_size_t));
            offSet += sizeof(block_size_t);

            std::memcpy(this->data + offSet, value.Data(), blockSize);
            offSet += blockSize;

        }
    }

    Page::Page(const page_id_t pageId, const DatabaseEngine::StorageTypes::Table* table, const bool isPageCreation){
        this->header.pageId = pageId;
        this->isDirty = isPageCreation;
        this->pinCount = 0;
        this->hasSecondChance = true;
        this->logSequenceNumber = 0;
        this->table = table;
        this->header.type = PageType::DATA;
        this->priority = PagePriority::LOW;
        this->data = static_cast<object_t*>(std::malloc(PAGE_SIZE_WITHOUT_HEADER));
    }

    Page::Page(const page_id_t pageId, const page_size_t size, const DatabaseEngine::StorageTypes::Table* table, const bool isPageCreation){
        this->header.pageId = pageId;
        this->isDirty = isPageCreation;
        this->table = table;
        this->pinCount = 0;
        this->hasSecondChance = true;
        this->logSequenceNumber = 0;
        this->header.type = PageType::DATA;
        this->priority = PagePriority::LOW;
        this->data = static_cast<object_t*>(std::malloc(size));
    }

    Page::Page(){
        this->isDirty = false;
        this->pinCount = 0;
        this->hasSecondChance = true;
        this->logSequenceNumber = 0;
        this->header.type = PageType::DATA;
        this->priority = PagePriority::LOW;
        this->data = static_cast<object_t*>(std::malloc(PAGE_SIZE_WITHOUT_HEADER));
        this->table = nullptr;
    }

    Page::Page(const PageHeader &pageHeader){
        this->header = pageHeader;
        this->isDirty = false;
        this->pinCount = 0;
        this->hasSecondChance = true;
        this->logSequenceNumber = 0;
        this->priority = PagePriority::LOW;
        this->data = static_cast<object_t*>(std::malloc(PAGE_SIZE_WITHOUT_HEADER));
    }

    Page::Page(const PageHeader& pageHeader, const page_size_t size){
        this->header = pageHeader;
        this->isDirty = false;
        this->pinCount = 0;
        this->hasSecondChance = true;
        this->logSequenceNumber = 0;
        this->priority = PagePriority::LOW;
        this->data = static_cast<object_t*>(std::malloc(size));
    }

    Page::~Page(){
        std::free(this->data);
        this->data = nullptr;
    }

    void Page::DeleteRow(const Int indexPosition) const{
        auto slot = this->GetSlotDirectory(indexPosition);
        slot.SetFlag(SlotDirectory::SLOT_DEAD);
        this->UpdateSlotDirectory(slot, indexPosition);
    }

    void Page::InsertFirstRow(const DatabaseEngine::StorageTypes::InsertPayload& payload){
        const auto rowSize = payload.Size();

        std::memcpy(this->data, payload.Data(), rowSize);

        const auto newSlot = SlotDirectory(0, rowSize, SlotDirectory::SLOT_USED);
        this->InsertNewSlot(newSlot);

        this->header.size++;
        this->isDirty = true;
        this->header.bytesLeft -= (rowSize + SlotDirectory::Size);
    }

    Int Page::InsertRow(const DatabaseEngine::StorageTypes::InsertPayload& payload){
        if (this->header.size == 0){
            this->InsertFirstRow(payload);
            return 0;
        }

        const auto rowSize = payload.Size();

        auto nextOffset = this->NewInsertOffset();
        const auto offSetCopy = nextOffset;

        std::memcpy(this->data + nextOffset, payload.Data(), rowSize);
        nextOffset += rowSize;

        const auto newSlot = SlotDirectory(offSetCopy, rowSize, SlotDirectory::SLOT_USED);
        this->InsertNewSlot(newSlot);

        this->header.size++;
        this->isDirty = true;
        this->header.bytesLeft -= (rowSize + SlotDirectory::Size);

        return this->header.size - 1;
    }

    void Page::InsertRow(const DatabaseEngine::StorageTypes::InsertPayload& payload, const Int indexPosition){
        if (indexPosition >= this->header.size){
            this->InsertRow(payload);
            return;
        }

        const auto nextOffset = this->NewInsertOffset();
        const auto offSetCopy = nextOffset;

        const auto rowSize = payload.Size();

        std::memcpy(this->data + nextOffset, payload.Data(), rowSize);

        this->AdjustSlotDirectories(indexPosition, offSetCopy, rowSize);

        this->header.bytesLeft -= (rowSize + SlotDirectory::Size);
        this->header.size++;
        this->isDirty = true;
    }

    bool Page::UpdateRow(
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
            std::memcpy(this->data + rowOffset, payload.Data(), size);
            // this->SerializeRow(rowHeader, row, offSet);
            this->header.bytesLeft -= totalSize;
            this->isDirty = true;
            return true;
        }

        //insert new row at the end
        auto nextOffset = this->NewInsertOffset();
        auto offSetCopy = nextOffset;

        //if next row cant fit in the remaining space, we need to compact the page
        //keep slot offset and set its size to 0 so defragmentation does nothing as it is last on the offset
        if (this->header.bytesLeft < totalSize){
            this->UpdateSlotDirectory(SlotDirectory(nextOffset, 0, SlotDirectory::SLOT_DEAD), rowPtr.indexPosition);
            this->Defragment();

            //even if after the defragment row cant fit, throw exception
            if (this->header.bytesLeft < totalSize)
                return false;

            nextOffset = this->NewInsertOffset();
            offSetCopy = nextOffset;
        }

        // key.Serialize(this->data, nextOffset);
        // this->SerializeRow(rowHeader, row, nextOffset);
        // Use memmove instead of memcpy when copying within the same buffer to handle potential overlap
        if (this->IsIndexPage()){
            std::memmove(this->data + nextOffset, this->data + slot.GetOffset(), rowPtr.keySize);
            nextOffset += rowPtr.keySize;
        }

        // Use memcpy for payload since it's from a different buffer (no overlap possible)
        std::memcpy(this->data + nextOffset, payload.Data(), size);

        //update slot directory
        const auto newSlot = SlotDirectory(offSetCopy, totalSize, SlotDirectory::SLOT_USED);
        this->UpdateSlotDirectory(newSlot, rowPtr.indexPosition);

        //update bytes
        //Decrease by total size even though the previous offset is freed, as it becomes fragmented and no row can be inserted
        //unless pages gets defragmented
        this->header.bytesLeft -= totalSize;
        this->isDirty = true;
        return true;
    }

    void Page::SetForwardPointer(const Int indexPosition, const DataTypes::RowIdentifier& rowId) const{
        const auto slot = this->GetSlotDirectory(indexPosition);

        const auto offSet = slot.GetOffset();

        std::memcpy(this->data + offSet, &rowId, ROW_ID_SIZE);

        this->UpdateSlotDirectory(SlotDirectory(offSet, ROW_ID_SIZE, SlotDirectory::SLOT_FORWARDED), indexPosition);
    }

    void Page::ReadFromDisk(
        const std::vector<char> &buffer,
        const DatabaseEngine::StorageTypes::Table *otherTable,
        page_offset_t &offSet,
        fstream *filePtr
    ){
        std::memcpy(this->data, buffer.data() + offSet, PAGE_SIZE_WITHOUT_HEADER);
        offSet += PAGE_SIZE_WITHOUT_HEADER;
    }


    void Page::WriteToDisk(fstream *filePtr){
        this->WritePageHeaderToDisk(filePtr);
        filePtr->write(reinterpret_cast<const char*>(this->data), PAGE_SIZE_WITHOUT_HEADER);
    }

    // void Page::Delete(vector<Row*> &deletedRows, const Expressions::Expression *expression){
    //     Expressions::EvaluationContext context(Expressions::EvaluationContext::EvaluationContextType::SingleRow);
    //
    //     for (int i = 0; i < this->rows.size(); i++) {
    //         auto* row = this->rows[i];
    //
    //         context.row = row;
    //
    //         const auto value = expression->Evaluate(context);
    //         if (value.GetBool()) {
    //             this->rows.erase(this->rows.begin() + i);
    //             i--;
    //
    //             //remove it from index as well
    //
    //             deletedRows.push_back(row);
    //
    //             //deleted row, mark it as dirty
    //             this->isDirty = true;
    //         }
    //     }
    //
    //     this->header.pageSize = this->rows.size();
    // }

    // void Page::Delete(const Expressions::Expression *expression){
    //     Expressions::EvaluationContext context(Expressions::EvaluationContext::EvaluationContextType::SingleRow);
    //
    //     for (int i = 0; i < this->rows.size(); i++) {
    //         auto* row = this->rows[i];
    //
    //         context.row = row;
    //         const auto value = expression->Evaluate(context);
    //         if (value.GetBool()) {
    //             this->rows.erase(this->rows.begin() + i);
    //             i--;
    //
    //             RowHeader *rowHeader = row->GetHeader();
    //
    //             for (const auto &block : row->GetData())
    //             {
    //                 if (rowHeader->largeObjectBitMap->Get(block->GetColumnIndex()))
    //                 {
    //                     DataObjectPointer objectPointer;
    //                     memcpy(&objectPointer, block->GetBlockData(), sizeof(DataObjectPointer));
    //                 }
    //             }
    //           //delete row to deallocate space
    //           delete row;
    //
    //           //deleted row, mark it as dirty
    //           this->isDirty = true;
    //         }
    //     }
    //
    //     this->header.pageSize = this->rows.size();
    //     this->UpdateBytesLeft();
    // }

    void Page::Delete(const Int indexPosition) {
        // this->rows.erase(this->rows.begin() + indexPosition);

        this->UpdateBytesLeft();
        this->isDirty = true;
        this->header.size--;
    }

    void Page::SetFileName(const std::string &otherFilename) { this->filename = otherFilename; }

    void Page::SetPageId(const page_id_t pageId) { this->header.pageId = pageId; }

    void Page::UpdatePageSize() { this->header.size = 0; }

    void Page::UpdateBytesLeft(){
        const auto lastSlot = this->GetSlotDirectory(this->header.size - 1);
        const auto usedBytes = lastSlot.GetOffset() + lastSlot.GetSize() + (this->header.size * SlotDirectory::Size);

        this->header.bytesLeft = PAGE_SIZE_WITHOUT_HEADER - usedBytes;
        this->isDirty = true;
    }

    void Page::UpdateBytesLeft(const row_size_t previousRowSize, const row_size_t currentRowSize){
        this->header.bytesLeft += static_cast<int64_t>(currentRowSize) -static_cast<int64_t>(previousRowSize);

        this->isDirty = true;
    }

    const string &Page::GetFileName() const { return this->filename; }

    page_id_t Page::PageId() const { return this->header.pageId; }

    bool Page::IsDirty() const { return this->isDirty; }

    page_size_t Page::BytesLeft() const { return this->header.bytesLeft; }

    void Page::SetDirty(){ this->isDirty = true; }

    void Page::SetLogSequenceNumber(const log_sequence_number_t &lsn){
        this->logSequenceNumber = lsn;
    }

    const log_sequence_number_t & Page::GetLogSequenceNumber() const{ return this->logSequenceNumber; }

    page_size_t Page::GetPageSize() const { return this->header.size; }

    PageType Page::GetPageType() const { return this->header.type; }

    void Page::Defragment(){
        if (this->header.size <= 1)
            return;

        std::vector<SlotDirectoryDefragment> defragmentationSlots;
        defragmentationSlots.reserve(this->header.size);

        for (Int i = 0; i < this->header.size; i++){
            auto slot = this->GetSlotDirectory(i);
            defragmentationSlots.emplace_back(slot, i);
        }

        ranges::sort(defragmentationSlots, SlotDirectoryDefragment::OrderAscendingByOffSet);

        page_offset_t offset = 0;
        for (Int i = 0;i < this->header.size;i++){
            auto slot = defragmentationSlots[i];

            if (slot.slotDirectory.GetOffset() == offset){
                offset += slot.slotDirectory.GetSize();
                continue;
            }

            std::memmove(
                this->data + offset,
                this->data + slot.slotDirectory.GetOffset(),
                slot.slotDirectory.GetSize()
            );

            slot.slotDirectory.SetOffset(offset);
            this->UpdateSlotDirectory(slot.slotDirectory, slot.indexPosition);
            offset += slot.slotDirectory.GetSize();
        }

        const auto usedBytes = offset + (this->header.size * SlotDirectory::Size);
        this->header.bytesLeft = this->RawDataSize() - usedBytes;
        this->isDirty = true;
    }

    void Page::IncreasePinCount(){
        this->pinCount.fetch_add(1, std::memory_order_relaxed);
    }

    void Page::DecreasePinCount(){
        this->pinCount.fetch_sub(1, std::memory_order_relaxed);
    }

    int Page::GetPinCount() const {
        return this->pinCount.load(std::memory_order_relaxed);
    }

    PagePriority Page::GetPriority() const {
        return this->priority.load(std::memory_order_relaxed);
    }

    bool Page::HasSecondChance() const{
        return this->hasSecondChance;
    }

    void Page::SetHasSecondChanceUnsafe(const bool secondChance) {
        this->hasSecondChance = secondChance;
    }

    void Page::UniqueLock()const {
        this->latch.UniqueLock();
    }

    void Page::UniqueUnlock() const{
        this->latch.UniqueUnlock();
    }

    void Page::SharedLock() const{
        this->latch.SharedLock();
    }

    void Page::SharedUnlock() const {
        this->latch.SharedUnlock();
    }

    void Page::SetTable(const DatabaseEngine::StorageTypes::Table* otherTable){
        this->table = otherTable;
    }

    MultiThreading::ReadWriteMutex & Page::Latch() const {
        return this->latch;
    }

    object_t* Page::GetData() const{ return this->data; }
}
