#include "../../include/Pages/Page.h"

#include <iostream>

#include "../../include/Database.h"
#include "../../include/DataStorage/Block.h"
#include "../../include/BufferPool/StorageManager.h"

namespace Pages{
    PageHeader::PageHeader(){
        this->type = PageType::DATA;
        this->pageId = INVALID_PAGE_ID;
        this->size = 0;
        this->bytesLeft = static_cast<page_size_t>(PAGE_SIZE - PAGE_HEADER_SIZE);
    }

    PageHeader::~PageHeader() = default;

    void Page::WritePageHeaderToDisk(fstream *filePtr) const{
        filePtr->write(reinterpret_cast<const char *>(&this->header.pageId), sizeof(page_id_t));
        filePtr->write(reinterpret_cast<const char *>(&this->header.size), sizeof(page_size_t));
        filePtr->write(reinterpret_cast<const char *>(&this->header.bytesLeft), sizeof(page_size_t));
        filePtr->write(reinterpret_cast<const char *>(&this->header.type), sizeof(PageType));
    }

    page_offset_t Page::NewInsertOffset() const{
        const auto defaultSize = this->header.type == PageType::DATA
                                     ? PAGE_SIZE_WITHOUT_HEADER
                                     : INDEX_PAGE_DEFAULT_SIZE;

        return defaultSize - this->header.bytesLeft - this->header.size * SlotDirectory::Size;
    }

    Int Page::SlotDirectoryOffSet(const Int& indexPosition) const{
        const auto defaultSize = this->header.type == PageType::DATA
                                     ? PAGE_SIZE_WITHOUT_HEADER
                                     : INDEX_PAGE_DEFAULT_SIZE;

        return defaultSize - (indexPosition + 1) * SlotDirectory::Size;
    }

    Int Page::SlotDirectoriesToMoveOffSet(const Int& indexPosition, const Int& slotToMove) const{
        const auto defaultSize = this->header.type == PageType::DATA
                                     ? PAGE_SIZE_WITHOUT_HEADER
                                     : INDEX_PAGE_DEFAULT_SIZE;

        return defaultSize - (indexPosition + slotToMove) * SlotDirectory::Size;
    }

    Int Page::RawDataSize() const{
        switch (this->header.type){
            case PageType::METADATA:
            case PageType::GAM:
            case PageType::FREESPACE:
            case PageType::OVERFLOWTYPE:
            case PageType::UNDO:
            case PageType::IAM:
            case PageType::LOB:
            case PageType::DATA:
                return PAGE_SIZE_WITHOUT_HEADER;
            case PageType::INDEX:
                return INDEX_PAGE_DEFAULT_SIZE;
        }

        return PAGE_SIZE_WITHOUT_HEADER;
    }

    void Page::WriteRowToDisk(fstream* filePtr, const Pointer<DatabaseEngine::StorageTypes::Row>& row){
        row->WriteHeaderToDisk(filePtr);
        row->WriteVersionHeaderToDisk(filePtr);
        row->WriteDataToDisk(filePtr);
    }

    SlotDirectory Page::GetSlotDirectory(const int& indexPosition) const{
        auto slot = SlotDirectory(0, 0);
        std::memcpy(&slot, this->data + this->SlotDirectoryOffSet(indexPosition), SlotDirectory::Size);
        return slot;
    }

    DatabaseEngine::StorageTypes::Row Page::MaterializeRow(
        const DatabaseEngine::StorageTypes::Table* table,
        const Int& indexId
    ) const{
        const auto slot = this->GetSlotDirectory(indexId);

        const auto& columns = table->GetColumns();

        auto row = DatabaseEngine::StorageTypes::Row(*table);
        page_offset_t offSet = slot.offset;

        std::cout << "Materializing row at slot " << indexId << " offset: " << slot.offset << " size: " << slot.size << std::endl;

        row.SetId(this->header.pageId, indexId);
        row.ReadHeaderFromDisk(this->data, offSet);
        row.ReadVersionHeaderFromDisk(this->data, offSet);
        row.ReadDataFromDisk(this->data, offSet, columns);

        return row;
    }

    void Page::UpdateSlotDirectory(const SlotDirectory& slotDirectory, const int& indexPosition) const{
        std::memcpy(this->data + this->SlotDirectoryOffSet(indexPosition), &slotDirectory, SlotDirectory::Size);
    }

    void Page::InsertNewSlot(const SlotDirectory& slotDirectory) const{
        std::memcpy(this->data + this->SlotDirectoryOffSet(this->header.size), &slotDirectory, SlotDirectory::Size);
    }

    bool Page::IndexOutOfBounds(const int& indexPosition) const{
        return indexPosition >= this->header.size;
    }

    void Page::AdjustSlotDirectories(const int& indexPosition, const page_offset_t& offset, const int& slotSize) const{
        const auto slotsToMove = this->header.size - indexPosition;
        const auto slotBytesToMove = slotsToMove * SlotDirectory::Size;
        const auto srcOffset = this->SlotDirectoriesToMoveOffSet(indexPosition, slotsToMove);
        const auto dstOffset = srcOffset - SlotDirectory::Size;

        std::memmove(
            this->data + dstOffset,
            this->data + srcOffset,
            slotBytesToMove
        );

        const auto slot = SlotDirectory(offset, slotSize);
        this->UpdateSlotDirectory(slot, indexPosition);
    }

    Page::Page(const page_id_t &pageId, const bool &isPageCreation){
        this->header.pageId = pageId;
        this->isDirty = isPageCreation;
        this->pinCount = 0;
        this->hasSecondChance = true;
        this->logSequenceNumber = 0;
        this->header.type = PageType::DATA;
        this->priority = PagePriority::LOW;
        this->data = static_cast<object_t*>(std::malloc(PAGE_SIZE_WITHOUT_HEADER));
    }

    Page::Page(const page_id_t& pageId, const page_size_t& size, const bool& isPageCreation){
        this->header.pageId = pageId;
        this->isDirty = isPageCreation;
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

    Page::Page(const PageHeader& pageHeader, const page_size_t& size){
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

    void Page::InsertFirstRow(DatabaseEngine::StorageTypes::Row*& row){
        const auto rowSize = row->TotalSize();

        page_offset_t pos = 0;
        row->Serialize(this->data, pos);

        column_number_t val = 0;
        std::memcpy(&val, this->data, sizeof(column_number_t));

        const auto newSlot = SlotDirectory(0, rowSize);
        this->InsertNewSlot(newSlot);

        this->header.size++;
        this->isDirty = true;
        this->header.bytesLeft -= (rowSize + SlotDirectory::Size);
    }

    Int Page::InsertRow(DatabaseEngine::StorageTypes::Row*& row){
        if (this->header.size == 0){
            this->InsertFirstRow(row);
            return 0;
        }

        const auto rowSize = row->TotalSize();

        auto nextOffset = this->NewInsertOffset();
        const auto offSetCopy = nextOffset;
        row->Serialize(this->data, nextOffset);

        const auto newSlot = SlotDirectory(offSetCopy, rowSize);
        this->InsertNewSlot(newSlot);

        this->header.size++;
        this->isDirty = true;
        this->header.bytesLeft -= (rowSize + SlotDirectory::Size);

        return this->header.size - 1;
    }

    void Page::InsertRow(DatabaseEngine::StorageTypes::Row*& row, const int& indexPosition){
        if (indexPosition >= this->header.size){
            this->InsertRow(row);
            return;
        }

        auto nextOffset = this->NewInsertOffset();
        const auto offSetCopy = nextOffset;

        row->Serialize(this->data, nextOffset);

        const auto slotSize = row->TotalSize();
        this->AdjustSlotDirectories(indexPosition, offSetCopy, slotSize);

        this->header.bytesLeft -= (slotSize + SlotDirectory::Size);
        this->header.size++;
        this->isDirty = true;
    }

    void Page::UpdateRow(DatabaseEngine::StorageTypes::Row*& row, const int& indexPosition){
        if (this->IndexOutOfBounds(indexPosition))
            throw std::out_of_range("Page::UpdateRow: Index position is out of bounds.");

        const auto slot = this->GetSlotDirectory(indexPosition);

        const auto previousRowSize = slot.size;
        const auto currentRowSize = row->TotalSize();

        const auto sizeDiff = static_cast<int>(currentRowSize) - static_cast<int>(previousRowSize);

        //if new row size is less than or equal to previous row size, update in place
        if (sizeDiff <= 0){
            page_offset_t pos = slot.offset;
            row->Serialize(this->data, pos);
            return;
        }

        //insert new row at the end
        auto nextOffset = this->NewInsertOffset();
        const auto offSetCopy = nextOffset;

        row->Serialize(this->data, nextOffset);

        //update slot directory
        const auto newSlot = SlotDirectory(offSetCopy, currentRowSize);
        this->UpdateSlotDirectory(newSlot, indexPosition);

        //update bytes left
        this->header.bytesLeft -= sizeDiff;
        this->isDirty = true;
    }

    void Page::ReadFromDisk(
        const std::vector<char> &buffer,
        const DatabaseEngine::StorageTypes::Table *table,
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

    void Page::Delete(const int &indexPosition) {
        // this->rows.erase(this->rows.begin() + indexPosition);

        this->UpdateBytesLeft();
        this->isDirty = true;
        this->header.size--;
    }

    void Page::SetFileName(const std::string &otherFilename) { this->filename = otherFilename; }

    void Page::SetPageId(const page_id_t &pageId) { this->header.pageId = pageId; }

    void Page::UpdatePageSize() { this->header.size = 0; }

    void Page::UpdateBytesLeft(){
        const auto lastSlot = this->GetSlotDirectory(this->header.size - 1);
        const auto usedBytes = lastSlot.offset + lastSlot.size + (this->header.size * SlotDirectory::Size);

        this->header.bytesLeft = PAGE_SIZE_WITHOUT_HEADER - usedBytes;
        this->isDirty = true;
    }

    void Page::UpdateBytesLeft(const row_size_t& previousRowSize, const row_size_t& currentRowSize)
    {
        this->header.bytesLeft += static_cast<int64_t>(currentRowSize) -static_cast<int64_t>(previousRowSize);

        this->isDirty = true;
    }

    const string &Page::GetFileName() const { return this->filename; }

    const page_id_t &Page::GetPageId() const { return this->header.pageId; }

    const bool &Page::IsDirty() const { return this->isDirty; }

    const page_size_t &Page::GetBytesLeft() const { return this->header.bytesLeft; }

    void Page::SetDirty(){ this->isDirty = true; }

    void Page::SetLogSequenceNumber(const log_sequence_number_t &lsn){
        this->logSequenceNumber = lsn;
    }

    const log_sequence_number_t & Page::GetLogSequenceNumber() const{ return this->logSequenceNumber; }

    page_size_t Page::GetPageSize() const { return this->header.size; }

    const PageType &Page::GetPageType() const { return this->header.type; }

    // int Page::GetRows(
    //     std::vector<Pointer<DatabaseEngine::StorageTypes::Row>> *result,
    //     const size_t &rowsToSelect,
    //     const int32_t& startingPosition
    // ) const
    // {
    //     if (startingPosition >= this->rows.size())
    //         return -1;
    //
    //     for (int i = startingPosition; i < this->rows.size(); i++) {
    //         result->push_back(this->rows.at(i));
    //
    //         if (result->size() == rowsToSelect)
    //             return i;
    //     }
    //
    //     return static_cast<int>(this->rows.size() - 1);
    // }

    // void Page::GetRowByIndex(std::vector<DatabaseEngine::StorageTypes::Row>* rows, const DatabaseEngine::StorageTypes::Table &table, const int &indexPosition) const
    // {
    //     const auto &row = this->rows[indexPosition];
    //
    //     const DatabaseEngine::StorageTypes::RowHeader *rowHeader = row->GetHeader();
    //
    //     std::vector<DatabaseEngine::StorageTypes::Block *> copyBlocks = row->GetBlockCopies();
    //
    //     rows->emplace_back(table, copyBlocks, rowHeader->nullBitMap);
    // }
    //
    // DatabaseEngine::StorageTypes::Row* Page::GetRow(const int &indexPosition)const{
    //     return this->MaterializeRow()
    // }

    DatabaseEngine::StorageTypes::Row Page::GetRow(const DatabaseEngine::StorageTypes::Table* table, const int& indexPosition) const{
        return this->MaterializeRow(table, indexPosition);
    }

    void Page::Defragment() const{
        if (this->header.size <= 1)
            return;

        std::vector<SlotDirectoryDefragment> defragmentationSlots;
        defragmentationSlots.reserve(this->header.size);

        for (int i = 0; i < this->header.size; i++)
            defragmentationSlots.emplace_back(this->GetSlotDirectory(i), i);

        ranges::sort(defragmentationSlots, SlotDirectoryDefragment::OrderAscendingByOffSet);

        auto& previousSlot = defragmentationSlots.front();

        for (int i = 1;i < this->header.size;i++){
            auto& slot = defragmentationSlots[i];

            //if slots are contiguous, continue
            const auto previousSlotEndOffset = previousSlot.slotDirectory.offset + previousSlot.slotDirectory.size;
            if (previousSlotEndOffset == slot.slotDirectory.offset){
                previousSlot = slot;
                continue;
            }

            std::memmove(
                this->data + previousSlotEndOffset,
                this->data + slot.slotDirectory.offset,
                slot.slotDirectory.size
            );

            slot.slotDirectory.offset = previousSlotEndOffset;
            this->UpdateSlotDirectory(slot.slotDirectory, slot.indexPosition);
        }
    }

    void Page::IncreasePinCount() {
        this->pinCount.fetch_add(1, std::memory_order_relaxed);
    }

    void Page::DecreasePinCount() {
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

    void Page::SetHasSecondChanceUnsafe(const bool &secondChance) {
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

    MultiThreading::ReadWriteMutex & Page::Latch() const {
        return this->latch;
    }
}