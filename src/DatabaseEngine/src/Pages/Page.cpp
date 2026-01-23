#include "../../include/Pages/Page.h"

#include <iostream>

#include "../../include/Database.h"
#include "../../include/DataStorage/Block.h"
#include "../../include/BufferPool/StorageManager.h"

namespace Pages{
    SlotDirectory::SlotDirectory(){
        this->flags_offset = 0;
        this->size = 0;
    }

    SlotDirectory::SlotDirectory(
        const UnsignedSmallInt offset,
        const UnsignedSmallInt size,
        const Flag flag
    ){
        this->SetOffset(offset);
        this->SetSize(size);
        this->SetFlag(flag);
    }

    void SlotDirectory::SetOffset(const UnsignedSmallInt otherOffset){
        this->flags_offset = (this->flags_offset & FLAGS_MASK) | (otherOffset & OFFSET_MASK);
    }


    inline UnsignedSmallInt SlotDirectory::GetOffset() const{
        return this->flags_offset & OFFSET_MASK;
    }

    void SlotDirectory::SetSize(const UnsignedSmallInt otherSize){
        this->size = otherSize;
    }

    inline UnsignedSmallInt SlotDirectory::GetSize() const{
        return this->size;
    }

    void SlotDirectory::SetFlag(const Flag otherFlag){
        this->flags_offset = (this->flags_offset & OFFSET_MASK) | ((otherFlag << 14) & FLAGS_MASK);
    }

    inline SlotDirectory::Flag SlotDirectory::GetFlag() const{
        return static_cast<Flag>((this->flags_offset & FLAGS_MASK) >> 14);
    }

    bool SlotDirectory::Empty() const { return this->GetFlag() == SLOT_EMPTY; }
    bool SlotDirectory::Used() const { return this->GetFlag() == SLOT_USED; }
    bool SlotDirectory::ForwardPointer() const { return this->GetFlag() == SLOT_FORWARDED; }
    bool SlotDirectory::Dead() const { return this->GetFlag() == SLOT_DEAD; }

    bool SlotDirectory::Default() const{
        return this->flags_offset == 0 && this->size == 0;
    }

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

    Int Page::SlotDirectoryOffSet(const Int indexPosition) const{
        const auto defaultSize = this->header.type == PageType::DATA
                                     ? PAGE_SIZE_WITHOUT_HEADER
                                     : INDEX_PAGE_DEFAULT_SIZE;

        return defaultSize - (indexPosition + 1) * SlotDirectory::Size;
    }

    Int Page::SlotDirectoriesToMoveOffSet(const Int indexPosition, const Int slotToMove) const{
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

    SlotDirectory Page::GetSlotDirectory(const Int indexPosition) const{
        SlotDirectory slot;
        std::memcpy(&slot, this->data + this->SlotDirectoryOffSet(indexPosition), SlotDirectory::Size);
        return slot;
    }

    QueryResult Page::MaterializeRow(const Int indexPosition, const Int offset) const{
        const auto slot = this->GetSlotDirectory(indexPosition);

        const auto& columns = table->GetColumns();

        auto row = DatabaseEngine::StorageTypes::Row(*table);
        page_offset_t offSet = offset != 0 ? offset : slot.GetOffset();
        row.SetId(this->header.pageId, indexPosition);
        row.ReadHeaderFromDisk(this->data, offSet);
        // row.ReadDataFromDisk(this->data, offSet, columns);

        auto result = QueryResult();

        for (int i = 0;i < columns.size(); i++){
            if (row.GetNullBitMapValue(i)){
                auto value = Value::Null();
                result.AddColumn(value);
                continue;
            }

            block_size_t blockSize = 0;
            std::memcpy(&blockSize, this->data + offSet, sizeof(block_size_t));
            offSet += sizeof(block_size_t);

            auto value = Value(this->data + offSet, blockSize, columns[i]->GetColumnType());
            offSet += blockSize;

            result.AddColumn(value);
        }

        return result;
    }

    Value Page::PartialMaterializeRow(const Int indexPosition, column_index_t columnIndex, const Int offset) const{
        const auto slot = this->GetSlotDirectory(indexPosition);

        const auto& columns = table->GetColumns();

        auto row = DatabaseEngine::StorageTypes::Row(*table);
        page_offset_t offSet = offset != 0 ? offset : slot.GetOffset();
        row.SetId(this->header.pageId, indexPosition);
        row.ReadHeaderFromDisk(this->data, offSet);

        for (int i = 0;i < columns.size(); i++){
            if (row.GetNullBitMapValue(i)){
                if (columnIndex == i)
                    return Value::Null();

                continue;
            }

            block_size_t blockSize = 0;
            std::memcpy(&blockSize, this->data + offSet, sizeof(block_size_t));
            offSet += sizeof(block_size_t);

            if (i != columnIndex)
                continue;

            return Value(this->data + offSet, blockSize, columns[i]->GetColumnType());
        }

        throw std::runtime_error("Page::PartialMaterializeRow: Column index out of range");
    }

    void Page::UpdateSlotDirectory(const SlotDirectory slotDirectory, const Int indexPosition) const{
        std::memcpy(this->data + this->SlotDirectoryOffSet(indexPosition), &slotDirectory, SlotDirectory::Size);
    }

    void Page::InsertNewSlot(const SlotDirectory slotDirectory) const{
        std::memcpy(this->data + this->SlotDirectoryOffSet(this->header.size), &slotDirectory, SlotDirectory::Size);
    }

    void Page::DistributeFromPage(Page* donorPage, const Int numberOfSlotsToMove, const Int donorResizeVariant){
        const auto* leftData = donorPage->GetData();
        page_offset_t offset = this->NewInsertOffset();

        for (Int index = numberOfSlotsToMove; index < donorPage->GetPageSize(); index++){
            const auto leftSlot = donorPage->GetSlotDirectory(index);
            std::memcpy(this->data + offset, leftData + leftSlot.GetOffset(), leftSlot.GetSize());

            const auto rightSlot = SlotDirectory(offset, leftSlot.GetSize(), SlotDirectory::SLOT_USED);
            this->InsertNewSlot(rightSlot);

            offset += leftSlot.GetSize();
            this->header.size++;
        }

        this->header.bytesLeft -= this->header.size * SlotDirectory::Size + offset;
        this->isDirty = true;

        donorPage->Resize(donorResizeVariant);
        donorPage->Defragment();
    }

    void Page::DistributeFromBeginningOfPage(Page* donorPage, const Int numberOfSlotsToMove, const Int donorResizeVariant){
        const auto* leftData = donorPage->GetData();
        page_offset_t offset = this->NewInsertOffset();

        for (Int index = 0; index < numberOfSlotsToMove; index++){
            const auto leftSlot = donorPage->GetSlotDirectory(index);
            std::memcpy(this->data + offset, leftData + leftSlot.GetOffset(), leftSlot.GetSize());

            const auto rightSlot = SlotDirectory(offset, leftSlot.GetSize(), SlotDirectory::SLOT_USED);
            this->InsertNewSlot(rightSlot);

            offset += leftSlot.GetSize();
            this->header.size++;
            this->header.bytesLeft -= leftSlot.GetSize() + SlotDirectory::Size;
        }

        this->isDirty = true;

        donorPage->Resize(donorResizeVariant);
        donorPage->Defragment();
    }

    void Page::DistributeSingleSlotFromPage(Page* donorPage, Int donorIndexPosition, Int donorResizeVariant){

    }

    void Page::Resize(const Int size){
        for (int i = size; i < this->header.size; i++){
            const auto slot = this->GetSlotDirectory(i);
            this->header.bytesLeft += slot.GetSize() + SlotDirectory::Size;
        }

        this->header.size = size;
        this->isDirty = true;
    }

    void Page::ResizeFromBeginning(const Int size){
        //mark slots as deleted from the beginning use flags
        for (int i = 0; i < this->header.size; i++){
            auto slot = this->GetSlotDirectory(i);
            this->header.bytesLeft += slot.GetSize() + SlotDirectory::Size;

            slot.SetSize(0);
            this->UpdateSlotDirectory(slot, i);
        }

        this->header.size = size;
        this->isDirty = true;
    }

    RowReference::RowReference(){
        this->pagePtr = nullptr;
        this->indexPosition = 0;
        this->offset = 0;
    }

    RowReference::RowReference(Page* pagePtr, const Int indexPosition, const Int offset){
        this->pagePtr = pagePtr;
        this->indexPosition = indexPosition;
        this->offset = offset;
        this->pagePtr->IncreasePinCount();
    }

    RowReference::RowReference(const RowReference& other){
        this->pagePtr = other.pagePtr;
        this->indexPosition = other.indexPosition;
        this->offset = other.offset;
        this->pagePtr->IncreasePinCount();
    }

    RowReference& RowReference::operator=(const RowReference& other){
        if (this == &other)
            return *this;

        this->pagePtr = other.pagePtr;
        this->indexPosition = other.indexPosition;
        this->offset = other.offset;
        this->pagePtr->IncreasePinCount();

        return *this;
    }

    RowReference::RowReference(RowReference&& other) noexcept{
        this->pagePtr = other.pagePtr;
        this->indexPosition = other.indexPosition;
        this->offset = other.offset;
        other.pagePtr = nullptr;
        other.indexPosition = 0;
        other.offset = 0;
    }

    RowReference& RowReference::operator=(RowReference&& other) noexcept{
        if (this == &other)
            return *this;

        this->pagePtr = other.pagePtr;
        this->indexPosition = other.indexPosition;
        other.pagePtr = nullptr;
        other.indexPosition = 0;
        other.offset = 0;

        return *this;
    }

    RowReference::~RowReference(){
        if (this->pagePtr)
            this->pagePtr->DecreasePinCount();

        this->pagePtr = nullptr;
    }

    QueryResult RowReference::Materialize()const{
        return this->pagePtr->MaterializeRow(this->indexPosition, this->offset);
    }

    Value RowReference::PartialMaterialize(const column_index_t columnIndex) const{
        return this->pagePtr->PartialMaterializeRow(this->indexPosition, this->offset, columnIndex);
    }

    Errors::RuntimeStatus RowReference::Update(const std::vector<Value>& updates) const{
        auto row = this->Materialize();
        auto header = this->pagePtr->PeekRowHeader(this->indexPosition, this->offset);

        for (int i = 0;i < updates.size(); i++){
            const auto& value = updates[i];

            if (value.IsNull()){
                header.nullBitMap.Set(i, true);
                continue;
            }

            auto result = row.UpdateColumnAt(value);

            if (result.code != Errors::RuntimeError::Ok)
                return result;
        }

        this->pagePtr->UpdateRow(
            header,
            row,
            this->indexPosition,
            this->offset
        );

        return {};
    }

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

        const auto& rowData = row.GetData();
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

    void Page::InsertFirstRow(DatabaseEngine::StorageTypes::Row*& row){
        const auto rowSize = row->TotalSize();

        page_offset_t pos = 0;
        row->Serialize(this->data, pos);

        const auto newSlot = SlotDirectory(0, rowSize, SlotDirectory::SLOT_USED);
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

        const auto newSlot = SlotDirectory(offSetCopy, rowSize, SlotDirectory::SLOT_USED);
        this->InsertNewSlot(newSlot);

        this->header.size++;
        this->isDirty = true;
        this->header.bytesLeft -= (rowSize + SlotDirectory::Size);

        return this->header.size - 1;
    }

    void Page::InsertRow(DatabaseEngine::StorageTypes::Row*& row, const Int indexPosition){
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

    void Page::UpdateRow(
        const DatabaseEngine::StorageTypes::RowHeader& rowHeader,
        QueryResult& row,
        const Int indexPosition,
        const Int offset
    ){
        if (this->IndexOutOfBounds(indexPosition))
            throw std::out_of_range("Page::UpdateRow: Index position is out of bounds.");

        const auto slot = this->GetSlotDirectory(indexPosition);

        const auto previousRowSize = slot.GetSize();

        const auto currentRowSize = row.GetPageByteSize() + rowHeader.Size();

        //if new row size is less than or equal to previous row size, update in place
        if (currentRowSize <= previousRowSize){
            page_offset_t offSet = slot.GetOffset();
            this->SerializeRow(rowHeader, row, offSet);
            return;
        }

        //insert new row at the end
        auto nextOffset = this->NewInsertOffset();
        const auto offSetCopy = nextOffset;

        this->SerializeRow(rowHeader, row, nextOffset);

        //update slot directory
        const auto newSlot = SlotDirectory(offSetCopy, currentRowSize, SlotDirectory::SLOT_USED);
        this->UpdateSlotDirectory(newSlot, indexPosition);

        //update bytes
        //Decrease by total size even though the previous offset is freed, as it becomes fragmented and no row can be inserted
        //unless pages gets defragmented
        this->header.bytesLeft -= currentRowSize;
        this->isDirty = true;
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

    page_id_t Page::GetPageId() const { return this->header.pageId; }

    bool Page::IsDirty() const { return this->isDirty; }

    page_size_t Page::GetBytesLeft() const { return this->header.bytesLeft; }

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
