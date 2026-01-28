#include "../../include/Pages/Page.h"

#include <cmath>
#include <iostream>
#include "../../include/Database.h"
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

    QueryResult Page::MaterializeRow(const Int indexPosition, const Int keySize) const{
        const auto slot = this->GetSlotDirectory(indexPosition);

        const auto& columns = this->table->GetColumns();
        const auto columnsSize = static_cast<const Int>(columns.size());

        page_offset_t offSet = keySize + slot.GetOffset();

        auto rowHeader = DatabaseEngine::StorageTypes::RowHeader();

        offSet += Constants::ROW_VERSION_HEADER_SIZE;
        rowHeader.nullBitMap.GetDataFromFile(this->data, offSet, columnsSize);
        rowHeader.largeObjectBitMap.GetDataFromFile(this->data, offSet, columnsSize);
        rowHeader.overflowBitMap.GetDataFromFile(this->data, offSet, columnsSize);

        auto result = QueryResult();

        block_size_t sizes[columnsSize];

        for (int i = 0;i < columnsSize; i++){
            if (rowHeader.nullBitMap.Get(i))
                continue;

            std::memcpy(&sizes[i], this->data + offSet, sizeof(block_size_t));
            offSet += sizeof(block_size_t);
        }

        for (int i = 0;i < columnsSize; i++){
            if (rowHeader.nullBitMap.Get(i)){
                auto value = Value::Null();
                result.AddColumn(value);
                continue;
            }

            auto value = Value(this->data + offSet, sizes[i], columns[i]->Type());
            offSet += sizes[i];

            result.AddColumn(value);
        }


        return result;
    }

    void Page::InitializeRowReferenceCache(const RowReference* rowPtr, const Int numberOfColumns)const{
        const auto slot = this->GetSlotDirectory(rowPtr->indexPosition);

        page_offset_t offSet = rowPtr->keySize + slot.GetOffset() + Constants::ROW_VERSION_HEADER_SIZE;

        rowPtr->isHeaderInitialized = true;
        rowPtr->header = DatabaseEngine::StorageTypes::RowHeader();
        rowPtr->header.nullBitMap.GetDataFromFile(this->data, offSet, numberOfColumns);
        rowPtr->header.largeObjectBitMap.GetDataFromFile(this->data, offSet, numberOfColumns);
        rowPtr->header.overflowBitMap.GetDataFromFile(this->data, offSet, numberOfColumns);

        rowPtr->sizes.resize(numberOfColumns, 0);

        for (int i = 0; i < numberOfColumns; i++){
            if (rowPtr->header.nullBitMap.Get(i))
                continue;

            std::memcpy(&rowPtr->sizes[i], this->data + offSet, sizeof(block_size_t));
            offSet += sizeof(block_size_t);
        }

        rowPtr->dataOffset = offSet;
    }

    Value Page::PartialMaterializeRow(const RowReference* rowPtr,const column_index_t columnIndex) const{
        const auto& columns = this->table->GetColumns();

        if (!rowPtr->isHeaderInitialized)
            this->InitializeRowReferenceCache(rowPtr, static_cast<Int>(columns.size()));

        block_size_t offSet = rowPtr->dataOffset;
        for (int i = 0; i < columnIndex; i++){
            if (rowPtr->header.nullBitMap.Get(i))
                continue;

            offSet += rowPtr->sizes[i];
        }

        if (rowPtr->header.nullBitMap.Get(columnIndex))
            return Value::Null();

        return Value(this->data + offSet, rowPtr->sizes[columnIndex], columns[columnIndex]->Type());
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
        this->keySize = 0;
        this->isHeaderInitialized = false;
        this->dataOffset = 0;
    }

    RowReference::RowReference(Page* pagePtr, const Int indexPosition, const Int offset){
        this->pagePtr = pagePtr;
        this->indexPosition = indexPosition;
        this->keySize = offset;
        this->pagePtr->IncreasePinCount();
        this->isHeaderInitialized = false;
        this->dataOffset = 0;
    }

    RowReference::RowReference(const RowReference& other){
        this->pagePtr = other.pagePtr;
        this->indexPosition = other.indexPosition;
        this->keySize = other.keySize;
        this->pagePtr->IncreasePinCount();
        this->header = other.header;
        this->isHeaderInitialized = other.isHeaderInitialized;
        this->sizes = other.sizes;
        this->dataOffset = other.dataOffset;
    }

    RowReference& RowReference::operator=(const RowReference& other){
        if (this == &other)
            return *this;

        this->pagePtr = other.pagePtr;
        this->indexPosition = other.indexPosition;
        this->keySize = other.keySize;
        this->pagePtr->IncreasePinCount();
        this->header = other.header;
        this->isHeaderInitialized = other.isHeaderInitialized;
        this->sizes = other.sizes;
        this->dataOffset = other.dataOffset;

        return *this;
    }

    RowReference::RowReference(RowReference&& other) noexcept{
        this->pagePtr = other.pagePtr;
        this->indexPosition = other.indexPosition;
        this->keySize = other.keySize;
        this->header = other.header;
        this->isHeaderInitialized = other.isHeaderInitialized;
        this->sizes = other.sizes;
        this->dataOffset = other.dataOffset;

        other.isHeaderInitialized = false;
        other.sizes.clear();
        other.pagePtr = nullptr;
        other.indexPosition = 0;
        other.keySize = 0;
        other.dataOffset = 0;
    }

    RowReference& RowReference::operator=(RowReference&& other) noexcept{
        if (this == &other)
            return *this;

        this->pagePtr = other.pagePtr;
        this->indexPosition = other.indexPosition;
        this->keySize = other.keySize;
        this->header = other.header;
        this->isHeaderInitialized = other.isHeaderInitialized;
        this->sizes = other.sizes;
        this->dataOffset = other.dataOffset;

        other.isHeaderInitialized = false;
        other.sizes.clear();
        other.pagePtr = nullptr;
        other.indexPosition = 0;
        other.keySize = 0;
        other.dataOffset = 0;

        return *this;
    }

    RowReference::~RowReference(){
        if (this->pagePtr)
            this->pagePtr->DecreasePinCount();

        this->pagePtr = nullptr;
    }

    QueryResult RowReference::Materialize()const{
        return this->pagePtr->MaterializeRow(this->indexPosition, this->keySize);
    }

    Value RowReference::PartialMaterialize(const column_index_t columnIndex) const{
        return this->pagePtr->PartialMaterializeRow(this, columnIndex);
    }

    Errors::RuntimeStatus RowReference::Update(const std::vector<Value>& updates) const{
        auto row = this->Materialize();
        auto header = this->pagePtr->PeekRowHeader(this->indexPosition, this->keySize);

        // row.Update(updates);
        //
        // this->pagePtr->UpdateRow();

        // this->pagePtr->UpdateRow(
        //     header,
        //     row,
        //     this->indexPosition,
        //     this->offset
        // );

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

    void Page::UpdateRow(
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
            return;
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
                throw std::runtime_error("IndexPage::UpdateRow: Not enough space to update the row after defragmentation.");

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
