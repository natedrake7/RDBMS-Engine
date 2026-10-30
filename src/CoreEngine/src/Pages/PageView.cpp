#include <CoreEngine/Pages/PageView.h>

#include <algorithm>

#include <CoreEngine/DataStorage/Table.h>
#include <CoreEngine/Pages/Additional/Frame.h>
#include <CoreEngine/Pages/Additional/RawRowReference.h>
#include <CoreEngine/DataStorage/Row/Row.h>
#include <Systemic/DataTypes/StringValue.h>
#include <CoreEngine/SystemDatabases/VersionDatabase.h>

namespace CoreEngine::Pages{
    PageHeader::PageHeader()
        :   _lsn(INVALID_LOG_SEQUENCE_NUMBER), _checkSum(0),
            _pageId(INVALID_PAGE_ID), _size(0),
            _type(static_cast<page_size_t>(PageLayout::PageType::DATA)),
            _bytesLeft(static_cast<page_size_t>(StorageLayout::PAGE_SIZE - StorageLayout::PAGE_HEADER_SIZE)),
            _fragmentedBytes(0), _flags(0){}


    void PageView::SetPageId(const page_id_t pageId) const{
        this->_frame->Header()->_pageId = pageId;
    }

    page_offset_t PageView::NewInsertOffset() const{
        const auto* headerPtr = this->_frame->Header();
        return StorageLayout::PAGE_SIZE
            - headerPtr->_bytesLeft
            - headerPtr->_size * SlotDirectory::SIZE;
    }

    Int PageView::SlotDirectoryOffSet(const Int indexPosition){
        return static_cast<Int>(StorageLayout::PAGE_SIZE) - (indexPosition + 1) * SlotDirectory::SIZE;
    }

    Int PageView::SlotDirectoriesToMoveOffSet(const Int indexPosition, const Int slotToMove){
        return static_cast<Int>(StorageLayout::PAGE_SIZE) - (indexPosition + slotToMove) * SlotDirectory::SIZE;
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
        return indexPosition >= this->_frame->Header()->_size;
    }

    void PageView::AdjustSlotDirectories(
        const Int indexPosition,
        const page_offset_t offset,
        const row_size_t rowSize,
        const key_size_t keySize
    ) const{
        const auto slotsToMove = this->_frame->Header()->_size - indexPosition;
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
        return this->IsIndexPage()
                   ? StorageLayout::INDEX_PAGE_DEFAULT_SIZE
                   : StorageLayout::PAGE_SIZE_WITHOUT_HEADER;
    }

    void PageView::InsertFirstRow(const StorageTypes::SerializedRow& payload) const{
        const auto rowSize = payload.Size();
        const auto offSet = this->NewInsertOffset();

        std::memcpy(this->_frame->_data + offSet, payload.Data(), rowSize);

        const auto newSlot = SlotDirectory(offSet, 0, rowSize, 0, SlotDirectory::SLOT_USED);
        this->InsertNewSlot(newSlot);

        this->_frame->MarkDirty();
        this->_frame->Header()->IncrementSizeNoLock();
        this->_frame->Header()->ConsumeBytesNoLock(rowSize + SlotDirectory::SIZE);
    }

    bool PageView::IsIndexPage() const{
        return this->_frame->Header()->Type() == PageLayout::PageType::INDEXED;
    }

    PageView::PageView()
        : _frame(nullptr), initialOffset(StorageLayout::PAGE_HEADER_SIZE){}

    PageView::PageView(Frame* framePtr)
        : _frame(framePtr), initialOffset(StorageLayout::PAGE_HEADER_SIZE){}

    PageView::PageView(PageView&& other) noexcept
        :   _frame(other._frame),
            initialOffset(other.initialOffset){
        other._frame = nullptr;
        other.initialOffset = 0;
    }

    PageView& PageView::operator=(PageView&& other) noexcept{
        if (this == &other)
            return *this;

        if (this->IsValid())
            this->_frame->Unpin();

        this->_frame = other._frame;
        this->initialOffset = other.initialOffset;

        other._frame = nullptr;
        other.initialOffset = 0;
        return *this;
    }

    PageView::~PageView(){
        this->Unpin();
    }

    PageHeader* PageView::GetHeader() const{
        return this->_frame->Header();
    }

    const StorageTypes::RowHeader* PageView::PeekRowHeader(const Int indexPosition) const{
        const auto slot = this->GetSlotDirectory(indexPosition);
        return reinterpret_cast<const StorageTypes::RowHeader*>(this->_frame->_data + slot.AbsoluteDataOffset());
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
            this->_frame->_data + Pages::PageView::SlotDirectoryOffSet(this->_frame->Header()->_size),
            &slotDirectory,
            SlotDirectory::SIZE
        );
    }

    void PageView::Defragment(const ::Memory::IAllocator* allocator) const{
        auto* header = this->_frame->Header();

        const auto slotCount = header->_size;

        DataStructures::PolymorphicArray<SlotDirectoryDefragment> liveSlots(allocator, header->_size);
        for (Int i = 0; i < header->_size; i++){
            const auto slot = this->GetSlotDirectory(i);

            if (slot.Dead() || slot.Empty()){
                const SlotDirectory updatedSlot(0, 0, 0, 0, slot.GetFlag());
                this->UpdateSlotDirectory(updatedSlot, i);
                continue;
            }

            liveSlots.Push(SlotDirectoryDefragment(slot, i));
        }

        std::ranges::sort(liveSlots, [](const SlotDirectoryDefragment& lhs, const SlotDirectoryDefragment& rhs){
            return lhs._slotDirectory.Offset() < rhs._slotDirectory.Offset();
        });

        page_offset_t offset = this->initialOffset;

        for (auto& [slot, index]: liveSlots){
            if (slot.Offset() != offset){
                std::memmove(this->_frame->_data + offset, this->_frame->_data + slot.Offset(), slot.Size());
                slot.SetOffset(offset);
                this->UpdateSlotDirectory(slot, index);
            }
            offset += slot.Size();
        }

        header->_bytesLeft = static_cast<page_size_t>(StorageLayout::PAGE_SIZE - offset - slotCount * SlotDirectory::SIZE);
        header->_fragmentedBytes = 0;
        this->_frame->MarkDirty();
    }

    void PageView::Truncate(const Int size) const{
        auto* header = this->_frame->Header();
        for (int i = size; i < this->_frame->Header()->_size; i++){
            const auto slot = this->GetSlotDirectory(i);
            header->FragmentBytesNoLock(slot.Size());
        }

        header->_bytesLeft = static_cast<page_size_t>(header->_bytesLeft + (header->_size - size) * SlotDirectory::SIZE);
        header->_size = size;
        this->_frame->MarkDirty();
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

        const auto movedSlots = donorPage->PageSize() - slotToMoveFrom;

        for (Int index = slotToMoveFrom; index < donorPage->PageSize(); index++){
            const auto leftSlot = donorPage->GetSlotDirectory(index);
            std::memcpy(this->_frame->_data + offset, leftData + leftSlot.Offset(), leftSlot.Size());

            const SlotDirectory rightSlot(
                offset, leftSlot.DataOffset(),
                leftSlot.DataSize(), leftSlot.KeySize(),
                SlotDirectory::SLOT_USED
            );
            this->InsertNewSlot(rightSlot);

            offset += leftSlot.Size();
            this->_frame->Header()->IncrementSizeNoLock();
        }

        this->_frame->Header()->ConsumeBytesNoLock(movedSlots * SlotDirectory::SIZE + (offset - startOffset));
        this->_frame->MarkDirty();

        donorPage->Truncate(donorNewSize);
        donorPage->Defragment(allocator);
    }

    bool PageView::EnsureContiguousSpace(const ::Memory::IAllocator* allocator, const UnsignedInt bytes) const{
        const auto* header = this->_frame->Header();
        if (bytes <= header->_bytesLeft)
            return true;
        if (bytes > header->TotalFreeBytesNoLock())
            return false;

        this->Defragment(allocator);
        return bytes <= header->_bytesLeft;
    }

    Int PageView::InsertRow(const StorageTypes::SerializedRow& payload) const{
        const auto rowSize = payload.Size();
        const auto offset = this->NewInsertOffset();
        auto* header = this->_frame->Header();

        PageView::CheckRowFits(header, rowSize);
        std::memcpy(this->_frame->_data + offset, payload.Data(), rowSize);

        const SlotDirectory newSlot(
            offset, 0,
            rowSize, 0, SlotDirectory::SLOT_USED
        );
        this->InsertNewSlot(newSlot);

        header->_size++;
        header->ConsumeBytesNoLock(rowSize + SlotDirectory::SIZE);
        this->_frame->MarkDirty();
        return header->_size - 1;
    }

    void PageView::InsertRow(
        const StorageTypes::SerializedRow& payload,
        const Int indexPosition
    ) const{
        if (indexPosition >= this->_frame->Header()->_size){
            const auto _ = this->InsertRow(payload);
            return;
        }

        const auto nextOffset = this->NewInsertOffset();
        const auto offSetCopy = nextOffset;
        const auto rowSize = payload.Size();

        auto* header = this->_frame->Header();

        PageView::CheckRowFits(header, rowSize);

        std::memcpy(this->_frame->_data + nextOffset, payload.Data(), rowSize);

        this->AdjustSlotDirectories(indexPosition, offSetCopy, rowSize, 0);

        header->_size++;
        header->ConsumeBytesNoLock(rowSize + SlotDirectory::SIZE);
        this->_frame->MarkDirty();
    }

    bool PageView::UpdateRow(
        const ::Memory::IAllocator* allocator,
        const StorageTypes::SerializedRow& payload,
        const page_offset_t indexPosition
    ) const{
        if (this->IndexOutOfBounds(indexPosition))
            throw std::out_of_range("Page::UpdateRow: Index position is out of bounds.");

        auto* header = this->_frame->Header();

        const auto slot = this->GetSlotDirectory(indexPosition);
        const auto newSize = payload.Size();

        // In-place update: new data fits in existing slot
        if (newSize <= slot.DataSize()){
            std::memcpy(this->_frame->_data + slot.AbsoluteDataOffset(), payload.Data(), newSize);

            // Update slot to reflect new (possibly smaller) size
            const SlotDirectory updatedSlot(
                slot.Offset(), slot.DataOffset(),
                newSize, slot.KeySize(), SlotDirectory::SLOT_USED
            );
            this->UpdateSlotDirectory(updatedSlot, indexPosition);

            header->FragmentBytesNoLock(slot.DataSize() - newSize);
            this->_frame->MarkDirty();
            return true;
        }

        const auto requiredSpace = newSize + slot.KeySize();

        if (requiredSpace > header->TotalFreeBytesNoLock() + slot.Size())
            return false;


        //preserve key by copying it into memory
        auto* key = static_cast<object_t*>(allocator->AllocateRaw(slot.KeySize()));
        std::memcpy(key, this->_frame->_data + slot.Offset(), slot.KeySize());

        this->UpdateSlotDirectory(SlotDirectory::DeadSlot(), indexPosition);
        header->FragmentBytesNoLock(slot.Size());
        [[maybe_unused]] const bool fits = this->EnsureContiguousSpace(allocator, requiredSpace);
        assert(fits && "PageView::UpdateRow: space accounting is wrong");

        const auto offset = this->NewInsertOffset();
        std::memcpy(this->_frame->_data + offset, key, slot.KeySize());
        std::memcpy(this->_frame->_data + offset + slot.KeySize(), payload.Data(), newSize);

        const SlotDirectory updatedSlot(offset, slot.KeySize(), newSize, slot.KeySize(), SlotDirectory::SLOT_USED);
        this->UpdateSlotDirectory(updatedSlot, indexPosition);

        header->ConsumeBytesNoLock(requiredSpace);
        this->_frame->MarkDirty();
        return true;
    }

    void PageView::SetForwardPointer(
        const Int indexPosition,
        const StorageTypes::RID* rid
    ) const{
        auto slot = this->GetSlotDirectory(indexPosition);
        assert(slot.DataSize() >= ROW_ID_SIZE && "PageView::SetForwardPointer: row smaller than a RID");

        std::memcpy(this->_frame->_data + slot.AbsoluteDataOffset(), rid, ROW_ID_SIZE);

        this->_frame->Header()->FragmentBytesNoLock(slot.DataSize() - ROW_ID_SIZE);

        slot.SetDataSize(ROW_ID_SIZE);
        slot.SetFlag(SlotDirectory::SLOT_FORWARDED);
        this->UpdateSlotDirectory(slot, indexPosition);
        this->_frame->MarkDirty();
    }

    static_assert(
        sizeof(StorageTypes::RowHeader) >= ROW_ID_SIZE,
        "PageView::SetForwardPointer: Row header size is too small."
    );

    bool PageView::ResolveRID(
        const Snapshot& snapshot,
        const Int indexPosition,
        StorageTypes::RID* outRID
    ) const{
        const auto slot = this->GetSlotDirectory(indexPosition);
        if (slot.GetFlag() != SlotDirectory::SLOT_USED)
            return false;

        return this->RetrieveVisibleRow(snapshot, indexPosition, outRID);
    }

    MaterializedRow PageView::MaterializeRow(
        const ::Memory::IAllocator* allocator,
        const StorageTypes::Table* tablePtr,
        const Int indexPosition
    ) const{
        const auto slot = this->GetSlotDirectory(indexPosition);

        const auto& columns = tablePtr->Columns();
        const auto columnsSize = columns.size();

        MaterializedRow result(allocator);

        const auto dataOffset = slot.AbsoluteDataOffset();
        const auto entryTableOffset = dataOffset + sizeof(StorageTypes::RowHeader);
        const auto* rowDataPtr = this->_frame->_data + dataOffset;
        auto* __restrict__ entries = reinterpret_cast<const StorageTypes::RowEntry*>(this->_frame->_data + entryTableOffset);

        for (int i = 0;i < columnsSize; i++){
            const auto* __restrict__ entry = entries + i;
            switch (entry->Type()){
            case StorageTypes::RowEntry::INLINE:{
                auto value = Value::FromExternalStorage(
                    rowDataPtr + entries[i].Offset(),
                    entries[i].Size(),
                    columns[i]._type,
                    allocator
                );
                result.AddColumn(value);
                break;
            }
            case StorageTypes::RowEntry::NULLVAL:
                result.AddColumn(Value::Null());
                break;
            case StorageTypes::RowEntry::LOB:{
                DataTypes::LobReference reference{};
                std::memcpy(&reference, rowDataPtr + entry->Offset(), sizeof(reference));
                result.AddColumn(Value::FromLobReference(reference, columns[i]._type));
                break;
            }
            case StorageTypes::RowEntry::OVERFLOWVAL:
                throw std::logic_error("MaterializeRow: overflow columns are not supported yet");
            default:
                break;
            }

        }

        return result;
    }

    void PageView::MarkDead(const Int indexPosition) const{
        const auto slot = this->GetSlotDirectory(indexPosition);
        if (slot.Dead())
            return;

        this->_frame->Header()->FragmentBytesNoLock(slot.Size());
        this->UpdateSlotDirectory(SlotDirectory::DeadSlot(), indexPosition);
        this->_frame->MarkDirty();
    }

    void PageView::RemoveSlot(const Int indexPosition) const{
        auto* header = this->_frame->Header();
        header->FragmentBytesNoLock(this->GetSlotDirectory(indexPosition).Size());

        const auto moved = header->_size - indexPosition - 1;
        const auto lowest = PageView::SlotDirectoryOffSet(header->_size - 1);
        std::memmove(this->_frame->_data + lowest + SlotDirectory::SIZE, this->_frame->_data + lowest, moved * SlotDirectory::SIZE);

        header->_size--;
        header->_bytesLeft = static_cast<page_size_t>(header->_bytesLeft + SlotDirectory::SIZE);   // the freed entry borders the gap
        this->_frame->MarkDirty();
    }

    template <typename T>
    T PageView::GetColumnAt(
        const ::Memory::IAllocator* allocator,
        const Int index,
        const Int columnIndex,
        bool* outNull
    ) const{
        const auto slot = this->GetSlotDirectory(index);
        const auto* rowDataPtr = this->_frame->_data + slot.AbsoluteDataOffset();

        const auto rowEntry = *reinterpret_cast<const StorageTypes::RowEntry*>(
            rowDataPtr + sizeof(StorageTypes::RowHeader) + columnIndex * sizeof(StorageTypes::RowEntry)
        );

        *outNull = rowEntry.IsNull();
        if (*outNull)
            return T();

        //TODO follow overflowVal and LOB
        if constexpr (DataTypes::Primitive<T>){
            T out;
            std::memcpy(&out, rowDataPtr + rowEntry._offset, sizeof(T));
            return out;
        }
        else if constexpr (DataTypes::IsStringValue<T>)
            return DataTypes::StringValue::Create(
                allocator,
                reinterpret_cast<const char*>(rowDataPtr + rowEntry._offset),
                rowEntry.Size()
            );
        else if constexpr (DataTypes::IsDecimal<T>)
            return DataTypes::Decimal(rowDataPtr + rowEntry._offset, rowEntry.Size());
        else if constexpr (DataTypes::IsJson<T>)
            return DataTypes::JsonBinary(allocator, rowDataPtr + rowEntry._offset, rowEntry.Size());
        else
            static_assert(DataTypes::AlwaysFalse<T>, "Unsupported type");

        return T();
    }

    template bool PageView::GetColumnAt<bool>(const ::Memory::IAllocator*, Int, Int, bool*) const;
    template TinyInt PageView::GetColumnAt<TinyInt>(const ::Memory::IAllocator*, Int, Int, bool*) const;
    template SmallInt PageView::GetColumnAt<SmallInt>(const ::Memory::IAllocator*, Int, Int, bool*) const;
    template BigInt PageView::GetColumnAt<BigInt>(const ::Memory::IAllocator*, Int, Int, bool*) const;
    template Int PageView::GetColumnAt<Int>(const ::Memory::IAllocator*, Int, Int, bool*) const;
    template DataTypes::DateTime PageView::GetColumnAt<DataTypes::DateTime>(const ::Memory::IAllocator*, Int, Int, bool*) const;
    template DataTypes::Guid PageView::GetColumnAt<DataTypes::Guid>(const ::Memory::IAllocator*, Int, Int, bool*) const;
    template DataTypes::StringValue PageView::GetColumnAt<DataTypes::StringValue>(const ::Memory::IAllocator*, Int, Int, bool*) const;
    template DataTypes::Decimal PageView::GetColumnAt<DataTypes::Decimal>(const ::Memory::IAllocator*, Int, Int, bool*) const;
    template DataTypes::JsonBinary PageView::GetColumnAt<DataTypes::JsonBinary>(const ::Memory::IAllocator*, Int, Int, bool*) const;

    const object_t* PageView::GetColumnAt(const Int rowIndex, const Int columnIndex) const{
        const auto slot = this->GetSlotDirectory(rowIndex);
        const auto* frame = this->GetFrame();
        const auto* rowDataPtr = frame->_data + slot.AbsoluteDataOffset();

        const auto rowEntry = *reinterpret_cast<const StorageTypes::RowEntry*>(
            rowDataPtr + sizeof(StorageTypes::RowHeader) + columnIndex * sizeof(StorageTypes::RowEntry)
        );

        if (rowEntry.IsNull())
            return nullptr;

        return rowDataPtr + rowEntry._offset;
    }

    //TODO Obsolete, will be removed along with rowKernels
    const object_t* PageView::GetColumnAt(
        const Int rowIndex,
        const Int columnIndex,
        UnsignedSmallInt& outSize,
        bool* outNull
    ) const{
        const auto slot = this->GetSlotDirectory(rowIndex);
        const auto* frame = this->GetFrame();
        const auto* rowDataPtr = frame->_data + slot.AbsoluteDataOffset();

        const auto rowEntry = *reinterpret_cast<const StorageTypes::RowEntry*>(
            rowDataPtr + sizeof(StorageTypes::RowHeader) + columnIndex * sizeof(StorageTypes::RowEntry)
        );

        // TODO: OVERFLOWVAL / LOB entries store a pointer here, not the bytes themselves.
        //       Mirror the row path once it follows those pointers; for now read inline,
        //       consistent with the existing GetColumnAt / Value::FromExternalStorage path.
        *outNull = rowEntry.IsNull();
        if (*outNull)
            return nullptr;

        outSize = rowEntry.Size();
        return rowDataPtr + rowEntry._offset;
    }

    RawRowReference PageView::RawRowData(const Int indexPosition) const{
        const auto slot = this->GetSlotDirectory(indexPosition);
        return RawRowReference(this->_frame->_data + slot.AbsoluteDataOffset(), slot.DataSize());
    }

    bool PageView::IsRowVisible(const Snapshot& snapshot, const Int indexPosition) const{
        const auto slot = this->GetSlotDirectory(indexPosition);
        if (!slot.Used())
            return false;

        const auto* rowHeader = reinterpret_cast<const StorageTypes::RowHeader*>(
            this->_frame->_data + slot.AbsoluteDataOffset()
        );

        return rowHeader->IsVisibleForTransaction(snapshot);
    }

    bool PageView::RetrieveVisibleRow(
        const Snapshot& snapshot,
        const Int indexPosition,
        StorageTypes::RID* outRID
    ) const{
        const auto slot = this->GetSlotDirectory(indexPosition);
        if (!slot.Used())
            return false;

        const auto* rowHeader = reinterpret_cast<const StorageTypes::RowHeader*>(
            this->_frame->_data + slot.AbsoluteDataOffset()
        );

        if (rowHeader->IsVisibleForTransaction(snapshot)){
            *outRID = StorageTypes::RID(this->PageId(), indexPosition, StorageTypes::RID::Table);
            return true;
        }

        return VersionDatabase::Get().RetrieveVersionedRID(snapshot, rowHeader, outRID);
    }

    void PageView::Unpin(){
        if (this->IsValid())
            this->_frame->Unpin();
        this->_frame = nullptr;
    }
}
