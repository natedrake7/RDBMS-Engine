#pragma once
#include <cassert>

#include <CoreEngine/DatabaseConstants.h>
#include <CoreEngine/BufferPool/StorageLayout.h>
#include <Systemic/MaterializedRow.h>
#include <CoreEngine/DataStorage/Row/Row.h>
#include <CoreEngine/Pages/Additional/Frame.h>
#include <CoreEngine/Pages/Additional/RawRowReference.h>
#include <CoreEngine/Pages/Additional/SlotDirectory.h>

namespace Memory{
    class IAllocator;
}

namespace CoreEngine::StorageTypes{
    class Table;
    class SerializedRow;
}

namespace MultiThreading{
    class Mutex;
}

namespace Pages{
    struct Frame;

    struct PageHeader{
        page_id_t pageId;
        page_size_t size: 12;
        page_size_t type: 4;
        page_size_t bytesLeft;

        PageHeader();

        [[nodiscard]] Constants::PageType Type() const;
        void SetType(Constants::PageType pageType);

        // Usable bytes of a page of this type. Read from the header, not the view type: the shared PageView
        // functions (InsertRow, Resize, DistributeFromPage, ...) also run on index pages.
        [[nodiscard]] page_size_t Capacity() const{
            return static_cast<Constants::PageType>(this->type) == Constants::PageType::INDEXED
                ? CoreEngine::StorageLayout::INDEX_PAGE_DEFAULT_SIZE
                : CoreEngine::StorageLayout::PAGE_SIZE_WITHOUT_HEADER;
        }

        // Every change to bytesLeft goes through these two: page_size_t is unsigned, so an overdraw would
        // silently wrap around and make a full page look empty
        void ConsumeBytes(const UnsignedInt bytes){
            assert(bytes <= this->bytesLeft && "PageHeader::ConsumeBytes: not enough free space in the page");
            this->bytesLeft = static_cast<page_size_t>(this->bytesLeft - bytes);
        }

        void ReleaseBytes(const UnsignedInt bytes){
            assert(this->bytesLeft + bytes <= this->Capacity() && "PageHeader::ReleaseBytes: more free space than the page holds");
            this->bytesLeft = static_cast<page_size_t>(this->bytesLeft + bytes);
        }
    };
    static_assert(sizeof(PageHeader) == CoreEngine::StorageLayout::PAGE_HEADER_SIZE);

    class PageView{
    protected:
        Frame* _frame;
        UnsignedSmallInt initialOffset;

        void SetPageId(page_id_t pageId) const;

        [[nodiscard]] page_offset_t NewInsertOffset() const;
        [[nodiscard]] static Int SlotDirectoryOffSet(Int indexPosition);
        [[nodiscard]] static Int SlotDirectoriesToMoveOffSet(Int indexPosition, Int slotToMove);
        void UpdateSlotDirectory(SlotDirectory slotDirectory, Int indexPosition)const;

        [[nodiscard]] bool IndexOutOfBounds(Int indexPosition) const;
        void AdjustSlotDirectories(
            Int indexPosition,
            page_offset_t offset,
            row_size_t rowSize,
            key_size_t keySize
        ) const;

        [[nodiscard]] Int RawDataSize()const;

        void InsertFirstRow(const CoreEngine::StorageTypes::SerializedRow& payload) const;

        [[nodiscard]] bool IsIndexPage()const;

        [[nodiscard]] size_t GetByteIndex(const extent_id_t extentId) const noexcept{
            return this->initialOffset + PackedByte::WordIndex(extentId);
        }

        [[nodiscard]] bool GetBit(const std::size_t bitIndex) const noexcept {
            return PackedByte::GetBit(
                this->_frame->_data[this->GetByteIndex(bitIndex)],
                PackedByte::BitIndex(bitIndex)
            );
        }

        void SetBit(const std::size_t bitIndex) const noexcept {
            PackedByte::SetBit<true>(
                &this->_frame->_data[this->GetByteIndex(bitIndex)],
                PackedByte::BitIndex(bitIndex)
            );
        }

        void ClearBit(const std::size_t bitIndex) const noexcept {
            PackedByte::SetBit<false>(
                &this->_frame->_data[this->GetByteIndex(bitIndex)],
                PackedByte::BitIndex(bitIndex)
            );
        }

    public:
        PageView();
        explicit PageView(Frame* framePtr);

        PageView(const PageView& other) = delete;
        PageView& operator=(const PageView& other) = delete;

        PageView& operator=(PageView&& other) noexcept;
        PageView(PageView&& other) noexcept;

        ~PageView();

        [[nodiscard]] PageHeader* GetHeader()const;

        [[nodiscard]] const CoreEngine::StorageTypes::RowHeader* PeekRowHeader(Int indexPosition)const;

        [[nodiscard]] SlotDirectory GetSlotDirectory(Int indexPosition) const;
        void InsertNewSlot(SlotDirectory slotDirectory) const;

        void Defragment(const ::Memory::IAllocator* allocator) const;
        void Resize(Int size) const;

        void DistributeFromPage(
            const ::Memory::IAllocator* allocator,
            const PageView* donorPage,
            Int slotToMoveFrom,
            Int donorNewSize
        ) const;
        void DistributeFromBeginningOfPage(
            const ::Memory::IAllocator* allocator,
            const PageView* donorPage,
            Int numberOfSlotsToMove,
            Int donorResizeVariant
        ) const;

        [[nodiscard]] Int InsertRow(const CoreEngine::StorageTypes::SerializedRow& payload) const;
        void InsertRow(const CoreEngine::StorageTypes::SerializedRow& payload, Int indexPosition) const;

        [[nodiscard]]
        bool UpdateRow(
            const ::Memory::IAllocator* allocator,
            const CoreEngine::StorageTypes::SerializedRow& payload,
            page_offset_t indexPosition
        ) const;
        void SetForwardPointer(
            Int indexPosition,
            const CoreEngine::StorageTypes::RID* rid
        ) const;

        [[nodiscard]]
        bool ResolveRID(
            const CoreEngine::Snapshot& snapshot,
            Int indexPosition,
            CoreEngine::StorageTypes::RID* outRID
        ) const;

        MaterializedRow MaterializeRow(
            const Memory::IAllocator* allocator,
            const CoreEngine::StorageTypes::Table* tablePtr,
            Int indexPosition
        ) const;

        void Delete(Int indexPosition) const;

        [[nodiscard]] page_id_t PageId()const;
        [[nodiscard]] page_size_t PageSize()const;
        [[nodiscard]] page_size_t BytesLeft()const;

        [[nodiscard]] bool IsEmpty()const;

        [[nodiscard]] object_t* GetData() const;

        [[nodiscard]] Frame* GetFrame()const;

        [[nodiscard]] MultiThreading::Mutex& Latch()const;

        [[nodiscard]] bool IsValid()const;

        [[nodiscard]] Constants::PageType GetPageType() const;

        template<typename T>
        [[nodiscard]] T GetColumnAt(
            const ::Memory::IAllocator* allocator,
            Int index,
            Int columnIndex,
            bool* outNull
        )const;

        DataTypes::String GetStringColumnAt(
            const ::Memory::IAllocator* allocator,
            Int index,
            Int columnIndex,
            bool* outNull
        )const;

        [[nodiscard]] const object_t* GetColumnAt(Int rowIndex, Int columnIndex) const;

        // Same as the above, but also reports the stored byte length (rowEntry.Size()).
        // Required for variable-length columns (Decimal/String/Json). Returns nullptr
        // and outSize == 0 when the column value is NULL.
        const object_t* GetColumnAt(
            Int rowIndex,
            Int columnIndex,
            UnsignedSmallInt& outSize,
            bool* outNull
        ) const;

        [[nodiscard]] RawRowReference RawRowData(Int indexPosition) const;

        [[nodiscard]] bool IsRowVisible(const CoreEngine::Snapshot& snapshot, Int indexPosition) const;

        [[nodiscard]]
        bool RetrieveVisibleRow(
            const CoreEngine::Snapshot& snapshot,
            Int indexPosition,
            CoreEngine::StorageTypes::RID* outRID
        )const;

        void Unpin();
    };
}
