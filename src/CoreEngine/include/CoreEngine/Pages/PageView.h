#pragma once
#include <cassert>

#include <CoreEngine/BufferPool/StorageLayout.h>
#include <Systemic/MaterializedRow.h>
#include <CoreEngine/DataStorage/Row/Row.h>
#include <CoreEngine/Pages/Additional/Frame.h>
#include <CoreEngine/Pages/Additional/RawRowReference.h>
#include <CoreEngine/Pages/Additional/SlotDirectory.h>

#include <CoreEngine/Pages/PageLayout.h>

namespace CoreEngine::StorageTypes{
    class Table;
    class SerializedRow;
}

namespace Memory{
    class IAllocator;
}

namespace MultiThreading{
    class Mutex;
}

namespace CoreEngine::Pages{
    struct Frame;

    struct PageHeader{
        log_sequence_number_t _lsn;
        UnsignedInt _checkSum;

        page_id_t _pageId;
        page_size_t _size: 12;
        page_size_t _type: 4;
        page_size_t _bytesLeft;

        page_size_t _fragmentedBytes;// 2: bytes in holes left by deletes/shrinks; Defragment reclaims them
        page_size_t _flags;// 2: reserved

        PageHeader();

        [[nodiscard]] inline PageLayout::PageType Type() const{
            return static_cast<PageLayout::PageType>(this->_type);
        }

        inline void SetType(PageLayout::PageType pageType){
            this->_type = static_cast<page_size_t>(pageType);
        }

        // Usable bytes of a page of this type. Read from the header, not the view type: the shared PageView
        // functions (InsertRow, Resize, DistributeFromPage, ...) also run on index pages.
        [[nodiscard]] page_size_t Capacity() const{
            return static_cast<PageLayout::PageType>(this->_type) == PageLayout::PageType::INDEXED
                ? StorageLayout::INDEX_PAGE_DEFAULT_SIZE
                : StorageLayout::PAGE_SIZE_WITHOUT_HEADER;
        }

        // Every change to bytesLeft goes through these two: page_size_t is unsigned, so an overdraw would
        // silently wrap around and make a full page look empty
        void ConsumeBytesNoLock(const UnsignedInt bytes){
            assert(bytes <= this->_bytesLeft && "PageHeader::ConsumeBytes: not enough free space in the page");
            this->_bytesLeft = static_cast<page_size_t>(this->_bytesLeft - bytes);
        }

        void FragmentBytesNoLock(const UnsignedInt bytes){
            this->_fragmentedBytes = static_cast<page_size_t>(this->_fragmentedBytes + bytes);
        }

        [[nodiscard]] UnsignedInt TotalFreeBytesNoLock() const{
            return this->_bytesLeft + this->_fragmentedBytes;
        }

        void IncrementSizeNoLock(const page_size_t size = 1){
            this->_size += size;
        }
    };
    static_assert(sizeof(PageHeader) == StorageLayout::PAGE_HEADER_SIZE);

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

        void InsertFirstRow(const StorageTypes::SerializedRow& payload) const;

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
        static constexpr auto TYPE = PageLayout::PageType::DATA;
        static constexpr bool IS_SYSTEM = false;

        static void Initialize(const Frame* frame){
            frame->Header()->_bytesLeft = StorageLayout::PAGE_SIZE_WITHOUT_HEADER;
        }

        PageView();
        explicit PageView(Frame* framePtr);

        PageView(const PageView& other) = delete;
        PageView& operator=(const PageView& other) = delete;

        PageView& operator=(PageView&& other) noexcept;
        PageView(PageView&& other) noexcept;

        ~PageView();

        [[nodiscard]] PageHeader* GetHeader()const;

        [[nodiscard]] const StorageTypes::RowHeader* PeekRowHeader(Int indexPosition)const;

        [[nodiscard]] SlotDirectory GetSlotDirectory(Int indexPosition) const;
        void InsertNewSlot(SlotDirectory slotDirectory) const;

        void Defragment(const ::Memory::IAllocator* allocator) const;
        void Truncate(Int size) const;

        void DistributeFromPage(
            const ::Memory::IAllocator* allocator,
            const PageView* donorPage,
            Int slotToMoveFrom,
            Int donorNewSize
        ) const;

        [[nodiscard]] bool EnsureContiguousSpace(const ::Memory::IAllocator* allocator, UnsignedInt bytes)const;

        [[nodiscard]] Int InsertRow(const StorageTypes::SerializedRow& payload) const;
        void InsertRow(const StorageTypes::SerializedRow& payload, Int indexPosition) const;

        [[nodiscard]]
        bool UpdateRow(
            const ::Memory::IAllocator* allocator,
            const StorageTypes::SerializedRow& payload,
            page_offset_t indexPosition
        ) const;
        void SetForwardPointer(
            Int indexPosition,
            const StorageTypes::RID* rid
        ) const;

        [[nodiscard]]
        bool ResolveRID(
            const Snapshot& snapshot,
            Int indexPosition,
            StorageTypes::RID* outRID
        ) const;

        MaterializedRow MaterializeRow(
            const ::Memory::IAllocator* allocator,
            const StorageTypes::Table* tablePtr,
            Int indexPosition
        ) const;

        void MarkDead(Int indexPosition)const;
        void RemoveSlot(Int indexPosition) const;

        [[nodiscard]] page_id_t PageId()const{
            return this->_frame->Header()->_pageId;
        }
        [[nodiscard]] page_size_t PageSize()const{
            return this->_frame->Header()->_size;
        }
        [[nodiscard]] page_size_t BytesLeft()const{
            return this->_frame->Header()->_bytesLeft;
        }
        [[nodiscard]] bool IsEmpty() const{
            return this->_frame->Header()->_size == 0;
        }
        [[nodiscard]] object_t* GetData() const{
            return this->_frame->_data;
        }
        [[nodiscard]] Frame* GetFrame() const{
            return this->_frame;
        }
        [[nodiscard]] MultiThreading::Mutex& Latch() const{
            return this->_frame->_latch;
        }
        [[nodiscard]] bool IsValid() const{
            return this->_frame != nullptr;
        }
        [[nodiscard]] PageLayout::PageType GetPageType() const{
            return this->_frame->Header()->Type();
        }

        template<typename T>
        [[nodiscard]] T GetColumnAt(
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

        [[nodiscard]] bool IsRowVisible(const Snapshot& snapshot, Int indexPosition) const;

        [[nodiscard]]
        bool RetrieveVisibleRow(
            const Snapshot& snapshot,
            Int indexPosition,
            StorageTypes::RID* outRID
        )const;

        void Unpin();
    };
}
