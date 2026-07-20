#pragma once
#include "../DatabaseConstants.h"
#include "../../Systemic/include/QueryResult.h"
#include "../DataStorage/Row.h"
#include "Additional/Frame.h"
#include "Additional/RawRowReference.h"
#include "Additional/SlotDirectory.h"

namespace Memory{
    class IAllocator;
}

namespace CoreEngine::StorageTypes{
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
    };
    static_assert(sizeof(PageHeader) == Constants::PAGE_HEADER_SIZE);

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
            return this->initialOffset + (extentId >> 3);
        }

        [[nodiscard]] bool GetBit(const std::size_t bitIndex) const noexcept {
            return PackedByte::GetBit(
                this->_frame->_data[this->GetByteIndex(bitIndex)],
                static_cast<UnsignedTinyInt>(bitIndex & 7u)
            );
        }

        void SetBit(const std::size_t bitIndex) const noexcept {
            PackedByte::SetBit<true>(
                &this->_frame->_data[this->GetByteIndex(bitIndex)],
                static_cast<UnsignedTinyInt>(bitIndex & 7u)
            );
        }

        void ClearBit(const std::size_t bitIndex) const noexcept {
            PackedByte::SetBit<false>(
                &this->_frame->_data[this->GetByteIndex(bitIndex)],
                static_cast<UnsignedTinyInt>(bitIndex & 7u)
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

        QueryResult MaterializeRow(
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

        static bool Filter(
            const Frame* frame,
            const CoreEngine::StorageTypes::RID* rowId,
            Int columnIndex
        );

        Value GetColumnAt(
            const ::Memory::IAllocator* allocator,
            const CoreEngine::StorageTypes::RID* row,
            const CoreEngine::StorageTypes::Table* table,
            Int columnIndex
        ) const;

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
    };
}
