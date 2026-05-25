#pragma once
#include "../DatabaseConstants.h"
#include "../../Systemic/include/QueryResult.h"
#include "../DataStorage/Row.h"
#include "Additional/RawRowReference.h"
#include "Additional/SlotDirectory.h"

namespace Expressions
{
    class Expression;
    struct EvaluationContext;
}

namespace Memory{
    class IAllocator;
}

namespace DataTypes{
    struct RowIdentifier;
}

namespace CoreEngine::StorageTypes{
    class InsertPayload;
}

namespace MultiThreading{
    class Mutex;
}

namespace Pages{
    struct Frame;

    struct PageHeader{
        page_id_t pageId;
        page_size_t size;
        page_size_t bytesLeft;

        PageHeader();
        ~PageHeader();
    };

    class PageView{
    protected:
        Frame* _frame;
        UnsignedSmallInt initialOffset;

        void SetFileName(const DataTypes::StringView& otherFilename) const;
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

        void InsertFirstRow(const CoreEngine::StorageTypes::InsertPayload& payload) const;

        [[nodiscard]] bool IsIndexPage()const;

    public:
        PageView();
        explicit PageView(Frame* framePtr);

        PageView(const PageView& other) = delete;
        PageView& operator=(const PageView& other) = delete;

        PageView& operator=(PageView&& other) noexcept;
        PageView(PageView&& other) noexcept;

        virtual ~PageView();

        [[nodiscard]] PageHeader* GetHeader()const;

        [[nodiscard]] CoreEngine::StorageTypes::RowHeader PeekRowHeader(Int indexPosition)const;

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

        [[nodiscard]] Int InsertRow(const CoreEngine::StorageTypes::InsertPayload& payload) const;
        void InsertRow(const CoreEngine::StorageTypes::InsertPayload& payload, Int indexPosition) const;

        [[nodiscard]]
        bool UpdateRow(
            const ::Memory::IAllocator* allocator,
            const CoreEngine::StorageTypes::InsertPayload& payload,
            page_offset_t indexPosition
        ) const;
        void SetForwardPointer(
            Int indexPosition,
            const CoreEngine::StorageTypes::RID& rowId
        ) const;

        QueryResult MaterializeRow(
            const Memory::IAllocator* allocator,
            Int indexPosition
        ) const;

        void Delete(Int indexPosition) const;

        [[nodiscard]] page_id_t PageId()const;
        [[nodiscard]] page_size_t PageSize()const;
        [[nodiscard]] page_size_t BytesLeft()const;

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

        static Value GetColumnAt(
            const ::Memory::IAllocator* allocator,
            const PageView* page,
            const CoreEngine::StorageTypes::RID* row,
            Int columnIndex
        );

        const object_t* GetColumnAt(
            const CoreEngine::StorageTypes::RID* row,
            Int columnIndex
        ) const;

        [[nodiscard]] RawRowReference RawRowData(Int indexPosition) const;
    };
}
