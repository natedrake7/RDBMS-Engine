#pragma once
#include "../DatabaseConstants.h"
#include "../../Systemic/include/QueryResult.h"
#include "../DataStorage/Row.h"
#include "Additional/RawRowReference.h"
#include "Additional/SlotDirectory.h"

namespace Memory
{
    class IAllocator;
}

namespace DataTypes{
    struct RowIdentifier;
}

namespace DatabaseEngine::StorageTypes{
    class InsertPayload;
}

namespace MultiThreading{
    class ReadWriteMutex;
}

namespace Pages{
    struct RowReference;
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
        Frame* framePtr;
        UnsignedSmallInt initialOffset;

        void SetFileName(const std::string &otherFilename) const;
        void SetPageId(page_id_t pageId) const;

        [[nodiscard]] page_offset_t NewInsertOffset() const;
        [[nodiscard]] static Int SlotDirectoryOffSet(Int indexPosition);
        [[nodiscard]] static Int SlotDirectoriesToMoveOffSet(Int indexPosition, Int slotToMove);
        void UpdateSlotDirectory(SlotDirectory slotDirectory, Int indexPosition)const;

        [[nodiscard]] bool IndexOutOfBounds(Int indexPosition) const;
        void AdjustSlotDirectories(Int indexPosition, const page_offset_t& offset, Int slotSize) const;

        [[nodiscard]] Int RawDataSize()const;

        void InsertFirstRow(const DatabaseEngine::StorageTypes::InsertPayload& payload) const;

        [[nodiscard]] bool IsIndexPage()const;

    public:
        PageView();
        explicit PageView(Frame* framePtr);

        PageView& operator=(PageView&& other) noexcept;
        PageView(PageView&& other) noexcept;

        virtual ~PageView();

        [[nodiscard]] PageHeader* GetHeader()const;

        [[nodiscard]] DatabaseEngine::StorageTypes::RowHeader PeekRowHeader(Int indexPosition, Int offSet)const;
        [[nodiscard]] RowReference PeekRow(
            const ::Memory::IAllocator* allocator,
            Int indexPosition,
            Int offSet
        ) const;

        [[nodiscard]] SlotDirectory GetSlotDirectory(Int indexPosition) const;
        void InsertNewSlot(SlotDirectory slotDirectory) const;

        void Defragment() const;
        void Resize(Int size) const;

        void DistributeFromPage(const PageView* donorPage, Int numberOfSlotsToMove, Int donorResizeVariant) const;
        void DistributeFromBeginningOfPage(const PageView* donorPage, Int numberOfSlotsToMove, Int donorResizeVariant) const;

        void DistributeSingleSlotFromPage(PageView* donorPage, Int donorIndexPosition, Int donorResizeVariant);

        [[nodiscard]] Int InsertRow(const DatabaseEngine::StorageTypes::InsertPayload& payload) const;
        void InsertRow(const DatabaseEngine::StorageTypes::InsertPayload& payload, Int indexPosition) const;

        [[nodiscard]]
        bool UpdateRow(
            const DatabaseEngine::StorageTypes::InsertPayload& payload,
            const RowReference& rowPtr
        ) const;
        void SetForwardPointer(
            Int indexPosition,
            const DataTypes::RowIdentifier& rowId
        ) const;

        void Delete(Int indexPosition) const;

        [[nodiscard]] page_id_t PageId()const;
        [[nodiscard]] page_size_t PageSize()const;
        [[nodiscard]] page_size_t BytesLeft()const;

        [[nodiscard]] object_t* GetData() const;

        [[nodiscard]] Frame* GetFrame()const;

        [[nodiscard]] MultiThreading::ReadWriteMutex& Latch()const;

        void InitializeRowReferenceCache(const RowReference* rowPtr, Int numberOfColumns)const;
        [[nodiscard]] QueryResult MaterializeRow(
            const Memory::IAllocator* allocator,
            Int indexPosition,
            Int keySize
        ) const;
        Value PartialMaterializeRow(const Memory::IAllocator* allocator, const RowReference* rowPtr, column_index_t columnIndex) const;

        [[nodiscard]] RawRowReference RowRawData(Int indexPosition, Int offSet) const;

        [[nodiscard]] bool IsValid()const;

        [[nodiscard]] Constants::PageType GetPageType() const;

        void IncreasePinCount() const;
        void DecreasePinCount() const;
    };
}
