#pragma once
#include "Page.h"

namespace Pages{
    class PageView final {
        Frame* framePtr;
        PageHeader* headerPtr;

    protected:
        void SetFileName(const std::string &otherFilename) const;
        void SetPageId(page_id_t pageId) const;

        [[nodiscard]] page_offset_t NewInsertOffset() const;
        [[nodiscard]] Int SlotDirectoryOffSet(Int indexPosition) const;
        [[nodiscard]] Int SlotDirectoriesToMoveOffSet(Int indexPosition, Int slotToMove) const;
        void UpdateSlotDirectory(SlotDirectory slotDirectory, Int indexPosition)const;

        bool IndexOutOfBounds(Int indexPosition) const;
        void AdjustSlotDirectories(Int indexPosition, const page_offset_t& offset, Int slotSize) const;

        [[nodiscard]] Int RawDataSize()const;

        void InsertFirstRow(const DatabaseEngine::StorageTypes::InsertPayload& payload) const;
        Int InsertRow(const DatabaseEngine::StorageTypes::InsertPayload& payload) const;
        void InsertRow(const DatabaseEngine::StorageTypes::InsertPayload& payload, Int indexPosition) const;

        [[nodiscard]]
        bool UpdateRow(
            const DatabaseEngine::StorageTypes::InsertPayload& payload,
            const RowReference& rowPtr
        );
        void SetForwardPointer(
            Int indexPosition,
            const DataTypes::RowIdentifier& rowId
        ) const;

        [[nodiscard]] bool IsIndexPage()const;

    public:
        explicit PageView(Frame* framePtr);
        ~PageView();

        PageHeader* GetHeader()const;

        DatabaseEngine::StorageTypes::RowHeader PeekRowHeader(Int indexPosition, Int offSet)const;
        [[nodiscard]] RowReference PeekRow(Int indexPosition, Int offSet);

        SlotDirectory GetSlotDirectory(Int indexPosition) const;
        void InsertNewSlot(SlotDirectory slotDirectory) const;

        void Defragment() const;
        void Resize(Int size) const;
    };
}
