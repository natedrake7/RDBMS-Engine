#pragma once
#include <vector>
#include <string>
#include "../DatabaseConstants.h"
#include "../../../Systemic/include/Guards/ReadWriteMutex.h"
#include "../../../Systemic/include/Constants.h"
#include "../../Systemic/include/QueryResult.h"
#include "../../Systemic/include/Errors.h"
#include "../DataStorage/Row.h"

#include <atomic>

#include "../DataStorage/InsertPayload.h"


namespace Errors
{
    struct Error;
}

namespace Expressions {
    class Expression;
}

namespace DatabaseEngine::StorageTypes{
    class InsertPayload;
    class Column;
    class Block;
    class Row;
    class Table;
}

namespace Pages{
    struct SlotDirectory{
        private:
            UnsignedSmallInt flags_offset;
            UnsignedSmallInt size;

            static constexpr UnsignedSmallInt OFFSET_MASK = 0x3FFF; // lower 14 bits
            static constexpr UnsignedSmallInt FLAGS_MASK  = 0xC000; // upper 2 bits

        public:
            enum Flag : UnsignedTinyInt {
                SLOT_EMPTY      = 0,
                SLOT_USED       = 1,
                SLOT_FORWARDED  = 2,
                SLOT_DEAD       = 3
            };

            static constexpr UnsignedTinyInt Size = 4;

            SlotDirectory();
            SlotDirectory(
                UnsignedSmallInt offset,
                UnsignedSmallInt size,
                Flag flag
            );

            [[nodiscard]] UnsignedSmallInt GetOffset() const;
            void SetOffset(UnsignedSmallInt otherOffset);

            [[nodiscard]] UnsignedSmallInt GetSize() const;
            void SetSize(UnsignedSmallInt otherSize);

            [[nodiscard]] Flag GetFlag() const;
            void SetFlag(Flag otherFlag);

            [[nodiscard]] bool Empty() const;
            [[nodiscard]] bool Used() const;
            [[nodiscard]] bool ForwardPointer() const;
            [[nodiscard]] bool Dead() const;
            [[nodiscard]] bool Default() const;
    };

    struct SlotDirectoryDefragment{
        Int indexPosition;
        SlotDirectory slotDirectory;

        SlotDirectoryDefragment(
            const SlotDirectory slotDirectory,
            const Int indexPosition
        ){
            this->slotDirectory = slotDirectory;
            this->indexPosition = indexPosition;
        }

        static bool OrderAscendingByOffSet(const SlotDirectoryDefragment& lhs, const SlotDirectoryDefragment& rhs){
            return lhs.slotDirectory.GetOffset() < rhs.slotDirectory.GetOffset();
        }
    };

    struct PageHeader{
        page_id_t pageId;
        page_size_t size;
        page_size_t bytesLeft;
        Constants::PageType type;

        PageHeader();
        ~PageHeader();
    };

    struct Frame{
        bool isDirty;
        std::atomic<int> pinCount;
        std::atomic<Constants::PagePriority> priority;
        bool hasSecondChance;

        mutable MultiThreading::ReadWriteMutex latch;

        log_sequence_number_t logSequenceNumber;

        std::string filename;
        const DatabaseEngine::StorageTypes::Table* table;

        object_t* data;
    };

    class Page{
    protected:
        bool isDirty;
        std::atomic<int> pinCount;
        std::atomic<Constants::PagePriority> priority;
        bool hasSecondChance;

        mutable MultiThreading::ReadWriteMutex latch;

        log_sequence_number_t logSequenceNumber;

        std::string filename;
        const DatabaseEngine::StorageTypes::Table* table;

        //persisted to disk
        PageHeader header;
        object_t* data;

        void WritePageHeaderToDisk(std::fstream *filePtr) const;
        [[nodiscard]] page_offset_t NewInsertOffset()const;
        [[nodiscard]] Int SlotDirectoryOffSet(Int indexPosition) const;
        [[nodiscard]] Int SlotDirectoriesToMoveOffSet(Int indexPosition, Int slotToMove) const;

        [[nodiscard]] Int RawDataSize()const;

        void UpdateSlotDirectory(SlotDirectory slotDirectory, Int indexPosition) const;

        bool IndexOutOfBounds(Int indexPosition) const;

        void AdjustSlotDirectories(Int indexPosition, const page_offset_t& offset, Int slotSize) const;

        void SerializeRow(
            const DatabaseEngine::StorageTypes::RowHeader& rowHeader,
            const QueryResult& row,
            page_offset_t& offSet
        );

    public:
        explicit Page(
            page_id_t pageId,
            const DatabaseEngine::StorageTypes::Table* table,
            bool isPageCreation = false
        );
        explicit Page(
            page_id_t pageId,
            page_size_t size,
            const DatabaseEngine::StorageTypes::Table* table,
            bool isPageCreation = false
        );
        explicit Page();
        explicit Page(const PageHeader &pageHeader);
        Page(const PageHeader &pageHeader, page_size_t size);
        virtual ~Page();

        void DeleteRow(Int indexPosition) const;

        void InsertFirstRow(const DatabaseEngine::StorageTypes::InsertPayload& payload);
        Int InsertRow(const DatabaseEngine::StorageTypes::InsertPayload& payload);
        void InsertRow(const DatabaseEngine::StorageTypes::InsertPayload& payload, Int indexPosition);

        [[nodiscard]]
        bool UpdateRow(
            const DatabaseEngine::StorageTypes::InsertPayload& payload,
            const RowReference& rowPtr
        );
        void SetForwardPointer(
            Int indexPosition,
            const DataTypes::RowIdentifier& rowId
        ) const;

        virtual void ReadFromDisk(
            const std::vector<char> &buffer,
            const DatabaseEngine::StorageTypes::Table *otherTable,
            page_offset_t &offSet,
            std::fstream *filePtr
        );
        virtual void WriteToDisk(std::fstream *filePtr);

        // void Delete(vector<DatabaseEngine::StorageTypes::Row*>& deletedRows, const Expressions::Expression* expression);
        // void Delete(const Expressions::Expression* expression);
        void Delete(Int indexPosition);

        void SetFileName(const std::string &otherFilename);
        void SetPageId(page_id_t pageId);
        virtual void UpdatePageSize();
        virtual void UpdateBytesLeft();
        void UpdateBytesLeft(row_size_t previousRowSize, row_size_t currentRowSize);

        [[nodiscard]] const std::string &GetFileName() const;
        [[nodiscard]] page_id_t PageId() const;
        [[nodiscard]] bool IsDirty() const;
        [[nodiscard]] page_size_t BytesLeft() const;
        void SetDirty();

        void SetLogSequenceNumber(const log_sequence_number_t &lsn);
        [[nodiscard]] const log_sequence_number_t &GetLogSequenceNumber() const;

        [[nodiscard]] page_size_t GetPageSize() const;
        [[nodiscard]] PageType GetPageType() const;

        void Defragment();

        void IncreasePinCount();
        void DecreasePinCount();

        int GetPinCount() const;
        PagePriority GetPriority() const;

        bool HasSecondChance()const;
        void SetHasSecondChanceUnsafe(bool secondChance);

        void UniqueLock()const;
        void SharedLock()const;
        void UniqueUnlock()const;
        void SharedUnlock()const;
        void SetTable(const DatabaseEngine::StorageTypes::Table* otherTable);

        [[nodiscard]] MultiThreading::ReadWriteMutex& Latch() const;

        [[nodiscard]] object_t* GetData() const;
        SlotDirectory GetSlotDirectory(Int indexPosition) const;
        void InsertNewSlot(SlotDirectory slotDirectory) const;

        void DistributeFromPage(Page* donorPage, Int numberOfSlotsToMove, Int donorResizeVariant);
        void DistributeFromBeginningOfPage(Page* donorPage, Int numberOfSlotsToMove, Int donorResizeVariant);

        void DistributeSingleSlotFromPage(Page* donorPage, Int donorIndexPosition, Int donorResizeVariant);

        void Resize(Int size);
        void ResizeFromBeginning(Int size);
        inline QueryResult MaterializeRow(Int indexPosition, Int keySize) const;

        void InitializeRowReferenceCache(const RowReference* rowPtr, Int numberOfColumns) const;
        Value PartialMaterializeRow(const RowReference* rowPtr, column_index_t columnIndex) const;
        DatabaseEngine::StorageTypes::RowHeader PeekRowHeader(Int indexPosition, Int offSet)const;

        [[nodiscard]] RowReference PeekRow(Int indexPosition, Int offSet);

        [[nodiscard]] bool IsIndexPage()const;
    };

    struct RowReference{
        Page* pagePtr;
        Int indexPosition;
        Int keySize;

        mutable DatabaseEngine::StorageTypes::RowHeader header;

        mutable Dictionary<column_index_t, Value> cache;
        mutable std::vector<Int> sizes;
        mutable page_offset_t dataOffset;
        mutable bool isHeaderInitialized;

        RowReference();
        RowReference(Page* pagePtr, Int indexPosition, Int offset);

        RowReference(const RowReference& other);
        RowReference& operator=(const RowReference& other);

        RowReference(RowReference&& other) noexcept;
        RowReference& operator=(RowReference&& other) noexcept;

        ~RowReference();

        [[nodiscard]] QueryResult Materialize()const;
        [[nodiscard]] Value PartialMaterialize(column_index_t columnIndex)const;
    };
}