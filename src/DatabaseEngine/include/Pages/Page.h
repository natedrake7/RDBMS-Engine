#pragma once
#include <vector>
#include <string>
#include "../DatabaseConstants.h"
#include "../../../Systemic/include/DataTypes/Pointer.h"
#include "../../../Systemic/include/Guards/ReadWriteMutex.h"
#include "../../../Systemic/include/Constants.h"

#include <atomic>

namespace Expressions {
    class Expression;
}

namespace DatabaseEngine::StorageTypes{
    class Column;
    class Block;
    class Row;
    class Table;
}

namespace Pages{
    struct SlotDirectory{
        UnsignedSmallInt offset;
        UnsignedSmallInt size;
        // UnsignedTinyInt flags : 1;

        static constexpr UnsignedTinyInt FLAG_IS_VALID = 0X00;
        static constexpr UnsignedTinyInt FLAG_IS_DELETED = 0X01;
        static constexpr UnsignedTinyInt Size = 4;

        SlotDirectory(){
            this->offset = 0;
            this->size = 0;
            // this->flags = FLAG_IS_VALID;
        }
         SlotDirectory(
            const UnsignedSmallInt& offset,
            const UnsignedSmallInt& size,
            const UnsignedTinyInt& flags = FLAG_IS_VALID
        ){
            this->offset = offset;
            this->size = size;
            // this->flags = flags;
        }

        [[nodiscard]] bool IsDefault() const{
            return this->offset == 0 && this->size == 0;
        }


    };

    struct SlotDirectoryDefragment{
        Int indexPosition;
        SlotDirectory slotDirectory;

        SlotDirectoryDefragment(
            const SlotDirectory& slotDirectory,
            const Int& indexPosition
        ){
            this->slotDirectory = slotDirectory;
            this->indexPosition = indexPosition;
        }

        static bool OrderAscendingByOffSet(const SlotDirectoryDefragment& lhs, const SlotDirectoryDefragment& rhs){
            return lhs.slotDirectory.offset < rhs.slotDirectory.offset;
        }
    };

    struct RowVersionPointer {
        page_id_t pageId;
        page_offset_t offset;

        RowVersionPointer() {
            this->pageId = INVALID_PAGE_ID;
            this->offset = 0;
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

    class Page{
    protected:
        bool isDirty;
        std::atomic<int> pinCount;
        std::atomic<Constants::PagePriority> priority;
        bool hasSecondChance;

        mutable MultiThreading::ReadWriteMutex latch;

        log_sequence_number_t logSequenceNumber;

        std::string filename;

        //persisted to disk
        PageHeader header;
        object_t* data;

        void WritePageHeaderToDisk(std::fstream *filePtr) const;
        [[nodiscard]] page_offset_t NewRowOffset()const;
        [[nodiscard]] Int SlotDirectoryOffSet(const Int& indexPosition) const;
        [[nodiscard]] Int SlotDirectoriesToMoveOffSet(const Int& indexPosition, const Int& slotToMove) const;

        [[nodiscard]] Int RawDataSize()const;

        static void WriteRowToDisk(std::fstream* filePtr, const Pointer<DatabaseEngine::StorageTypes::Row>& row);

        SlotDirectory GetSlotDirectory(const int& indexPosition) const;
        DatabaseEngine::StorageTypes::Row MaterializeRow(
            const DatabaseEngine::StorageTypes::Table* table,
            const Int& indexId
        ) const;
        void UpdateSlotDirectory(const SlotDirectory& slotDirectory, const int& indexPosition) const;

        void InsertNewSlot(const SlotDirectory& slotDirectory) const;

        bool IndexOutOfBounds(const int& indexPosition) const;

        void AdjustSlotDirectories(const int& indexPosition, const page_offset_t& offset, const int& slotSize) const;

    public:
        explicit Page(const page_id_t &pageId, const bool &isPageCreation = false);
        explicit Page(const page_id_t &pageId, const page_size_t& size, const bool &isPageCreation = false);
        explicit Page();
        explicit Page(const PageHeader &pageHeader);
        Page(const PageHeader &pageHeader, const page_size_t& size);
        virtual ~Page();

        void InsertFirstRow(DatabaseEngine::StorageTypes::Row*& row);
        Int InsertRow(DatabaseEngine::StorageTypes::Row*& row);
        void InsertRow(DatabaseEngine::StorageTypes::Row*& row, const int& indexPosition);

        virtual void UpdateRow(DatabaseEngine::StorageTypes::Row*& row, const int& indexPosition);

        virtual void ReadFromDisk(
            const std::vector<char> &buffer,
            const DatabaseEngine::StorageTypes::Table *table,
            page_offset_t &offSet,
            std::fstream *filePtr
        );
        virtual void WriteToDisk(std::fstream *filePtr);

        // void Delete(vector<DatabaseEngine::StorageTypes::Row*>& deletedRows, const Expressions::Expression* expression);
        // void Delete(const Expressions::Expression* expression);
        void Delete(const int& indexPosition);

        void SetFileName(const std::string &otherFilename);
        void SetPageId(const page_id_t &pageId);
        virtual void UpdatePageSize();
        virtual void UpdateBytesLeft();
        void UpdateBytesLeft(const row_size_t& previousRowSize, const row_size_t& currentRowSize);

        [[nodiscard]] const std::string &GetFileName() const;
        [[nodiscard]] const page_id_t &GetPageId() const;
        [[nodiscard]] const bool &IsDirty() const;
        [[nodiscard]] const page_size_t &GetBytesLeft() const;
        void SetDirty();

        void SetLogSequenceNumber(const log_sequence_number_t &lsn);
        [[nodiscard]] const log_sequence_number_t &GetLogSequenceNumber() const;

        [[nodiscard]] page_size_t GetPageSize() const;
        [[nodiscard]] const Constants::PageType &GetPageType() const;
        [[nodiscard]] DatabaseEngine::StorageTypes::Row GetRow(const DatabaseEngine::StorageTypes::Table* table, const int& indexPosition)const;

        void Defragment() const;

        void IncreasePinCount();
        void DecreasePinCount();

        int GetPinCount() const;
        Constants::PagePriority GetPriority() const;

        bool HasSecondChance()const;
        void SetHasSecondChanceUnsafe(const bool &secondChance);

        void UniqueLock()const;
        void SharedLock()const;
        void UniqueUnlock()const;
        void SharedUnlock()const;

        [[nodiscard]] MultiThreading::ReadWriteMutex& Latch() const;
    };
}