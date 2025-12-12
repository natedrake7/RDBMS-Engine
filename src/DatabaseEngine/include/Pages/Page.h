#pragma once
#include <vector>
#include <string>
#include "../Constants.h"
#include "../../../Systemic/include/Guards/ReadWriteMutex.h"
#include "../../../Systemic/include/Constants.h"

#include <atomic>

namespace Expressions {
    class Expression;
}

using namespace std;
using namespace Constants;

namespace DatabaseEngine::StorageTypes
{
    class Column;
    class Block;
    class Row;
    class Table;
}

namespace Pages
{
    struct RowVersionPointer {
        page_id_t pageId;
        page_offset_t offset;

        RowVersionPointer() {
            this->pageId = INVALID_PAGE_ID;
            this->offset = 0;
        }
    };

    struct PageHeader
    {
        page_id_t pageId;
        page_size_t pageSize;
        page_size_t bytesLeft;
        PageType pageType;

        PageHeader();
        ~PageHeader();
    };

    class Page
    {
    protected:

        bool isDirty;
        std::atomic<int> pinCount;
        std::atomic<Constants::PagePriority> priority;
        bool hasSecondChance;

        mutable MultiThreading::ReadWriteMutex latch;

        log_sequence_number_t logSequenceNumber;

        string filename;
        PageHeader header;

        vector<DatabaseEngine::StorageTypes::Row *> rows;
        void WritePageHeaderToDisk(fstream *filePtr) const;
        static DatabaseEngine::StorageTypes::Row* ReadRowFromDisk(
            const vector<char>& data,
            const DatabaseEngine::StorageTypes::Table *table,
            page_offset_t &offSet,
            const vector<DatabaseEngine::StorageTypes::Column*>& columns
        );

        static DatabaseEngine::StorageTypes::Row* ReadRowFromDisk(
            const vector<char>& data,
            page_offset_t &offSet
        );

        static void WriteRowToDisk(fstream* filePtr, const DatabaseEngine::StorageTypes::Row* row);

    public:
        explicit Page(const page_id_t &pageId, const bool &isPageCreation = false);
        explicit Page();
        explicit Page(const PageHeader &pageHeader);
        virtual ~Page();

        void InsertRow(DatabaseEngine::StorageTypes::Row *row, int* indexPosition = nullptr);
        void InsertRow(DatabaseEngine::StorageTypes::Row *row, const int& indexPosition);

        virtual void ReadFromDisk(const vector<char> &data, const DatabaseEngine::StorageTypes::Table *table, page_offset_t &offSet, fstream *filePtr);
        virtual void WriteToDisk(fstream *filePtr);

        // void Delete(vector<DatabaseEngine::StorageTypes::Row*>& deletedRows, const Expressions::Expression* expression);
        // void Delete(const Expressions::Expression* expression);
        void Delete(const int& indexPosition);

        void SetFileName(const string &filename);
        void SetPageId(const page_id_t &pageId);
        virtual void UpdatePageSize();
        virtual void UpdateBytesLeft();
        void UpdateBytesLeft(const row_size_t& previousRowSize, const row_size_t& currentRowSize);

        [[nodiscard]] const string &GetFileName() const;
        [[nodiscard]] const page_id_t &GetPageId() const;
        [[nodiscard]] const bool &IsDirty() const;
        [[nodiscard]] const page_size_t &GetBytesLeft() const;
        void SetDirty();

        void SetLogSequenceNumber(const log_sequence_number_t &logSequenceNumber);
        [[nodiscard]] const log_sequence_number_t &GetLogSequenceNumber() const;

        int GetRows(
            std::vector<const DatabaseEngine::StorageTypes::Row*> *result,
            const size_t &rowsToSelect,
            const int32_t& startingPosition = 0) const;

        [[nodiscard]] page_size_t GetPageSize() const;
        [[nodiscard]] const PageType &GetPageType() const;
        void GetRowByIndex(vector<DatabaseEngine::StorageTypes::Row>* rows, const DatabaseEngine::StorageTypes::Table &table, const int &indexPosition) const;
        [[nodiscard]] const DatabaseEngine::StorageTypes::Row* GetRow(const int& indexPosition)const;

        [[nodiscard]] vector<DatabaseEngine::StorageTypes::Row *> *GetDataRowsUnsafe();

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

        [[nodiscard]] MultiThreading::ReadWriteMutex& GetLatch() const;
    };
}