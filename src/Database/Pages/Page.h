#pragma once
#include <vector>
#include <string>
#include "../Constants.h"
#include "../../AdditionalLibraries/DataTypes/Value/Value.h"

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
    typedef struct PageHeader
    {
        page_id_t pageId;
        page_size_t pageSize;
        page_size_t bytesLeft;
        PageType pageType;

        PageHeader();
        ~PageHeader();
        static page_size_t GetPageHeaderSize();
    } PageHeader;

    class Page
    {
    protected:
        bool isDirty;
        Constants::log_sequence_number_t logSequenceNumber;

        string filename;
        PageHeader header;

        vector<DatabaseEngine::StorageTypes::Row *> rows;
        void WritePageHeaderToFile(fstream *filePtr) const;
        static DatabaseEngine::StorageTypes::Row* ReadRowFromFile(
            const vector<char>& data,
            const DatabaseEngine::StorageTypes::Table *table,
            page_offset_t &offSet,
            const vector<DatabaseEngine::StorageTypes::Column*>& columns);

        static void WriteRowToFile(fstream* filePtr, DatabaseEngine::StorageTypes::Row* row);

    public:
        explicit Page(const page_id_t &pageId, const bool &isPageCreation = false);
        explicit Page();
        explicit Page(const PageHeader &pageHeader);
        virtual ~Page();

        void InsertRow(DatabaseEngine::StorageTypes::Row *row, int* indexPosition = nullptr);
        void InsertRow(DatabaseEngine::StorageTypes::Row *row, const int& indexPosition);

        virtual void GetPageDataFromFile(const vector<char> &data, const DatabaseEngine::StorageTypes::Table *table, page_offset_t &offSet, fstream *filePtr);
        virtual void WritePageToFile(fstream *filePtr);

        void Delete(vector<DatabaseEngine::StorageTypes::Row*>& deletedRows, const Expressions::Expression* expression);
        void Delete(const Expressions::Expression* expression);

        void SetFileName(const string &filename);
        void SetPageId(const page_id_t &pageId);
        virtual void UpdatePageSize();
        virtual void UpdateBytesLeft();
        void UpdateBytesLeft(const row_size_t& previousRowSize, const row_size_t& currentRowSize);

        [[nodiscard]] const string &GetFileName() const;
        [[nodiscard]] const page_id_t &GetPageId() const;
        [[nodiscard]] const bool &GetPageDirtyStatus() const;
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
    };
}