#pragma once
#include <vector>
#include <string>
#include "../Constants.h"
#include "../../AdditionalLibraries/AdditionalObjects/Field/Field.h"

class RowCondition;

using namespace std;
using namespace Constants;

namespace DatabaseEngine::StorageTypes
{
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
        vector<DatabaseEngine::StorageTypes::Row *> rows;

    protected:
        bool isDirty;
        string filename;
        PageHeader header;
        void WritePageHeaderToFile(fstream *filePtr);

    public:
        explicit Page(const page_id_t &pageId, const bool &isPageCreation = false);
        explicit Page();
        explicit Page(const PageHeader &pageHeader);
        virtual ~Page();

        void InsertRow(DatabaseEngine::StorageTypes::Row *row);
        void InsertRow(DatabaseEngine::StorageTypes::Row *row, int indexPosition);
        void DeleteRow(DatabaseEngine::StorageTypes::Row *row);
        void UpdateRows(const vector<DatabaseEngine::StorageTypes::Block> *updates, const vector<Field> *conditions);

        virtual void GetPageDataFromFile(const vector<char> &data, const DatabaseEngine::StorageTypes::Table *table, page_offset_t &offSet, fstream *filePtr);
        virtual void WritePageToFile(fstream *filePtr);

        void SetFileName(const string &filename);
        void SetPageId(const page_id_t &pageId);
        void UpdatePageSize();
        void UpdateBytesLeft();

        [[nodiscard]] const string &GetFileName() const;
        [[nodiscard]] const page_id_t &GetPageId() const;
        [[nodiscard]] const bool &GetPageDirtyStatus() const;
        [[nodiscard]] const page_size_t &GetBytesLeft() const;

        void GetRows(vector<DatabaseEngine::StorageTypes::Row> *copiedRows, const DatabaseEngine::StorageTypes::Table &table, const size_t &rowsToSelect, const vector<Field> *conditions = nullptr) const;
        void UpdateRows(const vector<DatabaseEngine::StorageTypes::Block*>* updates, const vector<Field> *conditions = nullptr);

        [[nodiscard]] page_size_t GetPageSize() const;
        [[nodiscard]] const PageType &GetPageType() const;
        [[nodiscard]] DatabaseEngine::StorageTypes::Row GetRowByIndex(const DatabaseEngine::StorageTypes::Table &table, const int &indexPosition) const;
        [[nodiscard]] vector<DatabaseEngine::StorageTypes::Row *> *GetDataRowsUnsafe();
        void SplitPageRowByBranchingFactor(Page *nextLeafPage, const int &branchingFactor, const DatabaseEngine::StorageTypes::Table &table);
    };
}