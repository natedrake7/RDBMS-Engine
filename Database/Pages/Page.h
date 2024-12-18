#pragma once
#include <vector>
#include <string>
#include "../Constants.h"

class RowCondition;

using namespace std;
using namespace Constants;

namespace DatabaseEngine::StorageTypes {
    class Block;
    class Row;
    class Table;
}

namespace Pages {
    typedef struct PageHeader{
        page_id_t pageId;
        page_size_t pageSize;
        page_size_t bytesLeft;
        PageType pageType;

        PageHeader();
        ~PageHeader();
        static page_size_t GetPageHeaderSize();
    }PageHeader;

    class Page {
        vector<DatabaseEngine::StorageTypes::Row*> rows;

    protected:
        bool isDirty;
        string filename;
        PageHeader header;
        void WritePageHeaderToFile(fstream* filePtr);

    public:
        explicit Page(const page_id_t& pageId, const bool& isPageCreation = false);
        explicit Page();
        explicit Page(const PageHeader& pageHeader);
        virtual ~Page();
        void InsertRow(DatabaseEngine::StorageTypes::Row* row);
        void DeleteRow(DatabaseEngine::StorageTypes::Row* row);
        void UpdateRows(const vector<DatabaseEngine::StorageTypes::Block>* updates, const vector<RowCondition*>* conditions);
        virtual void GetPageDataFromFile(const vector<char>& data, const DatabaseEngine::StorageTypes::Table* table, page_offset_t& offSet, fstream* filePtr);
        virtual void WritePageToFile(fstream* filePtr);
        void SetFileName(const string& filename);
        void SetPageId(const page_id_t& pageId);
        const string& GetFileName() const;
        const page_id_t& GetPageId() const;
        const bool& GetPageDirtyStatus() const;
        const page_size_t& GetBytesLeft() const;
        void GetRows(vector<DatabaseEngine::StorageTypes::Row>* copiedRows, const DatabaseEngine::StorageTypes::Table& table, const size_t& rowsToSelect, const vector<RowCondition*>* conditions = nullptr) const;
        void UpdateRows(const vector<RowCondition*>* conditions = nullptr);
        page_size_t GetPageSize() const;
        const PageType& GetPageType() const;
    };
}