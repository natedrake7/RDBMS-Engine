#pragma once
#include <vector>
#include <string>
#include "../Constants.h"
#include "../../AdditionalLibraries/BitMap/BitMap.h"

class RowCondition;
using namespace std;

class Row;
class Table;
struct DataObjectPointer;

typedef struct PageMetadata{
    page_id_t pageId;
    page_size_t pageSize;
    page_id_t nextPageId;
    page_size_t bytesLeft;
    PageType pageType;

    PageMetadata();
    ~PageMetadata();
    static page_size_t GetPageMetaDataSize();
}PageMetaData;

class Page {
    vector<Row*> rows;

    protected:
        bool isDirty;
        string filename;
        PageMetaData metadata;
        void WritePageMetaDataToFile(fstream* filePtr);

    public:
        explicit Page(const page_id_t& pageId);
        explicit Page();
        explicit Page(const PageMetaData& pageMetaData);
        virtual ~Page();
        void InsertRow(Row* row);
        void DeleteRow(Row* row);
        void UpdateRow(Row* row);
        virtual void GetPageDataFromFile(const vector<char>& data, const Table* table, page_offset_t& offSet, fstream* filePtr);
        virtual void WritePageToFile(fstream* filePtr);
        void SetNextPageId(const page_id_t& nextPageId);
        void SetFileName(const string& filename);
        const string& GetFileName() const;
        const page_id_t& GetPageId() const;
        const bool& GetPageDirtyStatus() const;
        const page_size_t& GetBytesLeft() const;
        const page_id_t& GetNextPageId() const;
        void GetRows(vector<Row>* copiedRows, const Table& table, const vector<RowCondition*>* conditions = nullptr) const;
        page_size_t GetPageSize() const;
};

#include "../Database.h"
#include "../Table/Table.h"
#include "../Row/Row.h"
#include "LargeDataPage.h"
