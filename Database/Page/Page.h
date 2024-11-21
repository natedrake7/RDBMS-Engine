#pragma once
#include <vector>
#include <string>
#include "../Constants.h"

using namespace std;

class Row;
class Table;
struct DataObjectPointer;

typedef struct PageMetadata{
    uint16_t extentId;
    page_id_t pageId;
    page_size_t pageSize;
    page_id_t nextPageId;
    page_size_t bytesLeft;

    PageMetadata();
    ~PageMetadata();
}PageMetaData;

class Page {
    vector<Row*> rows;

    protected:
        bool isDirty;
        string filename;
        PageMetaData metadata;
        void GetPageMetaDataFromFile(const vector<char>& data, page_offset_t& offSet);
        void WritePageMetaDataToFile(fstream* filePtr);
        // static unsigned char* RetrieveDataFromLOBPage(DataObjectPointer& objectPointer
        //                                             , fstream* filePtr
        //                                             , const vector<char>& data
        //                                             , uint32_t& offSet);
    
    public:
        explicit Page(const page_id_t& pageId);
        explicit Page();
        virtual ~Page();
        void InsertRow(Row* row, const Table& table);
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
        vector<Row> GetRows(const Table& table) const;
        page_size_t GetPageSize() const;
};

#include "../Database.h"
#include "../Table/Table.h"
#include "../Row/Row.h"
#include "LargeDataPage.h"
