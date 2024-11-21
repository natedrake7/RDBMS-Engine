#pragma once
#include <vector>
#include <string>


using namespace std;

class Row;
class Table;
struct DataObjectPointer;

typedef struct PageMetadata{
    uint16_t pageId;
    uint16_t pageSize;
    uint16_t nextPageId;
    uint16_t bytesLeft;

    PageMetadata();
    ~PageMetadata();
}PageMetaData;

class Page {
    vector<Row*> rows;

    protected:
        bool isDirty;
        string filename;
        PageMetaData metadata;
        void GetPageMetaDataFromFile(const vector<char>& data, const Table* table, uint16_t& offSet);
        void WritePageMetaDataToFile(fstream* filePtr);
        // static unsigned char* RetrieveDataFromLOBPage(DataObjectPointer& objectPointer
        //                                             , fstream* filePtr
        //                                             , const vector<char>& data
        //                                             , uint32_t& offSet);
    
    public:
        explicit Page(const int& pageId);
        explicit Page();
        virtual ~Page();
        void InsertRow(Row* row, const Table& table);
        void DeleteRow(Row* row);
        void UpdateRow(Row* row);
        virtual void GetPageDataFromFile(const vector<char>& data, const Table* table, uint16_t& offSet, fstream* filePtr);
        virtual void WritePageToFile(fstream* filePtr);
        void SetNextPageId(const int& nextPageId);
        void SetFileName(const string& filename);
        const string& GetFileName() const;
        const uint16_t& GetPageId() const;
        const bool& GetPageDirtyStatus() const;
        const uint16_t& GetBytesLeft() const;
        const uint16_t& GetNextPageId() const;
        vector<Row> GetRows(const Table& table) const;
        uint16_t GetPageSize() const;
};

#include "../Database.h"
#include "../Table/Table.h"
#include "../Row/Row.h"
#include "LargeDataPage.h"
