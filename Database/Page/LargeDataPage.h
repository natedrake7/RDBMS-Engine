#pragma once
#include "Page.h"

typedef struct DataObject {
    unsigned char* object;
    page_size_t objectSize;
    page_id_t nextPageId;
    page_offset_t nextPageOffset;

    DataObject();
    ~DataObject();
}DataObject;

typedef struct DataObjectPointer
{
    page_size_t objectSize;
    page_offset_t objectOffset;
    page_id_t pageId;

    DataObjectPointer();
    DataObjectPointer(const page_size_t& objectSize, const page_offset_t& objectOffset, const page_id_t& pageId);
    ~DataObjectPointer();

}DataObjectPointer;

class LargeDataPage : public Page
{
    vector<DataObject*> data;

    public:
        explicit LargeDataPage(const page_id_t& pageId);
        ~LargeDataPage() override;
        void GetPageDataFromFile(const vector<char>& data, const Table* table, page_offset_t& offSet, fstream* filePtr) override;
        void WritePageToFile(fstream* filePtr) override;
        DataObject* InsertObject(const unsigned char* object, const page_size_t& size, page_offset_t* objectPosition);
};
