#pragma once
#include "Page.h"

typedef struct DataObject {
    unsigned char* object;
    page_size_t objectSize;
    page_id_t nextPageId;
    large_page_index_t nextObjectIndex;

    DataObject();
    ~DataObject();
}DataObject;

typedef struct DataObjectPointer
{
    large_page_index_t objectIndex;
    page_id_t pageId;

    DataObjectPointer();
    DataObjectPointer(const large_page_index_t& objectIndex, const page_id_t& pageId);
    ~DataObjectPointer();

}DataObjectPointer;

class LargeDataPage : public Page
{
    vector<DataObject*> data;

    public:
        explicit LargeDataPage(const page_id_t& pageId);
        explicit LargeDataPage();
        explicit LargeDataPage(const PageMetaData& pageMetaData);
        ~LargeDataPage() override;
        void GetPageDataFromFile(const vector<char>& data, const Table* table, page_offset_t& offSet, fstream* filePtr) override;
        void WritePageToFile(fstream* filePtr) override;
        DataObject* InsertObject(const unsigned char* object, const page_size_t& size, page_offset_t* objectPosition);
        DataObject* GetObject(const page_offset_t& offset);
};
