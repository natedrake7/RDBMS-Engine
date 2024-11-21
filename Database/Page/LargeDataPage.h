#pragma once
#include "Page.h"

typedef struct DataObject {
    unsigned char* object;
    uint16_t objectSize;
    uint16_t nextPageId;
    uint16_t nextPageOffset;

    DataObject();
    ~DataObject();
}DataObject;

typedef struct DataObjectPointer
{
    uint16_t objectSize;
    uint16_t objectOffset;
    uint16_t pageId;

    DataObjectPointer();
    DataObjectPointer(const uint16_t& objectSize, const uint16_t& objectOffset, const uint16_t& pageId);
    ~DataObjectPointer();

}DataObjectPointer;

class LargeDataPage : public Page
{
    vector<DataObject*> data;

    public:
        explicit LargeDataPage(const uint16_t& pageId);
        ~LargeDataPage() override;
        void GetPageDataFromFile(const vector<char>& data, const Table* table, uint16_t& offSet, fstream* filePtr) override;
        void WritePageToFile(fstream* filePtr) override;
        DataObject* InsertObject(const unsigned char* object, const uint16_t& size, uint16_t* objectPosition);
};
