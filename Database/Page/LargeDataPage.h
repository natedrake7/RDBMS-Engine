#pragma once
#include "Page.h"

typedef struct DataObject {
    unsigned char* object;
    uint32_t objectSize;

    DataObject();
    ~DataObject();
}DataObject;

typedef struct DataObjectPointer
{
    uint32_t objectSize;
    uint32_t objectOffset;
}DataObjectPointer;

class LargeDataPage : public Page {
    PageMetadata metadata;
    vector<DataObject*> data;

    public:
        explicit LargeDataPage(const uint16_t& pageId);
        ~LargeDataPage() override;
        void GetPageDataFromFile(const vector<char>& data, const Table* table, uint32_t& offSet) override;
        void WritePageToFile(fstream* filePtr) override;
        uint16_t InsertObject(const unsigned char* object, const uint32_t& size);
};
