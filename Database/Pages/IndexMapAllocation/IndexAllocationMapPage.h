#pragma once
#include "../Page.h"

typedef struct ExtentAvailability {
    vector<extent_id_t> extents;
    BitMap* freeExtents;

    ExtentAvailability();
    ~ExtentAvailability();
}ExtentAvailability;

class IndexAllocationMapPage final : public Page{
    unordered_map<table_id_t, ExtentAvailability*> allocationMap;

    public:
        explicit IndexAllocationMapPage(const page_id_t& pageId);
        explicit IndexAllocationMapPage(const PageMetaData& pageMetaData);
        ~IndexAllocationMapPage() override;
        void GetPageDataFromFile(const vector<char> &data, const Table *table, page_offset_t &offSet, fstream *filePtr) override;
        void WritePageToFile(fstream *filePtr) override;
};
