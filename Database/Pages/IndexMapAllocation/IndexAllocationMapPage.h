#pragma once
#include "../Page.h"

typedef struct IndexAllocationPageAdditionalHeader {
    table_id_t tableId;
    extent_id_t startingExtentId;

    IndexAllocationPageAdditionalHeader();
    IndexAllocationPageAdditionalHeader(const table_id_t& tableId, const extent_id_t& extentId);
    ~IndexAllocationPageAdditionalHeader();
}IndexAllocationPageAdditionalHeader;

class IndexAllocationMapPage final : public Page{
    BitMap* ownedExtents;
    IndexAllocationPageAdditionalHeader additionalHeader;

    protected:
        void GetAdditionalHeaderFromFile(const vector<char> &data, page_offset_t &offSet);
        void WriteAdditionalHeaderToFile(fstream* filePtr);
    
    public:
        IndexAllocationMapPage(const table_id_t& tableId, const page_id_t& pageId);
        IndexAllocationMapPage(const PageHeader& pageHeader, const extent_id_t& startingExtentId, const table_id_t& tableId);
        ~IndexAllocationMapPage() override;
        void SetAllocatedExtent(const extent_id_t& extentId);
        void SetDeallocatedExtent(const extent_id_t& extentId);
        void GetAllocatedExtents(vector<extent_id_t>* allocatedExtents) const;
        extent_id_t GetLastAllocatedExtent() const;
        void GetPageDataFromFile(const vector<char>& data, const Table* table, page_offset_t& offSet, fstream* filePtr) override;
        void WritePageToFile(fstream* filePtr) override;
};
