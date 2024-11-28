#pragma once
#include "../Page.h"

class IndexAllocationMapPage final : public Page{
    BitMap* ownedExtents;
    table_id_t tableId;
    extent_id_t startingExtentId;
    
    public:
        IndexAllocationMapPage(const table_id_t& tableId, const page_id_t& pageId);
        IndexAllocationMapPage(const PageMetadata &pageMetaData, const extent_id_t& startingExtentId, const table_id_t& tableId);
        ~IndexAllocationMapPage() override;
        void SetAllocatedExtent(const extent_id_t& extentId);
        void SetDeallocatedExtent(const extent_id_t& extentId);
        void GetAllocatedExtents(vector<extent_id_t>* allocatedExtents) const;
        extent_id_t GetLastAllocatedExtent() const;
        void GetPageDataFromFile(const vector<char>& data, const Table* table, page_offset_t& offSet, fstream* filePtr) override;
        void WritePageToFile(fstream* filePtr) override;
};
