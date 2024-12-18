#pragma once
#include "../Page.h"


namespace ByteMaps {
    class BitMap;
}

namespace Pages {
    class GlobalAllocationMapPage;
    
    typedef struct IndexAllocationPageAdditionalHeader {
        table_id_t tableId;
        extent_id_t startingExtentId;
        page_id_t nextPageId;

        IndexAllocationPageAdditionalHeader();
        IndexAllocationPageAdditionalHeader(const table_id_t& tableId, const extent_id_t& extentId, const page_id_t& nextPageId);
        ~IndexAllocationPageAdditionalHeader();
    }IndexAllocationPageAdditionalHeader;

    class IndexAllocationMapPage final : public Page{
        ByteMaps::BitMap* ownedExtents;
        IndexAllocationPageAdditionalHeader additionalHeader;

    protected:
        void GetAdditionalHeaderFromFile(const vector<char> &data, page_offset_t &offSet);
        void WriteAdditionalHeaderToFile(fstream* filePtr);
    
    public:
        IndexAllocationMapPage(const table_id_t& tableId, const page_id_t& pageId, const extent_id_t& startingExtentId);
        IndexAllocationMapPage(const PageHeader& pageHeader, const extent_id_t& startingExtentId, const table_id_t& tableId);
        ~IndexAllocationMapPage() override;
        void SetAllocatedExtent(const extent_id_t &extentId, const GlobalAllocationMapPage* globalAllocationMapPage);
        void SetDeallocatedExtent(const extent_id_t& extentId);
        void GetAllocatedExtents(vector<extent_id_t>* allocatedExtents) const;
        void GetAllocatedExtents(vector<extent_id_t>* allocatedExtents, const extent_id_t& startingExtentIndex) const;
        extent_id_t GetLastAllocatedExtent() const;
        void GetPageDataFromFile(const vector<char>& data, const DatabaseEngine::StorageTypes::Table* table, page_offset_t& offSet, fstream* filePtr) override;
        void WritePageToFile(fstream* filePtr) override;
        void SetNextPageId(const page_id_t& nextPageId);
    };
}
