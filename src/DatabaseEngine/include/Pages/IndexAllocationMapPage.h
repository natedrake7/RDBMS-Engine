#pragma once
#include "Page.h"


namespace ByteMaps {
    class BitMap;
}

namespace Pages {
    class GlobalAllocationMapPage;
    
    struct IndexAllocationPageAdditionalHeader {
        table_id_t tableId;
        extent_id_t startingExtentId;
        page_id_t nextPageId;

        IndexAllocationPageAdditionalHeader();
        IndexAllocationPageAdditionalHeader(table_id_t tableId, extent_id_t extentId, page_id_t nextPageId);
        ~IndexAllocationPageAdditionalHeader();
    };

    class IndexAllocationMapPage final : public Page{
        ByteMaps::BitMap* ownedExtents;
        IndexAllocationPageAdditionalHeader additionalHeader;
        uint16_t lastAllocatedExtentId;

    protected:
        void GetAdditionalHeaderFromFile(const std::vector<char> &data, page_offset_t &offSet);
        void WriteAdditionalHeaderToFile(std::fstream* filePtr)const;

    public:
        IndexAllocationMapPage(table_id_t tableId, page_id_t pageId, extent_id_t startingExtentId);
        IndexAllocationMapPage(const PageHeader& pageHeader, extent_id_t startingExtentId, table_id_t tableId);
        ~IndexAllocationMapPage() override;
        extent_id_t SetExtentsAllocated(
            const std::vector<extent_id_t>& extentIds,
            page_id_t globalAllocationMapPageId
        );
        void SetDeallocatedExtent(extent_id_t extentId);
        void GetAllocatedExtents(std::vector<extent_id_t>* allocatedExtents) const;
        void GetAllocatedExtents(std::vector<extent_id_t>* allocatedExtents, extent_id_t startingExtentIndex) const;
        // [[nodiscard]] extent_id_t GetLastAllocatedExtent() const;
        void ReadFromDisk(const std::vector<char>& data, const DatabaseEngine::StorageTypes::Table* table, page_offset_t& offSet, std::fstream* filePtr) override;
        void WriteToDisk(std::fstream* filePtr) override;
        void SetNextPageId(page_id_t nextPageId);
        page_id_t GetNextPageId() const;
        static page_id_t CalculatePageIdOffsetByGamPageId(page_id_t globalAllocationMapPageId);

    };
}
