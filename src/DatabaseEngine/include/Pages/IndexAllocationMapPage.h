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
        IndexAllocationPageAdditionalHeader(const table_id_t& tableId, const extent_id_t& extentId, const page_id_t& nextPageId);
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
        IndexAllocationMapPage(const table_id_t& tableId, const page_id_t& pageId, const extent_id_t& startingExtentId);
        IndexAllocationMapPage(const PageHeader& pageHeader, const extent_id_t& startingExtentId, const table_id_t& tableId);
        ~IndexAllocationMapPage() override;
        extent_id_t SetExtentsAllocated(
            const std::vector<extent_id_t>& extentIds,
            const page_id_t& globalAllocationMapPageId
        );
        void SetDeallocatedExtent(const extent_id_t& extentId);
        void GetAllocatedExtents(std::vector<extent_id_t>* allocatedExtents) const;
        void GetAllocatedExtents(std::vector<extent_id_t>* allocatedExtents, const extent_id_t& startingExtentIndex) const;
        // [[nodiscard]] extent_id_t GetLastAllocatedExtent() const;
        void ReadFromDisk(const std::vector<char>& data, const DatabaseEngine::StorageTypes::Table* table, page_offset_t& offSet, std::fstream* filePtr) override;
        void WriteToDisk(std::fstream* filePtr) override;
        void SetNextPageId(const page_id_t& nextPageId);
        const page_id_t& GetNextPageId() const;
        static page_id_t CalculatePageIdOffsetByGamPageId(const page_id_t& globalAllocationMapPageId);

    };
}
