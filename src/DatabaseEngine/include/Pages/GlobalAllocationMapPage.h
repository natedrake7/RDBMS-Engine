#pragma once
#include "Page.h"

namespace DatabaseEngine::StorageTypes {
    class Table;
}

namespace ByteMaps {
    class BitMap;
}

namespace Pages {
    class GlobalAllocationMapPage final : public Page{
        ByteMaps::BitMap* extentsMap;
        extent_id_t lastAllocatedExtentId;
    
    public:
        explicit GlobalAllocationMapPage(const page_id_t& pageId);
        explicit GlobalAllocationMapPage(const PageHeader& pageHeader);
        ~GlobalAllocationMapPage() override;
        int AllocateExtentsNoLock(std::vector<extent_id_t>& extents, const int& numberOfExtents);
        void DeallocateExtent(const extent_id_t& extentId);
        void WriteToDisk(fstream *filePtr) override;
        void ReadFromDisk(const vector<char>& data, const DatabaseEngine::StorageTypes::Table* table, page_offset_t& offSet, fstream* filePtr) override;
        [[nodiscard]] bool IsFull() const;
        std::vector<extent_id_t> GetAllocatedExtents(const extent_id_t& startingIndex = 0) const;
    };
}

