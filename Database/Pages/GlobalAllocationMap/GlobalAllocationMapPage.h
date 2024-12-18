#pragma once
#include "../Page.h"

namespace DatabaseEngine::StorageTypes {
    class Table;
}

namespace ByteMaps {
    class BitMap;
}

namespace Pages {
    class GlobalAllocationMapPage final : public Page{
        ByteMaps::BitMap* extentsMap;
    
    protected:
        static page_size_t GetAvailableSize();
    
    public:
        explicit GlobalAllocationMapPage(const page_id_t& pageId);
        explicit GlobalAllocationMapPage(const PageHeader& pageHeader);
        ~GlobalAllocationMapPage() override;
        extent_id_t AllocateExtent();
        void DeallocateExtent(const extent_id_t& extentId);
        void WritePageToFile(fstream *filePtr) override;
        void GetPageDataFromFile(const vector<char>& data, const DatabaseEngine::StorageTypes::Table* table, page_offset_t& offSet, fstream* filePtr) override;
        [[nodiscard]] bool IsFull() const;
    };
}

