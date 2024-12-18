#pragma once
#include "../Page.h"

namespace DatabaseEngine::StorageTypes {
    class Table;
}

namespace ByteMaps {
    class ByteMap;
}

namespace Pages {
    class PageFreeSpacePage final: public Page{
        ByteMaps::ByteMap* pageMap;

    protected:
        void SetPageAllocated(const page_id_t& pageId);
        void SetPageType(const page_id_t& pageId, const PageType& pageType);

    public:
        PageFreeSpacePage();
        explicit PageFreeSpacePage(const PageHeader& pageHeader);
        explicit PageFreeSpacePage(const page_id_t& pageId);
        ~PageFreeSpacePage() override;
        void SetPageFreed(const page_id_t& pageId);
        [[nodiscard]] bool IsPageAllocated(const page_id_t& pageId) const;
        [[nodiscard]] PageType GetPageType(const page_id_t& pageId) const;
        [[nodiscard]] Constants::byte GetPageSizeCategory(const page_id_t& pageId) const;
        void GetPageDataFromFile(const vector<char> &data, const DatabaseEngine::StorageTypes::Table *table, page_offset_t &offSet, fstream *filePtr) override;
        void WritePageToFile(fstream *filePtr) override;
        void SetPageMetaData(const Page* page);
        void SetPageAllocationStatus(const page_id_t &pageId, const page_size_t& bytesLeft);
        [[nodiscard]] bool IsFull() const;
    };
}

