#pragma once
#include "../Page.h"

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
        bool IsPageAllocated(const page_id_t& pageId) const;
        PageType GetPageType(const page_id_t& pageId) const;
        Constants::byte GetPageSizeCategory(const page_id_t& pageId) const;
        void GetPageDataFromFile(const vector<char> &data, const Table *table, page_offset_t &offSet, fstream *filePtr) override;
        void WritePageToFile(fstream *filePtr) override;
        void SetPageMetaData(const Page* page);
        void SetPageAllocationStatus(const page_id_t &pageId, const page_size_t& bytesLeft);
        bool IsFull() const;
    };
}

