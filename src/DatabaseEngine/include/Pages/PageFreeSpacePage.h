#pragma once
#include "../DatabaseConstants.h"
#include "Page.h"

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
        void SetPageType(page_id_t pageId, const Constants::PageType& pageType)const;
        static page_id_t GetPagePosition(page_id_t pageId) ;

    public:
        PageFreeSpacePage();
        explicit PageFreeSpacePage(const PageHeader& pageHeader);
        explicit PageFreeSpacePage(page_id_t pageId);
        ~PageFreeSpacePage() override;
        void SetPageFreed(page_id_t pageId)const;
        [[nodiscard]] bool IsPageAllocated(page_id_t pageId) const;
        [[nodiscard]] Constants::PageType GetPageType(page_id_t pageId) const;
        [[nodiscard]] byte_t GetPageSizeCategory(page_id_t pageId) const;
        void ReadFromDisk(const std::vector<char> &data, const DatabaseEngine::StorageTypes::Table *table, page_offset_t &offSet, std::fstream *filePtr) override;
        void WriteToDisk(std::fstream *filePtr) override;
        void SetPageMetaData(const Page* page);
        void SetPageAllocated(page_id_t pageId)const;
        void SetPageAllocationStatus(page_id_t pageId, page_size_t bytesLeft);
        [[nodiscard]] bool IsFull() const;
    };
}

