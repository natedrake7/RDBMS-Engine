#pragma once
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
        void SetPageType(const page_id_t& pageId, const PageType& pageType)const;
        static page_id_t GetPagePosition(const page_id_t& pageId) ;

    public:
        PageFreeSpacePage();
        explicit PageFreeSpacePage(const PageHeader& pageHeader);
        explicit PageFreeSpacePage(const page_id_t& pageId);
        ~PageFreeSpacePage() override;
        void SetPageFreed(const page_id_t& pageId)const;
        [[nodiscard]] bool IsPageAllocated(const page_id_t& pageId) const;
        [[nodiscard]] PageType GetPageType(const page_id_t& pageId) const;
        [[nodiscard]] Constants::byte GetPageSizeCategory(const page_id_t& pageId) const;
        void ReadFromDisk(const vector<char> &data, const DatabaseEngine::StorageTypes::Table *table, page_offset_t &offSet, fstream *filePtr) override;
        void WriteToDisk(fstream *filePtr) override;
        void SetPageMetaData(const Page* page);
        void SetPageAllocated(const page_id_t& pageId)const;
        void SetPageAllocationStatus(const page_id_t &pageId, const page_size_t& bytesLeft);
        [[nodiscard]] bool IsFull() const;
    };
}

