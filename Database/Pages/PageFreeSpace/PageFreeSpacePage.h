#pragma once
#include "../Page.h"
#include "../../../AdditionalLibraries/ByteMap/ByteMap.h"

class PageFreeSpacePage final: public Page{
    ByteMap* pageMap;

    public:
        PageFreeSpacePage();
        explicit PageFreeSpacePage(const PageHeader& pageHeader);
        explicit PageFreeSpacePage(const page_id_t& pageId);
        ~PageFreeSpacePage() override;
        void SetPageAllocated(const page_id_t& pageId);
        void SetPageFreed(const page_id_t& pageId);
        void SetPageType(const page_id_t& pageId, const PageType& pageType);
        void SetPageAllocationStatus(const page_id_t& pageId, const byte& pageAllocationStatus);
        bool IsPageAllocated(const page_id_t& pageId) const;
        PageType GetPageType(const page_id_t& pageId) const;
        byte GetPageAllocationStatus(const page_id_t& pageId) const;
        void GetPageDataFromFile(const vector<char> &data, const Table *table, page_offset_t &offSet, fstream *filePtr) override;
        void WritePageToFile(fstream *filePtr) override;
    
};
