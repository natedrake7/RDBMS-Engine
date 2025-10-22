#include "PageFreeSpacePage.h"
#include "../../../Systemic/DataStructures/ByteMap/ByteMap.h"

using namespace ByteMaps;
using namespace DatabaseEngine::StorageTypes;

namespace Pages {
    PageFreeSpacePage::PageFreeSpacePage() : Page()
    {
        this->pageMap = nullptr;
    }

    PageFreeSpacePage::PageFreeSpacePage(const PageHeader &pageHeader) : Page(pageHeader)
    {
        this->pageMap = new ByteMap(this->header.pageSize);
    }

    PageFreeSpacePage::PageFreeSpacePage(const page_id_t &pageId) : Page(pageId)
    {
        this->header.bytesLeft = PAGE_SIZE - PageHeader::GetPageHeaderSize();
        this->pageMap = new ByteMap(PAGE_FREE_SPACE_SIZE);
        this->header.pageSize = PAGE_FREE_SPACE_SIZE;
        this->header.bytesLeft = 0;
        this->header.pageType = PageType::FREESPACE;
    }

    PageFreeSpacePage::~PageFreeSpacePage()
    {
        delete this->pageMap;
    }

    void PageFreeSpacePage::SetPageAllocated(const page_id_t &pageId)
    {
        const auto pos = PageFreeSpacePage::GetPagePosition(pageId);
        this->pageMap->SetPageIsAllocated(PageFreeSpacePage::GetPagePosition(pageId), true);
    }

    bool PageFreeSpacePage::IsPageAllocated(const page_id_t &pageId) const {
      return this->pageMap->IsAllocated(PageFreeSpacePage::GetPagePosition(pageId));
    }

    void PageFreeSpacePage::SetPageFreed(const page_id_t &pageId) {
      this->pageMap->SetPageIsAllocated(PageFreeSpacePage::GetPagePosition(pageId), false);
    }

    void PageFreeSpacePage::SetPageType(const page_id_t &pageId, const PageType &pageType) {
      this->pageMap->SetPageType(PageFreeSpacePage::GetPagePosition(pageId), static_cast<Constants::byte>(pageType));
    }

    void PageFreeSpacePage::SetPageAllocationStatus(const page_id_t &pageId, const page_size_t& bytesLeft)
    {

        const Constants::byte pageAllocationStatus = static_cast<Constants::byte>(bytesLeft * 15 / PAGE_SIZE);

        this->pageMap->SetFreeSpace(PageFreeSpacePage::GetPagePosition(pageId), pageAllocationStatus);

        this->isDirty = true;
    }

    bool PageFreeSpacePage::IsFull() const { return this->pageMap->IsAllocated(this->header.pageSize - 1); }

    PageType PageFreeSpacePage::GetPageType(const page_id_t &pageId) const {

        return static_cast<PageType>(this->pageMap->GetPageType(PageFreeSpacePage::GetPagePosition(pageId)));
    }

    Constants::byte PageFreeSpacePage::GetPageSizeCategory(const page_id_t &pageId) const {
      return this->pageMap->GetFreeSpace(PageFreeSpacePage::GetPagePosition(pageId));
    }

    void PageFreeSpacePage::SetPageMetaData(const Page *page)
    {
        const page_id_t& pageId = page->GetPageId();

        this->SetPageAllocated(pageId);
        this->SetPageType(pageId, page->GetPageType());
        this->SetPageAllocationStatus(pageId, page->GetBytesLeft());

        this->isDirty = true;
    }

    void PageFreeSpacePage::GetPageDataFromFile(const vector<char> &data, const Table *table, page_offset_t &offSet,fstream *filePtr)
    {
        this->pageMap->GetDataFromFile(data, offSet, this->header.pageSize);
    }

    void PageFreeSpacePage::WritePageToFile(fstream *filePtr)
    {
        this->WritePageHeaderToFile(filePtr);

        this->pageMap->WriteDataToFile(filePtr);
    }

    page_id_t PageFreeSpacePage::GetPagePosition(const page_id_t & pageId) {
      const uint32_t numOfGamPages =  (pageId / GAM_NUMBER_OF_PAGES) + 1;
      const uint32_t  numOfPfsPages = (pageId / PAGE_FREE_SPACE_SIZE) + 1;

      return (pageId - numOfGamPages - numOfPfsPages - 1) % PAGE_FREE_SPACE_SIZE;
    }
}



