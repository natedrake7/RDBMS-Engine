#include "../../include/Pages/PageFreeSpacePage.h"
#include "../../../Systemic/include/DataStructures/ByteMap.h"

namespace Pages {
    PageFreeSpacePage::PageFreeSpacePage() : Page()
    {
        this->pageMap = nullptr;
    }

    PageFreeSpacePage::PageFreeSpacePage(const PageHeader &pageHeader) : Page(pageHeader)
    {
        this->pageMap = new ByteMaps::ByteMap(this->header.pageSize);
        this->priority = Constants::PagePriority::SYSTEM;
    }

    PageFreeSpacePage::PageFreeSpacePage(const page_id_t &pageId) : Page(pageId, true)
    {
        this->header.bytesLeft = Constants::PAGE_SIZE_WITHOUT_HEADER;
        this->pageMap = new ByteMaps::ByteMap(PAGE_FREE_SPACE_SIZE);
        this->header.pageSize = Constants::PAGE_FREE_SPACE_SIZE;
        this->header.bytesLeft = 0;
        this->header.pageType = Constants::PageType::FREESPACE;
        this->priority = Constants::PagePriority::SYSTEM;
    }

    PageFreeSpacePage::~PageFreeSpacePage()
    {
        delete this->pageMap;
    }

    void PageFreeSpacePage::SetPageAllocated(const page_id_t &pageId)const
    {
        this->pageMap->SetPageIsAllocated(PageFreeSpacePage::GetPagePosition(pageId), true);
    }

    bool PageFreeSpacePage::IsPageAllocated(const page_id_t &pageId) const {
      return this->pageMap->IsAllocated(PageFreeSpacePage::GetPagePosition(pageId));
    }

    void PageFreeSpacePage::SetPageFreed(const page_id_t &pageId)const {
      this->pageMap->SetPageIsAllocated(PageFreeSpacePage::GetPagePosition(pageId), false);
    }

    void PageFreeSpacePage::SetPageType(const page_id_t &pageId, const PageType &pageType)const {
      this->pageMap->SetPageType(PageFreeSpacePage::GetPagePosition(pageId), static_cast<byte_t>(pageType));
    }

    void PageFreeSpacePage::SetPageAllocationStatus(const page_id_t &pageId, const page_size_t& bytesLeft)
    {
        const auto pageAllocationStatus = static_cast<byte_t>(bytesLeft * 7 / PAGE_SIZE);

        this->pageMap->SetFreeSpace(PageFreeSpacePage::GetPagePosition(pageId), pageAllocationStatus);

        this->isDirty = true;
    }

    bool PageFreeSpacePage::IsFull() const { return this->pageMap->IsAllocated(this->header.pageSize - 1); }

    PageType PageFreeSpacePage::GetPageType(const page_id_t &pageId) const {

        return static_cast<PageType>(this->pageMap->GetPageType(PageFreeSpacePage::GetPagePosition(pageId)));
    }

    byte_t PageFreeSpacePage::GetPageSizeCategory(const page_id_t &pageId) const {
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

    void PageFreeSpacePage::ReadFromDisk(const vector<char> &data, const DatabaseEngine::StorageTypes::Table *table, page_offset_t &offSet,fstream *filePtr)
    {
        this->pageMap->GetDataFromFile(data, offSet, this->header.pageSize);
    }

    void PageFreeSpacePage::WriteToDisk(fstream *filePtr)
    {
        this->WritePageHeaderToDisk(filePtr);

        this->pageMap->WriteDataToFile(filePtr);
    }

    page_id_t PageFreeSpacePage::GetPagePosition(const page_id_t & pageId) {
      const uint32_t numOfGamPages =  (pageId / GAM_NUMBER_OF_PAGES) + 1;
      const uint32_t  numOfPfsPages = (pageId / PAGE_FREE_SPACE_SIZE) + 1;

      return (pageId - numOfGamPages - numOfPfsPages - 1) % PAGE_FREE_SPACE_SIZE;
    }
}



