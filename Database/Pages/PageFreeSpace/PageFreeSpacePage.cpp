#include "PageFreeSpacePage.h"

PageFreeSpacePage::PageFreeSpacePage() : Page()
{
    this->pageMap = nullptr;
}

PageFreeSpacePage::PageFreeSpacePage(const PageHeader &pageHeader) : Page(pageHeader)
{
    this->pageMap = nullptr;
}

PageFreeSpacePage::PageFreeSpacePage(const page_id_t &pageId) : Page(pageId)
{
    this->header.bytesLeft = PAGE_SIZE - PageHeader::GetPageHeaderSize();
    this->pageMap = new ByteMap(this->header.bytesLeft);
}

PageFreeSpacePage::~PageFreeSpacePage()
{
    delete this->pageMap;
}

void PageFreeSpacePage::SetPageAllocated(const page_id_t &pageId) { this->pageMap->SetPageIsAllocated(pageId, true); }

bool PageFreeSpacePage::IsPageAllocated(const page_id_t &pageId) const { return this->pageMap->IsAllocated(pageId); }

void PageFreeSpacePage::SetPageFreed(const page_id_t &pageId) { this->pageMap->SetPageIsAllocated(pageId, false); }

void PageFreeSpacePage::SetPageType(const page_id_t &pageId, const PageType &pageType) { this->pageMap->SetPageType(pageId, static_cast<byte>(pageType)); }

void PageFreeSpacePage::SetPageAllocationStatus(const page_id_t &pageId, const byte &pageAllocationStatus){ this->pageMap->SetFreeSpace(pageId, pageAllocationStatus); }

PageType PageFreeSpacePage::GetPageType(const page_id_t &pageId) const {return static_cast<PageType>(this->pageMap->GetPageType(pageId)); }

byte PageFreeSpacePage::GetPageAllocationStatus(const page_id_t &pageId) const { return this->pageMap->GetFreeSpace(pageId); }

void PageFreeSpacePage::GetPageDataFromFile(const vector<char> &data, const Table *table, page_offset_t &offSet,fstream *filePtr)
{
    this->pageMap->GetDataFromFile(data, offSet, this->header.pageSize);
}

void PageFreeSpacePage::WritePageToFile(fstream *filePtr)
{
    this->WritePageHeaderToFile(filePtr);

    this->pageMap->WriteDataToFile(filePtr);
}



