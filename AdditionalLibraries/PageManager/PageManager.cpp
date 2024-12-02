#include "PageManager.h"

PageManager::PageManager(FileManager* fileManager)
{
    this->fileManager = fileManager;
    this->database = nullptr;
}

PageManager::~PageManager()
{
    const size_t pageListSize = this->pageList.size();
    const size_t systemPageListSize = this->systemPageList.size();

    for(size_t i = 0; i < pageListSize; i++)
        this->RemovePage();

    for(size_t i = 0;i < systemPageListSize; i++)
        this->RemoveSystemPage();

    for (const auto& page : this->deletedPages) 
        delete page;
}

void PageManager::BindDatabase(const Database *database) { this->database = database; }

Page* PageManager::GetPage(const page_id_t& pageId, const extent_id_t& extentId, const Table* table)
{
    auto pageHashIterator = this->cache.find(pageId);

    if(pageHashIterator == this->cache.end())
    {
        this->OpenExtent(extentId,  table);
        pageHashIterator = this->cache.find(pageId);
    }

    this->pageList.push_front(*pageHashIterator->second);
    this->pageList.erase(pageHashIterator->second);
    this->cache[pageId] = this->pageList.begin();

    return *this->pageList.begin();
}

LargeDataPage* PageManager::GetLargeDataPage(const page_id_t &pageId, const extent_id_t& extentId, const Table *table)
{
    return dynamic_cast<LargeDataPage*>(this->GetPage(pageId, extentId, table));
}

Page* PageManager::CreatePage(const page_id_t& pageId)
{
    Page* page = new Page(pageId, true);

    const string& filename = this->database->GetFileName();

    this->MovePageToFrontOfList(page, pageId, filename);

    return page;
}

LargeDataPage* PageManager::CreateLargeDataPage(const page_id_t &pageId)
{
    LargeDataPage* page = new LargeDataPage(pageId, true);

    const string& filename = this->database->GetFileName();

    this->MovePageToFrontOfList(page, pageId, filename);

    return page;
}

void PageManager::RemovePage()
{
    Page* page = this->pageList.back();
    const page_id_t pageId = page->GetPageId();

    if(page->GetPageDirtyStatus())
    {
        const string& filename = page->GetFileName();

        fstream* file = this->fileManager->GetFile(filename);

        const streampos pageOffset = pageId * PAGE_SIZE;

        SetWriteFilePointerToOffset(file, pageOffset);

        page->WritePageToFile(file);
    }

    this->cache.erase(pageId);

    this->pageList.pop_back();

    // this->deletedPages.push_back(page);
        
    delete page;
}

void PageManager::OpenExtent(const extent_id_t& extentId, const Table* table)
{
    const string& filename = this->database->GetFileName();

    //read page from disk, call FileManager
    fstream* file = this->fileManager->GetFile(filename);

    const page_id_t firstExtentPageId = Database::CalculateSystemPageOffsetByExtentId(extentId);

    const streampos extentOffset = firstExtentPageId * PAGE_SIZE;

    vector<char> buffer(EXTENT_BYTE_SIZE);

    SetReadFilePointerToOffset(file, extentOffset);

    //take into account the metadata page all the others
    file->read(buffer.data(), EXTENT_BYTE_SIZE);

    page_offset_t offSet = 0;

    const auto& bytesRead = file->gcount();

    for(int i = 0; i < EXTENT_SIZE; i++)
    {
        offSet = i * PAGE_SIZE;

        if(bytesRead < offSet)
            break;

        const page_id_t currentPageId = firstExtentPageId + i;

        if (this->IsPageCached(currentPageId))
            continue;

        if(this->pageList.size() == MAX_NUMBER_OF_PAGES)
            this->RemovePage();

        const PageHeader pageHeader = PageManager::GetPageHeaderFromFile(buffer, offSet);
            
        Page* page = nullptr;

        PageManager::AllocateMemoryBasedOnPageType(&page, pageHeader);

        page->GetPageDataFromFile(buffer, table, offSet, file);
        
        this->pageList.push_front(page);

        this->cache[page->GetPageId()] = this->pageList.begin();

        page->SetFileName(filename);
    }
}

////////////////////////////////////////////////////
////////////////////System Pages///////////////////
//////////////////////////////////////////////////

HeaderPage* PageManager::CreateHeaderPage(const string& filename)
{
    const page_id_t pageId = 0;
    
    HeaderPage* page = new HeaderPage(pageId);

    this->MovePageToFrontOfSystemList(page, pageId, filename);

    return page;
}

GlobalAllocationMapPage* PageManager::CreateGlobalAllocationMapPage(const string &filename, const page_id_t& pageId)
{
    GlobalAllocationMapPage* page = new GlobalAllocationMapPage(pageId);

    this->MovePageToFrontOfSystemList(page, pageId, filename);

    return page;
}

GlobalAllocationMapPage* PageManager::CreateGlobalAllocationMapPage(const page_id_t& pageId)
{
    GlobalAllocationMapPage* page = new GlobalAllocationMapPage(pageId);
    
    const string& filename = this->database->GetFileName();

    this->MovePageToFrontOfSystemList(page, pageId, filename);

    return page;
}

IndexAllocationMapPage* PageManager::CreateIndexAllocationMapPage(const table_id_t &tableId, const page_id_t& pageId, const extent_id_t& startingExtentId)
{
    IndexAllocationMapPage* page = new IndexAllocationMapPage(tableId, pageId, startingExtentId);
    
    const string& filename = this->database->GetFileName();

    this->MovePageToFrontOfSystemList(page, pageId, filename);

    return page;
}

PageFreeSpacePage * PageManager::CreatePageFreeSpacePage(const page_id_t& pageId)
{
    PageFreeSpacePage* page = new PageFreeSpacePage(pageId);
    
    const string& filename = this->database->GetFileName();

    this->MovePageToFrontOfSystemList(page, pageId, filename);

    return page;
}

PageFreeSpacePage * PageManager::CreatePageFreeSpacePage(const string &filename, const page_id_t& pageId)
{
    PageFreeSpacePage* page = new PageFreeSpacePage(pageId);

    this->MovePageToFrontOfSystemList(page, pageId, filename);

    return page;
}

HeaderPage* PageManager::GetHeaderPage(const string& filename)
{
    constexpr page_id_t pageId = 0;

    HeaderPage* page = dynamic_cast<HeaderPage*>(this->GetSystemPage(pageId, filename));
    return page;
}

PageFreeSpacePage* PageManager::GetPageFreeSpacePage(const page_id_t &pageId)
{
    return dynamic_cast<PageFreeSpacePage*>(this->GetSystemPage(pageId)); 
}

IndexAllocationMapPage* PageManager::GetIndexAllocationMapPage(const page_id_t &pageId)
{
    return dynamic_cast<IndexAllocationMapPage*>(this->GetSystemPage(pageId));
}

GlobalAllocationMapPage* PageManager::GetGlobalAllocationMapPage(const page_id_t& pageId)
{
    return dynamic_cast<GlobalAllocationMapPage*>(this->GetSystemPage(pageId));
}

Page* PageManager::GetSystemPage(const page_id_t &pageId)
{
    auto pageHashIterator = this->systemCache.find(pageId);
    const string& filename = this->database->GetFileName();
    
    if(pageHashIterator == this->systemCache.end())
    {
        this->OpenSystemPage(pageId);
        pageHashIterator = this->systemCache.find(pageId);
    }
    
    this->systemPageList.push_front(*pageHashIterator->second);
    this->systemPageList.erase(pageHashIterator->second);
    this->systemCache[pageId] = this->systemPageList.begin();

    (*this->systemPageList.begin())->SetFileName(filename);

    return *this->systemPageList.begin();
}

Page* PageManager::GetSystemPage(const page_id_t &pageId, const string& filename)
{
    auto pageHashIterator = this->systemCache.find(pageId);
    
    if(pageHashIterator == this->systemCache.end())
    {
        this->OpenSystemPage(pageId, filename);
        pageHashIterator = this->systemCache.find(pageId);
    }
    
    this->systemPageList.push_front(*pageHashIterator->second);
    this->systemPageList.erase(pageHashIterator->second);
    this->systemCache[pageId] = this->systemPageList.begin();

    (*this->systemPageList.begin())->SetFileName(filename);

    return *this->systemPageList.begin();
}

void PageManager::OpenSystemPage(const page_id_t &pageId)
{
    if(this->systemPageList.size() == MAX_NUMBER_SYSTEM_PAGES)
        this->RemoveSystemPage();

    const string& filename = this->database->GetFileName();

    fstream* file = this->fileManager->GetFile(filename);

    const streampos pageOffset = pageId * PAGE_SIZE;
    vector<char> buffer(PAGE_SIZE);
    SetReadFilePointerToOffset(file, pageOffset);

    file->read(buffer.data(), PAGE_SIZE);

    page_offset_t offSet = 0;

    const PageHeader pageHeader = PageManager::GetPageHeaderFromFile(buffer, offSet);

    Page* page = nullptr;

    PageManager::AllocateMemoryBasedOnSystemPageType(&page, pageHeader);

    this->systemPageList.push_front(page);
    
    page->GetPageDataFromFile(buffer, nullptr, offSet, file);

    this->systemCache[pageId] = this->systemPageList.begin();
}

void PageManager::OpenSystemPage(const page_id_t &pageId, const string &filename)
{
    if(this->systemPageList.size() == MAX_NUMBER_SYSTEM_PAGES)
        this->RemoveSystemPage();

    fstream* file = this->fileManager->GetFile(filename);

    const streampos pageOffset = pageId * PAGE_SIZE;
    vector<char> buffer(PAGE_SIZE);
    SetReadFilePointerToOffset(file, pageOffset);

    file->read(buffer.data(), PAGE_SIZE);

    page_offset_t offSet = 0;

    const PageHeader pageHeader = PageManager::GetPageHeaderFromFile(buffer, offSet);

    Page* page = nullptr;
    PageManager::AllocateMemoryBasedOnSystemPageType(&page, pageHeader);
    
    this->systemPageList.push_front(page);
    
    page->GetPageDataFromFile(buffer, nullptr, offSet, file);

    this->systemCache[pageId] = this->systemPageList.begin();
}

void PageManager::RemoveSystemPage()
{
    Page* page = this->systemPageList.back();
    
    const page_id_t pageId = page->GetPageId();

    if(page->GetPageDirtyStatus())
    {
        const string& filename = page->GetFileName();
        
        fstream* file = this->fileManager->GetFile(filename);

        const streampos pageOffset = pageId * PAGE_SIZE;

        SetWriteFilePointerToOffset(file, pageOffset);

        page->WritePageToFile(file);
    }

    this->systemCache.erase(pageId);

    this->systemPageList.pop_back();

    delete page;
}

// IndexAllocationMapPage* PageManager::OpenIndexAllocationMapPage(const page_id_t &pageId)
// {
//     if(this->systemPageList.size() == MAX_NUMBER_SYSTEM_PAGES)
//         this->RemoveSystemPage();
//
//     const string& filename = this->database->GetFileName();
//
//     fstream* file = this->fileManager->GetFile(filename);
//
//     const streampos pageOffset = pageId * PAGE_SIZE;
//     vector<char> buffer(PAGE_SIZE);
//     SetReadFilePointerToOffset(file, pageOffset);
//
//     file->read(buffer.data(), PAGE_SIZE);
//
//     page_offset_t offSet = 0;
//
//     const PageHeader pageHeader = PageManager::GetPageHeaderFromFile(buffer, offSet);
//
//     IndexAllocationMapPage* page = new IndexAllocationMapPage(pageHeader, 0, 0);
//     this->systemPageList.push_front(page);
//     
//     page->GetPageDataFromFile(buffer, nullptr, offSet, file);
//
//     this->systemCache[pageId] = this->systemPageList.begin();
//
//     return page;
// }

// PageFreeSpacePage * PageManager::OpenPageFreeSpacePage(const page_id_t &pageId)
// {
//     if(this->systemPageList.size() == MAX_NUMBER_SYSTEM_PAGES)
//         this->RemoveSystemPage();
//
//     const string& filename = this->database->GetFileName();
//
//     fstream* file = this->fileManager->GetFile(filename);
//
//     const streampos pageOffset = pageId * PAGE_SIZE;
//     vector<char> buffer(PAGE_SIZE);
//     SetReadFilePointerToOffset(file, pageOffset);
//
//     file->read(buffer.data(), PAGE_SIZE);
//
//     page_offset_t offSet = 0;
//
//     const PageHeader pageHeader = PageManager::GetPageHeaderFromFile(buffer, offSet);
//
//     PageFreeSpacePage* page = new PageFreeSpacePage(pageHeader);
//     this->systemPageList.push_front(page);
//     
//     page->GetPageDataFromFile(buffer, nullptr, offSet, file);
//
//     this->systemCache[pageId] = this->systemPageList.begin();
//
//     return page;
// }

// HeaderPage* PageManager::OpenHeaderPage(const string& filename)
// {
//     if(this->systemPageList.size() == MAX_NUMBER_OF_PAGES)
//         this->RemoveSystemPage();
//
//     fstream* file = this->fileManager->GetFile(filename);
//
//     const streampos pageOffset = 0;
//     vector<char> buffer(PAGE_SIZE);
//     SetReadFilePointerToOffset(file, pageOffset);
//
//     file->read(buffer.data(), PAGE_SIZE);
//
//     page_offset_t offSet = 0;
//
//     const PageHeader pageHeader = PageManager::GetPageHeaderFromFile(buffer, offSet);
//
//     HeaderPage* page = new HeaderPage(pageHeader);
//     this->systemPageList.push_front(page);
//     
//     page->GetPageDataFromFile(buffer, nullptr, offSet, file);
//
//     this->systemCache[0] = this->systemPageList.begin();
//
//     return page;
// }

// GlobalAllocationMapPage* PageManager::OpenGlobalAllocationMapPage(const string& filename)
// {
//     if(this->systemPageList.size() == MAX_NUMBER_OF_PAGES)
//         this->RemovePage();
//
//     fstream* file = this->fileManager->GetFile(filename);
//
//     vector<char> buffer(PAGE_SIZE);
//     SetReadFilePointerToOffset(file, PAGE_SIZE);
//
//     file->read(buffer.data(), PAGE_SIZE);
//     page_offset_t offSet = 0;
//     
//     const PageHeader pageHeader = PageManager::GetPageHeaderFromFile(buffer, offSet);
//     
//     GlobalAllocationMapPage* page = new GlobalAllocationMapPage(pageHeader);
//     
//     page->GetPageDataFromFile(buffer, nullptr, offSet, file);
//
//     this->systemPageList.push_front(page);
//
//     this->systemCache[2] = this->systemPageList.begin();
//
//     return page;
// }

void PageManager::AllocateMemoryBasedOnSystemPageType(Page **page, const PageHeader &pageHeader) {
    switch (pageHeader.pageType)
    {
        case PageType::GAM:
            *page = new GlobalAllocationMapPage(pageHeader);
        break;
        case PageType::INDEX:
            *page = new IndexAllocationMapPage(pageHeader, 0, 0);
        break;
        case PageType::METADATA:
            *page = new HeaderPage(pageHeader);
        break;
        case PageType::FREESPACE:
            *page = new PageFreeSpacePage(pageHeader);
        break;
        default:
            throw runtime_error("Page type not recognized");
    }
}

////////////////////////////////////////////////////////////////////
/////////////////////////Globally Used Functions///////////////////
//////////////////////////////////////////////////////////////////

void PageManager::AllocateMemoryBasedOnPageType(Page **page, const PageHeader &pageHeader)
{
    switch (pageHeader.pageType)
    {
        case PageType::DATA:
            *page = new Page(pageHeader);
        break;
        case PageType::LOB:
            *page = new LargeDataPage(pageHeader);
        break;
        default:
            throw runtime_error("Page type not recognized");
    }
}

void PageManager::MovePageToFrontOfSystemList(Page *page, const page_id_t& pageId, const string& filename)
{
    if(this->systemPageList.size() == MAX_NUMBER_SYSTEM_PAGES)
        this->RemoveSystemPage();

    page->SetFileName(filename);

    this->systemPageList.push_front(page);
    this->systemCache[pageId] = this->systemPageList.begin();
}

void PageManager::MovePageToFrontOfList(Page *page, const page_id_t &pageId, const string &filename)
{
    if(this->pageList.size() == MAX_NUMBER_SYSTEM_PAGES)
        this->RemovePage();

    page->SetFileName(filename);

    this->pageList.push_front(page);
    this->cache[pageId] = this->pageList.begin();
}

PageHeader PageManager::GetPageHeaderFromFile(const vector<char> &data, page_offset_t &offSet)
{
    PageHeader pageHeader;
    memcpy(&pageHeader.pageId, data.data() + offSet, sizeof(page_id_t));
    offSet += sizeof(page_id_t);

    memcpy(&pageHeader.pageSize, data.data() + offSet, sizeof(page_size_t));

    offSet += sizeof(page_size_t);

    memcpy(&pageHeader.bytesLeft, data.data() + offSet, sizeof(page_size_t));
    offSet += sizeof(page_size_t);

    memcpy(&pageHeader.pageType, data.data() + offSet, sizeof(PageType));
    offSet += sizeof(PageType);

    return pageHeader;
}

bool PageManager::IsPageCached(const page_id_t &pageId)
{
    const auto& pageIterator = this->cache.find(pageId);
        
    if(pageIterator != this->cache.end())
    {
        this->pageList.push_front(*pageIterator->second);
        this->pageList.erase(pageIterator->second);
            
        this->cache[pageId] = this->pageList.begin();

        return true;
    }

    const auto& systemPageIterator = this->systemCache.find(pageId);

    if (systemPageIterator != this->systemCache.end())
    {
        this->systemPageList.push_front(*systemPageIterator->second);
        this->systemPageList.erase(systemPageIterator->second);
            
        this->systemCache[pageId] = this->systemPageList.begin();

        return true;
    }

    return false;
}

void PageManager::SetReadFilePointerToOffset(fstream *file,const streampos& offSet)
{
    file->clear();
    file->seekg(0, ios::beg);
    file->seekg(offSet);
}

void PageManager::SetWriteFilePointerToOffset(fstream *file, const streampos& offSet)
{
    file->clear();
    file->seekp(0, ios::beg);
    file->seekp(offSet);
}
