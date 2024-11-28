#include "PageManager.h"

#include "../../Database/Pages/GlobalAllocationMap/GlobalAllocationMapPage.h"

PageManager::PageManager(FileManager* fileManager)
{
    this->fileManager = fileManager;
    this->database = nullptr;
}

PageManager::~PageManager()
{
    const size_t pageListSize = this->pageList.size();
    const size_t systemPageListSize = this->systemPageList.size();

    for(size_t i = 0;i < pageListSize; i++)
        this->RemovePage();

    for(size_t i = 0;i < systemPageListSize; i++)
        this->RemoveSystemPage();
}

void PageManager::BindDatabase(const Database *database) { this->database = database; }

Page* PageManager::CreatePage(const page_id_t& pageId)
{
    Page* page = new Page(pageId);
    const string& filename = this->database->GetFileName();

    if(this->pageList.size() == MAX_NUMBER_OF_PAGES)
        this->RemovePage();

    this->pageList.push_front(page);
    this->cache[pageId] = this->pageList.begin();

    page->SetFileName(filename);

    return page;
}

LargeDataPage* PageManager::CreateLargeDataPage(const page_id_t &pageId)
{
    LargeDataPage* page = new LargeDataPage(pageId);
    const string& filename = this->database->GetFileName();

    if(this->pageList.size() == MAX_NUMBER_OF_PAGES)
        this->RemovePage();

    page->SetFileName(filename);

    this->pageList.push_front(page);
    this->cache[pageId] = this->pageList.begin();

    return page;
}

Page* PageManager::GetPage(const page_id_t& pageId, const Table* table)
{
    const auto& pageHashIterator = this->cache.find(pageId);

    if(pageHashIterator == this->cache.end())
        this->OpenPage(pageId, table);
    else
    {
        this->pageList.push_front(*pageHashIterator->second);
        this->pageList.erase(pageHashIterator->second);
        this->cache[pageId] = this->pageList.begin();
    }

    return *this->pageList.begin();
}

LargeDataPage* PageManager::GetLargeDataPage(const page_id_t &pageId, const Table *table)
{
    const auto& pageHashIterator = this->cache.find(pageId);

    if(pageHashIterator == this->cache.end())
        this->OpenPage(pageId, table);
    else
    {
        this->pageList.push_front(*pageHashIterator->second);
        this->pageList.erase(pageHashIterator->second);
        this->cache[pageId] = this->pageList.begin();
    }

    return dynamic_cast<LargeDataPage*>(*this->pageList.begin());
}

void PageManager::RemovePage()
{
    const auto& pageIterator = prev(this->pageList.end());

    if((*pageIterator)->GetPageDirtyStatus())
    {
        const string& filename = (*pageIterator)->GetFileName();
        fstream* file = this->fileManager->GetFile(filename);

        const page_id_t pageId = (*pageIterator)->GetPageId();

        const streampos pageOffset = pageId * PAGE_SIZE;

        SetWriteFilePointerToOffset(file, pageOffset);

        (*pageIterator)->WritePageToFile(file);
    }

    this->cache.erase((*pageIterator)->GetPageId());

    delete *pageIterator;

    this->pageList.erase(pageIterator);
}

void PageManager::OpenPage(const page_id_t& pageId, const Table* table)
{
    const string& filename = this->database->GetFileName();

    //read page from disk, call FileManager
    fstream* file = this->fileManager->GetFile(filename);

    const auto extent = pageId / EXTENT_SIZE; // >= 1 always
    const streampos extentOffset = ( extent * EXTENT_BYTE_SIZE ) + 3 * PAGE_SIZE;

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

        if(this->pageList.size() == MAX_NUMBER_OF_PAGES)
            this->RemovePage();

        const PageMetadata pageMetaData = PageManager::GetPageMetaDataFromFile(buffer, offSet);

        //page is already opened skip it
        if(this->cache.find(pageMetaData.pageId) != this->cache.end())
            continue;

        Page* page = nullptr;

        switch (pageMetaData.pageType)
        {
            case PageType::DATA:
                page = new Page(pageMetaData);
                break;
            case PageType::LOB:
                page = new LargeDataPage(pageMetaData);
                break;
            case PageType::INDEX:
                page = new IndexAllocationMapPage(pageMetaData, 0, 0);
                break;
            default:
                throw runtime_error("Page type not recognized");
        }

        page->GetPageDataFromFile(buffer, table, offSet, file);

        this->pageList.push_front(page);

        this->cache[page->GetPageId()] = this->pageList.begin();

        page->SetFileName(filename);
    }

    const auto& pageIterator = this->cache.find(pageId);

    this->pageList.push_front(*pageIterator->second);
    this->pageList.erase(pageIterator->second);

    this->cache[pageId] = this->pageList.begin();
}

////////////////////////////////////////////////////
////////////////////System Pages///////////////////
//////////////////////////////////////////////////

MetaDataPage* PageManager::CreateMetaDataPage(const string& filename)
{
    MetaDataPage* page = new MetaDataPage(0);

    if(this->systemPageList.size() == MAX_NUMBER_SYSTEM_PAGES)
        this->RemovePage();

    page->SetFileName(filename);

    this->systemPageList.push_front(page);
    this->systemCache[0] = this->systemPageList.begin();

    return page;
}

GlobalAllocationMapPage* PageManager::CreateGlobalAllocationMapPage(const string &filename)
{
    GlobalAllocationMapPage* page = new GlobalAllocationMapPage(1);

    if(this->systemPageList.size() == MAX_NUMBER_SYSTEM_PAGES)
        this->RemoveSystemPage();

    page->SetFileName(filename);

    this->systemPageList.push_front(page);
    this->systemCache[1] = this->systemPageList.begin();

    return page;
}

IndexAllocationMapPage * PageManager::CreateIndexAllocationMapPage(const table_id_t &tableId, const page_id_t& pageId)
{
    IndexAllocationMapPage* page = new IndexAllocationMapPage(tableId, pageId);

    const string& filename = this->database->GetFileName();
    
    if(this->systemPageList.size() == MAX_NUMBER_SYSTEM_PAGES)
        this->RemoveSystemPage();

    page->SetFileName(filename);

    this->systemPageList.push_front(page);
    this->systemCache[pageId] = this->systemPageList.begin();

    return page;
}

IndexAllocationMapPage* PageManager::GetIndexAllocationMapPage(const page_id_t &pageId)
{
    const auto& pageHashIterator = this->systemCache.find(pageId);
    const string& filename = this->database->GetFileName();
    
    if(pageHashIterator == this->systemCache.end())
        this->OpenIndexAllocationMapPage(pageId);
    else
    {
        this->systemPageList.push_front(*pageHashIterator->second);
        this->systemPageList.erase(pageHashIterator->second);
        this->systemCache[pageId] = this->systemPageList.begin();
    }

    (*this->systemPageList.begin())->SetFileName(filename);

    return dynamic_cast<IndexAllocationMapPage*>(*this->systemPageList.begin());
}

IndexAllocationMapPage* PageManager::OpenIndexAllocationMapPage(const page_id_t &pageId)
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

    const PageMetadata pageMetaData = PageManager::GetPageMetaDataFromFile(buffer, offSet);

    IndexAllocationMapPage* page = new IndexAllocationMapPage(pageMetaData, 0, 0);
    this->systemPageList.push_front(page);
    
    page->GetPageDataFromFile(buffer, nullptr, offSet, file);

    this->systemCache[pageId] = this->systemPageList.begin();

    return page;
}

MetaDataPage* PageManager::OpenMetaDataPage(const string& filename)
{
    if(this->systemPageList.size() == MAX_NUMBER_OF_PAGES)
        this->RemoveSystemPage();

    fstream* file = this->fileManager->GetFile(filename);

    const streampos pageOffset = 0;
    vector<char> buffer(PAGE_SIZE);
    SetReadFilePointerToOffset(file, pageOffset);

    file->read(buffer.data(), PAGE_SIZE);

    page_offset_t offSet = 0;

    const PageMetadata pageMetaData = PageManager::GetPageMetaDataFromFile(buffer, offSet);

    MetaDataPage* page = new MetaDataPage(pageMetaData);
    this->systemPageList.push_front(page);
    
    page->GetPageDataFromFile(buffer, nullptr, offSet, file);

    this->systemCache[0] = this->systemPageList.begin();

    return page;
}

GlobalAllocationMapPage* PageManager::OpenGlobalAllocationMapPage(const string& filename)
{
    if(this->systemPageList.size() == MAX_NUMBER_OF_PAGES)
        this->RemovePage();

    fstream* file = this->fileManager->GetFile(filename);

    vector<char> buffer(PAGE_SIZE);
    SetReadFilePointerToOffset(file, PAGE_SIZE);

    file->read(buffer.data(), PAGE_SIZE);
    page_offset_t offSet = 0;
    
    const PageMetadata pageMetaData = PageManager::GetPageMetaDataFromFile(buffer, offSet);
    
    GlobalAllocationMapPage* page = new GlobalAllocationMapPage(pageMetaData);
    
    page->GetPageDataFromFile(buffer, nullptr, offSet, file);

    this->systemPageList.push_front(page);

    this->systemCache[1] = this->systemPageList.begin();

    return page;
}

MetaDataPage* PageManager::GetMetaDataPage(const string& filename)
{
    const auto& pageHashIterator = this->systemCache.find(0);

    if(pageHashIterator == this->systemCache.end())
        this->OpenMetaDataPage(filename);
    else
    {
        this->systemPageList.push_front(*pageHashIterator->second);
        this->systemPageList.erase(pageHashIterator->second);
        this->systemCache[0] = this->systemPageList.begin();
    }

    (*this->systemPageList.begin())->SetFileName(filename);

    return dynamic_cast<MetaDataPage*>(*this->systemPageList.begin());
}

GlobalAllocationMapPage* PageManager::GetGlobalAllocationMapPage()
{
    const auto& pageHashIterator = this->systemCache.find(1);
    const string& filename = this->database->GetFileName();

    if(pageHashIterator == this->systemCache.end())
        this->OpenGlobalAllocationMapPage(filename);
    else
    {
        this->systemPageList.push_front(*pageHashIterator->second);
        this->systemPageList.erase(pageHashIterator->second);
        this->systemCache[1] = this->systemPageList.begin();
    }

    (*this->systemPageList.begin())->SetFileName(filename);

    return dynamic_cast<GlobalAllocationMapPage*>(*this->systemPageList.begin());
}

void PageManager::RemoveSystemPage()
{
    const auto& pageIterator = prev(this->systemPageList.end());

    if((*pageIterator)->GetPageDirtyStatus())
    {
        const string& filename = (*pageIterator)->GetFileName();
        fstream* file = this->fileManager->GetFile(filename);

        const page_id_t pageId = (*pageIterator)->GetPageId();

        const streampos pageOffset = pageId * PAGE_SIZE;

        SetWriteFilePointerToOffset(file, pageOffset);

        (*pageIterator)->WritePageToFile(file);
    }

    this->systemCache.erase((*pageIterator)->GetPageId());

    delete *pageIterator;

    this->systemPageList.erase(pageIterator);
}


PageMetadata PageManager::GetPageMetaDataFromFile(const vector<char> &data, page_offset_t &offSet)
{
    PageMetadata pageMetaData;
    memcpy(&pageMetaData.pageId, data.data() + offSet, sizeof(page_id_t));
    offSet += sizeof(page_id_t);

    memcpy(&pageMetaData.nextPageId, data.data() + offSet, sizeof(page_id_t));
    offSet += sizeof(page_id_t);

    memcpy(&pageMetaData.pageSize, data.data() + offSet, sizeof(page_size_t));
    offSet += sizeof(page_size_t);

    memcpy(&pageMetaData.bytesLeft, data.data() + offSet, sizeof(page_size_t));
    offSet += sizeof(page_size_t);

    memcpy(&pageMetaData.pageType, data.data() + offSet, sizeof(PageType));
    offSet += sizeof(PageType);

    return pageMetaData;
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
