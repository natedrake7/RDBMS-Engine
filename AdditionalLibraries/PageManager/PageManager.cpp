#include "PageManager.h"

PageManager::PageManager(FileManager* fileManager)
{
    this->fileManager = fileManager;
    this->database = nullptr;
}

PageManager::~PageManager()
{
    const size_t pageListSize = this->pageList.size();

    for(size_t i = 0;i < pageListSize; i++)
        this->RemovePage();
}

void PageManager::BindDatabase(const Database *database) { this->database = database; }

Page* PageManager::CreatePage(const uint16_t& pageId)
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

MetaDataPage* PageManager::CreateMetaDataPage(const string& filename)
{
    MetaDataPage* page = new MetaDataPage(0);

    if(this->pageList.size() == MAX_NUMBER_OF_PAGES)
        this->RemovePage();

    page->SetFileName(filename);

    this->pageList.push_front(page);
    this->cache[0] = this->pageList.begin();

    return page;
}

LargeDataPage* PageManager::CreateLargeDataPage(const uint16_t &pageId)
{
    LargeDataPage* page = new LargeDataPage(pageId);
    const string& filename = this->database->GetFileName();

    if(this->pageList.size() == MAX_NUMBER_OF_PAGES)
        this->RemovePage();

    page->SetFileName(filename);

    this->pageList.push_front(page);
    this->cache[0] = this->pageList.begin();

    return page;
}

Page* PageManager::GetPage(const uint16_t& pageId, const Table* table)
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

MetaDataPage* PageManager::GetMetaDataPage(const string& filename)
{
    const auto& pageHashIterator = this->cache.find(0);

    if(pageHashIterator == this->cache.end())
        this->OpenMetaDataPage(filename);
    else
    {
        this->pageList.push_front(*pageHashIterator->second);
        this->pageList.erase(pageHashIterator->second);
        this->cache[0] = this->pageList.begin();
    }

    (*this->pageList.begin())->SetFileName(filename);

    return dynamic_cast<MetaDataPage*>(*this->pageList.begin());
}

LargeDataPage * PageManager::GetLargeDataPage(const uint16_t &pageId, const Table *table)
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

        const int pageId = (*pageIterator)->GetPageId();

        const streampos pageOffset = pageId * PAGE_SIZE;

        SetWriteFilePointerToOffset(file, pageOffset);

        (*pageIterator)->WritePageToFile(file);

        // if(file->fail())
        //     throw runtime_error("Error reading page");
    }

    this->cache.erase((*pageIterator)->GetPageId());

    delete *pageIterator;

    this->pageList.erase(pageIterator);
}

void PageManager::OpenPage(const uint16_t& pageId, const Table* table)
{
    const string& filename = this->database->GetFileName();

    if(this->pageList.size() == MAX_NUMBER_OF_PAGES)
        this->RemovePage();

    //read page from disk, call FileManager
    fstream* file = this->fileManager->GetFile(filename);

    const auto extent = pageId / EXTENT_SIZE; // >= 1 always
    const streampos extentOffset = extent * EXTENT_BYTE_SIZE;

    vector<char> buffer(EXTENT_BYTE_SIZE);

    SetReadFilePointerToOffset(file, extentOffset);

    //take into account the metadata page all the others
    file->read(buffer.data(), EXTENT_BYTE_SIZE);

    uint32_t offSet = 0;
    uint16_t currentPageId = extent * EXTENT_SIZE;

    for(int i = 0; i < EXTENT_SIZE; i++)
    {
        offSet = i * PAGE_SIZE;

        Page* page = new Page(currentPageId);

        this->pageList.push_front(page);

        page->GetPageDataFromFile(buffer, table, offSet);

        this->cache[currentPageId] = this->pageList.begin();

        page->SetFileName(filename);

        currentPageId = page->GetNextPageId();

        if(currentPageId <= 0)
            break;
    }

    const auto& pageIterator = this->cache.find(pageId);

    this->pageList.push_front(*pageIterator->second);
    this->pageList.erase(pageIterator->second);

    this->cache[pageId] = this->pageList.begin();
}

MetaDataPage* PageManager::OpenMetaDataPage(const string& filename)
{
    if(this->pageList.size() == MAX_NUMBER_OF_PAGES)
        this->RemovePage();

    //read page from disk, call FileManager
    fstream* file = this->fileManager->GetFile(filename);

    const streampos pageOffset = 0;
    vector<char> buffer(PAGE_SIZE);
    SetReadFilePointerToOffset(file, pageOffset);

    file->read(buffer.data(), PAGE_SIZE);

    // if(file->fail())
    //     throw runtime_error("Error reading page");

    MetaDataPage* page = new MetaDataPage(0);
    this->pageList.push_front(page);

    uint32_t offset = 0;
    page->GetPageDataFromFile(buffer, nullptr, offset);

    this->cache[0] = this->pageList.begin();

    return page;
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
