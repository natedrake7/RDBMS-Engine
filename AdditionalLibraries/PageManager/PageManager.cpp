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

Page* PageManager::CreatePage(const int& pageId)
{
    Page* page = new Page(pageId);
    if(this->pageList.size() == MAX_NUMBER_OF_PAGES)
        this->RemovePage();

    this->pageList.push_front(page);
    this->cache[pageId] = this->pageList.begin();
    
    return page;
}

Page* PageManager::GetPage(const int& pageId, const Table* table)
{
    const auto& pageHashIterator = this->cache.find(pageId);
    const string& filename = this->database->GetFileName();

    if(pageHashIterator == this->cache.end())
        this->OpenPage(pageId, table);
    else
    {
        this->pageList.push_front(*pageHashIterator->second);
        this->pageList.erase(pageHashIterator->second);
        this->cache[pageId] = this->pageList.begin();
    }

    (*this->pageList.begin())->SetFileName(filename);
    
    return *this->pageList.begin();
}

MetaDataPage* PageManager::GetMetaDataPage()
{
    const auto& pageHashIterator = this->cache.find(0);
    const string& filename = this->database->GetFileName();

    if(pageHashIterator == this->cache.end())
        this->OpenMetaDataPage();
    else
    {
        this->pageList.push_front(*pageHashIterator->second);
        this->pageList.erase(pageHashIterator->second);
        this->cache[0] = this->pageList.begin();
    }

    (*this->pageList.begin())->SetFileName(filename);

    return dynamic_cast<MetaDataPage*>(*this->pageList.begin());
}

MetaDataPage* PageManager::CreateMetaDataPage()
{
    const string &filename = this->database->GetFileName();
    MetaDataPage* page = new MetaDataPage(0);

    if(this->pageList.size() == MAX_NUMBER_OF_PAGES)
        this->RemovePage();

    page->SetFileName(filename);

    this->pageList.push_front(page);
    this->cache[0] = this->pageList.begin();

    return page;
}

void PageManager::RemovePage()
{
    const auto& pageIterator = prev(this->pageList.end());

    if((*pageIterator)->GetPageDirtyStatus())
    {
        fstream* file = this->fileManager->GetFile((*pageIterator)->GetFileName());

        const int pageId = (*pageIterator)->GetPageId();

        const streampos pageOffset = pageId * PAGE_SIZE;

        file->seekp(pageOffset);

        (*pageIterator)->WritePageToFile(file);

        // if(file->fail())
        //     throw runtime_error("Error reading page");
    }

    this->cache.erase((*pageIterator)->GetPageId());

    delete *pageIterator;

    this->pageList.erase(pageIterator);
}

Page* PageManager::OpenPage(const int& pageId, const Table* table)
{
    const string& filename = this->database->GetFileName();
    if(this->pageList.size() == MAX_NUMBER_OF_PAGES)
        this->RemovePage();

    //read page from disk, call FileManager
    fstream* file = this->fileManager->GetFile(filename);

    const streampos pageOffset = pageId * PAGE_SIZE;
    vector<char> buffer(PAGE_SIZE);
    
    file->seekg(pageOffset);

    file->read(buffer.data(), PAGE_SIZE);

    if(file->fail())
        throw runtime_error("Error reading page");

    Page* page = new Page(pageId);
    this->pageList.push_front(page);

    page->GetPageDataFromFile(buffer, table);

    this->cache[pageId] = this->pageList.begin();

    return page;
}

MetaDataPage* PageManager::OpenMetaDataPage()
{
    if(this->pageList.size() == MAX_NUMBER_OF_PAGES)
        this->RemovePage();

    const string& filename = this->database->GetFileName();

    //read page from disk, call FileManager
    fstream* file = this->fileManager->GetFile(filename);

    const streampos pageOffset = 0;
    vector<char> buffer(PAGE_SIZE);

    file->seekg(pageOffset);

    file->read(buffer.data(), PAGE_SIZE);

    // if(file->fail())
    //     throw runtime_error("Error reading page");

    MetaDataPage* page = new MetaDataPage(0);
    this->pageList.push_front(page);

    page->GetPageDataFromFile(buffer, nullptr);

    this->cache[0] = this->pageList.begin();

    return page;
}