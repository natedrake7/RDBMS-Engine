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
    const string& filename = this->database->GetFileName();
    if(this->pageList.size() == MAX_NUMBER_OF_PAGES)
        this->RemovePage();

    this->pageList.push_front(page);
    this->cache[pageId] = this->pageList.begin();

    (*this->pageList.begin())->SetFileName(filename);

    return page;
}

Page* PageManager::GetPage(const int& pageId, const Table* table)
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

Page* PageManager::OpenPage(const int& pageId, const Table* table)
{
    const string& filename = this->database->GetFileName();
    if(this->pageList.size() == MAX_NUMBER_OF_PAGES)
        this->RemovePage();

    //read page from disk, call FileManager
    fstream* file = this->fileManager->GetFile(filename);

    const streampos pageOffset = pageId * PAGE_SIZE;
    vector<char> buffer(PAGE_SIZE);

    SetReadFilePointerToOffset(file, pageOffset);

    file->read(buffer.data(), PAGE_SIZE);

    // if(file->fail())
    //     throw runtime_error("Error reading page");

    Page* page = new Page(pageId);
    this->pageList.push_front(page);

    page->GetPageDataFromFile(buffer, table);

    this->cache[pageId] = this->pageList.begin();

    return page;
}

MetaDataPage* PageManager::OpenMetaDataPage(const string& filename)
{
    if(this->pageList.size() == MAX_NUMBER_OF_PAGES)
        this->RemovePage();

    //read page from disk, call FileManager
    fstream* file = this->fileManager->GetFile(filename);

    const streampos pageOffset = 0;
    vector<char> buffer(PAGE_SIZE);
\
    SetReadFilePointerToOffset(file, pageOffset);

    file->read(buffer.data(), PAGE_SIZE);

    // if(file->fail())
    //     throw runtime_error("Error reading page");

    MetaDataPage* page = new MetaDataPage(0);
    this->pageList.push_front(page);

    page->GetPageDataFromFile(buffer, nullptr);

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
