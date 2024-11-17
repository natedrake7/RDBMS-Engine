#include "PageManager.h"

PageManager::PageManager(FileManager* fileManager)
{
    this->fileManager = fileManager;
}

PageManager::~PageManager()
{
    for(size_t i = 0;i < this->pageList.size(); i++)
        this->RemovePage();
}

Page* PageManager::CreatePage(const int& pageId)
{
    Page* page = new Page(pageId);

    if(this->pageList.size() == MAX_NUMBER_OF_PAGES)
        this->RemovePage();

    this->pageList.push_front(page);
    this->cache[pageId] = this->pageList.begin();
    
    return page;
}

Page* PageManager::GetPage(const int& pageId, const string& filename)
{
    const auto& pageHashIterator = this->cache.find(pageId);

    if(pageHashIterator == this->cache.end())
        this->OpenPage(pageId, filename);
    else
    {
        this->pageList.push_front(*pageHashIterator->second);
        this->pageList.erase(pageHashIterator->second);
    }
    
    return *this->pageList.begin();
}

void PageManager::RemovePage()
{
    const auto& pageIterator = prev(this->pageList.end());

    this->cache.erase((*pageIterator)->GetPageId());

    if((*pageIterator)->GetPageDirtyStatus())
    {
        //if page dirty, write to disk
    }

    delete *pageIterator;

    this->pageList.erase(pageIterator);
}

void PageManager::OpenPage(const int& pageId, const string& filename)
{
    //read page from disk, call FileManager
    fstream* file = this->fileManager->GetFile(filename);

    const streampos pageOffset = pageId * PAGE_SIZE;
    vector<char> buffer(PAGE_SIZE);
    
    file->seekg(pageOffset);

    file->read(buffer.data(), PAGE_SIZE);

    if(file->fail())
        throw runtime_error("Error reading page");

    // Page* page = new Page(pageId);
    
}



