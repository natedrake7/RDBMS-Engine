#include "StorageManager.h"
#include "../../Constants.h"
#include "../../Database.h"
#include "../../Pages/GlobalAllocationMap/GlobalAllocationMapPage.h"
#include "../../Pages/Header/HeaderPage.h"
#include "../../Pages/IndexMapAllocation/IndexAllocationMapPage.h"
#include "../../Pages/IndexPage/IndexPage.h"
#include "../../Pages/LargeObject/LargeDataPage.h"
#include "../../Pages/Page.h"
#include "../../Pages/PageFreeSpace/PageFreeSpacePage.h"

using namespace DatabaseEngine;
using namespace DatabaseEngine::StorageTypes;
using namespace Pages;

namespace Storage {

bool StorageManager::IsSystemCacheFull() const { return this->systemCache.size() == MAX_NUMBER_SYSTEM_PAGES; }

StorageManager::StorageManager() 
{
  this->database = nullptr;
  this->cacheReaders = 0;
  this->cacheWriters = 0;
  this->dataReaders = 0;
  this->dataWriters = 0;
}

StorageManager::~StorageManager() 
{
  const size_t pageListSize = this->pageList.size();
  const size_t systemPageListSize = this->systemPageList.size();

  for (size_t i = 0; i < pageListSize; i++)
    this->RemovePage();

  for (size_t i = 0; i < systemPageListSize; i++)
    this->RemoveSystemPage();
}

StorageManager& StorageManager::Get()
{
  static StorageManager storageManager;

  return storageManager;
}

void StorageManager::BindDatabase(const Database *database) 
{
  this->database = database;
}

void StorageManager::CreateFile(const string& fileName, const string& extension)
{
  this->fileManager.CreateFile(fileName, extension);
}

Page *StorageManager::GetPage(const page_id_t &pageId, const extent_id_t &extentId,
                           const Table *table) {
  this->LockPageRead();

  auto pageHashIterator = this->cache.find(pageId);

  if (pageHashIterator == this->cache.end()) 
  {
    this->OpenExtent(extentId, table);
    pageHashIterator = this->cache.find(pageId);
  }

  // assign work to writer thread

  this->pageList.push_front(*pageHashIterator->second);
  this->pageList.erase(pageHashIterator->second);
  this->cache[pageId] = this->pageList.begin();

  Page *page = *pageHashIterator->second;

  this->UnlockPageRead();

  return page;
}

LargeDataPage *StorageManager::GetLargeDataPage(const page_id_t &pageId, const extent_id_t &extentId, const Table *table) 
{
  return dynamic_cast<LargeDataPage *>(this->GetPage(pageId, extentId, table));
}

Page *StorageManager::CreatePage(const page_id_t &pageId) 
{
  Page *page = new Page(pageId, true);

  const string &filename = this->database->GetFileName();

  this->MovePageToFrontOfList(page, pageId, filename);

  return page;
}

LargeDataPage *StorageManager::CreateLargeDataPage(const page_id_t &pageId) 
{
  LargeDataPage *page = new LargeDataPage(pageId, true);

  const string &filename = this->database->GetFileName();

  this->MovePageToFrontOfList(page, pageId, filename);

  return page;
}

void StorageManager::RemovePage() 
{
  Page *page = this->pageList.back();
  const page_id_t pageId = page->GetPageId();

  if (page->GetPageDirtyStatus()) 
  {
    const string &filename = page->GetFileName();

    fstream *file = this->fileManager.GetFile(filename);

    const streampos pageOffset = pageId * PAGE_SIZE;

    SetWriteFilePointerToOffset(file, pageOffset);

    page->WritePageToFile(file);
  }

  this->cache.erase(pageId);

  this->pageList.pop_back();

  delete page;
}

void StorageManager::OpenExtent(const extent_id_t &extentId, const Table *table) 
{
  const string &filename = this->database->GetFileName();

  // read page from disk, call this->fileManager
  fstream *file = this->fileManager.GetFile(filename);

  const page_id_t firstExtentPageId = DatabaseEngine::Database::CalculateSystemPageOffsetByExtentId(extentId);

  const streampos extentOffset = firstExtentPageId * PAGE_SIZE;

  vector<char> buffer(EXTENT_BYTE_SIZE);

  SetReadFilePointerToOffset(file, extentOffset);

  // take into account the metadata page all the others
  file->read(buffer.data(), EXTENT_BYTE_SIZE);

  page_offset_t offSet = 0;

  const auto &bytesRead = file->gcount();

  for (int i = 0; i < EXTENT_SIZE; i++) 
  {
    offSet = i * PAGE_SIZE;

    if (bytesRead < offSet)
      break;

    const page_id_t currentPageId = firstExtentPageId + i;

    if (this->IsPageCached(currentPageId))
      continue;

    if (this->pageList.size() == MAX_NUMBER_OF_PAGES)
      this->RemovePage();

    const PageHeader pageHeader = StorageManager::GetPageHeaderFromFile(buffer, offSet);

    Page *page = nullptr;

    StorageManager::AllocateMemoryBasedOnPageType(&page, pageHeader);

    page->GetPageDataFromFile(buffer, table, offSet, file);

    if(pageHeader.pageType == PageType::IAM)
    {
      this->systemPageList.push_front(page);

      this->systemCache[pageHeader.pageId] = this->systemPageList.begin();
    }
    else 
    {
      this->pageList.push_front(page);

      this->cache[page->GetPageId()] = this->pageList.begin();
    }
    page->SetFileName(filename);

  }
}

////////////////////////////////////////////////////
////////////////////System Pages///////////////////
//////////////////////////////////////////////////

HeaderPage *StorageManager::CreateHeaderPage(const string &filename) 
{
  const page_id_t pageId = 0;

  HeaderPage *page = new HeaderPage(pageId);

  this->MovePageToFrontOfSystemList(page, pageId, filename);

  return page;
}

GlobalAllocationMapPage *StorageManager::CreateGlobalAllocationMapPage(const string &filename, const page_id_t &pageId) 
{
  GlobalAllocationMapPage *page = new GlobalAllocationMapPage(pageId);

  this->MovePageToFrontOfSystemList(page, pageId, filename);

  return page;
}

GlobalAllocationMapPage *StorageManager::CreateGlobalAllocationMapPage(const page_id_t &pageId) 
{
  GlobalAllocationMapPage *page = new GlobalAllocationMapPage(pageId);

  const string &filename = this->database->GetFileName();

  this->MovePageToFrontOfSystemList(page, pageId, filename);

  return page;
}

IndexAllocationMapPage *StorageManager::CreateIndexAllocationMapPage(const table_id_t &tableId, const page_id_t &pageId, const extent_id_t &startingExtentId) 
{
  IndexAllocationMapPage *page = new IndexAllocationMapPage(tableId, pageId, startingExtentId);

  const string &filename = this->database->GetFileName();

  this->MovePageToFrontOfSystemList(page, pageId, filename);

  return page;
}

PageFreeSpacePage *StorageManager::CreatePageFreeSpacePage(const page_id_t &pageId) 
{
  PageFreeSpacePage *page = new PageFreeSpacePage(pageId);

  const string &filename = this->database->GetFileName();

  this->MovePageToFrontOfSystemList(page, pageId, filename);

  return page;
}

PageFreeSpacePage *StorageManager::CreatePageFreeSpacePage(const string &filename, const page_id_t &pageId) 
{
  PageFreeSpacePage *page = new PageFreeSpacePage(pageId);

  this->MovePageToFrontOfSystemList(page, pageId, filename);

  return page;
}

IndexPage *StorageManager::CreateIndexPage(const page_id_t &pageId) 
{
  IndexPage *page = new IndexPage(pageId, true);

  this->MovePageToFrontOfSystemList(page, pageId);

  return page;
}

HeaderPage *StorageManager::GetHeaderPage(const string &filename) 
{
  constexpr page_id_t pageId = 0;

  HeaderPage *page =
      dynamic_cast<HeaderPage *>(this->GetSystemPage(pageId, filename));
  return page;
}

PageFreeSpacePage *StorageManager::GetPageFreeSpacePage(const page_id_t &pageId) 
{
  return dynamic_cast<PageFreeSpacePage *>(this->GetSystemPage(pageId));
}

IndexPage *StorageManager::GetIndexPage(const page_id_t &pageId) 
{
  return dynamic_cast<IndexPage *>(this->GetSystemPage(pageId));
}

IndexPage *StorageManager::GetIndexPage(const page_id_t &pageId, const extent_id_t &extentId) 
{
  return dynamic_cast<IndexPage *>(this->GetSystemPage(pageId, extentId));
}

bool StorageManager::IsCacheFull() const 
{
  return this->pageList.size() == MAX_NUMBER_OF_PAGES;
}

IndexAllocationMapPage *StorageManager::GetIndexAllocationMapPage(const page_id_t &pageId) 
{
  return dynamic_cast<IndexAllocationMapPage *>(this->GetSystemPage(pageId));
}

GlobalAllocationMapPage *StorageManager::GetGlobalAllocationMapPage(const page_id_t &pageId) 
{
  return dynamic_cast<GlobalAllocationMapPage *>(this->GetSystemPage(pageId));
}

Page *StorageManager::GetSystemPage(const page_id_t &pageId) 
{
  this->LockSystemPageWrite();

  auto pageHashIterator = this->systemCache.find(pageId);
  const string &filename = this->database->GetFileName();

  if (pageHashIterator == this->systemCache.end()) 
  {
    this->OpenSystemPage(pageId);
    pageHashIterator = this->systemCache.find(pageId);
  }

  this->systemPageList.push_front(*pageHashIterator->second);
  this->systemPageList.erase(pageHashIterator->second);
  this->systemCache[pageId] = this->systemPageList.begin();

  (*pageHashIterator->second)->SetFileName(filename);

  Page *page = *pageHashIterator->second;

  this->UnlockSystemPageWrite();

  return page;
}

Page *StorageManager::GetSystemPage(const page_id_t &pageId, const string &filename) 
{
  auto pageHashIterator = this->SearchSystemPageInCache(pageId);

  if (pageHashIterator == this->systemCache.end()) {
    this->OpenSystemPage(pageId, filename);

    pageHashIterator = this->SearchSystemPageInCache(pageId);
  }
  this->systemPageList.push_front(*pageHashIterator->second);
  this->systemPageList.erase(pageHashIterator->second);
  this->systemCache[pageId] = this->systemPageList.begin();
  // this->MoveSystemPageToStart(pageHashIterator->second);

  (*pageHashIterator->second)->SetFileName(filename);

  return (*pageHashIterator->second);
}

Page *StorageManager::GetSystemPage(const page_id_t &pageId, const extent_id_t &extentId) 
{
  auto pageHashIterator = this->SearchSystemPageInCache(pageId);

  if (pageHashIterator == this->systemCache.end()) 
  {
    this->OpenSystemExtent(extentId);
    pageHashIterator = this->SearchSystemPageInCache(pageId);
  }

  this->systemPageList.push_front(*pageHashIterator->second);
  this->systemPageList.erase(pageHashIterator->second);
  this->systemCache[pageId] = this->systemPageList.begin();
  // this->MoveSystemPageToStart(pageHashIterator->second);

  return (*pageHashIterator->second);
}

void StorageManager::OpenSystemPage(const page_id_t &pageId) 
{
  if (this->IsSystemCacheFull())
    this->RemoveSystemPage();

  const string &filename = this->database->GetFileName();

  fstream *file = this->fileManager.GetFile(filename);

  const streampos pageOffset = pageId * PAGE_SIZE;
  vector<char> buffer(PAGE_SIZE);
  SetReadFilePointerToOffset(file, pageOffset);

  file->read(buffer.data(), PAGE_SIZE);

  page_offset_t offSet = 0;

  const PageHeader pageHeader =
      StorageManager::GetPageHeaderFromFile(buffer, offSet);

  Page *page = nullptr;

  StorageManager::AllocateMemoryBasedOnSystemPageType(&page, pageHeader);

  // this->LockSystemPageWrite();

  this->systemPageList.push_front(page);
  this->systemCache[pageId] = this->systemPageList.begin();

  // this->UnlockSystemPageWrite();

  page->GetPageDataFromFile(buffer, nullptr, offSet, file);
}

void StorageManager::OpenSystemExtent(const Constants::extent_id_t &extentId) 
{
  const string &filename = this->database->GetFileName();

  // read page from disk, call this->fileManager
  fstream *file = this->fileManager.GetFile(filename);

  const page_id_t firstExtentPageId = DatabaseEngine::Database::CalculateSystemPageOffsetByExtentId(extentId);

  const streampos extentOffset = firstExtentPageId * PAGE_SIZE;

  vector<char> buffer(EXTENT_BYTE_SIZE);

  SetReadFilePointerToOffset(file, extentOffset);

  // take into account the metadata page all the others
  file->read(buffer.data(), EXTENT_BYTE_SIZE);

  page_offset_t offSet = 0;

  const auto &bytesRead = file->gcount();

  for (int i = 0; i < EXTENT_SIZE; i++) {
    offSet = i * PAGE_SIZE;

    if (bytesRead < offSet)
      break;

    const page_id_t currentPageId = firstExtentPageId + i;

    if (this->IsPageCached(currentPageId))
      continue;

    if (this->systemPageList.size() == MAX_NUMBER_SYSTEM_PAGES)
        this->RemoveSystemPage();

    PageHeader pageHeader = StorageManager::GetPageHeaderFromFile(buffer, offSet);

    Page *page = nullptr;

    StorageManager::AllocateMemoryBasedOnSystemPageType(&page, pageHeader);

    page->GetPageDataFromFile(buffer, nullptr, offSet, file);

    this->systemPageList.push_front(page);

    this->systemCache[page->GetPageId()] = this->systemPageList.begin();

    page->SetFileName(filename);
  }
}

void StorageManager::OpenSystemPage(const page_id_t &pageId,const string &filename) 
{
  if (this->IsSystemCacheFull())
    this->RemoveSystemPage();

  fstream *file = this->fileManager.GetFile(filename);

  const streampos pageOffset = pageId * PAGE_SIZE;
  vector<char> buffer(PAGE_SIZE);
  SetReadFilePointerToOffset(file, pageOffset);

  file->read(buffer.data(), PAGE_SIZE);

  page_offset_t offSet = 0;

  const PageHeader pageHeader =
      StorageManager::GetPageHeaderFromFile(buffer, offSet);

  Page *page = nullptr;
  StorageManager::AllocateMemoryBasedOnSystemPageType(&page, pageHeader);

  // this->LockSystemPageWrite();

  this->systemPageList.push_front(page);

  this->systemCache[pageId] = this->systemPageList.begin();

  // this->UnlockSystemPageWrite();

  page->GetPageDataFromFile(buffer, nullptr, offSet, file);
}

void StorageManager::RemoveSystemPage() 
{
  // this->LockSystemPageWrite();

  Page *page = this->systemPageList.back();

  const page_id_t pageId = page->GetPageId();

  this->systemCache.erase(pageId);

  this->systemPageList.pop_back();

  // this->UnlockSystemPageWrite();

  if (page->GetPageDirtyStatus()) 
  {
    const string &filename = page->GetFileName();

    fstream *file = this->fileManager.GetFile(filename);

    const streampos pageOffset = pageId * PAGE_SIZE;

    SetWriteFilePointerToOffset(file, pageOffset);

    page->WritePageToFile(file);
  }

  delete page;
}

void StorageManager::AllocateMemoryBasedOnSystemPageType(Page **page, const PageHeader &pageHeader) 
{
  switch (pageHeader.pageType) 
  {
    case PageType::GAM:
      *page = new GlobalAllocationMapPage(pageHeader);
      break;
    case PageType::IAM:
      *page = new IndexAllocationMapPage(pageHeader, 0, 0);
      break;
    case PageType::METADATA:
      *page = new HeaderPage(pageHeader);
      break;
    case PageType::FREESPACE:
      *page = new PageFreeSpacePage(pageHeader);
      break;
    case PageType::INDEX:
      *page = new IndexPage(pageHeader);
      break;
    default:
      throw runtime_error("Page type not recognized");
  }
}

////////////////////////////////////////////////////////////////////
/////////////////////////Globally Used Functions///////////////////
//////////////////////////////////////////////////////////////////

unordered_map<page_id_t, PageIterator>::iterator
StorageManager::SearchSystemPageInCache(const page_id_t &pageId) 
{
  // Page* page = nullptr;

  // this->LockSystemPageRead();

  return this->systemCache.find(pageId);

  // this->UnlockSystemPageRead();
}

void StorageManager::MoveSystemPageToStart(const PageIterator &pageIterator) 
{
  // this->LockSystemPageWrite();

  this->systemPageList.push_front(*pageIterator);
  this->systemPageList.erase(pageIterator);
  this->systemCache[(*pageIterator)->GetPageId()] = this->systemPageList.begin();

  // this->UnlockSystemPageWrite();
}

void StorageManager::AllocateMemoryBasedOnPageType(Page **page, const PageHeader &pageHeader) 
{
  switch (pageHeader.pageType) 
  {
    case PageType::DATA:
      *page = new Page(pageHeader);
      break;
    case PageType::LOB:
      *page = new LargeDataPage(pageHeader);
      break;
    case PageType::IAM:
      *page = new IndexAllocationMapPage(pageHeader, 0, 0);
      break;
    default:
      throw runtime_error("Page type not recognized");
  }
}

void StorageManager::MovePageToFrontOfSystemList(Page *page,
                                              const page_id_t &pageId,
                                              const string &filename) {
  if (this->systemPageList.size() == MAX_NUMBER_SYSTEM_PAGES)
    this->RemoveSystemPage();

  page->SetFileName(filename);

  this->systemPageList.push_front(page);
  this->systemCache[pageId] = this->systemPageList.begin();
}

void StorageManager::MovePageToFrontOfSystemList(Page *page,
                                              const page_id_t &pageId) {
  if (this->systemPageList.size() == MAX_NUMBER_SYSTEM_PAGES)
    this->RemoveSystemPage();

  const string &filename = this->database->GetFileName();

  page->SetFileName(filename);

  this->systemPageList.push_front(page);
  this->systemCache[pageId] = this->systemPageList.begin();
}

void StorageManager::MovePageToFrontOfList(Page *page, const page_id_t &pageId,
                                        const string &filename) {
  if (this->pageList.size() == MAX_NUMBER_SYSTEM_PAGES)
    this->RemovePage();

  page->SetFileName(filename);

  this->pageList.push_front(page);
  this->cache[pageId] = this->pageList.begin();
}

PageHeader StorageManager::GetPageHeaderFromFile(const vector<char> &data,
                                              page_offset_t &offSet) {
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

bool StorageManager::IsPageCached(const page_id_t &pageId) 
{
  const auto &pageIterator = this->cache.find(pageId);

  if (pageIterator != this->cache.end()) 
  {
    this->pageList.push_front(*pageIterator->second);
    this->pageList.erase(pageIterator->second);

    this->cache[pageId] = this->pageList.begin();

    return true;
  }

  const auto &systemPageIterator = this->systemCache.find(pageId);

  if (systemPageIterator != this->systemCache.end()) 
  {
    this->systemPageList.push_front(*systemPageIterator->second);
    this->systemPageList.erase(systemPageIterator->second);

    this->systemCache[pageId] = this->systemPageList.begin();

    return true;
  }

  return false;
}

void StorageManager::SetReadFilePointerToOffset(fstream *file, const streampos &offSet) {
  file->clear();
  file->seekg(0, ios::beg);
  file->seekg(offSet);
}

void StorageManager::SetWriteFilePointerToOffset(fstream *file, const streampos &offSet) 
{
  file->clear();
  file->seekp(0, ios::beg);
  file->seekp(offSet);
}

///////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////Thread
/// Synchronization//////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////

void StorageManager::LockSystemPageRead() 
{
  unique_lock<mutex> lock(this->systemPageListMutex);

  this->systemConditionVariable.wait(
      lock, [this]() { return this->cacheWriters == 0; });

  this->cacheReaders++;
}

void StorageManager::UnlockSystemPageRead() 
{
  unique_lock<mutex> lock(this->systemPageListMutex);
  this->cacheReaders--;

  if (this->cacheReaders == 0)
    this->systemConditionVariable.notify_all();
}

void StorageManager::LockSystemPageWrite() 
{
  unique_lock<mutex> lock(this->systemPageListMutex);

  this->systemConditionVariable.wait(lock, [this]() {
    return this->cacheReaders == 0 && this->cacheWriters == 0;
  });

  this->cacheWriters++;
}

void StorageManager::UnlockSystemPageWrite() 
{
  unique_lock<mutex> lock(this->systemPageListMutex);

  this->cacheWriters--;

  this->systemConditionVariable.notify_all();
}

void StorageManager::LockPageRead() 
{
  unique_lock<mutex> lock(this->pageListMutex);

  this->dataConditionVariable.wait(lock,
                                   [this]() { return this->dataWriters == 0; });

  this->dataReaders++;
}

void StorageManager::UnlockPageRead() 
{
  unique_lock<mutex> lock(this->pageListMutex);
  this->dataReaders--;

  if (this->dataReaders == 0)
    this->dataConditionVariable.notify_all();
}

void StorageManager::LockPageWrite() 
{
  unique_lock<mutex> lock(this->pageListMutex);

  this->dataConditionVariable.wait(lock, [this]() {
    return this->dataReaders == 0 && this->dataWriters == 0;
  });

  this->dataWriters++;
}

void StorageManager::UnlockPageWrite() 
{
  unique_lock<mutex> lock(this->pageListMutex);

  this->dataWriters--;

  this->dataConditionVariable.notify_all();
}
} // namespace Storage