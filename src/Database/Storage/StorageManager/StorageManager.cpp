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

#include <cstring>
#include <iostream>

using namespace DatabaseEngine;
using namespace DatabaseEngine::StorageTypes;
using namespace Pages;

namespace Storage {

bool StorageManager::IsSystemCacheFull() const { return this->systemCache.size() == MAX_NUMBER_SYSTEM_PAGES; }

StorageManager::StorageManager() 
{
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

void StorageManager::CreateFile(const string& fileName, const string& extension)
{
  this->fileManager.CreateFile(fileName, extension);
}

Page *StorageManager::GetPage(
  const string& filename,
  const page_id_t &pageId,
  const extent_id_t &extentId,
  const Table *table) {
  this->LockPageRead();

  const auto key = filename + to_string(pageId);

  auto pageHashIterator = this->cache.find(key);

  if (pageHashIterator == this->cache.end()) 
  {
    this->OpenExtent(filename, extentId, table);
    pageHashIterator = this->cache.find(key);
  }

  // assign work to writer thread

  this->pageList.push_front(*pageHashIterator->second);
  this->pageList.erase(pageHashIterator->second);
  this->cache[key] = this->pageList.begin();

  Page *page = *pageHashIterator->second;

  this->UnlockPageRead();

  return page;
}

LargeDataPage *StorageManager::GetLargeDataPage(const string& filename, const page_id_t &pageId, const extent_id_t &extentId, const Table *table)
{
  return dynamic_cast<LargeDataPage *>(this->GetPage(filename, pageId, extentId, table));
}

OverflowPage *StorageManager::GetOverflowPage(const string& filename, const page_id_t &pageId, const extent_id_t &extentId, const Table *table)
{
  return dynamic_cast<OverflowPage *>(this->GetPage(filename, pageId, extentId, table));
}

Page *StorageManager::CreatePage(const string& filename, const page_id_t &pageId)
{
  Page *page = new Page(pageId, true);
  page->SetDirty();

  this->MovePageToFrontOfList(page, pageId, filename);

  return page;
}

LargeDataPage *StorageManager::CreateLargeDataPage(const string& filename, const page_id_t &pageId)
{
  LargeDataPage *page = new LargeDataPage(pageId, true);
  page->SetDirty();

  this->MovePageToFrontOfList(page, pageId, filename);

  return page;
}

Pages::OverflowPage* StorageManager::CreateOverflowPage(const string & filename, const page_id_t & pageId){
  auto* page = new OverflowPage(pageId, true);
  page->SetDirty();

  this->MovePageToFrontOfList(page, pageId, filename);

  return page;
}

void StorageManager::RemovePage() 
{
  Page *page = this->pageList.back();
  const page_id_t pageId = page->GetPageId();

  const string &filename = page->GetFileName();

  const auto key = filename + to_string(pageId);

  if (page->GetPageDirtyStatus()) 
  {
    fstream *file = this->fileManager.GetFile(filename);

    const streampos pageOffset = pageId * PAGE_SIZE;

    SetWriteFilePointerToOffset(file, pageOffset);

    page->WritePageToFile(file);
  }

  this->cache.erase(key);

  this->pageList.pop_back();

  delete page;

  page = nullptr;
}

void StorageManager::OpenExtent(const string& filename, const extent_id_t &extentId, const Table *table)
{
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

    if (this->IsPageCached(filename, currentPageId))
      continue;

    if (this->pageList.size() == MAX_NUMBER_OF_PAGES)
      this->RemovePage();

    const PageHeader pageHeader = StorageManager::GetPageHeaderFromFile(buffer, offSet);

    Page *page = nullptr;

    if(!StorageManager::AllocateMemoryBasedOnPageType(&page, pageHeader)){
      std::cerr << "Failed to read page id: " << currentPageId << endl;
//      throw runtime_error("failed to read page");
    }

    page->GetPageDataFromFile(buffer, table, offSet, file);

    this->pageList.push_front(page);

    const auto key = filename + to_string(page->GetPageId());

    this->cache[key] = this->pageList.begin();

    page->SetFileName(filename);

  }
}

////////////////////////////////////////////////////
////////////////////System Pages///////////////////
//////////////////////////////////////////////////

HeaderPage *StorageManager::CreateHeaderPage(const string &filename) 
{
   constexpr page_id_t pageId = 0;

  HeaderPage *page = new HeaderPage(pageId);
  page->SetDirty();
  
  this->MovePageToFrontOfSystemList(page, pageId, filename);

  return page;
}

GlobalAllocationMapPage *StorageManager::CreateGlobalAllocationMapPage(const string &filename, const page_id_t &pageId) 
{
  GlobalAllocationMapPage *page = new GlobalAllocationMapPage(pageId);
  page->SetDirty();
  
  this->MovePageToFrontOfSystemList(page, pageId, filename);

  return page;
}

IndexAllocationMapPage *StorageManager::CreateIndexAllocationMapPage(
  const string& filename,
  const table_id_t &tableId,
  const page_id_t &pageId,
  const extent_id_t &startingExtentId)
{
  IndexAllocationMapPage *page = new IndexAllocationMapPage(tableId, pageId, startingExtentId);
  page->SetDirty();
  
  this->MovePageToFrontOfList(page, pageId, filename);

  return page;
}


PageFreeSpacePage *StorageManager::CreatePageFreeSpacePage(const string &filename, const page_id_t &pageId) 
{
  PageFreeSpacePage *page = new PageFreeSpacePage(pageId);
  page->SetDirty();
  
  this->MovePageToFrontOfSystemList(page, pageId, filename);

  return page;
}

IndexPage *StorageManager::CreateIndexPage(const string& filename, const page_id_t &pageId)
{
  IndexPage *page = new IndexPage(pageId, true);
  page->SetDirty();
  
  this->MovePageToFrontOfList(page, pageId, filename);

  return page;
}

HeaderPage *StorageManager::GetHeaderPage(const string &filename) 
{
  constexpr page_id_t pageId = 0;

  return dynamic_cast<HeaderPage *>(this->GetSystemPage(pageId, filename));
}

PageFreeSpacePage *StorageManager::GetPageFreeSpacePage(const string& filename, const page_id_t &pageId)
{
  return dynamic_cast<PageFreeSpacePage *>(this->GetSystemPage(filename, pageId));
}

IndexPage *StorageManager::GetIndexPage(const string& filename, const page_id_t &pageId, const extent_id_t &extentId, const Table* table)
{
  return dynamic_cast<IndexPage *>(this->GetPage(filename, pageId, extentId, table));
}

bool StorageManager::IsCacheFull() const 
{
  return this->pageList.size() == MAX_NUMBER_OF_PAGES;
}

IndexAllocationMapPage *StorageManager::GetIndexAllocationMapPage(const string& filename, const page_id_t &pageId, const Constants::extent_id_t &extentId, const DatabaseEngine::StorageTypes::Table *table)
{
  return dynamic_cast<IndexAllocationMapPage *>(this->GetPage(filename, pageId, extentId, table));
}

GlobalAllocationMapPage *StorageManager::GetGlobalAllocationMapPage(const string& filename, const page_id_t &pageId)
{
  return dynamic_cast<GlobalAllocationMapPage *>(this->GetSystemPage(filename, pageId));
}

Page *StorageManager::GetSystemPage(const string& filename, const page_id_t &pageId)
{
  this->LockSystemPageWrite();

  const auto key = filename + to_string(pageId);

  auto pageHashIterator = this->systemCache.find(key);

  if (pageHashIterator == this->systemCache.end())
  {
    this->OpenSystemPage(filename, pageId);
    pageHashIterator = this->systemCache.find(key);
  }

  this->systemPageList.push_front(*pageHashIterator->second);
  this->systemPageList.erase(pageHashIterator->second);
  this->systemCache[key] = this->systemPageList.begin();

  (*pageHashIterator->second)->SetFileName(filename);

  Page *page = *pageHashIterator->second;

  this->UnlockSystemPageWrite();

  return page;
}

Page *StorageManager::GetSystemPage(const page_id_t &pageId, const string &filename) 
{
  const auto key = filename + to_string(pageId);

  auto pageHashIterator = this->SearchSystemPageInCache(key);

  if (pageHashIterator == this->systemCache.end()) {
    this->OpenSystemPage(filename, pageId);

    pageHashIterator = this->SearchSystemPageInCache(key);
  }
  this->systemPageList.push_front(*pageHashIterator->second);
  this->systemPageList.erase(pageHashIterator->second);
  this->systemCache[key] = this->systemPageList.begin();
  // this->MoveSystemPageToStart(pageHashIterator->second);

  (*pageHashIterator->second)->SetFileName(filename);

  return (*pageHashIterator->second);
}

Page *StorageManager::GetSystemPage(const string& filename, const page_id_t &pageId, const extent_id_t &extentId, const Table* table)
{
  const auto key = filename + to_string(pageId);

  auto pageHashIterator = this->SearchSystemPageInCache(key);

  if (pageHashIterator == this->systemCache.end()) 
  {
    this->OpenSystemExtent(filename, extentId, table);
    pageHashIterator = this->SearchSystemPageInCache(key);
  }

  this->systemPageList.push_front(*pageHashIterator->second);
  this->systemPageList.erase(pageHashIterator->second);
  this->systemCache[key] = this->systemPageList.begin();
  // this->MoveSystemPageToStart(pageHashIterator->second);

  return (*pageHashIterator->second);
}

void StorageManager::OpenSystemPage(const string& filename, const page_id_t &pageId)
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

  const auto key = filename + to_string(pageId);

  this->systemPageList.push_front(page);
  this->systemCache[key] = this->systemPageList.begin();

  // this->UnlockSystemPageWrite();

  page->GetPageDataFromFile(buffer, nullptr, offSet, file);
}

void StorageManager::OpenSystemExtent(const string &filename, const extent_id_t &extentId, const Table* table)
{
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

    if (this->IsPageCached(filename, currentPageId))
      continue;

    if (this->systemPageList.size() == MAX_NUMBER_SYSTEM_PAGES)
        this->RemoveSystemPage();

    PageHeader pageHeader = StorageManager::GetPageHeaderFromFile(buffer, offSet);

    Page *page = nullptr;

    StorageManager::AllocateMemoryBasedOnSystemPageType(&page, pageHeader);

    page->GetPageDataFromFile(buffer, table, offSet, file);

    this->systemPageList.push_front(page);

    const auto key = filename + to_string(page->GetPageId());

    this->systemCache[key] = this->systemPageList.begin();

    page->SetFileName(filename);
  }
}

void StorageManager::RemoveSystemPage() 
{
  // this->LockSystemPageWrite();

  Page *page = this->systemPageList.back();

  const page_id_t pageId = page->GetPageId();

  const string &filename = page->GetFileName();

  const auto key = filename + to_string(pageId);

  this->systemCache.erase(key);

  this->systemPageList.pop_back();

  // this->UnlockSystemPageWrite();

  if (page->GetPageDirtyStatus()) 
  {

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
    default:
      throw runtime_error("Page type not recognized");
  }
}

////////////////////////////////////////////////////////////////////
/////////////////////////Globally Used Functions///////////////////
//////////////////////////////////////////////////////////////////

unordered_map<string, PageIterator>::iterator
StorageManager::SearchSystemPageInCache(const string& key)
{
  // Page* page = nullptr;

  // this->LockSystemPageRead();

  return this->systemCache.find(key);

  // this->UnlockSystemPageRead();
}

bool StorageManager::AllocateMemoryBasedOnPageType(Page **page, const PageHeader &pageHeader)
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
    case PageType::INDEX:
      *page = new IndexPage(pageHeader);
      break;
    case PageType::OVERFLOW:
      *page = new OverflowPage(pageHeader);
      break;
    default:
      return false;
  }

  return true;
}

void StorageManager::MovePageToFrontOfSystemList(Page *page,
                                              const page_id_t &pageId,
                                              const string &filename) {
  if (this->systemPageList.size() == MAX_NUMBER_SYSTEM_PAGES)
    this->RemoveSystemPage();

  const auto key = filename + to_string(pageId);

  page->SetFileName(filename);

  this->systemPageList.push_front(page);
  this->systemCache[key] = this->systemPageList.begin();
}

void StorageManager::MovePageToFrontOfList(
  Page *page,
  const page_id_t &pageId,
  const string &filename) {
  if (this->pageList.size() == MAX_NUMBER_SYSTEM_PAGES)
    this->RemovePage();

  const auto key = filename + to_string(pageId);

  page->SetFileName(filename);

  this->pageList.push_front(page);
  this->cache[key] = this->pageList.begin();
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

bool StorageManager::IsPageCached(const string& filename, const page_id_t &pageId)
{
  const auto key = filename + to_string(pageId);
  const auto pageIterator = this->cache.find(key);

  if (pageIterator != this->cache.end()) 
  {
    this->pageList.push_front(*pageIterator->second);
    this->pageList.erase(pageIterator->second);

    this->cache[key] = this->pageList.begin();

    return true;
  }

  const auto &systemPageIterator = this->systemCache.find(key);

  if (systemPageIterator != this->systemCache.end()) 
  {
    this->systemPageList.push_front(*systemPageIterator->second);
    this->systemPageList.erase(systemPageIterator->second);

    this->systemCache[key] = this->systemPageList.begin();

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