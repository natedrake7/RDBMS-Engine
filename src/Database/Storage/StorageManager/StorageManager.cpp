#include "StorageManager.h"
#include "../../Constants.h"
#include "../../Database.h"
#include "../../../Systemic/MultiThreading/Guards/ReaderGuard/ReaderGuard.h"
#include "../../../Systemic/MultiThreading/Guards/WriterGuard/WriterGuard.h"
#include "../../Pages/GlobalAllocationMap/GlobalAllocationMapPage.h"
#include "../../Pages/Header/HeaderPage.h"
#include "../../Pages/IndexMapAllocation/IndexAllocationMapPage.h"
#include "../../Pages/IndexPage/IndexPage.h"
#include "../../Pages/LargeObject/LargeObjectPage.h"
#include "../../Pages/Page.h"
#include "../../Pages/PageFreeSpace/PageFreeSpacePage.h"
#include "../../Pages/PageGuard/PageGuard.h"

#include <cstring>
#include <iostream>
#include <ranges>

using namespace DatabaseEngine;
using namespace DatabaseEngine::StorageTypes;
using namespace Pages;

namespace Storage {

StorageManager::StorageManager()
{
  this->frames.resize(Constants::MAX_NUMBER_OF_PAGES, nullptr);
  this->capacity = Constants::MAX_NUMBER_OF_PAGES;
  this->clockHand = 0;
}

std::string StorageManager::CreateKey(const std::string &filename, const Constants::page_id_t &pageId){
  return filename + to_string(pageId);
}

StorageManager::~StorageManager() 
{
  for (const auto &frame : this->pageTable | views::values) {
    auto* page = this->frames[frame];

    if (page == nullptr)
      continue;

    this->RemovePageWithoutKeyDeletion(page);
  }
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

Page *StorageManager::GetRawPage(
  const string& filename,
  const page_id_t &pageId,
  const Table *table) {
  {
    MultiThreading::ReaderGuard lock(&this->tableMutex);

    int frame = 0;
    if (this->pageTable.TryGetValue(StorageManager::CreateKey(filename, pageId), frame))
      return this->frames[frame];
  }

  const auto extentId = Database::CalculateExtentIdByPageId(pageId);

  //cache miss
  return this->OpenExtent(pageId, filename, extentId, table);
}

Pages::PageGuard<LargeObjectPage> StorageManager::GetLargeDataPage(const string& filename, const page_id_t &pageId, const Table *table)
{
  auto* page = dynamic_cast<LargeObjectPage *>(this->GetRawPage(filename, pageId, table));
  return Pages::PageGuard<LargeObjectPage>(page);
}

Pages::PageGuard<OverflowPage> StorageManager::GetOverflowPage(const string& filename, const page_id_t &pageId, const Table *table)
{
  auto* page = dynamic_cast<OverflowPage *>(this->GetRawPage(filename, pageId, table));

  return Pages::PageGuard<OverflowPage>(page);
}

Pages::PageGuard<Pages::Page> StorageManager::CreatePage(const string& filename, const page_id_t &pageId)
{
  auto *page = new Pages::Page(pageId, true);
  page->SetDirty();

  this->InsertPageToCache(page, filename, pageId);

  return Pages::PageGuard<Pages::Page>(page);
}

Pages::PageGuard<Pages::Page> StorageManager::GetPage(const std::string &filename, const Constants::page_id_t &pageId, const DatabaseEngine::StorageTypes::Table *table){
  return Pages::PageGuard<Pages::Page>(this->GetRawPage(filename, pageId, table));
}

Pages::PageGuard<LargeObjectPage> StorageManager::CreateLargeDataPage(const string& filename, const page_id_t &pageId)
{
  auto* page = new Pages::LargeObjectPage(pageId, true);
  page->SetDirty();

  this->InsertPageToCache(page, filename, pageId);

  return Pages::PageGuard<LargeObjectPage>(page);
}

Pages::PageGuard<Pages::OverflowPage> StorageManager::CreateOverflowPage(const string & filename, const page_id_t & pageId){
  auto* page = new Pages::OverflowPage(pageId, true);
  page->SetDirty();

  this->InsertPageToCache(page, filename, pageId);

  return Pages::PageGuard<Pages::OverflowPage>(page);
}

Pages::Page* StorageManager::EvictPage() {
  MultiThreading::WriterGuard lock(&this->clockMutex_);

  while (true) {
    auto* page = this->frames[this->clockHand];

    if (page == nullptr || page->GetPinCount() > 0 || page->GetPriority() >= Constants::PagePriority::HIGH) {
      this->clockHand = (this->clockHand + 1) % this->capacity;
      continue;
    }

    MultiThreading::WriterGuard pageLock(&page->GetLatch());

      if (page->HasSecondChance()) {
        page->SetHasSecondChanceUnsafe(false);
        this->clockHand = (this->clockHand + 1) % this->capacity;

        continue;
      }

    this->frames[this->clockHand] = nullptr;
    this->clockHand = (this->clockHand + 1) % this->capacity;

    return page;
  }
}

void StorageManager::RemovePage(Pages::Page *page){
  const auto& filename = page->GetFileName();

  auto* file = this->fileManager.GetFile(filename);

  if (page->IsDirty()) {
    page->WriteToDisk(file);
    file->flush();
  }

  MultiThreading::WriterGuard lock(&this->tableMutex);

  this->pageTable.Remove(StorageManager::CreateKey(filename, page->GetPageId()));

  delete page;
}

void StorageManager::RemovePageWithoutKeyDeletion(Pages::Page *page){
  const auto& filename = page->GetFileName();

  auto* file = this->fileManager.GetFile(filename);

  if (page->IsDirty()) {
    StorageManager::SetWriteFilePointerToOffset(file, page->GetPageId() * Constants::PAGE_SIZE);

    page->WriteToDisk(file);
    file->flush();
  }

  delete page;
}

Pages::Page* StorageManager::OpenExtent(
  const Constants::page_id_t& pageId,
  const string& filename,
  const extent_id_t &extentId,
  const Table *table
){
  Pages::Page* returnPage = nullptr;

  // read page from disk, call this->fileManager
  auto *file = this->fileManager.GetFile(filename);

  const page_id_t firstExtentPageId = DatabaseEngine::Database::CalculateSystemPageOffsetByExtentId(extentId);

  const streampos extentOffset = firstExtentPageId * PAGE_SIZE;

  std::vector<char> buffer(EXTENT_BYTE_SIZE);

  SetReadFilePointerToOffset(file, extentOffset);

  // take into account the metadata page all the others
  file->read(buffer.data(), EXTENT_BYTE_SIZE);

  page_offset_t offSet = 0;

  const auto &bytesRead = file->gcount();

  for (int i = 0; i < Constants::EXTENT_SIZE; i++){
    offSet = i * Constants::PAGE_SIZE;

    if (bytesRead < offSet)
      break;

    const page_id_t currentPageId = firstExtentPageId + i;

    if (this->IsPageCached(filename, currentPageId))
      continue;

    if (this->pageTable.size() >= MAX_NUMBER_OF_PAGES) {
      auto* victim = this->EvictPage();
      this->RemovePage(victim);
    }

    const PageHeader pageHeader = StorageManager::GetPageHeaderFromFile(buffer, offSet);

    Page *page = nullptr;

    if(!StorageManager::AllocateMemoryBasedOnPageType(&page, pageHeader)){
      std::cerr << "Failed to read page id: " << currentPageId << endl;
      return nullptr;
    }

    {
      MultiThreading::WriterGuard pageLock(&page->GetLatch());

      page->ReadFromDisk(buffer, table, offSet, file);
      page->SetFileName(filename);
      page->SetHasSecondChanceUnsafe(true);
    }

    if (pageHeader.pageId == pageId)
      returnPage = page;

    {
      const auto key = StorageManager::CreateKey(filename, currentPageId);

      MultiThreading::WriterGuard tableLock(&this->tableMutex);
      const auto frame = this->clockHand % this->capacity;

      this->frames[frame] = page;
      this->pageTable[key] = frame;

      this->clockHand = (this->clockHand + 1) % this->capacity;
    }
  }

  return returnPage;
}

////////////////////////////////////////////////////
////////////////////System Pages///////////////////
//////////////////////////////////////////////////

Pages::PageGuard<HeaderPage> StorageManager::CreateHeaderPage(const string &filename)
{
  auto *page = new HeaderPage(Constants::HEADER_PAGE_ID);

  this->InsertPageToCache(page, filename, Constants::HEADER_PAGE_ID);

  return Pages::PageGuard<HeaderPage>(page);
}

Pages::PageGuard<GlobalAllocationMapPage> StorageManager::CreateGlobalAllocationMapPage(const string &filename, const page_id_t &pageId)
{
  auto *page = new GlobalAllocationMapPage(pageId);

  this->InsertPageToCache(page, filename, pageId);

  return Pages::PageGuard<GlobalAllocationMapPage>(page);
}

Pages::PageGuard<Pages::IndexAllocationMapPage> StorageManager::CreateIndexAllocationMapPage(
  const string& filename,
  const table_id_t &tableId,
  const page_id_t &pageId,
  const extent_id_t &startingExtentId)
{
  auto *page = new IndexAllocationMapPage(tableId, pageId, startingExtentId);

  this->InsertPageToCache(page, filename, pageId);

  return Pages::PageGuard<IndexAllocationMapPage>(page);
}


Pages::PageGuard<PageFreeSpacePage> StorageManager::CreatePageFreeSpacePage(const string &filename, const page_id_t &pageId)
{
  auto *page = new Pages::PageFreeSpacePage(pageId);

  this->InsertPageToCache(page, filename, pageId);

  return Pages::PageGuard<PageFreeSpacePage>(page);
}

Pages::PageGuard<Pages::IndexPage> StorageManager::CreateIndexPage(const string& filename, const page_id_t &pageId)
{
  auto *page = new Pages::IndexPage(pageId, true);

  this->InsertPageToCache(page, filename, pageId);

  return Pages::PageGuard<Pages::IndexPage>(page);
}

void StorageManager::InsertPageToCache(Pages::Page *page, const std::string &filename, const Constants::page_id_t &pageId){
  MultiThreading::WriterGuard lock(&this->tableMutex);

  if (this->pageTable.size() >= MAX_NUMBER_OF_PAGES) {
    auto* victim = this->EvictPage();
    this->RemovePage(victim);
  }

  page->SetFileName(filename);

  const size_t frameIndex = clockHand % capacity;
  this->frames[frameIndex] = page;
  this->pageTable[StorageManager::CreateKey(filename, pageId)] = frameIndex;
  clockHand = (clockHand + 1) % capacity;
}

Pages::PageGuard<HeaderPage> StorageManager::GetHeaderPage(const string &filename)
{
  auto* page = dynamic_cast<HeaderPage *>(this->GetRawPage(filename, Constants::HEADER_PAGE_ID, nullptr));

  return Pages::PageGuard<HeaderPage>(page);
}

Pages::PageGuard<PageFreeSpacePage>StorageManager::GetPageFreeSpacePage(const string& filename, const page_id_t &pageId)
{
  auto* page = dynamic_cast<PageFreeSpacePage *>(this->GetRawPage(filename, pageId, nullptr));

  return Pages::PageGuard<PageFreeSpacePage>(page);
}

Pages::PageGuard<Pages::IndexPage> StorageManager::GetIndexPage(const string& filename, const page_id_t &pageId, const Table* table)
{
  auto* page = dynamic_cast<IndexPage *>(this->GetRawPage(filename, pageId, table));

  return Pages::PageGuard<Pages::IndexPage>(page);
}

Pages::PageGuard<IndexAllocationMapPage> StorageManager::GetIndexAllocationMapPage(const string& filename, const page_id_t &pageId, const DatabaseEngine::StorageTypes::Table *table)
{
  auto* page = dynamic_cast<IndexAllocationMapPage *>(this->GetRawPage(filename, pageId, table));
  return Pages::PageGuard<IndexAllocationMapPage>(page);
}

Pages::PageGuard<GlobalAllocationMapPage> StorageManager::GetGlobalAllocationMapPage(const string& filename, const page_id_t &pageId)
{
  auto* page = dynamic_cast<GlobalAllocationMapPage *>(this->GetRawPage(filename, pageId,nullptr));

  return Pages::PageGuard<GlobalAllocationMapPage>(page);
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

bool StorageManager::AllocateMemoryBasedOnPageType(Page **page, const PageHeader &pageHeader)
{
  switch (pageHeader.pageType) 
  {
    case PageType::FREESPACE:
      *page = new PageFreeSpacePage(pageHeader);
      break;
    case PageType::METADATA:
      *page = new HeaderPage(pageHeader);
      break;
    case PageType::GAM:
      *page = new GlobalAllocationMapPage(pageHeader);
      break;
    case PageType::DATA:
      *page = new Page(pageHeader);
      break;
    case PageType::LOB:
      *page = new LargeObjectPage(pageHeader);
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

PageHeader StorageManager::GetPageHeaderFromFile(
  const vector<char> &data,
  page_offset_t &offSet
){
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

bool StorageManager::IsPageCached(const string& filename, const page_id_t &pageId)const{
    MultiThreading::ReaderGuard lock(&this->tableMutex);
    return this->pageTable.Contains(StorageManager::CreateKey(filename, pageId));
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
} // namespace Storage