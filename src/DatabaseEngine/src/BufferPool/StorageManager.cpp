#include "../../include/BufferPool/StorageManager.h"
#include "../../include/Pages/Page.h"
#include "../../include/Pages/HeaderPage.h"
#include "../../include/Pages/GlobalAllocationMapPage.h"
#include "../../include/Pages/IndexAllocationMapPage.h"
#include "../../include/Pages/IndexPage.h"
#include "../../include/Pages/LargeObjectPage.h"
#include "../../include/Pages/PageFreeSpacePage.h"
#include "../../include/Pages/PageGuard.h"
#include "../../include/Database.h"
#include "../../../Systemic/include/Guards/ReaderGuard.h"
#include "../../../Systemic/include/Guards/WriterGuard.h"

#include <cstring>
#include <iostream>
#include <ranges>

namespace Storage {
StorageManager::StorageManager(){
    this->frames.resize(Constants::MAX_NUMBER_OF_PAGES, nullptr);
    this->capacity = Constants::MAX_NUMBER_OF_PAGES;
    this->clockHand = 0;
    // this->memoryPool = DatabaseEngine::BufferPoolMemory(Constants::MAX_NUMBER_OF_PAGES);
}

std::string StorageManager::CreateKey(const std::string &filename, const page_id_t pageId){
  return filename + to_string(pageId);
}

StorageManager::~StorageManager() {
  for (const auto &frame : this->pageTable | views::values) {
    auto* page = this->frames[frame];

    if (page == nullptr)
      continue;

    this->RemovePageWithoutKeyDeletion(page);
  }
}

StorageManager& StorageManager::Get(){
  static StorageManager storageManager;
  return storageManager;
}

void StorageManager::CreateFile(const string& fileName, const string& extension)const{
  this->fileManager.CreateFile(fileName, extension);
}

Pages::Page *StorageManager::GetRawPage(
  const string& filename,
  const page_id_t pageId,
  const DatabaseEngine::StorageTypes::Table *table
) {
{
    MultiThreading::ReaderGuard lock(&this->tableMutex);

    auto frame = 0;
    if (this->pageTable.TryGetValue(StorageManager::CreateKey(filename, pageId), frame))
      return this->frames[frame];
  }

  const auto extentId = DatabaseEngine::Database::CalculateExtentId(pageId);

  //cache miss
  return this->OpenExtent(pageId, filename, extentId, table);
}

Pages::PageGuard<> StorageManager::CreatePage(const std::string& filename, const DatabaseEngine::StorageTypes::Table *table, const page_id_t pageId)
{
  auto *page = new Pages::Page(pageId, table, true);
  page->SetDirty();

  this->InsertPageToCache(page, filename, pageId);

  return Pages::PageGuard(page);
}

Pages::PageGuard<> StorageManager::GetPage(const std::string &filename, const page_id_t pageId, const DatabaseEngine::StorageTypes::Table *table){
  return Pages::PageGuard(this->GetRawPage(filename, pageId, table));
}

Pages::PageGuard<Pages::LargeObjectPage> StorageManager::GetLargeDataPage(const string& filename, const page_id_t pageId, const DatabaseEngine::StorageTypes::Table *table)
{
  auto* page = this->GetRawPage(filename, pageId, table);

  if (page->GetPageType() != PageType::LOB)
    return {};

  return Pages::PageGuard(static_cast<Pages::LargeObjectPage*>(page));
}

Pages::PageGuard<Pages::OverflowPage> StorageManager::GetOverflowPage(const string& filename, const page_id_t pageId, const DatabaseEngine::StorageTypes::Table *table)
{
  auto* page = this->GetRawPage(filename, pageId, table);

  if (page->GetPageType() != PageType::OVERFLOWTYPE)
    return {};

  return Pages::PageGuard(static_cast<Pages::OverflowPage*>(page));
}

Pages::PageGuard<Pages::LargeObjectPage> StorageManager::CreateLargeDataPage(const string& filename, const page_id_t pageId)
{
  auto* page = new Pages::LargeObjectPage(pageId, true);
  page->SetDirty();

  this->InsertPageToCache(page, filename, pageId);

  return Pages::PageGuard(page);
}

Pages::PageGuard<Pages::OverflowPage> StorageManager::CreateOverflowPage(const string & filename, const page_id_t  pageId){
  auto* page = new Pages::OverflowPage(pageId, true);
  page->SetDirty();

  this->InsertPageToCache(page, filename, pageId);

  return Pages::PageGuard(page);
}

Pages::Page* StorageManager::EvictPage() {
  MultiThreading::WriterGuard lock(&this->clockMutex_);

  while (true) {
    auto* page = this->frames[this->clockHand];

    if (page == nullptr || page->GetPinCount() > 0 || page->GetPriority() >= Constants::PagePriority::HIGH) {
      this->clockHand = (this->clockHand + 1) % this->capacity;
      continue;
    }

    MultiThreading::WriterGuard pageLock(&page->Latch());

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

  this->pageTable.Remove(StorageManager::CreateKey(filename, page->PageId()));

  delete page;
}

void StorageManager::RemovePageWithoutKeyDeletion(Pages::Page *page){
  const auto& filename = page->GetFileName();

  auto* file = this->fileManager.GetFile(filename);

  if (page->IsDirty()) {
    StorageManager::SetWriteFilePointerToOffset(file, page->PageId() * Constants::PAGE_SIZE);

    page->WriteToDisk(file);
    file->flush();
  }
}

Pages::Page* StorageManager::OpenExtent(
  const page_id_t& pageId,
  const std::string& filename,
  const extent_id_t extentId,
  const DatabaseEngine::StorageTypes::Table *table
){
  Pages::Page* returnPage = nullptr;

  // read page from disk, call this->fileManager
  auto *file = this->fileManager.GetFile(filename);

  const auto firstExtentPageId = DatabaseEngine::Database::CalculateExtentFirstPageId(extentId);

  const auto  extentOffset = static_cast<streampos>(firstExtentPageId * PAGE_SIZE);

  std::vector<char> buffer(EXTENT_BYTE_SIZE);

  SetReadFilePointerToOffset(file, extentOffset);

  // take into account the metadata page all the others
  file->read(buffer.data(), EXTENT_BYTE_SIZE);

  page_offset_t offSet = 0;

  const auto bytesRead = file->gcount();

  for (int i = 0; i < EXTENT_SIZE; i++){
    offSet = i * PAGE_SIZE;

    if (bytesRead < offSet)
      break;

    const page_id_t currentPageId = firstExtentPageId + i;

    if (this->IsPageCached(filename, currentPageId))
      continue;

    if (this->pageTable.size() >= MAX_NUMBER_OF_PAGES) {
      auto* victim = this->EvictPage();
      this->RemovePage(victim);
    }

    const auto pageHeader = StorageManager::GetPageHeaderFromFile(buffer, offSet);

    Pages::Page *page = nullptr;

    {
      const auto key = StorageManager::CreateKey(filename, currentPageId);

      MultiThreading::WriterGuard tableLock(&this->tableMutex);
      const auto frame = this->clockHand % this->capacity;

        if(!StorageManager::AllocateMemoryBasedOnPageType(&page, pageHeader)){
          std::cerr << "Failed to read page id: " << currentPageId << endl;
          return nullptr;
        }

        {
          MultiThreading::WriterGuard pageLock(&page->Latch());

          page->ReadFromDisk(buffer, table, offSet, file);
          page->SetTable(table);
          page->SetFileName(filename);
          page->SetHasSecondChanceUnsafe(true);
        }

        if (pageHeader.pageId == pageId)
          returnPage = page;


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

Pages::PageGuard<Pages::HeaderPage> StorageManager::CreateHeaderPage(const string &filename)
{
  auto *page = new Pages::HeaderPage(Constants::HEADER_PAGE_ID);

  this->InsertPageToCache(page, filename, Constants::HEADER_PAGE_ID);

  return Pages::PageGuard(page);
}

Pages::PageGuard<Pages::GlobalAllocationMapPage> StorageManager::CreateGlobalAllocationMapPage(const string &filename, const page_id_t pageId)
{
  auto *page = new Pages::GlobalAllocationMapPage(pageId);

  this->InsertPageToCache(page, filename, pageId);

  return Pages::PageGuard<Pages::GlobalAllocationMapPage>(page);
}

Pages::PageGuard<Pages::IndexAllocationMapPage> StorageManager::CreateIndexAllocationMapPage(
  const std::string& filename,
  const table_id_t tableId,
  const page_id_t pageId,
  const extent_id_t startingExtentId
){
  auto *page = new Pages::IndexAllocationMapPage(tableId, pageId, startingExtentId);

  this->InsertPageToCache(page, filename, pageId);

  return Pages::PageGuard(page);
}


Pages::PageGuard<Pages::PageFreeSpacePage> StorageManager::CreatePageFreeSpacePage(const string &filename, const page_id_t pageId)
{
  auto *page = new Pages::PageFreeSpacePage(pageId);

  this->InsertPageToCache(page, filename, pageId);

  return Pages::PageGuard(page);
}

Pages::PageGuard<Pages::IndexPage> StorageManager::CreateIndexPage(
  const std::string& filename,
  const DatabaseEngine::StorageTypes::Table* table,
  const page_id_t pageId
){
  auto *page = new Pages::IndexPage(pageId, table, true);

  this->InsertPageToCache(page, filename, pageId);

  return Pages::PageGuard(page);
}

void StorageManager::InsertPageToCache(Pages::Page *page, const std::string &filename, const page_id_t pageId){
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

Pages::PageGuard<Pages::HeaderPage> StorageManager::GetHeaderPage(const string &filename)
{
  auto* page = this->GetRawPage(filename, Constants::HEADER_PAGE_ID, nullptr);

  if (page->GetPageType() != PageType::METADATA)
    return {};

  return Pages::PageGuard(static_cast<Pages::HeaderPage*>(page));
}

Pages::PageGuard<Pages::PageFreeSpacePage>StorageManager::GetPageFreeSpacePage(const string& filename, const page_id_t pageId)
{
  auto* page = this->GetRawPage(filename, pageId, nullptr);

  if (page->GetPageType() != PageType::FREESPACE)
    return {};

  return Pages::PageGuard(static_cast<Pages::PageFreeSpacePage*>(page));
}

Pages::PageGuard<Pages::IndexPage> StorageManager::GetIndexPage(
  const string& filename,
  const page_id_t pageId,
  const DatabaseEngine::StorageTypes::Table* table
){
  auto* page = this->GetRawPage(filename, pageId, table);

  if (page->GetPageType() != PageType::INDEX)
    return {};

  return Pages::PageGuard(static_cast<Pages::IndexPage*>(page));
}

Pages::PageGuard<Pages::IndexAllocationMapPage> StorageManager::GetIndexAllocationMapPage(
  const string& filename,
  const page_id_t pageId,
  const DatabaseEngine::StorageTypes::Table *table
){
  auto* page = this->GetRawPage(filename, pageId, table);

  if (page->GetPageType() != PageType::IAM)
    return {};

  return Pages::PageGuard(static_cast<Pages::IndexAllocationMapPage*>(page));
}

Pages::PageGuard<Pages::GlobalAllocationMapPage> StorageManager::GetGlobalAllocationMapPage(const string& filename, const page_id_t pageId){
  auto* page = this->GetRawPage(filename, pageId,nullptr);

  if (page->GetPageType() != PageType::GAM)
    return {};

  return Pages::PageGuard(static_cast<Pages::GlobalAllocationMapPage*>(page));
}

void StorageManager::AllocateMemoryBasedOnSystemPageType(Pages::Page **page, const Pages::PageHeader &pageHeader)
{
  switch (pageHeader.type) 
  {
    case PageType::GAM:
      *page = new Pages::GlobalAllocationMapPage(pageHeader);
      break;
    case PageType::IAM:
      *page = new Pages::IndexAllocationMapPage(pageHeader, 0, 0);
      break;
    case PageType::METADATA:
      *page = new Pages::HeaderPage(pageHeader);
      break;
    case PageType::FREESPACE:
      *page = new Pages::PageFreeSpacePage(pageHeader);
      break;
    default:
      throw runtime_error("Page type not recognized");
  }
}

////////////////////////////////////////////////////////////////////
/////////////////////////Globally Used Functions///////////////////
//////////////////////////////////////////////////////////////////

bool StorageManager::AllocateMemoryBasedOnPageType(
    Pages::Page **page,
    const Pages::PageHeader &pageHeader
){
  switch (pageHeader.type) {
    case PageType::FREESPACE:
      *page = new Pages::PageFreeSpacePage(pageHeader);
      break;
    case PageType::METADATA:
      *page = new Pages::HeaderPage(pageHeader);
      break;
    case PageType::GAM:
      *page = new Pages::GlobalAllocationMapPage(pageHeader);
      break;
    case PageType::DATA:
      *page = new Pages::Page(pageHeader);
      break;
    case PageType::LOB:
      *page = new Pages::LargeObjectPage(pageHeader);
      break;
    case PageType::IAM:
      *page = new Pages::IndexAllocationMapPage(pageHeader, 0, 0);
      break;
    case PageType::INDEX:
      *page = new Pages::IndexPage(pageHeader);
      break;
    case PageType::OVERFLOWTYPE:
      *page = new Pages::OverflowPage(pageHeader);
      break;
    default:
      return false;
  }
  return true;
}

Pages::PageHeader StorageManager::GetPageHeaderFromFile(
  const vector<char> &data,
  page_offset_t &offSet
){
  Pages::PageHeader pageHeader;
  memcpy(&pageHeader.pageId, data.data() + offSet, sizeof(page_id_t));
  offSet += sizeof(page_id_t);

  memcpy(&pageHeader.size, data.data() + offSet, sizeof(page_size_t));
  offSet += sizeof(page_size_t);

  memcpy(&pageHeader.bytesLeft, data.data() + offSet, sizeof(page_size_t));
  offSet += sizeof(page_size_t);

  memcpy(&pageHeader.type, data.data() + offSet, sizeof(PageType));
  offSet += sizeof(PageType);

  return pageHeader;
}

bool StorageManager::IsPageCached(const string& filename, const page_id_t pageId)const{
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