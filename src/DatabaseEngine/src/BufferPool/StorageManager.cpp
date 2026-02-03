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

#include "Pages/GlobalAllocationPageView.h"
#include "Pages/IndexPageView.h"

namespace Storage {
StorageManager::StorageManager(){
    this->frames.resize(Constants::MAX_NUMBER_OF_PAGES);
    this->capacity = Constants::MAX_NUMBER_OF_PAGES;
    this->clockHand = 0;
    this->memoryPool.Allocate(Constants::MAX_NUMBER_OF_PAGES);
}

std::string StorageManager::CreateKey(const std::string &filename, const page_id_t pageId){
  return filename + to_string(pageId);
}

StorageManager::~StorageManager() {
  for (const auto frame : this->pageTable | views::values) {
    auto* page = &this->frames[frame];


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

Pages::Frame* StorageManager::GetRawPage(
  const string& filename,
  const page_id_t pageId,
  const DatabaseEngine::StorageTypes::Table *table
) {
{
    MultiThreading::ReaderGuard lock(&this->tableMutex);

    auto frame = 0;
    if (this->pageTable.TryGetValue(StorageManager::CreateKey(filename, pageId), frame))
      return &this->frames[frame];
  }

  const auto extentId = DatabaseEngine::Database::CalculateExtentId(pageId);

  //cache miss
  return this->OpenExtent(pageId, filename, extentId, table);
}

Pages::PageView StorageManager::CreatePage(const std::string& filename, const DatabaseEngine::StorageTypes::Table *table, const page_id_t pageId){
  // auto *page = new Pages::Page(pageId, table, true);
  // page->SetDirty();

  auto* frame = this->CreateFrame(filename, pageId);
  return Pages::PageView(frame);
}

Pages::PageView StorageManager::GetPage(const std::string &filename, const page_id_t pageId, const DatabaseEngine::StorageTypes::Table *table){
  return Pages::PageView(this->GetRawPage(filename, pageId, table));
}

Pages::PageGuard<Pages::LargeObjectPage> StorageManager::GetLargeDataPage(const std::string& filename, const page_id_t pageId, const DatabaseEngine::StorageTypes::Table *table){
  auto* page = this->GetRawPage(filename, pageId, table);

  // if (page->GetPageType() != PageType::LOB)
  //   return {};

  return Pages::PageGuard<Pages::LargeObjectPage>(nullptr);
}

Pages::PageGuard<Pages::OverflowPage> StorageManager::GetOverflowPage(const string& filename, const page_id_t pageId, const DatabaseEngine::StorageTypes::Table *table)
{
  auto* page = this->GetRawPage(filename, pageId, table);

  // if (page->GetPageType() != PageType::OVERFLOWTYPE)
  //   return {};

  // return Pages::PageGuard(static_cast<Pages::OverflowPage*>(page));
    return Pages::PageGuard<Pages::OverflowPage>(nullptr);
}

Pages::PageGuard<Pages::LargeObjectPage> StorageManager::CreateLargeDataPage(const string& filename, const page_id_t pageId)
{
  // auto* page = new Pages::LargeObjectPage(pageId, true);
  // page->SetDirty();
  //
  // this->CreateFrame(page, filename, pageId);

    return Pages::PageGuard<Pages::LargeObjectPage>(nullptr);
}

Pages::PageGuard<Pages::OverflowPage> StorageManager::CreateOverflowPage(const string & filename, const page_id_t  pageId){
  // auto* page = new Pages::OverflowPage(pageId, true);
  // page->SetDirty();
  //
  // this->CreateFrame(page, filename, pageId);
  //
  // return Pages::PageGuard(page);
    return Pages::PageGuard<Pages::OverflowPage>(nullptr);
}

Pages::Frame* StorageManager::EvictPage() {
    MultiThreading::WriterGuard lock(&this->clockMutex_);

    while (true) {
        auto* page = &this->frames[this->clockHand];

        if (page == nullptr || page->pinCount.load() > 0 || page->priority.load() >= PagePriority::HIGH) {
        this->clockHand = (this->clockHand + 1) % this->capacity;
        continue;
    }

    MultiThreading::WriterGuard pageLock(&page->latch);

    if (page->hasSecondChance) {
        page->hasSecondChance = false;
        this->clockHand = (this->clockHand + 1) % this->capacity;

        continue;
    }

        this->clockHand = (this->clockHand + 1) % this->capacity;
        return &this->frames[this->clockHand];
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

void StorageManager::RemovePageWithoutKeyDeletion(const Pages::Frame *framePtr){
    if (!framePtr->isDirty)
        return;

    auto* file = this->fileManager.GetFile(framePtr->filename);

    const auto offSet = static_cast<streampos>(*reinterpret_cast<const page_id_t*>(framePtr->data) * Constants::PAGE_SIZE);

    StorageManager::SetWriteFilePointerToOffset(file, offSet);

    file->write(reinterpret_cast<const char*>(framePtr->data), Constants::PAGE_SIZE);
    file->flush();
}

Pages::Frame* StorageManager::OpenExtent(
    const page_id_t& pageId,
    const std::string& filename,
    const extent_id_t extentId,
    const DatabaseEngine::StorageTypes::Table *table
){
    // read page from disk, call this->fileManager
    auto *file = this->fileManager.GetFile(filename);

    const auto firstExtentPageId = DatabaseEngine::Database::CalculateExtentFirstPageId(extentId);

    const auto extentOffset = static_cast<streampos>(firstExtentPageId * PAGE_SIZE);

    SetReadFilePointerToOffset(file, extentOffset);

    std::vector<char> buffer(EXTENT_BYTE_SIZE, 0);
    file->read(buffer.data(), EXTENT_BYTE_SIZE);

    // take into account the metadata page all the others

    page_offset_t offSet = 0;
    const auto bytesRead = file->gcount();

    Pages::Frame* framePtr = nullptr;

    for (int i = 0; i < EXTENT_SIZE; i++){
        offSet = i * PAGE_SIZE;

        if (bytesRead < offSet)
            break;

        const page_id_t currentPageId = firstExtentPageId + i;

        if (this->IsPageCached(filename, currentPageId))
            continue;

        if (this->pageTable.size() >= MAX_NUMBER_OF_PAGES) {
            auto* victim = this->EvictPage();
            // this->RemovePage(victim);
        }

        {
            const auto key = StorageManager::CreateKey(filename, currentPageId);

            MultiThreading::WriterGuard tableLock(&this->tableMutex);
            const auto frame = this->clockHand % this->capacity;

            auto* pageDataPtr = this->memoryPool.CopyToMemory(buffer.data(), frame * PAGE_SIZE, offSet);

            this->frames[frame] = Pages::Frame(
                pageDataPtr,
                table
            );

            if (currentPageId == pageId)
                framePtr = &this->frames[frame];

            this->pageTable[key] = frame;
            this->clockHand = (this->clockHand + 1) % this->capacity;
        }
    }

    return framePtr;
}

////////////////////////////////////////////////////
////////////////////System Pages///////////////////
//////////////////////////////////////////////////

Pages::PageGuard<Pages::HeaderPage> StorageManager::CreateHeaderPage(const string &filename)
{
  // auto *page = new Pages::HeaderPage(Constants::HEADER_PAGE_ID);

  auto* frame = this->CreateFrame(filename, Constants::HEADER_PAGE_ID);

  return Pages::PageGuard(page);
}

Pages::GlobalAllocationPageView StorageManager::CreateGlobalAllocationMapPage(const string &filename, const page_id_t pageId)
{
  // auto *page = new Pages::GlobalAllocationMapPage(pageId);

  auto* frame = this->CreateFrame(filename, pageId);

  return Pages::GlobalAllocationPageView(frame);
}

Pages::AllocationPageView StorageManager::CreateAllocationPage(
  const std::string& filename,
  const table_id_t tableId,
  const page_id_t pageId,
  const extent_id_t startingExtentId
){
  // auto *page = new Pages::IndexAllocationMapPage(tableId, pageId, startingExtentId);

  auto* frame = this->CreateFrame(filename, pageId);

  return Pages::AllocationPageView(frame);
}


Pages::PageFreeSpaceView StorageManager::CreatePageFreeSpacePage(const string &filename, const page_id_t pageId)
{
    // auto *page = new Pages::PageFreeSpacePage(pageId);

    auto* frame = this->CreateFrame(filename, pageId);

    return Pages::PageFreeSpaceView(frame);
}

Pages::IndexPageView StorageManager::CreateIndexPage(
  const std::string& filename,
  const DatabaseEngine::StorageTypes::Table* table,
  const page_id_t pageId
){
  // auto *frame = new Pages::IndexPage(pageId, table, true);

  auto* frame = this->CreateFrame(filename, pageId);

  return Pages::IndexPageView(frame);
}

Pages::Frame* StorageManager::CreateFrame(const std::string &filename, const page_id_t pageId){
    MultiThreading::WriterGuard lock(&this->tableMutex);

    if (this->pageTable.size() >= MAX_NUMBER_OF_PAGES) {
        auto* victim = this->EvictPage();
        // this->RemovePage(victim);
    }

    // page->SetFileName(filename);

    const size_t frameIndex = clockHand % capacity;

    this->frames[frameIndex] = Pages::Frame(
        this->memoryPool.Data() + frameIndex * PAGE_SIZE,
        page->GetTable()
    );
    this->pageTable[StorageManager::CreateKey(filename, pageId)] = frameIndex;
    clockHand = (clockHand + 1) % capacity;

    return &this->frames[frameIndex];
}

Pages::PageGuard<Pages::HeaderPage> StorageManager::GetHeaderPage(const string &filename)
{
  auto* page = this->GetRawPage(filename, Constants::HEADER_PAGE_ID, nullptr);

  // if (page->GetPageType() != PageType::METADATA)
  //   return {};

  return Pages::PageGuard(static_cast<Pages::HeaderPage*>(page));
}

Pages::PageFreeSpaceView StorageManager::GetPageFreeSpacePage(const string& filename, const page_id_t pageId)
{
    auto* page = this->GetRawPage(filename, pageId, nullptr);

    // if (page->GetPageType() != PageType::FREESPACE)
    //   return {};

    return Pages::PageFreeSpaceView(page);

    // return Pages::PageGuard(static_cast<Pages::PageFreeSpacePage*>(page));
}

Pages::IndexPageView StorageManager::GetIndexPage(
  const string& filename,
  const page_id_t pageId,
  const DatabaseEngine::StorageTypes::Table* table
){
  auto* frame = this->GetRawPage(filename, pageId, table);

  // if (page->GetPageType() != PageType::INDEX)
  //   return {};

  return Pages::IndexPageView(frame);
}

Pages::AllocationPageView StorageManager::GetAllocationPage(
  const string& filename,
  const page_id_t pageId,
  const DatabaseEngine::StorageTypes::Table *table
){
    auto* frame = this->GetRawPage(filename, pageId, table);

    // if (page->GetPageType() != PageType::IAM)
    //   return {};

    return Pages::AllocationPageView(frame);
}

Pages::GlobalAllocationPageView StorageManager::GetGlobalAllocationMapPage(const string& filename, const page_id_t pageId){
    auto* frame = this->GetRawPage(filename, pageId,nullptr);

    // if (page->GetPageType() != PageType::GAM)
    //   return {};

    return Pages::GlobalAllocationPageView(frame);
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