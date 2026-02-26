#include "../../include/BufferPool/StorageManager.h"
#include "../../include/Database.h"
#include "../../../Systemic/include/Guards/ReaderGuard.h"
#include "../../../Systemic/include/Guards/WriterGuard.h"
#include "Pages/GlobalAllocationPageView.h"
#include "Pages/HeaderPageView.h"
#include "Pages/IndexPageView.h"
#include "Pages/Additional/Frame.h"
#include <iostream>
#include <ranges>

namespace Storage {
StorageManager::StorageManager(){
    // this->frames.clear();
    // this->frames.reserve(Constants::MAX_NUMBER_OF_PAGES);
    // this->_memoryManager.AllocatePagePool(Constants::MAX_NUMBER_OF_PAGES);

    // for (auto i = 0; i < Constants::MAX_NUMBER_OF_PAGES; ++i) {
    //     auto frame = new Pages::Frame(
    //         this->_memoryManager.Data() + i * PAGE_SIZE,
    //         nullptr
    //     );
    //
    //     this->frames.push_back(frame);
    // }

    this->_memoryManager = &DatabaseEngine::BufferPoolMemoryManager::Get();
    this->capacity = this->_memoryManager->FramesCount();
    this->clockHand = 0;
}

std::string StorageManager::CreateKey(const std::string &filename, const page_id_t pageId){
  return filename + std::to_string(pageId);
}

StorageManager::~StorageManager() {
    for (const auto frame : this->pageTable | std::views::values) {
        const auto* page = this->_memoryManager->GetFrame(frame);

        if (page == nullptr)
            continue;

        this->RemovePageWithoutKeyDeletion(page);
        // delete page;
    }
}

StorageManager& StorageManager::Get(){
  static StorageManager storageManager;
  return storageManager;
}

void StorageManager::CreateFile(const std::string& fileName, const std::string& extension)const{
  this->fileManager.CreateFile(fileName, extension);
}

Pages::Frame* StorageManager::GetRawPage(
  const std::string& filename,
  const page_id_t pageId,
  const DatabaseEngine::StorageTypes::Table *table
) {
{
    MultiThreading::ReaderGuard lock(&this->tableMutex);

    auto frame = 0;
    if (this->pageTable.TryGetValue(StorageManager::CreateKey(filename, pageId), frame))
      return this->_memoryManager->GetFrame(frame);
  }

  const auto extentId = DatabaseEngine::Database::CalculateExtentId(pageId);

  //cache miss
  return this->OpenExtent(pageId, filename, extentId, table);
}

Pages::PageView StorageManager::CreatePage(
    const std::string& filename,
    const DatabaseEngine::StorageTypes::Table *table,
    const page_id_t pageId
){
    auto* frame = this->CreateFrame(filename, pageId, table);
    frame->type = PageType::DATA;
    frame->headerPtr->bytesLeft = PAGE_SIZE_WITHOUT_HEADER;
    return Pages::PageView(frame);
}

Pages::PageView StorageManager::GetPage(
    const std::string &filename,
    const page_id_t pageId,
    const DatabaseEngine::StorageTypes::Table *table
){
    auto* frame = this->GetRawPage(filename, pageId, table);
    frame->type = PageType::DATA;
    return Pages::PageView(frame);
}

Pages::LargeObjectView StorageManager::GetLargeDataPage(
    const std::string& filename,
    const page_id_t pageId,
    const DatabaseEngine::StorageTypes::Table *table
){
    auto* frame = this->GetRawPage(filename, pageId, table);
    frame->type = PageType::LOB;
    return Pages::LargeObjectView(frame);
}

Pages::OverflowPageView StorageManager::GetOverflowPage(
    const std::string& filename,
    const page_id_t pageId,
    const DatabaseEngine::StorageTypes::Table *table
){
    auto* frame = this->GetRawPage(filename, pageId, table);
    frame->type = PageType::OVERFLOWTYPE;
    return Pages::OverflowPageView(frame);
}

Pages::LargeObjectView StorageManager::CreateLargeDataPage(const std::string& filename, const page_id_t pageId){
    auto* frame = this->CreateFrame(filename, pageId, nullptr);
    frame->type = PageType::LOB;
    frame->headerPtr->bytesLeft = LARGE_OBJECT_PAGE_SIZE;
    return Pages::LargeObjectView(frame);
}

Pages::OverflowPageView StorageManager::CreateOverflowPage(const std::string & filename, const page_id_t  pageId){
    auto* frame = this->CreateFrame(filename, pageId, nullptr);
    frame->type = PageType::OVERFLOWTYPE;
    frame->headerPtr->bytesLeft = PAGE_SIZE_WITHOUT_HEADER;
    return Pages::OverflowPageView(frame);
}

Pages::Frame* StorageManager::EvictPage() {
    MultiThreading::WriterGuard lock(&this->clockMutex_);

    while (true) {
        auto* page = this->_memoryManager->GetFrame(this->clockHand);

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
        return this->_memoryManager->GetFrame(this->clockHand);
    }
}

void StorageManager::RemovePageWithoutKeyDeletion(const Pages::Frame *framePtr){
    if (!framePtr->isDirty)
        return;

    auto* file = this->fileManager.GetFile(framePtr->filename);

    const auto offSet = static_cast<std::streampos>(framePtr->headerPtr->pageId * Constants::PAGE_SIZE);

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

    const auto extentOffset = static_cast<std::streampos>(firstExtentPageId * PAGE_SIZE);

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

            const auto pageDataOffset = frame * PAGE_SIZE;
            auto* pageDataPtr = this->_memoryManager->CopyToMemory(buffer.data(), pageDataOffset, offSet);
            auto* newFramePtr = this->_memoryManager->GetFrame(frame);

            newFramePtr->data = pageDataPtr;
            newFramePtr->filename = filename;
            newFramePtr->isDirty = false;
            newFramePtr->hasSecondChance = false;
            newFramePtr->pinCount.store(0);
            newFramePtr->priority.store(PagePriority::LOW);
            newFramePtr->table = table;
            newFramePtr->headerPtr = reinterpret_cast<Pages::PageHeader*>(pageDataPtr);

            if (currentPageId == pageId)
                framePtr = this->_memoryManager->GetFrame(frame);

            this->pageTable[key] = frame;
            this->clockHand = (this->clockHand + 1) % this->capacity;
        }
    }

    return framePtr;
}

////////////////////////////////////////////////////
////////////////////System Pages///////////////////
//////////////////////////////////////////////////

Pages::HeaderPageView StorageManager::CreateHeaderPage(const std::string &filename){
    auto* frame = this->CreateFrame(filename, Constants::HEADER_PAGE_ID, nullptr);
    frame->type = PageType::METADATA;
    return Pages::HeaderPageView(frame);
}

Pages::GlobalAllocationPageView StorageManager::CreateGlobalAllocationMapPage(
    const std::string &filename,
    const page_id_t pageId
){
    auto* frame = this->CreateFrame(filename, pageId, nullptr);
    frame->type = PageType::GAM;
    return Pages::GlobalAllocationPageView(frame);
}

Pages::AllocationPageView StorageManager::CreateAllocationPage(
  const std::string& filename,
  const table_id_t tableId,
  const page_id_t pageId,
  const extent_id_t startingExtentId
){
    auto* frame = this->CreateFrame(filename, pageId, nullptr);
    frame->type = PageType::IAM;
    frame->additionalHeader.allocationHeaderPtr = reinterpret_cast<Pages::IndexAllocationPageAdditionalHeader*>(frame->data + PAGE_HEADER_SIZE);
    return Pages::AllocationPageView(frame);
}


Pages::PageFreeSpaceView StorageManager::CreatePageFreeSpacePage(
    const std::string &filename,
    const page_id_t pageId
){
    auto* frame = this->CreateFrame(filename, pageId, nullptr);
    frame->type = PageType::FREESPACE;
    return Pages::PageFreeSpaceView(frame);
}

Pages::IndexPageView StorageManager::CreateIndexPage(
  const std::string& filename,
  const DatabaseEngine::StorageTypes::Table* table,
  const page_id_t pageId
){
    auto* frame = this->CreateFrame(filename, pageId, table);

    frame->type = PageType::INDEX;
    frame->headerPtr->bytesLeft = INDEX_PAGE_DEFAULT_SIZE;
    frame->additionalHeader.indexHeaderPtr = reinterpret_cast<Pages::IndexPageAdditionalHeader*>(frame->data + PAGE_HEADER_SIZE);

    frame->additionalHeader.indexHeaderPtr->nextNode = INVALID_PAGE_ID;
    frame->additionalHeader.indexHeaderPtr->previousNode = INVALID_PAGE_ID;

    return Pages::IndexPageView(frame);
}

Pages::Frame* StorageManager::CreateFrame(const std::string &filename, const page_id_t pageId, const DatabaseEngine::StorageTypes::Table *table){
    MultiThreading::WriterGuard lock(&this->tableMutex);

    if (this->pageTable.size() >= MAX_NUMBER_OF_PAGES) {
        auto* victim = this->EvictPage();
    }

    const size_t frameIndex = this->clockHand % this->capacity;

    auto* framePtr = this->_memoryManager->GetFrame(frameIndex);

    framePtr->data = this->_memoryManager->Data() + frameIndex * PAGE_SIZE;
    framePtr->table = table;
    framePtr->filename = filename;
    framePtr->isDirty = true;
    framePtr->hasSecondChance = false;
    framePtr->pinCount.store(0);
    framePtr->priority.store(PagePriority::LOW);
    framePtr->headerPtr = reinterpret_cast<Pages::PageHeader*>(framePtr->data);
    framePtr->headerPtr->pageId = pageId;

    this->pageTable[StorageManager::CreateKey(filename, pageId)] = frameIndex;
    clockHand = (clockHand + 1) % capacity;

    return framePtr;
}

Pages::HeaderPageView StorageManager::GetHeaderPage(const std::string &filename)
{
    auto* frame = this->GetRawPage(filename, Constants::HEADER_PAGE_ID, nullptr);
    return Pages::HeaderPageView(frame);
}

Pages::PageFreeSpaceView StorageManager::GetPageFreeSpacePage(const std::string& filename, const page_id_t pageId)
{
    auto* page = this->GetRawPage(filename, pageId, nullptr);
    return Pages::PageFreeSpaceView(page);
}

Pages::IndexPageView StorageManager::GetIndexPage(
  const std::string& filename,
  const page_id_t pageId,
  const DatabaseEngine::StorageTypes::Table* table
){
    auto* frame = this->GetRawPage(filename, pageId, table);

    if (frame->additionalHeader.indexHeaderPtr == nullptr)
        frame->additionalHeader.indexHeaderPtr = reinterpret_cast<Pages::IndexPageAdditionalHeader*>(frame->data + PAGE_HEADER_SIZE);

    return Pages::IndexPageView(frame);
}

Pages::AllocationPageView StorageManager::GetAllocationPage(
  const std::string& filename,
  const page_id_t pageId,
  const DatabaseEngine::StorageTypes::Table *table
){
    auto* frame = this->GetRawPage(filename, pageId, table);
    frame->additionalHeader.allocationHeaderPtr = reinterpret_cast<Pages::IndexAllocationPageAdditionalHeader*>(frame->data + PAGE_HEADER_SIZE);
    return Pages::AllocationPageView(frame);
}

Pages::GlobalAllocationPageView StorageManager::GetGlobalAllocationMapPage(const std::string& filename, const page_id_t pageId){
    auto* frame = this->GetRawPage(filename, pageId,nullptr);
    return Pages::GlobalAllocationPageView(frame);
}

////////////////////////////////////////////////////////////////////
/////////////////////////Globally Used Functions///////////////////
//////////////////////////////////////////////////////////////////
bool StorageManager::IsPageCached(const std::string& filename, const page_id_t pageId)const{
    MultiThreading::ReaderGuard lock(&this->tableMutex);
    return this->pageTable.Contains(StorageManager::CreateKey(filename, pageId));
}

void StorageManager::SetReadFilePointerToOffset(std::fstream *file, const std::streampos &offSet) {
  file->clear();
  file->seekg(0, std::ios::beg);
  file->seekg(offSet);
}

void StorageManager::SetWriteFilePointerToOffset(std::fstream *file, const std::streampos &offSet)
{
  file->clear();
  file->seekp(0, std::ios::beg);
  file->seekp(offSet);
}
} // namespace Storage