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
    this->_memoryManager = &DatabaseEngine::BufferPoolMemoryManager::Get();
    this->capacity = this->_memoryManager->FramesCount();
    this->clockHand = 0;
}

StorageManager::~StorageManager() {
    for (const auto frameIndex : this->pageTable | std::views::values) {
        const auto* frame = this->_memoryManager->GetFrame(frameIndex);

        if (frame == nullptr)
            continue;

        this->RemovePageWithoutKeyDeletion(frame);
    }
}

StorageManager& StorageManager::Get(){
    static StorageManager storageManager;
    return storageManager;
}

void StorageManager::CreateFile(
    const FileKey key,
    const DataTypes::StringView& filename,
    const DataTypes::StringView& extension
){
  this->fileManager.CreateFile(key, filename, extension);
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

    const auto file = this->fileManager.GetFile(framePtr->fileKey, framePtr->filename);

    const auto offSet = framePtr->headerPtr->pageId * Constants::PAGE_SIZE;
    file.Write(framePtr->data, Constants::PAGE_SIZE, offSet);
    file.Flush();
}

Pages::Frame* StorageManager::OpenExtent(
    const FileKey fileKey,
    const page_id_t pageId,
    const extent_id_t extentId,
    const DataTypes::StringView& filename,
    const DatabaseEngine::StorageTypes::Table *table
){

    // read page from disk, call this->fileManager
    auto file = this->fileManager.GetFile(fileKey, filename);

    const auto firstExtentPageId = DatabaseEngine::Database::CalculateExtentFirstPageId(extentId);

    std::vector<char> buffer(EXTENT_BYTE_SIZE, 0);
    const auto bytesRead = file.Read(buffer.data(), EXTENT_BYTE_SIZE, firstExtentPageId * PAGE_SIZE);

    // take into account the metadata page all the others
    page_offset_t offSet = 0;
    Pages::Frame* framePtr = nullptr;

    for (int i = 0; i < EXTENT_SIZE; i++){
        offSet = i * PAGE_SIZE;

        if (bytesRead < offSet)
            break;

        const page_id_t currentPageId = firstExtentPageId + i;
        const auto key = PageKey::Create(currentPageId, fileKey.databaseId);

        if (this->IsPageCached(key))
            continue;

        if (this->pageTable.size() >= MAX_NUMBER_OF_PAGES) {
            auto* victim = this->EvictPage();
            // this->RemovePage(victim);
        }

        {
            MultiThreading::WriterGuard tableLock(&this->tableMutex);
            const auto frame = this->clockHand % this->capacity;

            const auto pageDataOffset = frame * PAGE_SIZE;
            auto* pageDataPtr = this->_memoryManager->CopyToMemory(buffer.data(), pageDataOffset, offSet);
            auto* newFramePtr = this->_memoryManager->GetFrame(frame);

            newFramePtr->data = pageDataPtr;
            newFramePtr->filename = filename;
            newFramePtr->fileKey = fileKey;
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

Pages::Frame* StorageManager::GetRawPage(
    const FileKey fileKey,
    const DataTypes::StringView& filename,
    const page_id_t pageId,
    const DatabaseEngine::StorageTypes::Table *table
) {
    {
        MultiThreading::ReaderGuard lock(&this->tableMutex);

        auto frame = 0;
        if (this->pageTable.TryGetValue(PageKey::Create(fileKey.databaseId, pageId), frame))
            return this->_memoryManager->GetFrame(frame);
    }

    const auto extentId = DatabaseEngine::Database::CalculateExtentId(pageId);

    //cache miss
    return this->OpenExtent(fileKey, pageId, extentId, filename, table);
}

Pages::PageView StorageManager::CreatePage(
    const FileKey fileKey,
    const DataTypes::StringView& filename,
    const DatabaseEngine::StorageTypes::Table *table,
    const page_id_t pageId
){
    auto* frame = this->CreateFrame(fileKey, filename, pageId, table);
    frame->type = PageType::DATA;
    frame->headerPtr->bytesLeft = PAGE_SIZE_WITHOUT_HEADER;
    return Pages::PageView(frame);
}

Pages::PageView StorageManager::GetPage(
    const FileKey fileKey,
    const DataTypes::StringView& filename,
    const page_id_t pageId,
    const DatabaseEngine::StorageTypes::Table *table
){
    auto* frame = this->GetRawPage(fileKey, filename, pageId, table);
    frame->type = PageType::DATA;
    return Pages::PageView(frame);
}

Pages::LargeObjectView StorageManager::CreateLargeDataPage(const FileKey fileKey, const DataTypes::StringView& filename, const page_id_t pageId){
    auto* frame = this->CreateFrame(fileKey, filename, pageId, nullptr);
    frame->type = PageType::LOB;
    frame->headerPtr->bytesLeft = LARGE_OBJECT_PAGE_SIZE;
    return Pages::LargeObjectView(frame);
}

Pages::LargeObjectView StorageManager::GetLargeDataPage(
    const FileKey fileKey,
    const DataTypes::StringView& filename,
    const page_id_t pageId,
    const DatabaseEngine::StorageTypes::Table *table
){
    auto* frame = this->GetRawPage(fileKey, filename, pageId, table);
    frame->type = PageType::LOB;
    return Pages::LargeObjectView(frame);
}

Pages::OverflowPageView StorageManager::CreateOverflowPage(const FileKey fileKey, const DataTypes::StringView& filename, const page_id_t pageId){
    auto* frame = this->CreateFrame(fileKey, filename, pageId, nullptr);
    frame->type = PageType::OVERFLOWTYPE;
    frame->headerPtr->bytesLeft = PAGE_SIZE_WITHOUT_HEADER;
    return Pages::OverflowPageView(frame);
}

Pages::OverflowPageView StorageManager::GetOverflowPage(
    const FileKey fileKey,
    const DataTypes::StringView& filename,
    const page_id_t pageId,
    const DatabaseEngine::StorageTypes::Table *table
){
    auto* frame = this->GetRawPage(fileKey, filename, pageId, table);
    frame->type = PageType::OVERFLOWTYPE;
    return Pages::OverflowPageView(frame);
}

////////////////////////////////////////////////////
////////////////////System Pages///////////////////
//////////////////////////////////////////////////

Pages::HeaderPageView StorageManager::CreateHeaderPage(const FileKey fileKey, const DataTypes::StringView& filename){
    auto* frame = this->CreateFrame(fileKey, filename, Constants::HEADER_PAGE_ID, nullptr);
    frame->type = PageType::METADATA;
    return Pages::HeaderPageView(frame);
}

Pages::GlobalAllocationPageView StorageManager::CreateGlobalAllocationMapPage(
    const FileKey fileKey,
    const DataTypes::StringView& filename,
    const page_id_t pageId
){
    auto* frame = this->CreateFrame(fileKey, filename, pageId, nullptr);
    frame->type = PageType::GAM;
    return Pages::GlobalAllocationPageView(frame);
}

Pages::AllocationPageView StorageManager::CreateAllocationPage(
    const FileKey fileKey,
    const DataTypes::StringView& filename,
    const table_id_t tableId,
    const page_id_t pageId,
    const extent_id_t startingExtentId
){
    auto* frame = this->CreateFrame(fileKey, filename, pageId, nullptr);
    frame->type = PageType::IAM;
    frame->additionalHeader.allocationHeaderPtr = reinterpret_cast<Pages::IndexAllocationPageAdditionalHeader*>(frame->data + PAGE_HEADER_SIZE);
    return Pages::AllocationPageView(frame);
}


Pages::PageFreeSpaceView StorageManager::CreatePageFreeSpacePage(
    const FileKey fileKey,
    const DataTypes::StringView& filename,
    const page_id_t pageId
){
    auto* frame = this->CreateFrame(fileKey, filename, pageId, nullptr);
    frame->type = PageType::FREESPACE;
    return Pages::PageFreeSpaceView(frame);
}

Pages::IndexPageView StorageManager::CreateIndexPage(
    const FileKey fileKey,
    const DataTypes::StringView& filename,
    const DatabaseEngine::StorageTypes::Table* table,
    const page_id_t pageId
){
    auto* frame = this->CreateFrame(fileKey, filename, pageId, table);

    frame->type = PageType::INDEX;
    frame->headerPtr->bytesLeft = INDEX_PAGE_DEFAULT_SIZE;
    frame->additionalHeader.indexHeaderPtr = reinterpret_cast<Pages::IndexPageAdditionalHeader*>(frame->data + PAGE_HEADER_SIZE);

    frame->additionalHeader.indexHeaderPtr->nextNode = INVALID_PAGE_ID;
    frame->additionalHeader.indexHeaderPtr->previousNode = INVALID_PAGE_ID;

    return Pages::IndexPageView(frame);
}

Pages::Frame* StorageManager::CreateFrame(const FileKey fileKey, const DataTypes::StringView& filename, const page_id_t pageId, const DatabaseEngine::StorageTypes::Table *table){
    MultiThreading::WriterGuard lock(&this->tableMutex);

    if (this->pageTable.size() >= MAX_NUMBER_OF_PAGES) {
        auto* victim = this->EvictPage();
    }

    const size_t frameIndex = this->clockHand % this->capacity;

    auto* framePtr = this->_memoryManager->GetFrame(frameIndex);

    framePtr->data = this->_memoryManager->Data() + frameIndex * PAGE_SIZE;
    framePtr->table = table;
    framePtr->filename = filename;
    framePtr->fileKey = fileKey;
    framePtr->isDirty = true;
    framePtr->hasSecondChance = false;
    framePtr->pinCount.store(0);
    framePtr->priority.store(PagePriority::LOW);
    framePtr->headerPtr = reinterpret_cast<Pages::PageHeader*>(framePtr->data);
    framePtr->headerPtr->pageId = pageId;

    this->pageTable[PageKey::Create(fileKey.databaseId, pageId)] = frameIndex;
    clockHand = (clockHand + 1) % capacity;

    return framePtr;
}

Pages::HeaderPageView StorageManager::GetHeaderPage(const FileKey fileKey, const DataTypes::StringView& filename)
{
    auto* frame = this->GetRawPage(fileKey, filename, Constants::HEADER_PAGE_ID, nullptr);
    return Pages::HeaderPageView(frame);
}

Pages::PageFreeSpaceView StorageManager::GetPageFreeSpacePage(const FileKey fileKey, const DataTypes::StringView& filename, const page_id_t pageId)
{
    auto* page = this->GetRawPage(fileKey, filename, pageId, nullptr);
    return Pages::PageFreeSpaceView(page);
}

Pages::IndexPageView StorageManager::GetIndexPage(
  const FileKey fileKey,
  const DataTypes::StringView& filename,
  const page_id_t pageId,
  const DatabaseEngine::StorageTypes::Table* table
){
    auto* frame = this->GetRawPage(fileKey, filename, pageId, table);

    if (frame->additionalHeader.indexHeaderPtr == nullptr)
        frame->additionalHeader.indexHeaderPtr = reinterpret_cast<Pages::IndexPageAdditionalHeader*>(frame->data + PAGE_HEADER_SIZE);

    return Pages::IndexPageView(frame);
}

Pages::AllocationPageView StorageManager::GetAllocationPage(
  const FileKey fileKey,
  const DataTypes::StringView& filename,
  const page_id_t pageId,
  const DatabaseEngine::StorageTypes::Table *table
){
    auto* frame = this->GetRawPage(fileKey, filename, pageId, table);
    frame->additionalHeader.allocationHeaderPtr = reinterpret_cast<Pages::IndexAllocationPageAdditionalHeader*>(frame->data + PAGE_HEADER_SIZE);
    return Pages::AllocationPageView(frame);
}

Pages::GlobalAllocationPageView StorageManager::GetGlobalAllocationMapPage(
    const FileKey fileKey,
    const DataTypes::StringView& filename,
    const page_id_t pageId
    ){
    auto* frame = this->GetRawPage(fileKey, filename, pageId, nullptr);
    return Pages::GlobalAllocationPageView(frame);
}

////////////////////////////////////////////////////////////////////
/////////////////////////Globally Used Functions///////////////////
//////////////////////////////////////////////////////////////////
bool StorageManager::IsPageCached(const PageKey key) const{
    MultiThreading::ReaderGuard lock(&this->tableMutex);
    return this->pageTable.Contains(key);
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