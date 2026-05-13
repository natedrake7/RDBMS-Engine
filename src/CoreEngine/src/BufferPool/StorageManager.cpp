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
    this->_memoryManager = &CoreEngine::BufferPoolMemoryManager::Get();
    this->capacity = this->_memoryManager->FramesCount();
    this->pageTable.reserve(this->capacity);
    this->clockHand = 0;
}

StorageManager::~StorageManager() {
    for (const auto* framePtr : this->pageTable | std::views::values) {
        if (framePtr == nullptr)
            continue;
        this->FlushFrameToDisk(framePtr);
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

void StorageManager::EvictPage() {
    const Pages::Frame* victim = nullptr;
    PageKey victimKey;

    MultiThreading::WriterGuard lock(&this->clockMutex_);
    while (victim == nullptr) {
        auto* frame = this->_memoryManager->GetFrame(this->clockHand);

        this->clockHand = (this->clockHand + 1) % this->capacity;
        if (frame == nullptr)
            continue;

        MultiThreading::WriterGuard pageLock(&frame->latch);

        if (frame->pinCount.load() > 0 || frame->priority.load() >= Constants::PagePriority::HIGH)
            continue;

        if (frame->hasSecondChance){
            frame->hasSecondChance = false;
            continue;
        }

        victim = frame;
        victimKey = PageKey::Create(victim->fileKey, victim->headerPtr->pageId);
    }

    this->FlushFrameToDisk(victim);

    {
        MultiThreading::WriterGuard tableLock(&this->tableMutex);
        this->pageTable.Remove(victimKey);
    }
}

void StorageManager::FlushFrameToDisk(const Pages::Frame *framePtr){
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
    const CoreEngine::StorageTypes::Table *table
){

    // read page from disk, call this->fileManager
    const auto file = this->fileManager.GetFile(fileKey, filename);
    const auto firstExtentPageId = CoreEngine::Database::CalculateExtentFirstPageId(extentId);

    char buffer[Constants::EXTENT_BYTE_SIZE];
    const auto bytesRead = file.Read(buffer, Constants::EXTENT_BYTE_SIZE, firstExtentPageId * Constants::PAGE_SIZE);

    // take into account the metadata page all the others
    page_offset_t offSet = 0;
    Pages::Frame* framePtr = nullptr;

    for (int i = 0; i < Constants::EXTENT_SIZE; i++){
        offSet = i * Constants::PAGE_SIZE;

        if (bytesRead < offSet)
            break;

        const page_id_t currentPageId = firstExtentPageId + i;
        const auto key = PageKey::Create(fileKey, currentPageId);

        if (this->IsPageCached(key)) continue;

        if (this->pageTable.size() >= Constants::MAX_NUMBER_OF_PAGES)
            this->EvictPage();

        {
            MultiThreading::WriterGuard tableLock(&this->tableMutex);
            const auto frame = this->clockHand % this->capacity;

            const auto pageDataOffset = frame * Constants::PAGE_SIZE;
            auto* pageDataPtr = this->_memoryManager->CopyToMemory(buffer, pageDataOffset, offSet);
            auto* newFramePtr = this->_memoryManager->GetFrame(frame);

            newFramePtr->data = pageDataPtr;
            newFramePtr->filename = filename;
            newFramePtr->fileKey = fileKey;
            newFramePtr->isDirty = false;
            newFramePtr->hasSecondChance = false;
            newFramePtr->pinCount.store(0);
            newFramePtr->priority.store(Constants::PagePriority::LOW);
            newFramePtr->table = table;
            newFramePtr->headerPtr = reinterpret_cast<Pages::PageHeader*>(pageDataPtr);

            if (currentPageId == pageId)
                framePtr = newFramePtr;

            this->pageTable[key] = newFramePtr;
            this->clockHand = (this->clockHand + 1) % this->capacity;
        }
    }

    if (framePtr == nullptr)
    {
        std::cout << "Page not found in extent" << std::endl;
        return nullptr;
    }


    return framePtr;
}

Pages::Frame* StorageManager::GetRawPage(
    const FileKey fileKey,
    const DataTypes::StringView& filename,
    const page_id_t pageId,
    const CoreEngine::StorageTypes::Table *table
) {
    {
        MultiThreading::ReaderGuard lock(&this->tableMutex);

        Pages::Frame* framePtr = nullptr;
        if (this->pageTable.TryGetValue(PageKey::Create(fileKey, pageId), framePtr))
            return framePtr;
    }

    const auto extentId = CoreEngine::Database::CalculateExtentId(pageId);
    //cache miss
    return this->OpenExtent(fileKey, pageId, extentId, filename, table);
}

Pages::PageView StorageManager::CreatePage(
    const FileKey fileKey,
    const DataTypes::StringView& filename,
    const CoreEngine::StorageTypes::Table *table,
    const page_id_t pageId
){
    auto* frame = this->CreateFrame(fileKey, filename, pageId, table);
    frame->type = Constants::PageType::DATA;
    frame->headerPtr->bytesLeft = Constants::PAGE_SIZE_WITHOUT_HEADER;
    return Pages::PageView(frame);
}

Pages::PageView StorageManager::GetPage(
    const FileKey fileKey,
    const DataTypes::StringView& filename,
    const page_id_t pageId,
    const CoreEngine::StorageTypes::Table *table
){
    auto* frame = this->GetRawPage(fileKey, filename, pageId, table);
    frame->type = Constants::PageType::DATA;
    return Pages::PageView(frame);
}

Pages::LargeObjectView StorageManager::CreateLargeDataPage(const FileKey fileKey, const DataTypes::StringView& filename, const page_id_t pageId){
    auto* frame = this->CreateFrame(fileKey, filename, pageId, nullptr);
    frame->type = Constants::PageType::LOB;
    frame->headerPtr->bytesLeft = Constants::LARGE_OBJECT_PAGE_SIZE;
    return Pages::LargeObjectView(frame);
}

Pages::LargeObjectView StorageManager::GetLargeDataPage(
    const FileKey fileKey,
    const DataTypes::StringView& filename,
    const page_id_t pageId,
    const CoreEngine::StorageTypes::Table *table
){
    auto* frame = this->GetRawPage(fileKey, filename, pageId, table);
    frame->type = Constants::PageType::LOB;
    return Pages::LargeObjectView(frame);
}

Pages::OverflowPageView StorageManager::CreateOverflowPage(const FileKey fileKey, const DataTypes::StringView& filename, const page_id_t pageId){
    auto* frame = this->CreateFrame(fileKey, filename, pageId, nullptr);
    frame->type = Constants::PageType::OVERFLOWTYPE;
    frame->headerPtr->bytesLeft = Constants::PAGE_SIZE_WITHOUT_HEADER;
    return Pages::OverflowPageView(frame);
}

Pages::OverflowPageView StorageManager::GetOverflowPage(
    const FileKey fileKey,
    const DataTypes::StringView& filename,
    const page_id_t pageId,
    const CoreEngine::StorageTypes::Table *table
){
    auto* frame = this->GetRawPage(fileKey, filename, pageId, table);
    frame->type = Constants::PageType::OVERFLOWTYPE;
    return Pages::OverflowPageView(frame);
}

////////////////////////////////////////////////////
////////////////////System Pages///////////////////
//////////////////////////////////////////////////

Pages::HeaderPageView StorageManager::CreateHeaderPage(const FileKey fileKey, const DataTypes::StringView& filename){
    auto* frame = this->CreateFrame(fileKey, filename, Constants::HEADER_PAGE_ID, nullptr);
    frame->type = Constants::PageType::METADATA;
    return Pages::HeaderPageView(frame);
}

Pages::GlobalAllocationPageView StorageManager::CreateGlobalAllocationMapPage(
    const FileKey fileKey,
    const DataTypes::StringView& filename,
    const page_id_t pageId
){
    auto* frame = this->CreateFrame(fileKey, filename, pageId, nullptr);
    frame->type = Constants::PageType::GAM;
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
    frame->type = Constants::PageType::IAM;
    frame->additionalHeader.allocationHeaderPtr = reinterpret_cast<Pages::IndexAllocationPageAdditionalHeader*>(frame->data + Constants::PAGE_HEADER_SIZE);
    return Pages::AllocationPageView(frame);
}


Pages::PageFreeSpaceView StorageManager::CreatePageFreeSpacePage(
    const FileKey fileKey,
    const DataTypes::StringView& filename,
    const page_id_t pageId
){
    auto* frame = this->CreateFrame(fileKey, filename, pageId, nullptr);
    frame->type = Constants::PageType::FREESPACE;
    return Pages::PageFreeSpaceView(frame);
}

Pages::IndexPageView StorageManager::CreateIndexPage(
    const FileKey fileKey,
    const DataTypes::StringView& filename,
    const CoreEngine::StorageTypes::Table* table,
    const page_id_t pageId
){
    auto* frame = this->CreateFrame(fileKey, filename, pageId, table);

    frame->type = Constants::PageType::INDEX;
    frame->headerPtr->bytesLeft = Constants::INDEX_PAGE_DEFAULT_SIZE;
    frame->additionalHeader.indexHeaderPtr = reinterpret_cast<Pages::IndexPageAdditionalHeader*>(frame->data + Constants::PAGE_HEADER_SIZE);

    frame->additionalHeader.indexHeaderPtr->nextNode = INVALID_PAGE_ID;
    frame->additionalHeader.indexHeaderPtr->previousNode = INVALID_PAGE_ID;

    return Pages::IndexPageView(frame);
}

Pages::Frame* StorageManager::CreateFrame(const FileKey fileKey, const DataTypes::StringView& filename, const page_id_t pageId, const CoreEngine::StorageTypes::Table *table){
    MultiThreading::WriterGuard lock(&this->tableMutex);

    if (this->pageTable.size() >= Constants::MAX_NUMBER_OF_PAGES)
        this->EvictPage();

    const auto frameIndex = this->clockHand % this->capacity;

    auto* framePtr = this->_memoryManager->GetFrame(frameIndex);

    framePtr->data = this->_memoryManager->Data() + frameIndex * Constants::PAGE_SIZE;
    framePtr->table = table;
    framePtr->filename = filename;
    framePtr->fileKey = fileKey;
    framePtr->isDirty = true;
    framePtr->hasSecondChance = false;
    framePtr->pinCount.store(0);
    framePtr->priority.store(Constants::PagePriority::LOW);
    framePtr->headerPtr = reinterpret_cast<Pages::PageHeader*>(framePtr->data);
    framePtr->headerPtr->pageId = pageId;

    this->pageTable[PageKey::Create(fileKey, pageId)] = framePtr;
    this->clockHand = (this->clockHand + 1) % this->capacity;

    return framePtr;
}

Pages::HeaderPageView StorageManager::GetHeaderPage(
    const FileKey fileKey,
    const DataTypes::StringView& filename
){
    auto* frame = this->GetRawPage(fileKey, filename, Constants::HEADER_PAGE_ID, nullptr);
    auto view =  Pages::HeaderPageView(frame);
    return view;
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
  const CoreEngine::StorageTypes::Table* table
){
    auto* frame = this->GetRawPage(fileKey, filename, pageId, table);

    if (frame->additionalHeader.indexHeaderPtr == nullptr)
        frame->additionalHeader.indexHeaderPtr = reinterpret_cast<Pages::IndexPageAdditionalHeader*>(frame->data + Constants::PAGE_HEADER_SIZE);

    return Pages::IndexPageView(frame);
}

Pages::AllocationPageView StorageManager::GetAllocationPage(
  const FileKey fileKey,
  const DataTypes::StringView& filename,
  const page_id_t pageId,
  const CoreEngine::StorageTypes::Table *table
){
    auto* frame = this->GetRawPage(fileKey, filename, pageId, table);
    frame->additionalHeader.allocationHeaderPtr = reinterpret_cast<Pages::IndexAllocationPageAdditionalHeader*>(frame->data + Constants::PAGE_HEADER_SIZE);
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