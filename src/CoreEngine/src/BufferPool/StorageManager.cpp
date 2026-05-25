#include "../../include/BufferPool/StorageManager.h"
#include "../../include/Database.h"
#include "../../../Systemic/include/Guards/ReaderGuard.h"
#include "../../../Systemic/include/Guards/WriterGuard.h"
#include "Pages/GlobalAllocationPageView.h"
#include "Pages/HeaderPageView.h"
#include "Pages/IndexPageView.h"
#include "Pages/Additional/Frame.h"
#include <iostream>

namespace Storage {
Segment::Segment(){
    std::memset(frames, INVALID_FRAME, sizeof(FrameId) * SEGMENT_SIZE);
}

StorageManager::StorageManager(){
    this->_memoryManager = &CoreEngine::BufferPoolMemoryManager::Get();
    this->_pageTable.SetAllocator(&this->_allocator);
    this->capacity = this->_memoryManager->Capacity();
    this->clockHand = 0;
}

void StorageManager::EvictPageNoLock() {
    const Pages::Frame* victim = nullptr;
    while (victim == nullptr) {
        auto* frame = this->_memoryManager->GetFrame(this->clockHand);

        this->clockHand = (this->clockHand + 1) % this->capacity;
        if (!frame->IsValid())
            continue;

        MultiThreading::WriterGuard pageLock(&frame->latch);

        if (frame->pinCount.load() > 0 || frame->priority.load() >= Constants::PagePriority::HIGH)
            continue;

        if (frame->hasSecondChance){
            frame->hasSecondChance = false;
            continue;
        }

        victim = frame;
    }

    this->TryFlushFrameToDiskNoLock(victim);

    const auto& victimKey = victim->fileKey;
    const auto segmentId = victim->headerPtr->pageId / SEGMENT_SIZE;
    const auto segmentOffset = victim->headerPtr->pageId % SEGMENT_SIZE;
    this->_pageTable[victimKey.databaseId]->files[static_cast<size_t>(victimKey.type)].segments[segmentId]->frames[segmentOffset] = INVALID_FRAME;
    this->_memoryManager->EvictFrame();
}

void StorageManager::TryFlushFrameToDiskNoLock(const Pages::Frame *framePtr){
    if (!framePtr->isDirty)
        return;

    const auto file = this->fileManager.GetFile(framePtr->fileKey, framePtr->filename);
    const auto offSet = framePtr->headerPtr->pageId * Constants::PAGE_SIZE;

    file.Write(framePtr->_data, Constants::PAGE_SIZE, offSet);
    file.Flush();
}

Pages::Frame* StorageManager::OpenExtentNoLock(
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

        if (bytesRead < offSet) break;

        const page_id_t currentPageId = firstExtentPageId + i;
        const auto segmentId = static_cast<Int>(currentPageId / SEGMENT_SIZE);
        const auto segmentOffset = static_cast<Int>(currentPageId % SEGMENT_SIZE);

        this->EnsureSegmentExistsNoLock(fileKey, currentPageId);
        auto* segment = this->_pageTable[fileKey.databaseId]->files[static_cast<size_t>(fileKey.type)].segments[segmentId];

        if (segment->frames[segmentOffset] != INVALID_FRAME)
            continue;

        if (this->_memoryManager->IsFull())
            this->EvictPageNoLock();

        const auto frameId = this->clockHand % this->capacity;

        const auto pageDataOffset = frameId * Constants::PAGE_SIZE;
        auto* newFramePtr = this->_memoryManager->AllocateFrame(frameId);
        newFramePtr->_data = this->_memoryManager->CopyToMemory(buffer, pageDataOffset, offSet);
        newFramePtr->filename = filename;
        newFramePtr->fileKey = fileKey;
        newFramePtr->isDirty = false;
        newFramePtr->hasSecondChance = false;
        newFramePtr->pinCount.store(0);
        newFramePtr->priority.store(Constants::PagePriority::LOW);
        newFramePtr->table = table;
        newFramePtr->headerPtr = reinterpret_cast<Pages::PageHeader*>(newFramePtr->_data);

        if (currentPageId == pageId)
            framePtr = newFramePtr;

        segment->frames[segmentOffset] = frameId;
        this->clockHand = (this->clockHand + 1) % this->capacity;
    }

    return framePtr;
}

Pages::Frame* StorageManager::GetFrame(
    const FileKey fileKey,
    const DataTypes::StringView& filename,
    const page_id_t pageId,
    const CoreEngine::StorageTypes::Table *table
){
    MultiThreading::ReaderGuard lock(&this->tableMutex);

    if (fileKey.databaseId >= this->_pageTable.Size())
        return this->HandlePageCacheMiss(fileKey, filename, pageId, table, lock);

    auto* dbTable = this->_pageTable[fileKey.databaseId];
    if (dbTable == nullptr)
        return this->HandlePageCacheMiss(fileKey, filename, pageId, table, lock);

    auto& [segments] = dbTable->files[static_cast<size_t>(fileKey.type)];

    const auto segmentId = pageId / SEGMENT_SIZE;
    const auto offset = pageId % SEGMENT_SIZE;

    if (segmentId >= segments.Size())
        return this->HandlePageCacheMiss(fileKey, filename, pageId, table, lock);

    const auto* segment = segments[segmentId];
    if (segment == nullptr)
        return this->HandlePageCacheMiss(fileKey, filename, pageId, table, lock);

    const auto frameId = segment->frames[offset];
    if (frameId == INVALID_FRAME)
        return this->HandlePageCacheMiss(fileKey, filename, pageId, table, lock);

    return _memoryManager->GetFrame(frameId);
}

StorageManager& StorageManager::Get(){
    static StorageManager storageManager;
    return storageManager;
}

StorageManager::~StorageManager(){
    for (Int i = 0; i < this->capacity; i++){
        const auto* frame = this->_memoryManager->GetFrame(i);
        if (frame == nullptr || !frame->IsValid())
            continue;

        MultiThreading::WriterGuard pageLock(&frame->latch);
        this->TryFlushFrameToDiskNoLock(frame);
    }
}

void StorageManager::CreateFile(
    const FileKey key,
    const DataTypes::StringView& filename,
    const DataTypes::StringView& extension
){
    this->fileManager.CreateFile(key, filename, extension);
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
    auto* frame = this->GetFrame(fileKey, filename, pageId, table);
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
    auto* frame = this->GetFrame(fileKey, filename, pageId, table);
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
    auto* frame = this->GetFrame(fileKey, filename, pageId, table);
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
    frame->additionalHeader.allocationHeaderPtr = reinterpret_cast<Pages::IndexAllocationPageAdditionalHeader*>(frame->_data + Constants::PAGE_HEADER_SIZE);
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
    frame->additionalHeader.indexHeaderPtr = reinterpret_cast<Pages::IndexPageAdditionalHeader*>(frame->_data + Constants::PAGE_HEADER_SIZE);

    frame->additionalHeader.indexHeaderPtr->nextNode = INVALID_PAGE_ID;
    frame->additionalHeader.indexHeaderPtr->previousNode = INVALID_PAGE_ID;

    return Pages::IndexPageView(frame);
}

Pages::Frame* StorageManager::CreateFrame(
    const FileKey fileKey,
    const DataTypes::StringView& filename,
    const page_id_t pageId,
    const CoreEngine::StorageTypes::Table *table
){
    MultiThreading::WriterGuard lock(&this->tableMutex);

    this->EnsureDatabaseTableExistsNoLock(fileKey);
    this->EnsureSegmentExistsNoLock(fileKey, pageId);

    if (this->_memoryManager->IsFull())
        this->EvictPageNoLock();

    const auto frameId = this->clockHand % this->capacity;

    auto* framePtr = this->_memoryManager->AllocateFrame(frameId);
    framePtr->_data = this->_memoryManager->Data() + frameId * Constants::PAGE_SIZE;
    framePtr->table = table;
    framePtr->filename = filename;
    framePtr->fileKey = fileKey;
    framePtr->isDirty = true;
    framePtr->hasSecondChance = false;
    framePtr->pinCount.store(0);
    framePtr->priority.store(Constants::PagePriority::LOW);
    framePtr->headerPtr = reinterpret_cast<Pages::PageHeader*>(framePtr->_data);
    framePtr->headerPtr->pageId = pageId;

    this->CacheFrameToPageTableNoLock(fileKey, pageId, frameId);
    this->clockHand = (this->clockHand + 1) % this->capacity;

    return framePtr;
}

void StorageManager::EnsureDatabaseTableExistsNoLock(const FileKey fileKey){
    if (this->_pageTable.Size() < fileKey.databaseId + 1)
        this->_pageTable.Resize(fileKey.databaseId + 1);

    if (this->_pageTable[fileKey.databaseId] == nullptr){
        auto* databaseTable = this->_allocator.Allocate<DatabaseTable>();
        this->_pageTable[fileKey.databaseId] = databaseTable;
        databaseTable->files[0].segments.SetAllocator(&this->_allocator);
        databaseTable->files[1].segments.SetAllocator(&this->_allocator);
    }
}

void StorageManager::EnsureSegmentExistsNoLock(const FileKey fileKey, const page_id_t pageId){
    auto& [segments] = this->_pageTable[fileKey.databaseId]->files[static_cast<size_t>(fileKey.type)];
    const auto segmentId = static_cast<Int>(pageId / SEGMENT_SIZE);

    if (segments.Size() < segmentId + 1)
        segments.Resize(segmentId + 1);

    if (segments[segmentId] == nullptr)
        segments[segmentId] = this->_allocator.Allocate<Segment>();
}

Pages::Frame* StorageManager::HandlePageCacheMiss(
    const FileKey fileKey,
    const DataTypes::StringView& filename,
    const page_id_t pageId,
    const CoreEngine::StorageTypes::Table* table,
    MultiThreading::ReaderGuard& readGuard
){
    MultiThreading::WriterGuard::Promote(&this->tableMutex, readGuard);
    this->EnsureDatabaseTableExistsNoLock(fileKey);
    this->EnsureSegmentExistsNoLock(fileKey, pageId);

    MultiThreading::WriterGuard clockLock(&this->clockMutex_);
    return this->OpenExtentNoLock(
        fileKey, pageId,
        CoreEngine::Database::CalculateExtentId(pageId),
        filename, table
    );
}


Pages::HeaderPageView StorageManager::GetHeaderPage(
    const FileKey fileKey,
    const DataTypes::StringView& filename
){
    auto* frame = this->GetFrame(fileKey, filename, Constants::HEADER_PAGE_ID, nullptr);
    auto view =  Pages::HeaderPageView(frame);
    return view;
}

Pages::PageFreeSpaceView StorageManager::GetPageFreeSpacePage(const FileKey fileKey, const DataTypes::StringView& filename, const page_id_t pageId){
    auto* page = this->GetFrame(fileKey, filename, pageId, nullptr);
    return Pages::PageFreeSpaceView(page);
}

Pages::IndexPageView StorageManager::GetIndexPage(
  const FileKey fileKey,
  const DataTypes::StringView& filename,
  const page_id_t pageId,
  const CoreEngine::StorageTypes::Table* table
){
    auto* frame = this->GetFrame(fileKey, filename, pageId, table);

    if (frame->additionalHeader.indexHeaderPtr == nullptr)
        frame->additionalHeader.indexHeaderPtr = reinterpret_cast<Pages::IndexPageAdditionalHeader*>(frame->_data + Constants::PAGE_HEADER_SIZE);

    return Pages::IndexPageView(frame);
}

Pages::AllocationPageView StorageManager::GetAllocationPage(
  const FileKey fileKey,
  const DataTypes::StringView& filename,
  const page_id_t pageId,
  const CoreEngine::StorageTypes::Table *table
){
    auto* frame = this->GetFrame(fileKey, filename, pageId, table);
    frame->additionalHeader.allocationHeaderPtr = reinterpret_cast<Pages::IndexAllocationPageAdditionalHeader*>(frame->_data + Constants::PAGE_HEADER_SIZE);
    return Pages::AllocationPageView(frame);
}

Pages::GlobalAllocationPageView StorageManager::GetGlobalAllocationMapPage(
    const FileKey fileKey,
    const DataTypes::StringView& filename,
    const page_id_t pageId
    ){
    auto* frame = this->GetFrame(fileKey, filename, pageId, nullptr);
    return Pages::GlobalAllocationPageView(frame);
}

////////////////////////////////////////////////////////////////////
/////////////////////////Globally Used Functions///////////////////
//////////////////////////////////////////////////////////////////
void StorageManager::CacheFrameToPageTableNoLock(FileKey key, const page_id_t pageId, const FrameId frameId){
    const auto segmentId = pageId / SEGMENT_SIZE;
    const auto offset = pageId % SEGMENT_SIZE;
    this->_pageTable[key.databaseId]->files[static_cast<size_t>(key.type)].segments[segmentId]->frames[offset] = frameId;
}
} // namespace Storage