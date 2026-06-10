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
    std::memset(frames, INVALID_FRAME, sizeof(Pages::FrameId) * SEGMENT_SIZE);
}

StorageManager::StorageManager(){
    this->_memoryManager = &CoreEngine::BufferPoolMemoryManager::Get();
    this->_pageTable.SetAllocator(&this->_allocator);
    this->capacity = this->_memoryManager->Capacity();
    this->clockHand = 0;
}

void StorageManager::EvictPageNoLock() {
    const Pages::Frame* victim = nullptr;
    Pages::FrameId victimId = INVALID_FRAME;
    while (victim == nullptr) {
        const auto candidateId = this->clockHand;
        auto* frame = this->_memoryManager->GetFrame(candidateId);

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
        victimId = candidateId;
    }

    this->TryFlushFrameToDiskNoLock(victim);

    const auto& victimKey = victim->fileKey;
    const auto segmentId = victim->Header()->pageId / SEGMENT_SIZE;
    const auto segmentOffset = victim->Header()->pageId % SEGMENT_SIZE;
    this->_pageTable[victimKey.databaseId]->files[static_cast<size_t>(victimKey.type)].segments[segmentId]->frames[segmentOffset] = INVALID_FRAME;
    this->_memoryManager->PushStackNoLock(victimId);
}

void StorageManager::TryFlushFrameToDiskNoLock(const Pages::Frame *framePtr){
    if (!framePtr->isDirty)
        return;

    const auto fd = this->fileManager.GetFile(framePtr->fileKey);
    const auto offSet = framePtr->Header()->pageId * Constants::PAGE_SIZE;

    const auto _ = File::Write(fd, framePtr->_data, Constants::PAGE_SIZE, offSet);
    File::Flush(fd);
}

Pages::Frame* StorageManager::OpenExtentNoLock(
    const FileKey fileKey,
    const page_id_t pageId,
    const extent_id_t extentId,
    const CoreEngine::StorageTypes::Table *table
){
    // read page from disk, call this->fileManager
    const auto fd = this->fileManager.GetFile(fileKey);
    const auto firstExtentPageId = CoreEngine::Database::CalculateExtentFirstPageId(extentId);

    // take into account the metadata page all the others
    Pages::Frame* framePtr = nullptr;
    for (Int i = 0; i < Constants::EXTENT_SIZE; i++){
        const page_id_t currentPageId = firstExtentPageId + i;
        const auto segmentId = static_cast<Int>(currentPageId / SEGMENT_SIZE);
        const auto segmentOffset = static_cast<Int>(currentPageId % SEGMENT_SIZE);

        this->EnsureSegmentExistsNoLock(fileKey, currentPageId);
        auto* segment = this->_pageTable[fileKey.databaseId]->files[static_cast<size_t>(fileKey.type)].segments[segmentId];

        if (segment->frames[segmentOffset] != INVALID_FRAME)
            continue;

        const auto frameId = this->AcquireFrameId();
        auto* destination = this->_memoryManager->Data() + frameId * Constants::PAGE_SIZE;

        const auto bytesRead = File::Read(fd, destination, Constants::PAGE_SIZE, currentPageId * Constants::PAGE_SIZE);

        if (bytesRead <= 0){
            this->_memoryManager->PushStackNoLock(frameId);
            break;
        }
        if (bytesRead < Constants::PAGE_SIZE)
            std::memset(destination, 0, Constants::PAGE_SIZE - bytesRead);

        auto* newFramePtr = this->_memoryManager->AllocateFrame(frameId);
        newFramePtr->_data = destination;
        newFramePtr->fileKey = fileKey;
        newFramePtr->isDirty = false;
        newFramePtr->hasSecondChance = false;
        newFramePtr->pinCount.store(0);
        newFramePtr->priority.store(Constants::PagePriority::LOW);
        newFramePtr->table = table;

        if (currentPageId == pageId)
            framePtr = newFramePtr;

        segment->frames[segmentOffset] = frameId;
    }

    return framePtr;
}

Pages::Frame* StorageManager::GetFrame(
    const FileKey fileKey,
    const page_id_t pageId,
    const CoreEngine::StorageTypes::Table *table
){
    MultiThreading::ReaderGuard lock(&this->tableMutex);

    if (fileKey.databaseId >= this->_pageTable.Size())
        return this->HandlePageCacheMiss(fileKey, pageId, table, lock);

    auto* dbTable = this->_pageTable[fileKey.databaseId];
    if (dbTable == nullptr)
        return this->HandlePageCacheMiss(fileKey, pageId, table, lock);

    auto& [segments] = dbTable->files[static_cast<size_t>(fileKey.type)];

    const auto segmentId = pageId / SEGMENT_SIZE;
    const auto offset = pageId % SEGMENT_SIZE;

    if (segmentId >= segments.Size())
        return this->HandlePageCacheMiss(fileKey, pageId, table, lock);

    const auto* segment = segments[segmentId];
    if (segment == nullptr)
        return this->HandlePageCacheMiss(fileKey, pageId, table, lock);

    const auto frameId = segment->frames[offset];
    if (frameId == INVALID_FRAME)
        return this->HandlePageCacheMiss(fileKey, pageId, table, lock);

    auto* frame = _memoryManager->GetFrame(frameId);
    frame->hasSecondChance = true;
    return frame;
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

void StorageManager::OpenFile(const FileKey key, const DataTypes::StringView& filename){
    this->fileManager.OpenFile(key, filename);
}

Pages::PageView StorageManager::CreatePage(
    const FileKey fileKey,
    const CoreEngine::StorageTypes::Table *table,
    const page_id_t pageId
){
    auto* frame = this->CreateFrame(fileKey, pageId, table);
    frame->type = Constants::PageType::DATA;
    frame->Header()->bytesLeft = Constants::PAGE_SIZE_WITHOUT_HEADER;
    return Pages::PageView(frame);
}

Pages::PageView StorageManager::GetPage(
    const FileKey fileKey,
    const page_id_t pageId,
    const CoreEngine::StorageTypes::Table *table
){
    auto* frame = this->GetFrame(fileKey, pageId, table);
    frame->type = Constants::PageType::DATA;
    return Pages::PageView(frame);
}

Pages::LargeObjectView StorageManager::CreateLargeDataPage(const FileKey fileKey, const page_id_t pageId){
    auto* frame = this->CreateFrame(fileKey, pageId, nullptr);
    frame->type = Constants::PageType::LOB;
    frame->Header()->bytesLeft = Constants::LARGE_OBJECT_PAGE_SIZE;
    return Pages::LargeObjectView(frame);
}

Pages::LargeObjectView StorageManager::GetLargeDataPage(
    const FileKey fileKey,
    const page_id_t pageId,
    const CoreEngine::StorageTypes::Table *table
){
    auto* frame = this->GetFrame(fileKey, pageId, table);
    frame->type = Constants::PageType::LOB;
    return Pages::LargeObjectView(frame);
}

Pages::OverflowPageView StorageManager::CreateOverflowPage(const FileKey fileKey, const page_id_t pageId){
    auto* frame = this->CreateFrame(fileKey, pageId, nullptr);
    frame->type = Constants::PageType::OVERFLOWTYPE;
    frame->Header()->bytesLeft = Constants::PAGE_SIZE_WITHOUT_HEADER;
    return Pages::OverflowPageView(frame);
}

Pages::OverflowPageView StorageManager::GetOverflowPage(
    const FileKey fileKey,
    const page_id_t pageId,
    const CoreEngine::StorageTypes::Table *table
){
    auto* frame = this->GetFrame(fileKey, pageId, table);
    frame->type = Constants::PageType::OVERFLOWTYPE;
    return Pages::OverflowPageView(frame);
}

////////////////////////////////////////////////////
////////////////////System Pages///////////////////
//////////////////////////////////////////////////

Pages::HeaderPageView StorageManager::CreateHeaderPage(const FileKey fileKey){
    auto* frame = this->CreateFrame(fileKey, Constants::HEADER_PAGE_ID, nullptr);
    frame->type = Constants::PageType::METADATA;
    return Pages::HeaderPageView(frame);
}

Pages::GlobalAllocationPageView StorageManager::CreateGlobalAllocationMapPage(const FileKey fileKey, const page_id_t pageId){
    auto* frame = this->CreateFrame(fileKey, pageId, nullptr);
    frame->type = Constants::PageType::GAM;
    return Pages::GlobalAllocationPageView(frame);
}

Pages::AllocationPageView StorageManager::CreateAllocationPage(
    const FileKey fileKey,
    const table_id_t tableId,
    const page_id_t pageId,
    const extent_id_t startingExtentId
){
    auto* frame = this->CreateFrame(fileKey, pageId, nullptr);
    frame->type = Constants::PageType::IAM;
    return Pages::AllocationPageView(frame);
}


Pages::PageFreeSpaceView StorageManager::CreatePageFreeSpacePage(const FileKey fileKey,const page_id_t pageId){
    auto* frame = this->CreateFrame(fileKey, pageId, nullptr);
    frame->type = Constants::PageType::FREESPACE;
    return Pages::PageFreeSpaceView(frame);
}

Pages::IndexPageView StorageManager::CreateIndexPage(
    const FileKey fileKey,
    const CoreEngine::StorageTypes::Table* table,
    const page_id_t pageId
){
    auto* frame = this->CreateFrame(fileKey, pageId, table);

    frame->type = Constants::PageType::INDEX;
    frame->Header()->bytesLeft = Constants::INDEX_PAGE_DEFAULT_SIZE;

    auto* additionalHeader = reinterpret_cast<Pages::IndexPageAdditionalHeader*>(frame->_data + Constants::PAGE_HEADER_SIZE);
    additionalHeader->nextNode = INVALID_PAGE_ID;
    additionalHeader->previousNode = INVALID_PAGE_ID;

    return Pages::IndexPageView(frame);
}

Pages::Frame* StorageManager::CreateFrame(
    const FileKey fileKey,
    const page_id_t pageId,
    const CoreEngine::StorageTypes::Table *table
){
    MultiThreading::WriterGuard lock(&this->tableMutex);

    this->EnsureDatabaseTableExistsNoLock(fileKey);
    this->EnsureSegmentExistsNoLock(fileKey, pageId);

    const auto frameId = this->AcquireFrameId();

    auto* framePtr = this->_memoryManager->AllocateFrame(frameId);
    framePtr->_data = this->_memoryManager->Data() + frameId * Constants::PAGE_SIZE;
    framePtr->table = table;
    framePtr->fileKey = fileKey;
    framePtr->isDirty = true;
    framePtr->hasSecondChance = false;
    framePtr->pinCount.store(0);
    framePtr->priority.store(Constants::PagePriority::LOW);
    framePtr->Header()->pageId = pageId;

    this->CacheFrameToPageTableNoLock(fileKey, pageId, frameId);

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
    const page_id_t pageId,
    const CoreEngine::StorageTypes::Table* table,
    MultiThreading::ReaderGuard& readGuard
){
    MultiThreading::WriterGuard::Promote(&this->tableMutex, readGuard);
    this->EnsureDatabaseTableExistsNoLock(fileKey);
    this->EnsureSegmentExistsNoLock(fileKey, pageId);

    return this->OpenExtentNoLock(
        fileKey, pageId,
        CoreEngine::Database::CalculateExtentId(pageId),
        table
    );
}

Pages::FrameId StorageManager::AcquireFrameId(){
    Pages::FrameId frameId = INVALID_FRAME;
    while (this->_memoryManager->PopStackNoLock(frameId) == false)
        this->EvictPageNoLock();
    return frameId;
}


Pages::HeaderPageView StorageManager::GetHeaderPage(const FileKey fileKey){
    auto* frame = this->GetFrame(fileKey, Constants::HEADER_PAGE_ID, nullptr);
    auto view =  Pages::HeaderPageView(frame);
    return view;
}

Pages::PageFreeSpaceView StorageManager::GetPageFreeSpacePage(const FileKey fileKey, const page_id_t pageId){
    auto* page = this->GetFrame(fileKey, pageId, nullptr);
    return Pages::PageFreeSpaceView(page);
}

Pages::IndexPageView StorageManager::GetIndexPage(
  const FileKey fileKey,
  const page_id_t pageId,
  const CoreEngine::StorageTypes::Table* table
){
    auto* frame = this->GetFrame(fileKey, pageId, table);
    return Pages::IndexPageView(frame);
}

Pages::AllocationPageView StorageManager::GetAllocationPage(
    const FileKey fileKey,
    const page_id_t pageId,
    const CoreEngine::StorageTypes::Table *table
){
    auto* frame = this->GetFrame(fileKey, pageId, table);
    return Pages::AllocationPageView(frame);
}

Pages::GlobalAllocationPageView StorageManager::GetGlobalAllocationMapPage(const FileKey fileKey, const page_id_t pageId){
    auto* frame = this->GetFrame(fileKey, pageId, nullptr);
    return Pages::GlobalAllocationPageView(frame);
}

////////////////////////////////////////////////////////////////////
/////////////////////////Globally Used Functions///////////////////
//////////////////////////////////////////////////////////////////
void StorageManager::CacheFrameToPageTableNoLock(FileKey key, const page_id_t pageId, const Pages::FrameId frameId){
    const auto segmentId = pageId / SEGMENT_SIZE;
    const auto offset = pageId % SEGMENT_SIZE;
    this->_pageTable[key.databaseId]->files[static_cast<size_t>(key.type)].segments[segmentId]->frames[offset] = frameId;
}
} // namespace Storage