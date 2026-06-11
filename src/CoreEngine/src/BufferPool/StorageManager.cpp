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
    Int PageAddress::DirectorySlot(page_id_t pageId){
        const auto segmentId = PageAddress::SegmentSlot(pageId);
        return (segmentId >> PageAddress::GROUP_BITS) & (PageAddress::DIRECTORY_SIZE - 1);
    }

    Int PageAddress::GroupSlot(const page_id_t pageId){
        const auto segmentId = PageAddress::SegmentSlot(pageId);
        return segmentId & (PageAddress::GROUP_SIZE - 1);
    }

    Int PageAddress::SegmentSlot(const page_id_t pageId){
        return pageId >> PageAddress::SEGMENT_BITS;
    }

    Int PageAddress::PageSlot(const page_id_t pageId){
        return pageId & (PageAddress::SEGMENT_SIZE - 1);
    }

    Segment::Segment(){
    std::memset(frames, INVALID_FRAME, sizeof(Pages::FrameId) * PageAddress::SEGMENT_SIZE);
}

StorageManager::StorageManager(){
    this->_memoryManager = &CoreEngine::BufferPoolMemoryManager::Get();
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

        if (frame->priority.load() >= Constants::PagePriority::HIGH)
            continue;

        if (frame->hasSecondChance){
            frame->hasSecondChance = false;
            continue;
        }

        if (!frame->TryClaimForEviction())
            continue;

        victim = frame;
        victimId = candidateId;
    }

    this->TryFlushFrameToDiskNoLock(victim);
    auto* segment = this->GetSegmentNoLock(victim->fileKey, victim->Header()->pageId);
    segment->frames[PageAddress::PageSlot(victim->Header()->pageId)].store(INVALID_FRAME, std::memory_order_release);
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

        Segment* segment = this->EnsureSegmentExistsNoLock(fileKey, currentPageId);
        const auto pageSlot = PageAddress::PageSlot(currentPageId);

        if (segment->frames[pageSlot].load(std::memory_order_acquire) != INVALID_FRAME)
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

        const auto isRequestedPage = (currentPageId == pageId);

        auto* newFramePtr = this->_memoryManager->AllocateFrame(frameId);
        newFramePtr->_data = destination;
        newFramePtr->fileKey = fileKey;
        newFramePtr->isDirty = false;
        newFramePtr->hasSecondChance = false;
        newFramePtr->pinCount.store(isRequestedPage ? 1 : 0);   // producer pins the page it hands back
        newFramePtr->priority.store(Constants::PagePriority::LOW);
        newFramePtr->table = table;

        if (isRequestedPage)
            framePtr = newFramePtr;

        segment->frames[pageSlot].store(frameId, std::memory_order_release);
    }

    return framePtr;
}

Pages::Frame* StorageManager::GetFrame(
    const FileKey fileKey,
    const page_id_t pageId,
    const CoreEngine::StorageTypes::Table *table
){
    auto* segment = this->GetSegmentNoLock(fileKey, pageId);
    if (segment == nullptr)
        return this->HandlePageCacheMiss(fileKey, pageId, table);

    const auto frameId = segment->frames[PageAddress::PageSlot(pageId)].load(std::memory_order_acquire);

    if (frameId == INVALID_FRAME)
        return this->HandlePageCacheMiss(fileKey, pageId, table);

    auto* frame = _memoryManager->GetFrame(frameId);

    if (!frame->TryPin())
        return this->HandlePageCacheMiss(fileKey, pageId, table);

    if (frame->fileKey != fileKey
        || frame->Header()->pageId != pageId
        || segment->frames[PageAddress::PageSlot(pageId)].load(std::memory_order_acquire) != frameId
    ){
        frame->Unpin();
        return this->HandlePageCacheMiss(fileKey, pageId, table);
    }

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
    auto* frame = this->CreateFrame(fileKey, pageId, Constants::PageType::DATA, table);
    frame->Header()->bytesLeft = Constants::PAGE_SIZE_WITHOUT_HEADER;
    return Pages::PageView(frame);
}

Pages::LargeObjectView StorageManager::CreateLargeDataPage(const FileKey fileKey, const page_id_t pageId){
    auto* frame = this->CreateFrame(fileKey, pageId, Constants::PageType::LOB, nullptr);
    frame->Header()->bytesLeft = Constants::LARGE_OBJECT_PAGE_SIZE;
    return Pages::LargeObjectView(frame);
}

Pages::OverflowPageView StorageManager::CreateOverflowPage(const FileKey fileKey, const page_id_t pageId){
    auto* frame = this->CreateFrame(fileKey, pageId, Constants::PageType::OVERFLOWTYPE, nullptr);
    frame->Header()->bytesLeft = Constants::PAGE_SIZE_WITHOUT_HEADER;
    return Pages::OverflowPageView(frame);
}

////////////////////////////////////////////////////
////////////////////System Pages///////////////////
//////////////////////////////////////////////////

Pages::HeaderPageView StorageManager::CreateHeaderPage(const FileKey fileKey){
    auto* frame = this->CreateFrame(fileKey, Constants::HEADER_PAGE_ID, Constants::PageType::HEADER, nullptr);
    return Pages::HeaderPageView(frame);
}

Pages::GlobalAllocationPageView StorageManager::CreateGlobalAllocationMapPage(const FileKey fileKey, const page_id_t pageId){
    auto* frame = this->CreateFrame(fileKey, pageId, Constants::PageType::GAM, nullptr);
    return Pages::GlobalAllocationPageView(frame);
}

Pages::AllocationPageView StorageManager::CreateAllocationPage(
    const FileKey fileKey,
    const table_id_t tableId,
    const page_id_t pageId,
    const extent_id_t startingExtentId
){
    auto* frame = this->CreateFrame(fileKey, pageId, Constants::PageType::IAM, nullptr);
    return Pages::AllocationPageView(frame);
}


Pages::PageFreeSpaceView StorageManager::CreatePageFreeSpacePage(const FileKey fileKey,const page_id_t pageId){
    auto* frame = this->CreateFrame(fileKey, pageId, Constants::PageType::FREESPACE, nullptr);
    return Pages::PageFreeSpaceView(frame);
}

Pages::IndexPageView StorageManager::CreateIndexPage(
    const FileKey fileKey,
    const CoreEngine::StorageTypes::Table* table,
    const page_id_t pageId
){
    auto* frame = this->CreateFrame(fileKey, pageId, Constants::PageType::INDEX, table);

    frame->Header()->bytesLeft = Constants::INDEX_PAGE_DEFAULT_SIZE;
    auto* additionalHeader = reinterpret_cast<Pages::IndexPageAdditionalHeader*>(frame->_data + Constants::PAGE_HEADER_SIZE);
    additionalHeader->nextNode = INVALID_PAGE_ID;
    additionalHeader->previousNode = INVALID_PAGE_ID;

    return Pages::IndexPageView(frame);
}

Pages::Frame* StorageManager::CreateFrame(
    const FileKey fileKey,
    const page_id_t pageId,
    const Constants::PageType type,
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
    framePtr->pinCount.store(1);   // producer pins the page it hands back
    framePtr->priority.store(Constants::PagePriority::LOW);

    auto* header = framePtr->Header();
    header->pageId = pageId;
    header->SetType(type);

    this->CacheFrameToPageTableNoLock(fileKey, pageId, frameId);
    return framePtr;
}

Segment* StorageManager::GetSegmentNoLock(FileKey fileKey, const page_id_t pageId) const{
        if (fileKey.databaseId >= MAX_DATABASES)
            return nullptr;

        const auto* db = this->_pageTable[fileKey.databaseId].load(std::memory_order_acquire);
        if (db == nullptr)
            return nullptr;

        const auto& directory = db->files[static_cast<size_t>(fileKey.type)];
        const auto* group = directory.groups[PageAddress::DirectorySlot(pageId)].load(std::memory_order_acquire);
        if (group == nullptr)
            return nullptr;

        return group->segments[PageAddress::GroupSlot(pageId)].load(std::memory_order_acquire);
}

void StorageManager::EnsureDatabaseTableExistsNoLock(const FileKey fileKey){
    if (this->_pageTable[fileKey.databaseId].load(std::memory_order_relaxed) == nullptr)
        this->_pageTable[fileKey.databaseId].store(
            this->_allocator.Allocate<DatabaseTable>(),
            std::memory_order_release
        );
}

Segment* StorageManager::EnsureSegmentExistsNoLock(const FileKey fileKey, const page_id_t pageId) const{
    auto& directory = this->_pageTable[fileKey.databaseId].load(std::memory_order_relaxed)
                                   ->files[static_cast<size_t>(fileKey.type)];

    auto& groupSlot = directory.groups[PageAddress::DirectorySlot(pageId)];
    auto* group = groupSlot.load(std::memory_order_relaxed);
    if (group == nullptr){
        group = this->_allocator.Allocate<SegmentGroup>();
        groupSlot.store(group, std::memory_order_release);
    }

    auto& segmentSlot = group->segments[PageAddress::GroupSlot(pageId)];
    auto* segment = segmentSlot.load(std::memory_order_relaxed);
    if (segment == nullptr){
        segment = this->_allocator.Allocate<Segment>();
        segmentSlot.store(segment, std::memory_order_release);
    }

    return segment;
}

Pages::Frame* StorageManager::HandlePageCacheMiss(
    const FileKey fileKey,
    const page_id_t pageId,
    const CoreEngine::StorageTypes::Table* table
){
    MultiThreading::WriterGuard guard(&this->tableMutex);
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

////////////////////////////////////////////////////////////////////
/////////////////////////Globally Used Functions///////////////////
//////////////////////////////////////////////////////////////////
void StorageManager::CacheFrameToPageTableNoLock(
    const FileKey key,
    const page_id_t pageId,
    const Pages::FrameId frameId
) const{
    auto* segment = this->GetSegmentNoLock(key, pageId);   // exists: caller ran EnsureSegmentExistsNoLock
    segment->frames[PageAddress::PageSlot(pageId)].store(frameId, std::memory_order_release);
}
} // namespace Storage