#include <CoreEngine/BufferPool/StorageManager.h>
#include <CoreEngine/Database.h>
#include <Systemic/Guards/WriterGuard.h>
#include <CoreEngine/Pages/GlobalAllocationPageView.h>
#include <CoreEngine/Pages/HeaderPageView.h>
#include <CoreEngine/Pages/Additional/Frame.h>
#include <iostream>

#include "CoreEngine/BufferPool/StorageLayout.h"
#include "Systemic/Guards/ReaderGuard.h"

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
    this->_capacity = this->_memoryManager.Capacity();
    this->_clockHand = 0;
}

void StorageManager::EvictPageNoLock() {
    Pages::Frame* victim = nullptr;
    Pages::FrameId victimId = INVALID_FRAME;
    while (victim == nullptr) {
        const auto candidateId = this->_clockHand;
        auto* frame = this->_memoryManager.GetFrame(candidateId);

        this->_clockHand = (this->_clockHand + 1) % this->_capacity;
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
    auto* segment = this->GetSegmentNoLock(victim->_fileKey, victim->Header()->pageId);
    segment->frames[PageAddress::PageSlot(victim->Header()->pageId)].store(INVALID_FRAME, std::memory_order_release);
    this->_memoryManager.PushStackNoLock(victimId);
}

void StorageManager::TryFlushFrameToDiskNoLock(Pages::Frame *framePtr){
    if (!framePtr->IsDirty())
        return;

    const auto fd = this->_fileManager.GetFile(framePtr->_fileKey);

    const auto pageId = framePtr->Header()->pageId;
    const auto offSet = pageId * CoreEngine::StorageLayout::PAGE_SIZE;

    const auto _ = File::Write(fd, framePtr->_data, CoreEngine::StorageLayout::PAGE_SIZE, offSet);

    framePtr->MarkFlushed();
    File::Flush(fd);
}

Pages::Frame* StorageManager::OpenExtentNoLock(
    const FileKey fileKey,
    const page_id_t pageId,
    const extent_id_t extentId
){
    // read page from disk, call this->fileManager
    const auto fd = this->_fileManager.GetFile(fileKey);
    const auto firstExtentPageId = CoreEngine::Database::CalculateExtentFirstPageId(extentId);

    // take into account the metadata page all the others
    Pages::Frame* framePtr = nullptr;
    for (Int i = 0; i < CoreEngine::StorageLayout::EXTENT_SIZE; i++){
        const page_id_t currentPageId = firstExtentPageId + i;

        Segment* segment = this->EnsureSegmentExistsNoLock(fileKey, currentPageId);
        const auto pageSlot = PageAddress::PageSlot(currentPageId);

        if (segment->frames[pageSlot].load(std::memory_order_acquire) != INVALID_FRAME)
            continue;

        const auto frameId = this->AcquireFrameId();
        auto* destination = this->_memoryManager.Data() + frameId * CoreEngine::StorageLayout::PAGE_SIZE;

        const auto bytesRead = File::Read(fd, destination, CoreEngine::StorageLayout::PAGE_SIZE, currentPageId * CoreEngine::StorageLayout::PAGE_SIZE);

        if (bytesRead <= 0){
            this->_memoryManager.PushStackNoLock(frameId);
            break;
        }
        if (bytesRead < CoreEngine::StorageLayout::PAGE_SIZE)
            std::memset(destination, 0, CoreEngine::StorageLayout::PAGE_SIZE - bytesRead);

        const auto isRequestedPage = (currentPageId == pageId);

        auto* newFramePtr = this->_memoryManager.AllocateFrame(frameId);
        newFramePtr->_data = destination;
        newFramePtr->_fileKey = fileKey;
        newFramePtr->isDirty = false;
        newFramePtr->hasSecondChance = false;
        newFramePtr->_pinCount.store(isRequestedPage ? 1 : 0);   // producer pins the page it hands back
        newFramePtr->priority.store(Constants::PagePriority::LOW);
        // newFramePtr->table = table;

        if (isRequestedPage)
            framePtr = newFramePtr;

        segment->frames[pageSlot].store(frameId, std::memory_order_release);
    }

    return framePtr;
}

Pages::Frame* StorageManager::GetFrame(
    const FileKey fileKey,
    const page_id_t pageId
){
    auto* segment = this->GetSegmentNoLock(fileKey, pageId);
    if (segment == nullptr)
        return this->HandlePageCacheMiss(fileKey, pageId);

    const auto frameId = segment->frames[PageAddress::PageSlot(pageId)].load(std::memory_order_acquire);

    if (frameId == INVALID_FRAME)
        return this->HandlePageCacheMiss(fileKey, pageId);

    auto* frame = this->_memoryManager.GetFrame(frameId);

    if (!frame->TryPin())
        return this->HandlePageCacheMiss(fileKey, pageId);

    if (frame->_fileKey != fileKey
        || frame->Header()->pageId != pageId
        || segment->frames[PageAddress::PageSlot(pageId)].load(std::memory_order_acquire) != frameId
    ){
        frame->Unpin();
        return this->HandlePageCacheMiss(fileKey, pageId);
    }

    frame->hasSecondChance = true;
    return frame;
}

StorageManager& StorageManager::Get(){
    static StorageManager storageManager;
    return storageManager;
}

StorageManager::~StorageManager(){
    for (Int i = 0; i < this->_capacity; i++){
        auto* frame = this->_memoryManager.GetFrame(i);
        if (frame == nullptr || !frame->IsValid())
            continue;

        MultiThreading::WriterGuard pageLock(&frame->_latch);
        this->TryFlushFrameToDiskNoLock(frame);
    }
}

void StorageManager::Initialize(const UnsignedBigInt size){
    this->_memoryManager.Initialize(size);
}

void StorageManager::CreateFile_(
    const FileKey key,
    const DataTypes::StringView& filename,
    const DataTypes::StringView& extension
){
    this->_fileManager.CreateFile_(key, filename, extension);
}

void StorageManager::OpenFile(const FileKey key, const DataTypes::StringView& filename){
    this->_fileManager.OpenFile(key, filename);
}

void StorageManager::CloseAllFiles(){
    this->_fileManager.CloseAllFiles();
}

void StorageManager::FlushAll(){
        MultiThreading::WriterGuard lock(&this->_tableMutex);

        std::vector<file_descriptor_t> touchedFiles;
        for (Int i = 0;i < this->_memoryManager.Capacity(); i++){
            auto* frame = this->_memoryManager.GetFrame(i);
            if (!frame->IsValid() || !frame->IsDirty())
                continue;

            MultiThreading::ReaderGuard latch(&frame->_latch);
            const auto fd = this->_fileManager.GetFile(frame->_fileKey);
            const auto pageId = frame->Header()->pageId;

            const auto bytes = File::Write(
                fd,
                frame->_data,
                CoreEngine::StorageLayout::PAGE_SIZE,
                static_cast<UnsignedBigInt>(pageId) * CoreEngine::StorageLayout::PAGE_SIZE
            );

            frame->MarkFlushed();
            if (std::ranges::find(touchedFiles, fd) == touchedFiles.end())
                touchedFiles.push_back(fd);
        }

        for (const auto fd: touchedFiles)
            File::Flush(fd);
}

////////////////////////////////////////////////////
////////////////////System Pages///////////////////
//////////////////////////////////////////////////

Pages::HeaderPageView StorageManager::CreateHeaderPage(const FileKey fileKey){
    auto* frame = this->CreateFrame(fileKey, CoreEngine::StorageLayout::HEADER_PAGE_ID, Constants::PageType::HEADER);
    return Pages::HeaderPageView(frame);
}

Pages::GlobalAllocationPageView StorageManager::CreateGlobalAllocationMapPage(const FileKey fileKey, const gam_index_t gamIndex){
    const auto pageId = CoreEngine::StorageLayout::GamPageId(gamIndex);
    auto* frame = this->CreateFrame(fileKey, pageId, Constants::PageType::GLOBAL_ALLOCATION);

    auto* header = reinterpret_cast<Pages::GlobalAllocationPageAdditionalHeader*>(frame->_data + CoreEngine::StorageLayout::PAGE_HEADER_SIZE);

    header->_gamIndex = gamIndex;
    header->_appendBitId = 0;
    header->_firstFreeBitId = 0;
    header->_freeExtentCount = CoreEngine::StorageLayout::GAM_BIT_MAP_SIZE;

    std::memset(header->_reserved, 0, CoreEngine::StorageLayout::GAM_HEADER_RESERVED_SPACE);

    return Pages::GlobalAllocationPageView(frame);
}

Pages::AllocationPageView StorageManager::CreateAllocationPage(
    const FileKey fileKey,
    const page_id_t pageId,
    const page_id_t gamPageId
){
    auto* frame = this->CreateFrame(fileKey, pageId, Constants::PageType::ALLOCATION);

    auto* additionalHeader = reinterpret_cast<Pages::AllocationPageAdditionalHeader*>(frame->_data + CoreEngine::StorageLayout::PAGE_HEADER_SIZE);

    additionalHeader->_nextPageId = INVALID_PAGE_ID;
    additionalHeader->_lastAllocatedExtentId = 0;
    additionalHeader->_gamIndex = gamPageId;

    return Pages::AllocationPageView(frame);
}


Pages::PageFreeSpaceView StorageManager::CreatePageFreeSpacePage(const FileKey fileKey,const page_id_t pageId){
    auto* frame = this->CreateFrame(fileKey, pageId, Constants::PageType::FREE_SPACE);
    return Pages::PageFreeSpaceView(frame);
}

Pages::Frame* StorageManager::CreateFrame(
    const FileKey fileKey,
    const page_id_t pageId,
    const Constants::PageType type
){
    MultiThreading::WriterGuard lock(&this->_tableMutex);

    this->EnsureDatabaseTableExistsNoLock(fileKey);
    this->EnsureSegmentExistsNoLock(fileKey, pageId);

    const auto frameId = this->AcquireFrameId();

    auto* framePtr = this->_memoryManager.AllocateFrame(frameId);
    framePtr->_data = this->_memoryManager.Data() + frameId * CoreEngine::StorageLayout::PAGE_SIZE;
    framePtr->_fileKey = fileKey;
    framePtr->isDirty = true;
    framePtr->hasSecondChance = true;
    framePtr->_pinCount.store(1);   // producer pins the page it hands back
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
    const page_id_t pageId
){
    MultiThreading::WriterGuard guard(&this->_tableMutex);
    this->EnsureDatabaseTableExistsNoLock(fileKey);
    this->EnsureSegmentExistsNoLock(fileKey, pageId);

    return this->OpenExtentNoLock(
        fileKey, pageId,
        CoreEngine::Database::CalculateExtentId(pageId)
    );
}

Pages::FrameId StorageManager::AcquireFrameId(){
    Pages::FrameId frameId = INVALID_FRAME;
    while (this->_memoryManager.PopStackNoLock(frameId) == false)
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