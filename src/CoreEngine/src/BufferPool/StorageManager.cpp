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
StorageManager::StorageManager()
    : _freeDbStack{}, _fileManager(), _memoryManager{}{
    this->_clockHand = 0;
    this->_freeTopDbStack = 0;
}

void StorageManager::EvictPageNoLock() {
    const auto limit = 2 * this->_memoryManager.Capacity();

    for (Int i = 0; i < limit; i++){
        const auto candidateId = this->_clockHand;
        auto* frame = this->_memoryManager.GetFrame(candidateId);

        this->_clockHand = (this->_clockHand + 1) % this->_memoryManager.Capacity();

        if (!frame->IsValid() || frame->_priority.load() >= CoreEngine::PageLayout::PagePriority::HIGH)
            continue;

        if (frame->HasSecondChance()){
            frame->ClearHasSecondChance();
            continue;
        }

        if (!frame->TryClaimForEviction())
            continue;

        if (frame->IsDirty())
            this->FlushFrameToDiskNoLock(frame);

        const auto pageId = frame->Header()->pageId;
        auto* segment = this->GetSegmentNoLock(frame->_slotIndex, pageId);
        segment->_frames[CoreEngine::StorageLayout::PageAddress::PageSlot(pageId)].store(CoreEngine::StorageLayout::INVALID_FRAME, std::memory_order_release);
        this->_memoryManager.PushStackNoLock(candidateId);
    }

    throw std::runtime_error("StorageManager::EvictPageNoLock: Cannot evict from Buffer Pool, memory is capped");
}

void StorageManager::FlushFrameToDiskNoLock(CoreEngine::Pages::Frame *framePtr) const{
    const auto handle = this->_fileManager.GetHandle(framePtr->_slotIndex);

    const auto pageId = framePtr->Header()->pageId;
    const auto offSet = pageId * CoreEngine::StorageLayout::PAGE_SIZE;

    FileIO::Write(handle, framePtr->_data, CoreEngine::StorageLayout::PAGE_SIZE, offSet);

    framePtr->MarkFlushed();
    FileIO::Sync(handle);
}

CoreEngine::Pages::Frame* StorageManager::OpenExtentNoLock(
    const buffer_pool_slot_t slotIndex,
    const page_id_t pageId,
    const extent_id_t extentId
){
    // read page from disk, call this->fileManager
    const auto handle = this->_fileManager.GetHandle(slotIndex);

    const auto firstPageId = CoreEngine::StorageLayout::ExtentFirstPageId(extentId);

    // take into account the metadata page all the others
    CoreEngine::Pages::Frame* framePtr = nullptr;
    for (Int i = 0; i < CoreEngine::StorageLayout::EXTENT_SIZE; i++){
        const page_id_t currentPageId = firstPageId + i;

        Segment* segment = this->EnsureSegmentExistsNoLock(slotIndex, currentPageId);
        const auto pageSlot = CoreEngine::StorageLayout::PageAddress::PageSlot(currentPageId);

        if (segment->_frames[pageSlot].load(std::memory_order_acquire) != CoreEngine::StorageLayout::INVALID_FRAME)
            continue;

        const auto frameId = this->AcquireFrameId();
        auto* destination = this->_memoryManager.Data() + frameId * CoreEngine::StorageLayout::PAGE_SIZE;

        const auto bytesRead = FileIO::Read(
            handle, destination,
            CoreEngine::StorageLayout::PAGE_SIZE,
            currentPageId * CoreEngine::StorageLayout::PAGE_SIZE
        );

        if (bytesRead <= 0){
            this->_memoryManager.PushStackNoLock(frameId);
            break;
        }
        if (bytesRead < CoreEngine::StorageLayout::PAGE_SIZE)
            std::memset(destination, 0, CoreEngine::StorageLayout::PAGE_SIZE - bytesRead);

        const auto isRequestedPage = (currentPageId == pageId);

        auto* frame = this->_memoryManager.GetFrame(frameId);
        frame->_data = destination;
        frame->_slotIndex = slotIndex;
        frame->_isDirty = false;
        frame->_hasSecondChance = false;
        frame->_pinCount.store(isRequestedPage ? 1 : 0);   // producer pins the page it hands back
        frame->_priority.store(CoreEngine::PageLayout::PagePriority::LOW);

        if (isRequestedPage)
            framePtr = frame;

        segment->_frames[pageSlot].store(frameId, std::memory_order_release);
    }

    return framePtr;
}

CoreEngine::Pages::Frame* StorageManager::GetFrame(
    const buffer_pool_slot_t slotIndex,
    const page_id_t pageId
){
    auto* segment = this->GetSegmentNoLock(slotIndex, pageId);
    if (segment == nullptr)
        return this->HandlePageCacheMiss(slotIndex, pageId);

    const auto frameId = segment->_frames[CoreEngine::StorageLayout::PageAddress::PageSlot(pageId)].load(std::memory_order_acquire);

    if (frameId == CoreEngine::StorageLayout::INVALID_FRAME)
        return this->HandlePageCacheMiss(slotIndex, pageId);

    auto* frame = this->_memoryManager.GetFrame(frameId);

    if (!frame->TryPin())
        return this->HandlePageCacheMiss(slotIndex, pageId);

    if (frame->_slotIndex != slotIndex
        || frame->Header()->pageId != pageId
        || segment->_frames[CoreEngine::StorageLayout::PageAddress::PageSlot(pageId)].load(std::memory_order_acquire) != frameId
    ){
        frame->Unpin();
        return this->HandlePageCacheMiss(slotIndex, pageId);
    }

    frame->_hasSecondChance = true;
    return frame;
}

bool StorageManager::ReserveDbPageTableNoLock(buffer_pool_slot_t& outIndex){
    if (this->_freeTopDbStack == 0)
        return false;

    outIndex = this->_freeDbStack[--this->_freeTopDbStack];

    if (this->_freeTopDbStack == 0)
        return false;

    return true;
}

StorageManager& StorageManager::Get(){
    static StorageManager storageManager;
    return storageManager;
}

StorageManager::~StorageManager(){
    for (Int i = 0; i < this->_memoryManager.Capacity(); i++){
        auto* frame = this->_memoryManager.GetFrame(i);
        if (frame == nullptr || !frame->IsValid())
            continue;

        MultiThreading::WriterGuard pageLock(&frame->_latch);
        this->FlushFrameToDiskNoLock(frame);
    }
}

void StorageManager::Initialize(const UnsignedBigInt size){
    this->_memoryManager.Initialize(size);

    for (SmallInt i = 0; i < CoreEngine::StorageLayout::MAX_BUFFER_POOL_SLOTS; ++i)
        this->_freeDbStack[i] = i;

    this->_freeTopDbStack = CoreEngine::StorageLayout::MAX_BUFFER_POOL_SLOTS;
}

CoreEngine::Accessors::PageAccessor StorageManager::ReserveDbPageTables(){
    MultiThreading::WriterGuard guard(&this->_stackMutex);

    CoreEngine::Accessors::PageAccessor accessor{};

    if (
        !this->ReserveDbPageTableNoLock(accessor._dataSlot)
        || !this->ReserveDbPageTableNoLock(accessor._systemSlot)
    ) throw std::runtime_error("Failed to reserve db page table");

    return accessor;
}

void StorageManager::FreeDbPageTable(const SmallInt index){
    MultiThreading::WriterGuard guard(&this->_stackMutex);
    this->_freeDbStack[this->_freeTopDbStack++] = index;
}

void StorageManager::OpenFile(
    const buffer_pool_slot_t slotIndex,
    const DataTypes::StringView& filename,
    const FileOpenMode mode

){
    this->_fileManager.OpenFile(slotIndex, filename, mode, FileOpenFlags::Direct);
}

void StorageManager::CloseAllFiles(){
    this->_fileManager.CloseAllFiles();
}

void StorageManager::FlushAll() const{
    MultiThreading::WriterGuard lock(&this->_tableMutex);

    std::vector<native_file_t> touchedFiles;
    for (Int i = 0;i < this->_memoryManager.Capacity(); i++){
        auto* frame = this->_memoryManager.GetFrame(i);
        if (!frame->IsValid() || !frame->IsDirty())
            continue;

        MultiThreading::ReaderGuard latch(&frame->_latch);
        const auto fd = this->_fileManager.GetHandle(frame->_slotIndex);
        const auto pageId = frame->Header()->pageId;

        FileIO::Write(
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
        FileIO::Sync(fd);
}

////////////////////////////////////////////////////
////////////////////System Pages///////////////////
//////////////////////////////////////////////////

CoreEngine::Pages::HeaderPageView StorageManager::CreateHeaderPage(const buffer_pool_slot_t slotIndex){
    auto* frame = this->CreateFrame(
        slotIndex,
        CoreEngine::StorageLayout::HEADER_PAGE_ID,
        CoreEngine::PageLayout::PageType::HEADER
    );
    return CoreEngine::Pages::HeaderPageView(frame);
}

CoreEngine::Pages::GlobalAllocationPageView StorageManager::CreateGlobalAllocationMapPage(
    const buffer_pool_slot_t slotIndex,
    const gam_index_t gamIndex
){
    const auto pageId = CoreEngine::StorageLayout::GamPageId(gamIndex);
    auto* frame = this->CreateFrame(slotIndex, pageId, CoreEngine::PageLayout::PageType::GLOBAL_ALLOCATION);

    auto* header = reinterpret_cast<CoreEngine::Pages::GlobalAllocationPageAdditionalHeader*>(frame->_data + CoreEngine::StorageLayout::PAGE_HEADER_SIZE);

    header->_gamIndex = gamIndex;
    header->_appendBitId = 0;
    header->_firstFreeBitId = 0;
    header->_freeExtentCount = CoreEngine::StorageLayout::GAM_BIT_MAP_SIZE;

    std::memset(header->_reserved, 0, CoreEngine::StorageLayout::GAM_HEADER_RESERVED_SPACE);

    return CoreEngine::Pages::GlobalAllocationPageView(frame);
}

CoreEngine::Pages::AllocationPageView StorageManager::CreateAllocationPage(
    const buffer_pool_slot_t slotIndex,
    const page_id_t pageId,
    const page_id_t gamPageId
){
    auto* frame = this->CreateFrame(slotIndex, pageId, CoreEngine::PageLayout::PageType::ALLOCATION);

    auto* additionalHeader = reinterpret_cast<CoreEngine::Pages::AllocationPageAdditionalHeader*>(frame->_data + CoreEngine::StorageLayout::PAGE_HEADER_SIZE);

    additionalHeader->_nextPageId = INVALID_PAGE_ID;
    additionalHeader->_lastAllocatedExtentId = 0;
    additionalHeader->_gamIndex = gamPageId;

    return CoreEngine::Pages::AllocationPageView(frame);
}


CoreEngine::Pages::PageFreeSpaceView StorageManager::CreatePageFreeSpacePage(buffer_pool_slot_t slotIndex,const page_id_t pageId){
    auto* frame = this->CreateFrame(slotIndex, pageId, CoreEngine::PageLayout::PageType::FREE_SPACE);
    return CoreEngine::Pages::PageFreeSpaceView(frame);
}

CoreEngine::Pages::Frame* StorageManager::CreateFrame(
    const buffer_pool_slot_t slotIndex,
    const page_id_t pageId,
    const CoreEngine::PageLayout::PageType type
){
    MultiThreading::WriterGuard lock(&this->_tableMutex);

    auto* segment = this->EnsureSegmentExistsNoLock(slotIndex, pageId);
    const auto segmentSlot = CoreEngine::StorageLayout::PageAddress::PageSlot(pageId);

    const auto* slot = &segment->_frames[segmentSlot];

    if (const auto existing = slot->load(std::memory_order_relaxed); existing != CoreEngine::StorageLayout::INVALID_FRAME){
        auto* frame = this->_memoryManager.GetFrame(existing);
        const auto pinned = frame->TryPin();
        assert(pinned && frame->_pinCount.load() == 1 && "CoreEngine::Pages::Frame* StorageManager::CreateFrame: Re-creating a page that is still in use");
        return frame;
    }

    const auto frameId = this->AcquireFrameId();
    auto* frame = this->_memoryManager.GetFrame(frameId);

    frame->_data = this->_memoryManager.Data() + frameId * CoreEngine::StorageLayout::PAGE_SIZE;
    frame->_slotIndex = slotIndex;
    frame->MarkDirty();
    frame->MarkHasSecondChance();
    frame->_pinCount.store(1);   // producer pins the page it hands back
    frame->_priority.store(CoreEngine::PageLayout::PagePriority::LOW);

    std::memset(frame->_data, 0, CoreEngine::StorageLayout::PAGE_SIZE);

    auto* header = frame->Header();
    header->pageId = pageId;
    header->SetType(type);

    segment->_frames[segmentSlot].store(frameId, std::memory_order_release);

    return frame;
}

Segment* StorageManager::GetSegmentNoLock(const buffer_pool_slot_t slotIndex, const page_id_t pageId) const{
    const auto* group = this->_pageTable[slotIndex]._groups[CoreEngine::StorageLayout::PageAddress::DirectorySlot(pageId)].load(std::memory_order_acquire);
    if (group == nullptr)
        return nullptr;

    return group->_segments[CoreEngine::StorageLayout::PageAddress::GroupSlot(pageId)].load(std::memory_order_acquire);
}

Segment* StorageManager::EnsureSegmentExistsNoLock(const buffer_pool_slot_t slotIndex, const page_id_t pageId){
    auto& groupSlot = this->_pageTable[slotIndex]._groups[CoreEngine::StorageLayout::PageAddress::DirectorySlot(pageId)];
    auto* group = groupSlot.load(std::memory_order_relaxed);
    if (group == nullptr){
        group = this->_allocator.Allocate<SegmentGroup>();
        groupSlot.store(group, std::memory_order_release);
    }

    auto& segmentSlot = group->_segments[CoreEngine::StorageLayout::PageAddress::GroupSlot(pageId)];
    auto* segment = segmentSlot.load(std::memory_order_relaxed);
    if (segment == nullptr){
        segment = this->_allocator.Allocate<Segment>();
        segmentSlot.store(segment, std::memory_order_release);
    }

    return segment;
}

CoreEngine::Pages::Frame* StorageManager::HandlePageCacheMiss(
    const buffer_pool_slot_t slotIndex,
    const page_id_t pageId
){
    MultiThreading::WriterGuard guard(&this->_tableMutex);
    const auto* segment = this->EnsureSegmentExistsNoLock(slotIndex, pageId);

    const auto frameId = segment->_frames[CoreEngine::StorageLayout::PageAddress::PageSlot(pageId)].load(std::memory_order_acquire);
    if (frameId != CoreEngine::StorageLayout::INVALID_FRAME){
        auto* frame = this->_memoryManager.GetFrame(frameId);

        const auto pinned = frame->TryPin();
        assert(pinned && "StorageManager::HandlePageCacheMiss: Failed to pin frame");
        frame->MarkHasSecondChance();
        return frame;
    }

    return this->OpenExtentNoLock(
        slotIndex, pageId,
        CoreEngine::StorageLayout::ExtentId(pageId)
    );
}

CoreEngine::StorageLayout::frame_id_t StorageManager::AcquireFrameId(){
    auto frameId = CoreEngine::StorageLayout::INVALID_FRAME;
    while (this->_memoryManager.PopStackNoLock(frameId) == false)
        this->EvictPageNoLock();
    return frameId;
}
} // namespace Storage