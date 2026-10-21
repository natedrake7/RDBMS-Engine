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
    const auto capacity = this->_memoryManager.Capacity();

    for (Int i = 0; i < 2 * capacity; i++){
        const auto candidateId = this->_clockHand.load(std::memory_order_relaxed);
        this->_clockHand.store((candidateId + 1) % capacity, std::memory_order_relaxed);

        auto* frame = this->_memoryManager.GetFrame(candidateId);

        if (
            frame->StateEquals(CoreEngine::Pages::FrameState::Free)
            || frame->PriorityEquals(CoreEngine::PageLayout::PagePriority::HIGH)
        ) continue;

        if (frame->HasSecondChance()){
            frame->ClearHasSecondChance();
            continue;
        }

        //TODO
        // if (scanned < capacity && frame->IsDirty()){
        //     this->_writer.Wake();
        //     continue;
        // }

        if (!frame->TryClaimForEviction())
            continue;

        const auto identity = frame->Identity();
        if (frame->IsDirty())
            this->FlushFrameToDiskNoLock(frame);

        if (identity != CoreEngine::Pages::INVALID_FRAME_IDENTITY){
            const auto pageId = CoreEngine::Pages::PageOf(identity);
            auto* segment = this->GetSegmentNoLock(CoreEngine::Pages::SlotOf(identity), pageId);
            auto expected = candidateId;

            segment->_frames[CoreEngine::StorageLayout::PageAddress::PageSlot(pageId)].compare_exchange_strong(
            expected,
                CoreEngine::StorageLayout::INVALID_FRAME,
                std::memory_order_release
            );
        }

        frame->SetIdentity(CoreEngine::Pages::INVALID_FRAME_IDENTITY);
        frame->SetState(CoreEngine::Pages::FrameState::Free);
        this->_memoryManager.PushStackNoLock(candidateId);
        return;
    }

    throw std::runtime_error("StorageManager::EvictPageNoLock: Cannot evict from Buffer Pool, memory is capped");
}

void StorageManager::FlushFrameToDiskNoLock(CoreEngine::Pages::Frame *frame) const{
    const auto identity = frame->Identity();
    const auto handle = this->_fileManager.GetHandle(CoreEngine::Pages::SlotOf(identity));

    FileIO::Write(
        handle,
        frame->_data, CoreEngine::StorageLayout::PAGE_SIZE,
        CoreEngine::Pages::PageOf(identity) * CoreEngine::StorageLayout::PAGE_SIZE
    );

    frame->MarkFlushed();
    FileIO::Sync(handle);
}

CoreEngine::Pages::Frame* StorageManager::OpenExtentNoLock(
    const buffer_pool_slot_t slotIndex,
    const page_id_t pageId
){
    const auto extentId = CoreEngine::StorageLayout::ExtentId(pageId);
    const auto firstPageId = CoreEngine::StorageLayout::ExtentFirstPageId(extentId);

    auto* segment = this->EnsureSegmentExistsNoLock(slotIndex, pageId);

    const auto file = this->_fileManager.GetHandle(slotIndex);
    CoreEngine::Pages::Frame* requestedFrame = nullptr;

    const auto isCached = [&](const page_id_t id){
        return segment->_frames[CoreEngine::StorageLayout::PageAddress::PageSlot(id)]
            .load(std::memory_order_acquire) != CoreEngine::StorageLayout::INVALID_FRAME;
    };

    UnsignedInt i = 0;
    bool readAhead = true;
    while (i < CoreEngine::StorageLayout::EXTENT_SIZE){
        if (isCached(firstPageId + i)){
            i++;
            continue;
        }

        const auto runStart = i;
        CoreEngine::StorageLayout::frame_id_t frameIds[CoreEngine::StorageLayout::EXTENT_SIZE];
        object_t* buffers[CoreEngine::StorageLayout::EXTENT_SIZE];
        UnsignedInt count = 0;
        while (i < CoreEngine::StorageLayout::EXTENT_SIZE && !isCached(firstPageId + i)){
            const auto runPageId = firstPageId + i;
            auto frameId = CoreEngine::StorageLayout::INVALID_FRAME;
            if (runPageId == pageId)
                frameId = this->AcquireFrameIdNoLock();
            else(!readAhead || !this->_memoryManager.PopStackNoLock(frameId)){
                readAhead = false;
                break;
            }

            frameIds[count] = frameId;
            buffers[count] = this->_memoryManager.Data() + frameId * CoreEngine::StorageLayout::PAGE_SIZE;
            count++;
            i++;
        }

        const auto loaded = FileIO::ReadScatter(
            file,
            std::span<object_t* const>(buffers, count),
            (firstPageId + runStart) * CoreEngine::StorageLayout::PAGE_SIZE
        );

        for (UnsignedInt j = 0;j < count; j++){
            const auto current = firstPageId + runStart + j;
            const auto frameId = frameIds[j];

            if (j >= loaded){
                this->_memoryManager.PushStackNoLock(frameId);
                continue;
            }

            auto* frame = this->_memoryManager.GetFrame(frameId);
            frame->_data = buffers[j];
            frame->SetIdentity(slotIndex, current);
            frame->MarkFlushed();
            frame->ClearHasSecondChance();
            frame->SetPinCount(current == pageId);
            frame->SetPriority(CoreEngine::PageLayout::PagePriority::LOW);
            frame->SetState(CoreEngine::Pages::FrameState::Ready);

            segment->_frames[CoreEngine::StorageLayout::PageAddress::PageSlot(current)].store(frameId, std::memory_order_release);

            if (current == pageId)
                requestedFrame = frame;
        }
    }

    assert(requestedFrame != nullptr && "StorageManager::OpenExtentNoLock: Frame is nullptr when exiting function");
    return requestedFrame;
}

CoreEngine::Pages::Frame* StorageManager::GetFrame(
    const buffer_pool_slot_t slotIndex,
    const page_id_t pageId
){
    const auto frameIdentity = CoreEngine::Pages::FrameIdentity(slotIndex, pageId);

    if (const auto* segment = this->GetSegmentNoLock(slotIndex, pageId)){
        const auto frameId = segment->_frames[CoreEngine::StorageLayout::PageAddress::PageSlot(pageId)]
            .load(std::memory_order_acquire);

        if (frameId != CoreEngine::StorageLayout::INVALID_FRAME){
            if (auto* frame = this->PinIfHolds(frameId, frameIdentity)){
                if (!frame->HasSecondChance())
                    frame->MarkHasSecondChance();
                return frame;
            }
        }
    }

    return this->HandlePageCacheMiss(slotIndex, pageId);
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

    if (this->_freeTopDbStack < 2)
        throw std::runtime_error("StorageManager::ReserveDbPageTables: too many open databases");

    const auto dataSlot = this->_freeDbStack[--this->_freeTopDbStack];
    const auto systemSlot = this->_freeDbStack[--this->_freeTopDbStack];

    return CoreEngine::Accessors::PageAccessor(dataSlot, systemSlot);
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
#ifdef _WIN32
    constexpr auto PAGE_FILE_FLAGS = FileOpenFlags::Direct | FileOpenFlags::Async;
#else
    constexpr auto PAGE_FILE_FLAGS = FileOpenFlags::Direct;
#endif

    this->_fileManager.OpenFile(slotIndex, filename, mode, PAGE_FILE_FLAGS);
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
        const auto fd = this->_fileManager.GetHandle(frame->SlotIndex());
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


CoreEngine::Pages::PageFreeSpaceView StorageManager::CreatePageFreeSpacePage(
    const buffer_pool_slot_t slotIndex,
    const page_id_t pageId
){
    auto* frame = this->CreateFrame(slotIndex, pageId, CoreEngine::PageLayout::PageType::FREE_SPACE);
    return CoreEngine::Pages::PageFreeSpaceView(frame);
}

CoreEngine::Pages::Frame* StorageManager::CreateFrame(
    const buffer_pool_slot_t slotIndex,
    const page_id_t pageId,
    const CoreEngine::PageLayout::PageType type
){
    CoreEngine::Pages::Frame* frame = nullptr;
    Segment* segment = nullptr;
    const auto segmentSlot = CoreEngine::StorageLayout::PageAddress::PageSlot(pageId);
    CoreEngine::StorageLayout::frame_id_t frameId = CoreEngine::StorageLayout::INVALID_FRAME;

    {
        MultiThreading::WriterGuard lock(&this->_tableMutex);

        segment = this->EnsureSegmentExistsNoLock(slotIndex, pageId);
        const auto* entry = &segment->_frames[segmentSlot];

        frameId = entry->load(std::memory_order_relaxed);
        if (const auto existing = entry->load(std::memory_order_relaxed); existing != CoreEngine::StorageLayout::INVALID_FRAME){
            frame = this->_memoryManager.GetFrame(existing);
            const auto pinned = frame->TryPin();
            assert(pinned && "CoreEngine::Pages::Frame* StorageManager::CreateFrame: Re-creating a page that is still in use");
        }
        else{
            frameId = this->AcquireFrameIdNoLock();
            frame = this->_memoryManager.GetFrame(frameId);
            frame->_data = this->_memoryManager.Data() + frameId * CoreEngine::StorageLayout::PAGE_SIZE;
            frame->SetPinCount(1);
        }
    }

    {
        MultiThreading::WriterGuard pageLock(&frame->_latch);

        std::memset(frame->_data, 0, CoreEngine::StorageLayout::PAGE_SIZE);

        auto* header = frame->Header();
        header->pageId = pageId;
        header->SetType(type);
    }

    frame->MarkDirty();
    frame->MarkHasSecondChance();
    frame->SetPriority(CoreEngine::PageLayout::PagePriority::LOW);
    frame->SetIdentity(slotIndex, pageId);
    frame->SetState(CoreEngine::Pages::FrameState::Ready);

    segment->_frames[segmentSlot].store(frameId, std::memory_order_release);
    return frame;
}

CoreEngine::Pages::Frame* StorageManager::PinIfHolds(
    const CoreEngine::StorageLayout::frame_id_t frameId,
    const CoreEngine::StorageLayout::frame_identity_t frameIdentity
) const{
    auto* frame = this->_memoryManager.GetFrame(frameId);
    if (
        frame->_identity.load(std::memory_order_acquire) != frameIdentity
        || !frame->TryPin()
    ) return nullptr;

    auto state = frame->_state.load(std::memory_order_acquire);
    while (state == CoreEngine::Pages::FrameState::Loading){
        frame->_state.wait(CoreEngine::Pages::FrameState::Loading, std::memory_order_acquire);
        state = frame->_state.load(std::memory_order_acquire);
    }

    if (
        state != CoreEngine::Pages::FrameState::Ready
        || frame->_identity.load(std::memory_order_acquire) != frameIdentity
    ){
        frame->Unpin();
        return nullptr;
    }

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
    const auto identity = CoreEngine::Pages::FrameIdentity(slotIndex, pageId);

    while (true){
        CoreEngine::StorageLayout::frame_id_t frameId = CoreEngine::StorageLayout::INVALID_FRAME;
        {
            MultiThreading::WriterGuard guard(&this->_tableMutex);
            const auto* segment = this->EnsureSegmentExistsNoLock(slotIndex, pageId);

            const auto requestedFrameId = segment->_frames[CoreEngine::StorageLayout::PageAddress::PageSlot(pageId)]
                .load(std::memory_order_relaxed);
            if (requestedFrameId != CoreEngine::StorageLayout::INVALID_FRAME)
                frameId = requestedFrameId;
            else
                return this->OpenExtentNoLock(slotIndex, pageId);
        }

        if (frameId != CoreEngine::StorageLayout::INVALID_FRAME){
            if (auto* frame = this->PinIfHolds(frameId, identity))
                return frame;
        }
    }

    std::unreachable();
}

CoreEngine::StorageLayout::frame_id_t StorageManager::AcquireFrameIdNoLock(){
    auto frameId = CoreEngine::StorageLayout::INVALID_FRAME;
    while (this->_memoryManager.PopStackNoLock(frameId) == false)
        this->EvictPageNoLock();
    return frameId;
}
} // namespace Storage