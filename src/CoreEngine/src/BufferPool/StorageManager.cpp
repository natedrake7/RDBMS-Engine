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
            || frame->Priority() >= CoreEngine::PageLayout::PagePriority::HIGH
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

        if (frame->IsDirty()){

            this->FlushFrameToDiskNoLock(frame);
        }

        this->ReleaseClaimedFrameNoLock(candidateId, frame);
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

void StorageManager::ReleaseClaimedFrameNoLock(
    const CoreEngine::StorageLayout::frame_id_t frameId,
    CoreEngine::Pages::Frame* frame
){
    const auto identity = frame->Identity();
    if (identity != CoreEngine::Pages::INVALID_FRAME_IDENTITY){
        const auto pageId = CoreEngine::Pages::PageOf(identity);
        auto expected = frameId;
        auto* segment = this->GetSegmentNoLock(CoreEngine::Pages::SlotOf(identity), pageId);
        segment->_frames[CoreEngine::StorageLayout::PageAddress::PageSlot(pageId)]
            .compare_exchange_strong(
                expected,
                CoreEngine::StorageLayout::INVALID_FRAME_ID,
                std::memory_order_release
            );
    }

    frame->SetIdentity(CoreEngine::Pages::INVALID_FRAME_IDENTITY);
    frame->SetState(CoreEngine::Pages::FrameState::Free);
    this->_memoryManager.PushStackNoLock(frameId);
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
            .load(std::memory_order_acquire) != CoreEngine::StorageLayout::INVALID_FRAME_ID;
    };

    UnsignedInt i = 0;
    bool readAhead = true;
    while (i < CoreEngine::StorageLayout::EXTENT_SIZE){
        const auto current = firstPageId + i;
        if (isCached(current) || (!readAhead && current != pageId)){
            i++;
            continue;
        }

        const auto runStart = i;
        CoreEngine::StorageLayout::frame_id_t frameIds[CoreEngine::StorageLayout::EXTENT_SIZE];
        object_t* buffers[CoreEngine::StorageLayout::EXTENT_SIZE];
        UnsignedInt count = 0;
        while (i < CoreEngine::StorageLayout::EXTENT_SIZE && !isCached(current)){
            const auto runPageId = current;
            auto frameId = CoreEngine::StorageLayout::INVALID_FRAME_ID;
            if (runPageId == pageId)
                frameId = this->AcquireFrameIdNoLock();
            else if(!readAhead || !this->_memoryManager.PopStackNoLock(frameId)){
                readAhead = false;
                break;
            }

            frameIds[count] = frameId;
            buffers[count] = this->_memoryManager.Data() + frameId * CoreEngine::StorageLayout::PAGE_SIZE;
            count++;
            i++;
        }

        if (count == 0){
            i++;
            continue;
        }

        const auto loaded = FileIO::ReadScatter(
            file,
            std::span<object_t* const>(buffers, count),
            (firstPageId + runStart) * CoreEngine::StorageLayout::PAGE_SIZE
        );

        for (UnsignedInt j = 0;j < count; j++){
            const auto currentLoadedPageId = firstPageId + runStart + j;
            const auto frameId = frameIds[j];

            if (j >= loaded){
                this->_memoryManager.PushStackNoLock(frameId);
                continue;
            }

            auto* frame = this->_memoryManager.GetFrame(frameId);
            frame->_data = buffers[j];
            frame->SetIdentity(slotIndex, currentLoadedPageId);
            frame->MarkFlushed();
            frame->ClearHasSecondChance();
            frame->SetPinCount(currentLoadedPageId == pageId);
            frame->SetPriority(CoreEngine::PageLayout::PagePriority::LOW);
            frame->SetState(CoreEngine::Pages::FrameState::Ready);

            segment->_frames[CoreEngine::StorageLayout::PageAddress::PageSlot(currentLoadedPageId)].store(frameId, std::memory_order_release);

            if (currentLoadedPageId == pageId)
                requestedFrame = frame;
        }
    }

    if (requestedFrame == nullptr)
        throw std::runtime_error("StorageManager::OpenExtentNoLock: Frame is nullptr when exiting function");

    return requestedFrame;
}

ExtentLoadInfo StorageManager::ReserveExtentNoLock(
    const buffer_pool_slot_t slotIndex,
    const page_id_t pageId
){
    const auto extentId = CoreEngine::StorageLayout::ExtentId(pageId);
    ExtentLoadInfo info{
        ._firstPageId = pageId,
        ._requestedPageId =  CoreEngine::StorageLayout::ExtentFirstPageId(extentId),
        ._slot = slotIndex
    };

    auto* segment = this->EnsureSegmentExistsNoLock(slotIndex, pageId);

    bool readAhead = true;
    for (UnsignedInt i = 0;i < CoreEngine::StorageLayout::EXTENT_SIZE;i++){
        const auto currentPageId = info._firstPageId + i;
        info._frames[i] = CoreEngine::StorageLayout::INVALID_FRAME_ID;

        auto* entry = &segment->_frames[CoreEngine::StorageLayout::PageAddress::PageSlot(currentPageId)];
        //Page is already loaded by another thread (skip)
        if (entry->load(std::memory_order_relaxed) != CoreEngine::StorageLayout::INVALID_FRAME_ID)
            continue;

        auto frameId = CoreEngine::StorageLayout::INVALID_FRAME_ID;
        if (currentPageId == pageId)
            frameId = this->AcquireFrameIdNoLock();
        else if(!readAhead || !this->_memoryManager.PopStackNoLock(frameId)){
            readAhead = false;
            continue;
        }

        auto* frame = this->_memoryManager.GetFrame(frameId);
        frame->SetIdentity(slotIndex, currentPageId);
        frame->MarkFlushed();
        frame->ClearHasSecondChance();
        frame->SetPinCount(1);
        frame->SetPriority(CoreEngine::PageLayout::PagePriority::LOW);
        frame->SetState(CoreEngine::Pages::FrameState::Loading);

        entry->store(frameId, std::memory_order_release);
        info._frames[i] = frameId;

    }

    return info;
}

CoreEngine::Pages::Frame* StorageManager::CompleteExtentLoad(const ExtentLoadInfo& info) const{
    const auto file = this->_fileManager.GetHandle(info._slot);
    CoreEngine::Pages::Frame* requested = nullptr;
    std::exception_ptr error;

    UnsignedInt i = 0;
    auto* __restrict__ frameIds = info._frames;
    while (i < CoreEngine::StorageLayout::EXTENT_SIZE){
        if (frameIds[i] != CoreEngine::StorageLayout::INVALID_FRAME_ID){
            i++;
            continue;
        }

        const auto runStart = i;
        object_t* buffers[CoreEngine::StorageLayout::EXTENT_SIZE];
        UnsignedInt counter = 0;

        while (i < CoreEngine::StorageLayout::EXTENT_SIZE && frameIds[i] != CoreEngine::StorageLayout::INVALID_FRAME_ID)
            buffers[counter++] = this->_memoryManager.GetFrame(frameIds[i++])->_data;

        UnsignedInt loadedFrames = 0;
        try{
            loadedFrames = FileIO::ReadScatter(
                file,
                std::span<object_t* const>(buffers, counter),
                (info._firstPageId + runStart) * CoreEngine::StorageLayout::PAGE_SIZE
            );
        }
        catch (...){
            error = std::current_exception();
        }

        ExtentFailLoadInfo failedInfo{
            ._count = 0,
            ._slot = info._slot,
        };

        for (UnsignedInt j = 0;j < counter;j++){
            const auto currentPageId = info._firstPageId + runStart + j;
            auto* frame = this->_memoryManager.GetFrame(currentPageId);

            if (j >= loadedFrames){
                failedInfo._frames[failedInfo._count] = frameIds[runStart + j];
                failedInfo._pages[failedInfo._count] = currentPageId;
                failedInfo._count++;
                continue;
            }

            frame->SetState(CoreEngine::Pages::FrameState::Ready);
            frame->_state.notify_all();

            if (currentPageId == info._requestedPageId)
                requested = frame;
            else
                frame->Unpin();
        }

        if (failedInfo._count > 0){
            this->FailExtentLoad(failedInfo);
        }
    }

    if (requested == nullptr){
        if (error)
            std::rethrow_exception(error);
        throw std::runtime_error("StorageManager::CompleteExtentLoad: page is beyond the end of the file or corrupt");
    }
    return requested;
}

void StorageManager::FailExtentLoad(const ExtentFailLoadInfo& failedInfo) const{
    const auto* __restrict__ frames = failedInfo._frames;

    {
        MultiThreading::WriterGuard guard(&this->_tableMutex);
        const auto* __restrict__ pageIds = failedInfo._pages;

        auto* segment = this->GetSegmentNoLock(failedInfo._slot, pageIds[0]);
        for (UnsignedInt i = 0; i < failedInfo._count; i++){
            auto expectedFrame = failedInfo._frames[i];
            segment->_frames[CoreEngine::StorageLayout::PageAddress::PageSlot(pageIds[i])].compare_exchange_strong(
               expectedFrame,
                CoreEngine::StorageLayout::INVALID_FRAME_ID,
                std::memory_order_release
            );
        }
    }

    for (UnsignedInt i = 0; i < failedInfo._count; i++){
        auto* frame = this->_memoryManager.GetFrame(frames[i]);
        frame->SetIdentity(CoreEngine::Pages::INVALID_FRAME_IDENTITY);
        frame->SetState(CoreEngine::Pages::FrameState::Failed);
        frame->_state.notify_all();
        frame->Unpin();
    }
}

CoreEngine::Pages::Frame* StorageManager::GetFrame(
    const buffer_pool_slot_t slotIndex,
    const page_id_t pageId
){
    const auto frameIdentity = CoreEngine::Pages::FrameIdentity(slotIndex, pageId);

    if (const auto* segment = this->GetSegmentNoLock(slotIndex, pageId)){
        const auto frameId = segment->_frames[CoreEngine::StorageLayout::PageAddress::PageSlot(pageId)]
            .load(std::memory_order_acquire);

        if (frameId != CoreEngine::StorageLayout::INVALID_FRAME_ID){
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

void StorageManager::FreeDbPageTable(const CoreEngine::Accessors::PageAccessor accessor){
    MultiThreading::WriterGuard guard(&this->_stackMutex);
    this->_freeDbStack[this->_freeTopDbStack++] = accessor._dataSlot;
    this->_freeDbStack[this->_freeTopDbStack++] = accessor._systemSlot;
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
        if (!frame->StateEquals(CoreEngine::Pages::FrameState::Ready) || !frame->IsDirty())
            continue;

        MultiThreading::ReaderGuard latch(&frame->_latch);
        const auto identity = frame->Identity();
        const auto fd = this->_fileManager.GetHandle(CoreEngine::Pages::SlotOf(identity));

        FileIO::Write(
            fd,
            frame->_data,
            CoreEngine::StorageLayout::PAGE_SIZE,
            static_cast<UnsignedBigInt>(CoreEngine::Pages::PageOf(identity)) * CoreEngine::StorageLayout::PAGE_SIZE
        );

        frame->MarkFlushed();
        if (std::ranges::find(touchedFiles, fd) == touchedFiles.end())
            touchedFiles.push_back(fd);
    }

    for (const auto fd: touchedFiles)
        FileIO::Sync(fd);
}

CoreEngine::Pages::Frame* StorageManager::BeginCreateFrame(
    const buffer_pool_slot_t slotIndex,
    const page_id_t pageId
){
    CoreEngine::Pages::Frame* frame = nullptr;
    const auto segmentSlot = CoreEngine::StorageLayout::PageAddress::PageSlot(pageId);
    CoreEngine::StorageLayout::frame_id_t frameId = CoreEngine::StorageLayout::INVALID_FRAME_ID;

    MultiThreading::WriterGuard lock(&this->_tableMutex);

    auto* segment = this->EnsureSegmentExistsNoLock(slotIndex, pageId);
    auto* entry = &segment->_frames[segmentSlot];

    frameId = entry->load(std::memory_order_relaxed);
    if (const auto existing = entry->load(std::memory_order_relaxed); existing != CoreEngine::StorageLayout::INVALID_FRAME_ID){
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

    frame->SetIdentity(slotIndex, pageId);
    frame->SetState(CoreEngine::Pages::FrameState::Loading);

    entry->store(frameId, std::memory_order_release);
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
    auto frameId = CoreEngine::StorageLayout::INVALID_FRAME_ID;
    ExtentLoadInfo extentInfo{};

    while (true){
        {
            MultiThreading::WriterGuard guard(&this->_tableMutex);
            const auto* segment = this->EnsureSegmentExistsNoLock(slotIndex, pageId);

            frameId = segment->_frames[CoreEngine::StorageLayout::PageAddress::PageSlot(pageId)]
                .load(std::memory_order_relaxed);

            if (frameId == CoreEngine::StorageLayout::INVALID_FRAME_ID)
                extentInfo = this->ReserveExtentNoLock(slotIndex, pageId);
        }

        if (frameId == CoreEngine::StorageLayout::INVALID_FRAME_ID)
            return this->CompleteExtentLoad(extentInfo);

        if (auto* frame = this->PinIfHolds(frameId, identity))
            return frame;
    }

    std::unreachable();
}

CoreEngine::StorageLayout::frame_id_t StorageManager::AcquireFrameIdNoLock(){
    auto frameId = CoreEngine::StorageLayout::INVALID_FRAME_ID;
    while (this->_memoryManager.PopStackNoLock(frameId) == false)
        this->EvictPageNoLock();
    return frameId;
}
} // namespace Storage