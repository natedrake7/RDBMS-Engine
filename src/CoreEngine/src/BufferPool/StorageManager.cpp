#include <CoreEngine/BufferPool/StorageManager.h>
#include <CoreEngine/Database.h>
#include <Systemic/Guards/WriterGuard.h>
#include <CoreEngine/Pages/GlobalAllocationPageView.h>
#include <CoreEngine/Pages/Additional/Frame.h>
#include "CoreEngine/BufferPool/StorageLayout.h"
#include "Systemic/Guards/ReaderGuard.h"

namespace CoreEngine::Storage{
    StorageManager::StorageManager()
        :   _freeDbStack{}, _freeTopDbStack(0),
            _memoryManager(), _clockHand(0), _writerLastHand(0){}

    void StorageManager::EvictPageNoLock() {
        const auto capacity = this->_memoryManager.Capacity();

        for (Int i = 0; i < 2 * capacity; i++){
            const auto candidateId = this->_clockHand.load(std::memory_order_relaxed);
            this->_clockHand.store((candidateId + 1) % capacity, std::memory_order_relaxed);

            auto* frame = this->_memoryManager.GetFrame(candidateId);

            if (
                frame->StateEquals(Pages::FrameState::Free)
                || frame->Priority() >= PageLayout::PagePriority::HIGH
            ) continue;

            if (frame->HasSecondChance()){
                frame->ClearHasSecondChance();
                continue;
            }

             if (i < capacity && frame->IsDirty()){
                 this->_writer.Wake();
                 continue;
             }

            if (!frame->TryClaimForEviction())
                continue;

            if (frame->IsDirty()){
                this->WriteFrameToDiskNoLock(frame);
            }

            this->ReleaseClaimedFrameNoLock(candidateId, frame);
            return;
        }

        throw std::runtime_error("StorageManager::EvictPageNoLock: Cannot evict from Buffer Pool, memory is capped");
    }

    void StorageManager::WriteFrameToDiskNoLock(Pages::Frame *frame) const{
        const auto identity = frame->Identity();

        auto* header = frame->Header();

        header->_checkSum = 0;
        //use systemic.checksum

        const auto handle = this->_fileManager.GetHandle(Pages::SlotOf(identity));

        try{
            ::Storage::FileIO::Write(
                handle,
                frame->_data, StorageLayout::PAGE_SIZE,
                Pages::PageOf(identity) * StorageLayout::PAGE_SIZE
            );
        }
        catch (...){
            frame->SetPinCount(0);
            throw;
        }

        frame->MarkFlushed();
    }

    void StorageManager::ReleaseClaimedFrameNoLock(
        const StorageLayout::frame_id_t frameId,
        Pages::Frame* frame
    ){
        const auto identity = frame->Identity();
        if (identity != Pages::INVALID_FRAME_IDENTITY){
            const auto pageId = Pages::PageOf(identity);
            auto expected = frameId;
            auto* segment = this->GetSegmentNoLock(Pages::SlotOf(identity), pageId);
            segment->_frames[StorageLayout::PageAddress::PageSlot(pageId)]
                .compare_exchange_strong(
                    expected,
                    StorageLayout::INVALID_FRAME_ID,
                    std::memory_order_release
                );
        }

        frame->SetIdentity(Pages::INVALID_FRAME_IDENTITY);
        frame->SetState(Pages::FrameState::Free);
        this->_memoryManager.PushStackNoLock(frameId);
    }

   ExtentLoadInfo StorageManager::ReserveExtentNoLock(
       const buffer_pool_slot_t slotIndex,
       const page_id_t pageId
   ){
       ExtentLoadInfo info(
           StorageLayout::ExtentFirstPageId(StorageLayout::ExtentId(pageId)),
           pageId,
           slotIndex
       );

       auto* segment = this->EnsureSegmentExistsNoLock(slotIndex, pageId);

       bool readAhead = true;
       for (UnsignedInt i = 0;i < StorageLayout::EXTENT_SIZE;i++){
           const auto currentPageId = info._firstPageId + i;
           info._frames[i] = StorageLayout::INVALID_FRAME_ID;

           auto* entry = &segment->_frames[StorageLayout::PageAddress::PageSlot(currentPageId)];
           //Page is already loaded by another thread (skip)
           if (entry->load(std::memory_order_relaxed) != StorageLayout::INVALID_FRAME_ID)
               continue;

           auto frameId = StorageLayout::INVALID_FRAME_ID;
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
           frame->SetPriority(PageLayout::PagePriority::LOW);
           frame->SetState(Pages::FrameState::Loading);

           entry->store(frameId, std::memory_order_release);
           info._frames[i] = frameId;

       }

       return info;
   }

    Pages::Frame* StorageManager::CompleteExtentLoad(const ExtentLoadInfo& info) const{
        const auto file = this->_fileManager.GetHandle(info._slot);
        Pages::Frame* requested = nullptr;
        std::exception_ptr error;

        UnsignedInt i = 0;
        auto* __restrict__ frameIds = info._frames;
        while (i < StorageLayout::EXTENT_SIZE){
            if (frameIds[i] == StorageLayout::INVALID_FRAME_ID){
                i++;
                continue;
            }

            const auto runStart = i;
            object_t* buffers[StorageLayout::EXTENT_SIZE];
            UnsignedInt counter = 0;

            while (i < StorageLayout::EXTENT_SIZE && frameIds[i] != StorageLayout::INVALID_FRAME_ID)
                buffers[counter++] = this->_memoryManager.GetFrame(frameIds[i++])->_data;

            UnsignedInt loadedFrames = 0;
            try{
                loadedFrames = ::Storage::FileIO::ReadScatter(
                    file,
                    std::span<object_t* const>(buffers, counter),
                    (info._firstPageId + runStart) * StorageLayout::PAGE_SIZE
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
                const auto frameId = frameIds[runStart + j];

                auto* frame = this->_memoryManager.GetFrame(frameId);

                if (j >= loadedFrames) { //!VerifyChecksum(frame)
                    failedInfo._frames[failedInfo._count] = frameId;
                    failedInfo._pages[failedInfo._count] = currentPageId;
                    failedInfo._count++;
                    continue;
                }

                frame->SetState(Pages::FrameState::Ready);
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
                segment->_frames[StorageLayout::PageAddress::PageSlot(pageIds[i])].compare_exchange_strong(
                    expectedFrame,
                    StorageLayout::INVALID_FRAME_ID,
                    std::memory_order_release
                );
            }
        }

        for (UnsignedInt i = 0; i < failedInfo._count; i++){
            auto* frame = this->_memoryManager.GetFrame(frames[i]);
            frame->SetIdentity(Pages::INVALID_FRAME_IDENTITY);
            frame->SetState(Pages::FrameState::Failed);
            frame->_state.notify_all();
            frame->Unpin();
        }
    }

    Pages::Frame* StorageManager::BeginCreateFrame(
        const buffer_pool_slot_t slotIndex,
        const page_id_t pageId
    ){
        const auto identity = Pages::FrameIdentity(slotIndex, pageId);

        StorageLayout::frame_id_t loadingFrameId = StorageLayout::INVALID_FRAME_ID;
        while (true){
            {
                MultiThreading::WriterGuard lock(&this->_tableMutex);

                auto* segment = this->EnsureSegmentExistsNoLock(slotIndex, pageId);
                auto* entry = &segment->_frames[StorageLayout::PageAddress::PageSlot(pageId)];
                const auto frameId = entry->load(std::memory_order_relaxed);
                if (frameId == StorageLayout::INVALID_FRAME_ID){
                    const auto newFrameId = this->AcquireFrameIdNoLock();
                    auto* frame = this->_memoryManager.GetFrame(newFrameId);

                    frame->SetState(Pages::FrameState::Loading);
                    frame->SetPinCount(1);
                    frame->SetIdentity(slotIndex, pageId);

                    entry->store(newFrameId, std::memory_order_release);

                    return frame;
                }

                auto* frame = this->_memoryManager.GetFrame(frameId);
                if (!frame->StateEquals(Pages::FrameState::Loading)){
                    if (!frame->TryPin())
                        throw std::logic_error("StorageManager::BeginCreateFrame: cached frame is being evicted outside _tableMutex");
                    frame->SetState(Pages::FrameState::Loading);
                    return frame;
                }
                loadingFrameId = frameId;
            }

            if (auto* frame = this->PinIfHolds(loadingFrameId, identity))
                frame->Unpin();
        }

        std::unreachable();
    }

    Pages::Frame* StorageManager::PinIfHolds(
        const StorageLayout::frame_id_t frameId,
        const StorageLayout::frame_identity_t frameIdentity
    ) const{
        auto* frame = this->_memoryManager.GetFrame(frameId);
        if (
            frame->_identity.load(std::memory_order_acquire) != frameIdentity
            || !frame->TryPin()
        ) return nullptr;

        auto state = frame->_state.load(std::memory_order_acquire);
        while (state == Pages::FrameState::Loading){
            frame->_state.wait(Pages::FrameState::Loading, std::memory_order_acquire);
            state = frame->_state.load(std::memory_order_acquire);
        }

        if (
            state != Pages::FrameState::Ready
            || frame->_identity.load(std::memory_order_acquire) != frameIdentity
        ){
            frame->Unpin();
            return nullptr;
        }

        return frame;
    }

    Segment* StorageManager::GetSegmentNoLock(const buffer_pool_slot_t slotIndex, const page_id_t pageId) const{
        const auto* group = this->_pageTable[slotIndex]._groups[StorageLayout::PageAddress::DirectorySlot(pageId)].load(std::memory_order_acquire);
        if (group == nullptr)
            return nullptr;

        return group->_segments[StorageLayout::PageAddress::GroupSlot(pageId)].load(std::memory_order_acquire);
    }

    Segment* StorageManager::EnsureSegmentExistsNoLock(const buffer_pool_slot_t slotIndex, const page_id_t pageId){
        auto& groupSlot = this->_pageTable[slotIndex]._groups[StorageLayout::PageAddress::DirectorySlot(pageId)];
        auto* group = groupSlot.load(std::memory_order_relaxed);
        if (group == nullptr){
            group = this->_allocator.Allocate<SegmentGroup>();
            groupSlot.store(group, std::memory_order_release);
        }

        auto& segmentSlot = group->_segments[StorageLayout::PageAddress::GroupSlot(pageId)];
        auto* segment = segmentSlot.load(std::memory_order_relaxed);
        if (segment == nullptr){
            segment = this->_allocator.Allocate<Segment>();
            segmentSlot.store(segment, std::memory_order_release);
        }

        return segment;
    }

    Pages::Frame* StorageManager::HandlePageCacheMiss(
        const buffer_pool_slot_t slotIndex,
        const page_id_t pageId
    ){
        const auto identity = Pages::FrameIdentity(slotIndex, pageId);
        auto frameId = StorageLayout::INVALID_FRAME_ID;
        ExtentLoadInfo extentInfo;

        while (true){
            {
                MultiThreading::WriterGuard guard(&this->_tableMutex);
                const auto* segment = this->EnsureSegmentExistsNoLock(slotIndex, pageId);

                frameId = segment->_frames[StorageLayout::PageAddress::PageSlot(pageId)]
                    .load(std::memory_order_relaxed);

                if (frameId == StorageLayout::INVALID_FRAME_ID)
                    extentInfo = this->ReserveExtentNoLock(slotIndex, pageId);
            }

            if (frameId == StorageLayout::INVALID_FRAME_ID)
                return this->CompleteExtentLoad(extentInfo);

            if (auto* frame = this->PinIfHolds(frameId, identity))
                return frame;
        }

        std::unreachable();
    }

    StorageLayout::frame_id_t StorageManager::AcquireFrameIdNoLock(){
        auto frameId = StorageLayout::INVALID_FRAME_ID;
        while (this->_memoryManager.PopStackNoLock(frameId) == false)
            this->EvictPageNoLock();
        return frameId;
    }

    Pages::Frame* StorageManager::GetFrame(
        const buffer_pool_slot_t slotIndex,
        const page_id_t pageId
    ){
        const auto frameIdentity = Pages::FrameIdentity(slotIndex, pageId);

        if (const auto* segment = this->GetSegmentNoLock(slotIndex, pageId)){
            const auto frameId = segment->_frames[StorageLayout::PageAddress::PageSlot(pageId)]
                .load(std::memory_order_acquire);

            if (frameId != StorageLayout::INVALID_FRAME_ID){
                if (auto* frame = this->PinIfHolds(frameId, frameIdentity)){
                    if (!frame->HasSecondChance())
                        frame->MarkHasSecondChance();
                    return frame;
                }
            }
        }

        return this->HandlePageCacheMiss(slotIndex, pageId);
    }

    void StorageManager::FlushAll() const{
        MultiThreading::WriterGuard lock(&this->_tableMutex);

        std::vector<native_file_t> touchedFiles;
        for (Int i = 0;i < this->_memoryManager.Capacity(); i++){
            auto* frame = this->_memoryManager.GetFrame(i);
            if (!frame->StateEquals(Pages::FrameState::Ready) || !frame->IsDirty())
                continue;

            const auto handle = this->_fileManager.GetHandle(frame->SlotIndex());

            this->WriteFrameToDiskNoLock(frame);
            if (std::ranges::find(touchedFiles, handle) == touchedFiles.end())
                touchedFiles.push_back(handle);
        }

        for (const auto handle: touchedFiles)
            ::Storage::FileIO::Sync(handle);
    }

    void StorageManager::CleanAheadOfClock(){
        const auto capacity = this->_memoryManager.Capacity();
        const auto clockHand = this->_clockHand.load(std::memory_order_relaxed);

        const auto moved = (clockHand + capacity - this->_writerLastHand) % capacity;
        this->_writerLastHand = clockHand;

        const auto lookAhead = Math::Min(Math::Max(2 * moved, WRITER_MIN_LOOKAHEAD), capacity / 4);

        UnsignedInt framesWritten = 0;

        for (auto step = 0; step < lookAhead && framesWritten < 8u; step++){
            const auto frameId = (clockHand + step) % capacity;
            auto* frame = this->_memoryManager.GetFrame(frameId);

            if (
                !frame->IsDirty()
                || frame->HasSecondChance()
                || !frame->StateEquals(Pages::FrameState::Ready)
            ) continue;

            if (!frame->TryPin())
                continue;

            const Pages::FramePin framePin(frame);// unpins even if the write throws

            if (!frame->StateEquals(Pages::FrameState::Ready) || !frame->IsDirty())
                continue;// changed between the check and the pin

            bool latched = false;
            const auto latch = MultiThreading::ReaderGuard::TryLock(&frame->_latch, latched);
            if (!latched)
                continue;// someone is modifying it: skip, don't stall

            this->WriteFrameToDiskNoLock(frame);
            framesWritten++;
        }
    }

    StorageManager& StorageManager::Get(){
        static StorageManager storageManager;
        return storageManager;
    }

    void StorageManager::Initialize(const UnsignedBigInt size){
        this->_memoryManager.Initialize(size);

        for (SmallInt i = 0; i < StorageLayout::MAX_BUFFER_POOL_SLOTS; ++i)
            this->_freeDbStack[i] = i;

        this->_freeTopDbStack = StorageLayout::MAX_BUFFER_POOL_SLOTS;

        this->_writer.Start([this]{ this->CleanAheadOfClock(); });
    }

    void StorageManager::Shutdown(){
        this->_writer.Stop();
        this->FlushAll();
        this->_fileManager.CloseAllFiles();
    }

    Accessors::PageAccessor StorageManager::ReserveDbPageTables(){
        MultiThreading::WriterGuard guard(&this->_stackMutex);

        if (this->_freeTopDbStack < 2)
            throw std::runtime_error("StorageManager::ReserveDbPageTables: too many open databases");

        const auto dataSlot = this->_freeDbStack[--this->_freeTopDbStack];
        const auto systemSlot = this->_freeDbStack[--this->_freeTopDbStack];

        return Accessors::PageAccessor(dataSlot, systemSlot);
    }

    template<bool Flush>
    void StorageManager::ReleasePageTables(const Accessors::PageAccessor accessor){
        const auto owns = [&](const StorageLayout::frame_identity_t identity){
            if (identity == Pages::INVALID_FRAME_IDENTITY)
                return false;
            const auto slot = Pages::SlotOf(identity);
            return slot == accessor._dataSlot || slot == accessor._systemSlot;
        };

        std::vector<StorageLayout::frame_id_t> claimedFrames;

        {
            MultiThreading::WriterGuard guard(&this->_tableMutex);
            for (StorageLayout::frame_id_t id = 0; id < this->_memoryManager.Capacity(); id++){
                auto* frame = this->_memoryManager.GetFrame(id);
                if (!owns(frame->Identity()))
                    continue;

                if (!Pages::ClaimWaitingForTransientPins(frame)){
                    for (const auto claimedFrame : claimedFrames)
                        this->_memoryManager.GetFrame(claimedFrame)->SetPinCount(0);

                    throw std::runtime_error("StorageManager::ReleaseDbPageTables: a page of the database is still pinned");
                }

                claimedFrames.push_back(id);
            }
        }

        for (auto i = 0;i < claimedFrames.size();i++){
            const auto frameId = claimedFrames[i];
            auto* frame = this->_memoryManager.GetFrame(frameId);

            if constexpr (Flush){
                if (frame->IsDirty()){
                    try{
                        this->WriteFrameToDiskNoLock(frame);
                    }
                    catch (...){
                        for (auto j = i; j < claimedFrames.size(); j++)
                            this->_memoryManager.GetFrame(claimedFrames[j])->SetPinCount(0);
                        throw;
                    }
                }
            }

            frame->MarkFlushed();
            this->ReleaseClaimedFrameNoLock(frameId, frame);
        }

        if constexpr (Flush){
            const auto dataHandle = this->_fileManager.GetHandle(accessor._dataSlot);
            const auto systemHandle = this->_fileManager.GetHandle(accessor._systemSlot);

            ::Storage::FileIO::Sync(dataHandle);
            ::Storage::FileIO::Sync(systemHandle);
        }

        this->_fileManager.CloseFile(accessor._dataSlot);
        this->_fileManager.CloseFile(accessor._systemSlot);

        MultiThreading::WriterGuard guard(&this->_stackMutex);
        this->_freeDbStack[this->_freeTopDbStack++] = accessor._dataSlot;
        this->_freeDbStack[this->_freeTopDbStack++] = accessor._systemSlot;
    }

    template void StorageManager::ReleasePageTables<true>(Accessors::PageAccessor);
    template void StorageManager::ReleasePageTables<false>(Accessors::PageAccessor);

    void StorageManager::OpenFile(
        const buffer_pool_slot_t slotIndex,
        const DataTypes::StringView& filename,
        const ::Storage::FileOpenMode mode

    ){
#ifdef _WIN32
        constexpr auto PAGE_FILE_FLAGS = ::Storage::FileOpenFlags::Direct | ::Storage::FileOpenFlags::Async;
#else
        constexpr auto PAGE_FILE_FLAGS = FileOpenFlags::Direct;
#endif

        this->_fileManager.OpenFile(slotIndex, filename, mode, PAGE_FILE_FLAGS);
    }
}