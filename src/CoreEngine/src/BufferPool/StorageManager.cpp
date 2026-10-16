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
    : _freeDbStack{}, _memoryManager{}{
    this->_clockHand = 0;
    this->_freeTopDbStack = 0;
}

template<FileType Type>
void StorageManager::EvictPageNoLock() {
    const auto limit = 2 * this->_memoryManager.Capacity();

    for (Int i = 0; i < limit; i++){
        const auto candidateId = this->_clockHand;
        auto* frame = this->_memoryManager.GetFrame(candidateId);

        this->_clockHand = (this->_clockHand + 1) % this->_memoryManager.Capacity();

        if (!frame->IsValid() || frame->_priority.load() >= Constants::PagePriority::HIGH)
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
        auto* segment = this->GetSegmentNoLock<Type>(frame->_fileKey, pageId);
        segment->_frames[PageAddress::PageSlot(pageId)].store(INVALID_FRAME, std::memory_order_release);
        this->_memoryManager.PushStackNoLock(candidateId);
    }

    throw std::runtime_error("StorageManager::EvictPageNoLock: Cannot evict from Buffer Pool, memory is capped");
}

void StorageManager::FlushFrameToDiskNoLock(Pages::Frame *framePtr){
    const auto fd = this->_fileManager.GetFile(framePtr->_fileKey);

    const auto pageId = framePtr->Header()->pageId;
    const auto offSet = pageId * CoreEngine::StorageLayout::PAGE_SIZE;

    const auto _ = File::Write(fd, framePtr->_data, CoreEngine::StorageLayout::PAGE_SIZE, offSet);

    framePtr->MarkFlushed();
    File::Flush(fd);
}

template<FileType Type>
Pages::Frame* StorageManager::OpenExtentNoLock(
    buffer_pool_index_t slotIndex,
    const page_id_t pageId,
    const extent_id_t extentId
){
    // read page from disk, call this->fileManager
    const auto fd = this->_fileManager.GetFile(slotIndex);

    const auto firstPageId = CoreEngine::StorageLayout::ExtentFirstPageId(extentId);

    // take into account the metadata page all the others
    Pages::Frame* framePtr = nullptr;
    for (Int i = 0; i < CoreEngine::StorageLayout::EXTENT_SIZE; i++){
        const page_id_t currentPageId = firstPageId + i;

        Segment* segment = this->EnsureSegmentExistsNoLock<Type>(slotIndex, currentPageId);
        const auto pageSlot = PageAddress::PageSlot(currentPageId);

        if (segment->_frames[pageSlot].load(std::memory_order_acquire) != INVALID_FRAME)
            continue;

        const auto frameId = this->AcquireFrameId<Type>();
        auto* destination = this->_memoryManager.Data() + frameId * CoreEngine::StorageLayout::PAGE_SIZE;

        const auto bytesRead = File::Read(fd, destination, CoreEngine::StorageLayout::PAGE_SIZE, currentPageId * CoreEngine::StorageLayout::PAGE_SIZE);

        if (bytesRead <= 0){
            this->_memoryManager.PushStackNoLock(frameId);
            break;
        }
        if (bytesRead < CoreEngine::StorageLayout::PAGE_SIZE)
            std::memset(destination, 0, CoreEngine::StorageLayout::PAGE_SIZE - bytesRead);

        const auto isRequestedPage = (currentPageId == pageId);

        auto* newFramePtr = this->_memoryManager.GetFrame(frameId);
        newFramePtr->_data = destination;
        newFramePtr->_fileKey = fileKey;
        newFramePtr->_isDirty = false;
        newFramePtr->_hasSecondChance = false;
        newFramePtr->_pinCount.store(isRequestedPage ? 1 : 0);   // producer pins the page it hands back
        newFramePtr->_priority.store(Constants::PagePriority::LOW);

        if (isRequestedPage)
            framePtr = newFramePtr;

        segment->_frames[pageSlot].store(frameId, std::memory_order_release);
    }

    return framePtr;
}

template<FileType Type>
Pages::Frame* StorageManager::GetFrame(
    buffer_pool_index_t slotIndex,
    const page_id_t pageId
){
    auto* segment = this->GetSegmentNoLock<Type>(fileKey, pageId);
    if (segment == nullptr)
        return this->HandlePageCacheMiss<Type>(fileKey, pageId);

    const auto frameId = segment->_frames[PageAddress::PageSlot(pageId)].load(std::memory_order_acquire);

    if (frameId == INVALID_FRAME)
        return this->HandlePageCacheMiss<Type>(fileKey, pageId);

    auto* frame = this->_memoryManager.GetFrame(frameId);

    if (!frame->TryPin())
        return this->HandlePageCacheMiss<Type>(fileKey, pageId);

    if (frame->_fileKey != fileKey
        || frame->Header()->pageId != pageId
        || segment->_frames[PageAddress::PageSlot(pageId)].load(std::memory_order_acquire) != frameId
    ){
        frame->Unpin();
        return this->HandlePageCacheMiss<Type>(fileKey, pageId);
    }

    frame->_hasSecondChance = true;
    return frame;
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

    for (SmallInt i = 0; i < MAX_DATABASES; ++i)
        this->_freeDbStack[i] = i;

    this->_freeTopDbStack = MAX_DATABASES;
}

bool StorageManager::ReserveDbPageTable(UnsignedTinyInt& outIndex){
    MultiThreading::WriterGuard guard(&this->_stackMutex);

    if (this->_freeTopDbStack == 0)
        return false;

    outIndex = this->_freeDbStack[--this->_freeTopDbStack];
    return true;
}

void StorageManager::FreeDbPageTable(const SmallInt index){
    MultiThreading::WriterGuard guard(&this->_stackMutex);
    this->_freeDbStack[this->_freeTopDbStack++] = index;
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
    auto* frame = this->CreateFrame<FileType::System>(fileKey, CoreEngine::StorageLayout::HEADER_PAGE_ID, Constants::PageType::HEADER);
    return Pages::HeaderPageView(frame);
}

Pages::GlobalAllocationPageView StorageManager::CreateGlobalAllocationMapPage(buffer_pool_index_t slotIndex, const gam_index_t gamIndex){
    const auto pageId = CoreEngine::StorageLayout::GamPageId(gamIndex);
    auto* frame = this->CreateFrame<FileType::System>(fileKey, pageId, Constants::PageType::GLOBAL_ALLOCATION);

    auto* header = reinterpret_cast<Pages::GlobalAllocationPageAdditionalHeader*>(frame->_data + CoreEngine::StorageLayout::PAGE_HEADER_SIZE);

    header->_gamIndex = gamIndex;
    header->_appendBitId = 0;
    header->_firstFreeBitId = 0;
    header->_freeExtentCount = CoreEngine::StorageLayout::GAM_BIT_MAP_SIZE;

    std::memset(header->_reserved, 0, CoreEngine::StorageLayout::GAM_HEADER_RESERVED_SPACE);

    return Pages::GlobalAllocationPageView(frame);
}

Pages::AllocationPageView StorageManager::CreateAllocationPage(
    buffer_pool_index_t slotIndex,
    const page_id_t pageId,
    const page_id_t gamPageId
){
    auto* frame = this->CreateFrame<FileType::Data>(slotIndex, pageId, Constants::PageType::ALLOCATION);

    auto* additionalHeader = reinterpret_cast<Pages::AllocationPageAdditionalHeader*>(frame->_data + CoreEngine::StorageLayout::PAGE_HEADER_SIZE);

    additionalHeader->_nextPageId = INVALID_PAGE_ID;
    additionalHeader->_lastAllocatedExtentId = 0;
    additionalHeader->_gamIndex = gamPageId;

    return Pages::AllocationPageView(frame);
}


Pages::PageFreeSpaceView StorageManager::CreatePageFreeSpacePage(buffer_pool_index_t slotIndex,const page_id_t pageId){
    auto* frame = this->CreateFrame<FileType::System>(slotIndex, pageId, Constants::PageType::FREE_SPACE);
    return Pages::PageFreeSpaceView(frame);
}

template<FileType Type>
Pages::Frame* StorageManager::CreateFrame(
    const buffer_pool_index_t slotIndex,
    const page_id_t pageId,
    const Constants::PageType type
){
    MultiThreading::WriterGuard lock(&this->_tableMutex);

    auto* segment = this->EnsureSegmentExistsNoLock<Type>(slotIndex, pageId);
    const auto segmentSlot = PageAddress::PageSlot(pageId);

    const auto* slot = &segment->_frames[segmentSlot];

    if (const auto existing = slot->load(std::memory_order_relaxed); existing != INVALID_FRAME){
        auto* frame = this->_memoryManager.GetFrame(existing);
        const auto pinned = frame->TryPin();
        assert(pinned && frame->_pinCount.load() == 1 && "Pages::Frame* StorageManager::CreateFrame: Re-creating a page that is still in use");
        return frame;
    }

    const auto frameId = this->AcquireFrameId<Type>();
    auto* frame = this->_memoryManager.GetFrame(frameId);

    frame->_data = this->_memoryManager.Data() + frameId * CoreEngine::StorageLayout::PAGE_SIZE;
    frame->_fileKey = slotIndex;
    frame->MarkDirty();
    frame->MarkHasSecondChance();
    frame->_pinCount.store(1);   // producer pins the page it hands back
    frame->_priority.store(Constants::PagePriority::LOW);

    std::memset(frame->_data, 0, CoreEngine::StorageLayout::PAGE_SIZE);

    auto* header = frame->Header();
    header->pageId = pageId;
    header->SetType(type);

    segment->_frames[segmentSlot].store(frameId, std::memory_order_release);

    return frame;
}

template<FileType Type>
Segment* StorageManager::GetSegmentNoLock(buffer_pool_index_t slotIndex, const page_id_t pageId) const{
    const auto& [_files] = this->_pageTable[fileKey._slotIndex].load(std::memory_order_acquire);
    const auto& directory = _files[static_cast<size_t>(Type)];
    const auto* group = directory._groups[PageAddress::DirectorySlot(pageId)].load(std::memory_order_acquire);
    if (group == nullptr)
        return nullptr;

    return group->_segments[PageAddress::GroupSlot(pageId)].load(std::memory_order_acquire);
}

template<FileType Type>
Segment* StorageManager::EnsureSegmentExistsNoLock(buffer_pool_index_t slotIndex, const page_id_t pageId) const{
    auto& directory = this->_pageTable[fileKey._slotIndex].load(std::memory_order_relaxed)
                                   ._files[static_cast<std::size_t>(Type)];

    auto& groupSlot = directory._groups[PageAddress::DirectorySlot(pageId)];
    auto* group = groupSlot.load(std::memory_order_relaxed);
    if (group == nullptr){
        group = this->_allocator.Allocate<SegmentGroup>();
        groupSlot.store(group, std::memory_order_release);
    }

    auto& segmentSlot = group->_segments[PageAddress::GroupSlot(pageId)];
    auto* segment = segmentSlot.load(std::memory_order_relaxed);
    if (segment == nullptr){
        segment = this->_allocator.Allocate<Segment>();
        segmentSlot.store(segment, std::memory_order_release);
    }

    return segment;
}

template<FileType Type>
Pages::Frame* StorageManager::HandlePageCacheMiss(
    buffer_pool_index_t slotIndex,
    const page_id_t pageId
){
    MultiThreading::WriterGuard guard(&this->_tableMutex);
    const auto* segment = this->EnsureSegmentExistsNoLock<Type>(fileKey, pageId);

    const auto frameId = segment->_frames[PageAddress::PageSlot(pageId)].load(std::memory_order_acquire);
    if (frameId != INVALID_FRAME){
        auto* frame = this->_memoryManager.GetFrame(frameId);

        const auto pinned = frame->TryPin();
        assert(pinned && "StorageManager::HandlePageCacheMiss: Failed to pin frame");
        frame->MarkHasSecondChance();
        return frame;
    }

    return this->OpenExtentNoLock<Type>(
        fileKey, pageId,
        CoreEngine::StorageLayout::ExtentId(pageId)
    );
}

template<FileType Type>
Pages::FrameId StorageManager::AcquireFrameId(){
    Pages::FrameId frameId = INVALID_FRAME;
    while (this->_memoryManager.PopStackNoLock(frameId) == false)
        this->EvictPageNoLock<Type>();
    return frameId;
}
} // namespace Storage