#pragma once
#include <CoreEngine/Memory/PersistentAllocator.h>
#include <CoreEngine/DatabaseConstants.h>
#include <CoreEngine/BufferPool/BufferPoolMemoryManager.h>
#include <CoreEngine/BufferPool/FileManager.h>
#include <CoreEngine/Pages/AllocationPageView.h>
#include <CoreEngine/Pages/GlobalAllocationPageView.h>
#include <CoreEngine/Pages/HeaderPageView.h>
#include <CoreEngine/Pages/PageFreeSpaceView.h>

#include <CoreEngine/Accessors/PageAccessor.h>
#include <CoreEngine/Pages/PageLayout.h>

namespace CoreEngine {
    class Database;

  namespace StorageTypes {
    class Table;
  }
} // namespace DatabaseEngine

namespace Storage {
    struct Segment{
        std::atomic<CoreEngine::StorageLayout::frame_id_t> _frames[CoreEngine::StorageLayout::PageAddress::SEGMENT_SIZE];
        Segment(){
            for (auto& slot : this->_frames)
                slot.store(CoreEngine::StorageLayout::INVALID_FRAME, std::memory_order_relaxed);
        }
    };

    struct SegmentGroup{
        std::atomic<Segment*> _segments[CoreEngine::StorageLayout::PageAddress::GROUP_SIZE];
        SegmentGroup(){ for (auto& slot : _segments) slot.store(nullptr, std::memory_order_relaxed); }
    };

    struct FileDirectory{
        std::atomic<SegmentGroup*> _groups[CoreEngine::StorageLayout::PageAddress::DIRECTORY_SIZE];
        FileDirectory(){
            for (auto& slot : this->_groups)
                slot.store(nullptr, std::memory_order_relaxed);
        }
    };

    class StorageManager final{
        FileDirectory _pageTable[CoreEngine::StorageLayout::MAX_BUFFER_POOL_SLOTS];
        UnsignedTinyInt _freeDbStack[CoreEngine::StorageLayout::MAX_BUFFER_POOL_SLOTS];
        UnsignedTinyInt _freeTopDbStack;

        FileManager _fileManager;
        CoreEngine::BufferPoolMemoryManager _memoryManager;

        mutable MultiThreading::Mutex _tableMutex; // protects pageTable_ and frame insertion
        mutable MultiThreading::Mutex _stackMutex; //protects dbStack
        const CoreEngine::Memory::PersistentAllocator _allocator;

        UnsignedInt _clockHand;

        explicit StorageManager();

        void EvictPageNoLock();

        void FlushFrameToDiskNoLock(CoreEngine::Pages::Frame* framePtr) const;

        [[nodiscard]]CoreEngine::Pages::Frame* OpenExtentNoLock(
            buffer_pool_slot_t slotIndex,
            page_id_t pageId,
            extent_id_t extentId
        );

        [[nodiscard]]CoreEngine::Pages::Frame* CreateFrame(
            buffer_pool_slot_t slotIndex,
            page_id_t pageId,
            CoreEngine::PageLayout::PageType type
        );

        Segment* GetSegmentNoLock(buffer_pool_slot_t slotIndex, page_id_t pageId) const;

        Segment* EnsureSegmentExistsNoLock(buffer_pool_slot_t slotIndex, page_id_t pageId);

       CoreEngine::Pages::Frame* HandlePageCacheMiss(
            buffer_pool_slot_t slotIndex,
            page_id_t pageId
        );

        CoreEngine::StorageLayout::frame_id_t AcquireFrameId();

       CoreEngine::Pages::Frame* GetFrame(
            buffer_pool_slot_t slotIndex,
            page_id_t pageId
        );

        [[nodiscard]] bool ReserveDbPageTableNoLock(buffer_pool_slot_t& outIndex);

    public:
        static StorageManager& Get();
        ~StorageManager();

        void Initialize(UnsignedBigInt size);

        CoreEngine::Accessors::PageAccessor ReserveDbPageTables();

        void FreeDbPageTable(SmallInt index);

        void OpenFile(
            buffer_pool_slot_t slotIndex,
            const DataTypes::StringView& filename,
            FileOpenMode mode
        );

        void CloseAllFiles();

        void FlushAll() const;

       CoreEngine::Pages::HeaderPageView CreateHeaderPage(buffer_pool_slot_t slotIndex);
       CoreEngine::Pages::GlobalAllocationPageView CreateGlobalAllocationMapPage(buffer_pool_slot_t slotIndex, gam_index_t gamIndex);
       CoreEngine::Pages::AllocationPageView CreateAllocationPage(
            buffer_pool_slot_t slotIndex,
            page_id_t pageId,
            page_id_t gamPageId
        );
       CoreEngine::Pages::PageFreeSpaceView CreatePageFreeSpacePage(buffer_pool_slot_t slotIndex, page_id_t pageId);

        template<typename TView>
        TView CreatePage(
            buffer_pool_slot_t slotIndex,
            page_id_t pageId
        );
        template<typename TView>
        TView GetPage(
            buffer_pool_slot_t slotIndex,
            page_id_t pageId
        );

        template<typename TView>
        inline static constexpr CoreEngine::PageLayout::PageType DeducePageType();
    };
}
