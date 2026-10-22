#pragma once
#include <CoreEngine/Memory/PersistentAllocator.h>
#include <CoreEngine/DatabaseConstants.h>
#include <CoreEngine/BufferPool/BufferPoolMemoryManager.h>
#include <CoreEngine/BufferPool/FileManager.h>
#include <CoreEngine/Pages/AllocationPageView.h>
#include <CoreEngine/Pages/GlobalAllocationPageView.h>
#include <CoreEngine/Pages/HeaderPageView.h>
#include <CoreEngine/Pages/PageFreeSpaceView.h>

#include <CoreEngine/Pages/PageLayout.h>

namespace CoreEngine::Accessors{
    struct PageAccessor;
}

namespace Storage {
    struct Segment{
        std::atomic<CoreEngine::StorageLayout::frame_id_t> _frames[CoreEngine::StorageLayout::PageAddress::SEGMENT_SIZE];
        Segment(){
            for (auto& slot : this->_frames)
                slot.store(CoreEngine::StorageLayout::INVALID_FRAME_ID, std::memory_order_relaxed);
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

    struct ExtentLoadInfo{
        CoreEngine::StorageLayout::frame_id_t _frames[CoreEngine::StorageLayout::EXTENT_SIZE];

        page_id_t _firstPageId;
        page_id_t _requestedPageId;
        buffer_pool_slot_t _slot;

        ExtentLoadInfo() = default;
    };

    struct ExtentFailLoadInfo{
        CoreEngine::StorageLayout::frame_id_t _frames[CoreEngine::StorageLayout::EXTENT_SIZE];
        page_id_t _pages[CoreEngine::StorageLayout::EXTENT_SIZE];

        UnsignedInt _count;
        buffer_pool_slot_t _slot;
    };

    static_assert(
        CoreEngine::StorageLayout::PageAddress::SEGMENT_SIZE % CoreEngine::StorageLayout::EXTENT_SIZE == 0,
        "an extent must never straddle two segments"
    );

    class StorageManager final{
        FileDirectory _pageTable[CoreEngine::StorageLayout::MAX_BUFFER_POOL_SLOTS];
        UnsignedTinyInt _freeDbStack[CoreEngine::StorageLayout::MAX_BUFFER_POOL_SLOTS];
        UnsignedSmallInt _freeTopDbStack;

        FileManager _fileManager;
        CoreEngine::BufferPoolMemoryManager _memoryManager;

        mutable MultiThreading::Mutex _tableMutex; // protects pageTable_ and frame insertion
        mutable MultiThreading::Mutex _stackMutex; //protects dbStack
        const CoreEngine::Memory::PersistentAllocator _allocator;

        std::atomic<UnsignedInt> _clockHand;

        explicit StorageManager();

        void EvictPageNoLock();

        void FlushFrameToDiskNoLock(CoreEngine::Pages::Frame* frame) const;

        void ReleaseClaimedFrameNoLock(
            CoreEngine::StorageLayout::frame_id_t frameId,
            CoreEngine::Pages::Frame* frame
        );

        [[nodiscard]]CoreEngine::Pages::Frame* OpenExtentNoLock(
            buffer_pool_slot_t slotIndex,
            page_id_t pageId
        );

        [[nodiscard]] ExtentLoadInfo ReserveExtentNoLock(
            buffer_pool_slot_t slotIndex,
            page_id_t pageId
        );

        [[nodiscard]]CoreEngine::Pages::Frame* CompleteExtentLoad(const ExtentLoadInfo& info) const;

        void FailExtentLoad(const ExtentFailLoadInfo& failedInfo) const;

        [[nodiscard]]CoreEngine::Pages::Frame* BeginCreateFrame(
            buffer_pool_slot_t slotIndex,
            page_id_t pageId
        );

        [[nodiscard]]CoreEngine::Pages::Frame* PinIfHolds(
            CoreEngine::StorageLayout::frame_id_t frameId,
            CoreEngine::StorageLayout::frame_identity_t frameIdentity
        ) const;

        Segment* GetSegmentNoLock(buffer_pool_slot_t slotIndex, page_id_t pageId) const;

        Segment* EnsureSegmentExistsNoLock(buffer_pool_slot_t slotIndex, page_id_t pageId);

       CoreEngine::Pages::Frame* HandlePageCacheMiss(
            buffer_pool_slot_t slotIndex,
            page_id_t pageId
        );

        CoreEngine::StorageLayout::frame_id_t AcquireFrameIdNoLock();

       CoreEngine::Pages::Frame* GetFrame(
            buffer_pool_slot_t slotIndex,
            page_id_t pageId
        );

        [[nodiscard]] bool ReserveDbPageTableNoLock(buffer_pool_slot_t& outIndex);

    public:
        static StorageManager& Get();

        void Initialize(UnsignedBigInt size);

        CoreEngine::Accessors::PageAccessor ReserveDbPageTables();

        void FreeDbPageTable(CoreEngine::Accessors::PageAccessor accessor);

        void OpenFile(
            buffer_pool_slot_t slotIndex,
            const DataTypes::StringView& filename,
            FileOpenMode mode
        );

        void CloseAllFiles();

        void FlushAll() const;

        template<typename TView, typename... Args>
        TView CreatePage(
            buffer_pool_slot_t slotIndex,
            page_id_t pageId,
            Args&&... args
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
