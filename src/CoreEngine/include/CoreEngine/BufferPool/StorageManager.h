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

#include "BackgroundWriter.h"

namespace CoreEngine::Accessors{
    struct PageAccessor;
}

namespace CoreEngine::Storage{
    struct Segment{
        std::atomic<StorageLayout::frame_id_t> _frames[StorageLayout::PageAddress::SEGMENT_SIZE];
        Segment(){
            for (auto& slot : this->_frames)
                slot.store(StorageLayout::INVALID_FRAME_ID, std::memory_order_relaxed);
        }
    };

    struct SegmentGroup{
        std::atomic<Segment*> _segments[StorageLayout::PageAddress::GROUP_SIZE];
        SegmentGroup(){ for (auto& slot : _segments) slot.store(nullptr, std::memory_order_relaxed); }
    };

    struct FileDirectory{
        std::atomic<SegmentGroup*> _groups[StorageLayout::PageAddress::DIRECTORY_SIZE];
        FileDirectory(){
            for (auto& slot : this->_groups)
                slot.store(nullptr, std::memory_order_relaxed);
        }
    };

    struct ExtentLoadInfo{
        StorageLayout::frame_id_t _frames[StorageLayout::EXTENT_SIZE];

        page_id_t _firstPageId;
        page_id_t _requestedPageId;
        buffer_pool_slot_t _slot;

        explicit ExtentLoadInfo()
            :  _frames{}, _firstPageId(INVALID_PAGE_ID),
                _requestedPageId(INVALID_PAGE_ID), _slot(0){}
        explicit ExtentLoadInfo(
            const page_id_t firstPageId,
            const page_id_t requestedPageId,
            const buffer_pool_slot_t slotIndex
        ):  _frames{}, _firstPageId(firstPageId),
            _requestedPageId(requestedPageId), _slot(slotIndex){}
    };

    struct ExtentFailLoadInfo{
        StorageLayout::frame_id_t _frames[StorageLayout::EXTENT_SIZE];
        page_id_t _pages[StorageLayout::EXTENT_SIZE];

        UnsignedInt _count;
        buffer_pool_slot_t _slot;
    };

    static_assert(
        StorageLayout::PageAddress::SEGMENT_SIZE % StorageLayout::EXTENT_SIZE == 0,
        "an extent must never straddle two segments"
    );

    class StorageManager final{
        FileDirectory _pageTable[StorageLayout::MAX_BUFFER_POOL_SLOTS];
        UnsignedTinyInt _freeDbStack[StorageLayout::MAX_BUFFER_POOL_SLOTS];
        UnsignedSmallInt _freeTopDbStack;

        FileManager _fileManager;
        BufferPoolMemoryManager _memoryManager;

        mutable MultiThreading::Mutex _tableMutex; // protects pageTable_ and frame insertion
        mutable MultiThreading::Mutex _stackMutex; //protects dbStack
        const Memory::PersistentAllocator _allocator;

        std::atomic<UnsignedInt> _clockHand;
        UnsignedInt _writerLastHand; // touched only by the writer thread

        BackgroundWriter _writer;

        inline static constexpr UnsignedInt WRITER_MIN_LOOKAHEAD = 256;
        inline static constexpr UnsignedInt WRITER_MAX_WRITES = 128;   // 1 MB per round at 8 KB pages

        static_assert(
            WRITER_MAX_WRITES * StorageLayout::PAGE_SIZE == 1 * Constants::MB,
            "StorageManager Background Writer max writes spans more than 1 MB"
        );

        explicit StorageManager();

        void EvictPageNoLock();

        void WriteFrameToDiskNoLock(Pages::Frame* frame) const;

        void ReleaseClaimedFrameNoLock(
            StorageLayout::frame_id_t frameId,
            Pages::Frame* frame
        );

        [[nodiscard]] ExtentLoadInfo ReserveExtentNoLock(
            buffer_pool_slot_t slotIndex,
            page_id_t pageId
        );

        [[nodiscard]]Pages::Frame* CompleteExtentLoad(const ExtentLoadInfo& info) const;

        void FailExtentLoad(const ExtentFailLoadInfo& failedInfo) const;

        [[nodiscard]]Pages::Frame* BeginCreateFrame(
            buffer_pool_slot_t slotIndex,
            page_id_t pageId
        );

        [[nodiscard]]Pages::Frame* PinIfHolds(
            StorageLayout::frame_id_t frameId,
            StorageLayout::frame_identity_t frameIdentity
        ) const;

        Segment* GetSegmentNoLock(buffer_pool_slot_t slotIndex, page_id_t pageId) const;

        Segment* EnsureSegmentExistsNoLock(buffer_pool_slot_t slotIndex, page_id_t pageId);

       Pages::Frame* HandlePageCacheMiss(
            buffer_pool_slot_t slotIndex,
            page_id_t pageId
        );

        StorageLayout::frame_id_t AcquireFrameIdNoLock();

       Pages::Frame* GetFrame(
            buffer_pool_slot_t slotIndex,
            page_id_t pageId
        );

        void FlushAll() const;

        void CleanAheadOfClock();

    public:
        static StorageManager& Get();

        void Initialize(UnsignedBigInt size);

        void Shutdown();

        Accessors::PageAccessor ReserveDbPageTables();

        template<bool Flush>
        void ReleasePageTables(Accessors::PageAccessor accessor);

        void OpenFile(
            buffer_pool_slot_t slotIndex,
            const DataTypes::StringView& filename,
            ::Storage::FileOpenMode mode
        );

        template<PageLayout::PageViewType TView, typename... Args>
        TView CreatePage(
            buffer_pool_slot_t slotIndex,
            page_id_t pageId,
            Args&&... args
        );
        template<PageLayout::PageViewType TView>
        TView GetPage(
            buffer_pool_slot_t slotIndex,
            page_id_t pageId
        );
    };
}
