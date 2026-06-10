#pragma once
#include "../DatabaseConstants.h"
#include <string>
#include <vector>

#include "BufferPoolMemoryManager.h"
#include "FileManager.h"
#include "Pages/AllocationPageView.h"
#include "Pages/GlobalAllocationPageView.h"
#include "Pages/HeaderPageView.h"
#include "Pages/IndexPageView.h"
#include "Pages/LargeObjectView.h"
#include "Pages/OverflowPageView.h"
#include "Pages/PageFreeSpaceView.h"

namespace CoreEngine {
  class Database;

  namespace StorageTypes {
    class Table;
  }
} // namespace DatabaseEngine

namespace Storage {
    static constexpr Int MAX_DATABASES = 100;
    static constexpr Int FILE_TABLE_SIZE = 2;
    static constexpr Int INVALID_FRAME = -1;

    struct PageAddress{
        static constexpr Int SEGMENT_BITS = 12;
        static constexpr Int GROUP_BITS = 10;
        static constexpr Int DIRECTORY_BITS = 10;

        static constexpr Int SEGMENT_SIZE = 1 << SEGMENT_BITS;
        static constexpr Int GROUP_SIZE = 1 << GROUP_BITS;
        static constexpr Int DIRECTORY_SIZE = 1 << DIRECTORY_BITS;

        [[nodiscard]] static Int DirectorySlot(page_id_t pageId);
        [[nodiscard]] static Int GroupSlot(page_id_t pageId);
        [[nodiscard]] static Int SegmentSlot(page_id_t pageId);
        [[nodiscard]] static Int PageSlot(page_id_t pageId);
    };

    struct Segment{
        std::atomic<Pages::FrameId> frames[PageAddress::SEGMENT_SIZE];
        Segment();
    };

    struct SegmentGroup{
        std::atomic<Segment*> segments[PageAddress::GROUP_SIZE];
        SegmentGroup(){ for (auto& slot : segments) slot.store(nullptr, std::memory_order_relaxed); }
    };

    struct FileDirectory{
        std::atomic<SegmentGroup*> groups[PageAddress::DIRECTORY_SIZE];
        FileDirectory(){ for (auto& slot : groups) slot.store(nullptr, std::memory_order_relaxed); }
    };

    struct DatabaseTable{
        FileDirectory files[FILE_TABLE_SIZE];
    };

    class StorageManager final{
        FileManager fileManager;
        mutable MultiThreading::Mutex tableMutex; // protects pageTable_ and frame insertion

        std::atomic<DatabaseTable*> _pageTable[MAX_DATABASES];
        const CoreEngine::Memory::PersistentAllocator _allocator;
        CoreEngine::BufferPoolMemoryManager* _memoryManager;

        Int capacity;
        Int clockHand;

        explicit StorageManager();

        void EvictPageNoLock();
        void TryFlushFrameToDiskNoLock(const Pages::Frame* framePtr);
        Pages::Frame* OpenExtentNoLock(
            FileKey fileKey,
            page_id_t pageId,
            extent_id_t extentId,
            const CoreEngine::StorageTypes::Table *table
        );

        void CacheFrameToPageTableNoLock(FileKey key, page_id_t pageId, Pages::FrameId frameId);
        Pages::Frame* CreateFrame(FileKey fileKey, page_id_t pageId, const CoreEngine::StorageTypes::Table *table);

        Segment* GetSegmentNoLock(FileKey fileKey, page_id_t pageId) const;

        void EnsureDatabaseTableExistsNoLock(FileKey fileKey);
        Segment* EnsureSegmentExistsNoLock(FileKey fileKey, page_id_t pageId) const;
        Pages::Frame* HandlePageCacheMiss(
            FileKey fileKey,
            page_id_t pageId,
            const CoreEngine::StorageTypes::Table *table
        );

        Pages::FrameId AcquireFrameId();
        Pages::Frame* GetFrame(
            FileKey fileKey,
            page_id_t pageId,
            const CoreEngine::StorageTypes::Table *table
        );
        void RemovePage(Pages::Frame* framePtr);

    public:
        static StorageManager& Get();
        ~StorageManager();
        void CreateFile(
            FileKey key,
            const DataTypes::StringView& filename,
            const DataTypes::StringView& extension
        );
        void OpenFile(
            FileKey key,
            const DataTypes::StringView& filename
        );
        Pages::PageView CreatePage(
            FileKey fileKey,
            const CoreEngine::StorageTypes::Table *table,
            page_id_t pageId
        );
        Pages::PageView GetPage(
            FileKey fileKey,
            page_id_t pageId,
            const CoreEngine::StorageTypes::Table *table
        );
        Pages::HeaderPageView GetHeaderPage(FileKey fileKey);
        Pages::HeaderPageView CreateHeaderPage(FileKey fileKey);
        Pages::LargeObjectView CreateLargeDataPage(FileKey fileKey, page_id_t pageId);
        Pages::LargeObjectView GetLargeDataPage(
            FileKey fileKey,
            page_id_t pageId,
            const CoreEngine::StorageTypes::Table *table
        );
        Pages::OverflowPageView CreateOverflowPage(FileKey fileKey, page_id_t pageId);
        Pages::OverflowPageView GetOverflowPage(
            FileKey fileKey,
            page_id_t pageId,
            const CoreEngine::StorageTypes::Table *table
        );
        Pages::GlobalAllocationPageView CreateGlobalAllocationMapPage(FileKey fileKey, page_id_t pageId);
        Pages::GlobalAllocationPageView GetGlobalAllocationMapPage(FileKey fileKey, page_id_t pageId);
        Pages::AllocationPageView CreateAllocationPage(
            FileKey fileKey,
            table_id_t tableId,
            page_id_t pageId,
            extent_id_t startingExtentId
        );
        Pages::AllocationPageView GetAllocationPage(
            FileKey fileKey,
            page_id_t pageId,
            const CoreEngine::StorageTypes::Table *table
        );
        Pages::PageFreeSpaceView CreatePageFreeSpacePage(FileKey fileKey, page_id_t pageId);
        Pages::PageFreeSpaceView GetPageFreeSpacePage(FileKey fileKey, page_id_t pageId);
        Pages::IndexPageView CreateIndexPage(
            FileKey fileKey,
            const CoreEngine::StorageTypes::Table* table,
            page_id_t pageId
        );
        Pages::IndexPageView GetIndexPage(
            FileKey fileKey,
            page_id_t pageId,
            const CoreEngine::StorageTypes::Table* table
        );
    };
}
