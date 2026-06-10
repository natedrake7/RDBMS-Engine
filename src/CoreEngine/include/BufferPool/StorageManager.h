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
    static auto constexpr SEGMENT_SIZE = 4096;
    static auto constexpr FILE_TABLE_SIZE = 2;
    static auto constexpr INVALID_FRAME = -1;

    struct Segment{
        Pages::FrameId frames[SEGMENT_SIZE];
        Segment();
    };

    struct FileTable{
        DataStructures::PolymorphicArray<Segment*> segments;
    };

    struct DatabaseTable{
        FileTable files[FILE_TABLE_SIZE];
    };

    class StorageManager final{
        FileManager fileManager;
        mutable MultiThreading::Mutex tableMutex; // protects pageTable_ and frame insertion

        DataStructures::PolymorphicArray<DatabaseTable*> _pageTable;
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

        void EnsureDatabaseTableExistsNoLock(FileKey fileKey);
        void EnsureSegmentExistsNoLock(FileKey fileKey, page_id_t pageId);
        Pages::Frame* HandlePageCacheMiss(
            FileKey fileKey,
            page_id_t pageId,
            const CoreEngine::StorageTypes::Table *table,
            MultiThreading::ReaderGuard& readGuard
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
