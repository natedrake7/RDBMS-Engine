#pragma once
#include "../Memory/PersistentAllocator.h"
#include "../DatabaseConstants.h"
#include "BufferPoolMemoryManager.h"
#include "FileManager.h"
#include "../Pages/AllocationPageView.h"
#include "../Pages/GlobalAllocationPageView.h"
#include "../Pages/HeaderPageView.h"
#include "../Pages/IndexPageView.h"
#include "../Pages/LargeObjectView.h"
#include "../Pages/OverflowPageView.h"
#include "../Pages/PageFreeSpaceView.h"

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
            extent_id_t extentId
        );

        void CacheFrameToPageTableNoLock(FileKey key, page_id_t pageId, Pages::FrameId frameId) const;

        [[nodiscard]]
        Pages::Frame* CreateFrame(
            FileKey fileKey,
            page_id_t pageId,
            Constants::PageType type
        );

        Segment* GetSegmentNoLock(FileKey fileKey, page_id_t pageId) const;

        void EnsureDatabaseTableExistsNoLock(FileKey fileKey);
        Segment* EnsureSegmentExistsNoLock(FileKey fileKey, page_id_t pageId) const;
        Pages::Frame* HandlePageCacheMiss(
            FileKey fileKey,
            page_id_t pageId
        );

        Pages::FrameId AcquireFrameId();
        Pages::Frame* GetFrame(
            FileKey fileKey,
            page_id_t pageId
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

        Pages::HeaderPageView CreateHeaderPage(FileKey fileKey);
        Pages::GlobalAllocationPageView CreateGlobalAllocationMapPage(FileKey fileKey, page_id_t pageId);
        Pages::AllocationPageView CreateAllocationPage(
            FileKey fileKey,
            page_id_t pageId,
            page_id_t gamPageId
        );
        Pages::PageFreeSpaceView CreatePageFreeSpacePage(FileKey fileKey, page_id_t pageId);

        template<typename TView>
        TView CreatePage(
            FileKey fileKey,
            page_id_t pageId
        );
        template<typename TView>
        TView GetPage(
            FileKey fileKey,
            page_id_t pageId
        );

        template<typename TView>
        static constexpr Constants::PageType DeducePageType();
    };

    template <typename TView>
    constexpr Constants::PageType StorageManager::DeducePageType(){
        if constexpr (std::is_same_v<TView, Pages::PageView>)
            return Constants::PageType::DATA;
        else if constexpr (std::is_same_v<TView, Pages::IndexPageView>)
            return Constants::PageType::INDEX;
        else if constexpr (std::is_same_v<TView, Pages::LargeObjectView>)
            return Constants::PageType::LOB;
        else if constexpr (std::is_same_v<TView, Pages::OverflowPageView>)
            return Constants::PageType::OVERFLOWTYPE;
        else
            static_assert(DataTypes::AlwaysFalse<TView>, "Invalid Page Type");

        return Constants::PageType::DATA;
    }

    template<typename TView>
    TView StorageManager::CreatePage(
        const FileKey fileKey,
        const page_id_t pageId
    ){
        auto* frame = this->CreateFrame(fileKey, pageId, StorageManager::DeducePageType<TView>());

        if constexpr (std::is_same_v<TView, Pages::PageView>)
            frame->Header()->bytesLeft = Constants::PAGE_SIZE_WITHOUT_HEADER;
        else if constexpr (std::is_same_v<TView, Pages::IndexPageView>){
            frame->Header()->bytesLeft = Constants::INDEX_PAGE_DEFAULT_SIZE;
            auto* additionalHeader = reinterpret_cast<Pages::IndexPageAdditionalHeader*>(frame->_data + Constants::PAGE_HEADER_SIZE);
            additionalHeader->nextNode = INVALID_PAGE_ID;
            additionalHeader->previousNode = INVALID_PAGE_ID;
        }
        else if constexpr (std::is_same_v<TView, Pages::LargeObjectView>)
            frame->Header()->bytesLeft = Constants::LARGE_OBJECT_PAGE_SIZE;
        else if constexpr (std::is_same_v<TView, Pages::OverflowPageView>)
            frame->Header()->bytesLeft = Constants::PAGE_SIZE_WITHOUT_HEADER;
        else
            static_assert(DataTypes::AlwaysFalse<TView>, "Invalid Page Type");

        return TView(frame);
    }

    template <typename TView>
    TView StorageManager::GetPage(
        const FileKey fileKey,
        const page_id_t pageId
    ){
        auto* frame = this->GetFrame(fileKey, pageId);
        return TView(frame);
    }

    template Pages::PageView StorageManager::GetPage(FileKey, page_id_t);
    template Pages::IndexPageView StorageManager::GetPage(FileKey, page_id_t);
    template Pages::HeaderPageView StorageManager::GetPage(FileKey, page_id_t);
    template Pages::LargeObjectView StorageManager::GetPage(FileKey, page_id_t);
    template Pages::OverflowPageView StorageManager::GetPage(FileKey, page_id_t);
    template Pages::GlobalAllocationPageView StorageManager::GetPage(FileKey, page_id_t);
    template Pages::AllocationPageView StorageManager::GetPage(FileKey, page_id_t);
    template Pages::PageFreeSpaceView StorageManager::GetPage(FileKey, page_id_t);
}
