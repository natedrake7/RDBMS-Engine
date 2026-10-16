#pragma once
#include <CoreEngine/Memory/PersistentAllocator.h>
#include <CoreEngine/DatabaseConstants.h>
#include <CoreEngine/BufferPool/BufferPoolMemoryManager.h>
#include <CoreEngine/BufferPool/FileManager.h>
#include <CoreEngine/Pages/AllocationPageView.h>
#include <CoreEngine/Pages/GlobalAllocationPageView.h>
#include <CoreEngine/Pages/HeaderPageView.h>
#include <CoreEngine/Pages/IndexPageView.h>
#include <CoreEngine/Pages/LargeObjects/LobDataView.h>
#include <CoreEngine/Pages/OverflowPageView.h>
#include <CoreEngine/Pages/PageFreeSpaceView.h>
#include <CoreEngine/Pages/LargeObjects/LobIndexView.h>
#include <CoreEngine/Pages/LargeObjects/LobRootView.h>

namespace CoreEngine {
  class Database;

  namespace StorageTypes {
    class Table;
  }
} // namespace DatabaseEngine

namespace Storage {
    static constexpr Int INVALID_FRAME = -1;
    static constexpr UnsignedInt FILE_TABLE_SIZE = 2;
    static constexpr UnsignedInt MAX_DATABASES = 100;

    namespace PageAddress{
        inline constexpr UnsignedInt SEGMENT_BITS = 12;
        inline constexpr UnsignedInt GROUP_BITS = 10;
        inline constexpr UnsignedInt DIRECTORY_BITS = 10;

        inline constexpr UnsignedInt SEGMENT_SIZE = 1 << SEGMENT_BITS;
        inline constexpr UnsignedInt GROUP_SIZE = 1 << GROUP_BITS;
        inline constexpr UnsignedInt DIRECTORY_SIZE = 1 << DIRECTORY_BITS;

        [[nodiscard]] inline constexpr UnsignedInt SegmentSlot(const page_id_t pageId){
            return pageId >> PageAddress::SEGMENT_BITS;
        }

        [[nodiscard]] inline constexpr UnsignedInt PageSlot(const page_id_t pageId){
            return pageId & (PageAddress::SEGMENT_SIZE - 1);
        }

        [[nodiscard]] inline constexpr UnsignedInt DirectorySlot(const page_id_t pageId){
            const auto segmentId = PageAddress::SegmentSlot(pageId);
            return (segmentId >> PageAddress::GROUP_BITS) & (PageAddress::DIRECTORY_SIZE - 1);
        }

        [[nodiscard]] inline constexpr UnsignedInt GroupSlot(const page_id_t pageId){
            const auto segmentId = PageAddress::SegmentSlot(pageId);
            return segmentId & (PageAddress::GROUP_SIZE - 1);
        }
    }

    struct Segment{
        std::atomic<Pages::FrameId> _frames[PageAddress::SEGMENT_SIZE];
        Segment(){
            for (auto& slot : this->_frames)
                slot.store(INVALID_FRAME, std::memory_order_relaxed);
        }
    };

    struct SegmentGroup{
        std::atomic<Segment*> _segments[PageAddress::GROUP_SIZE];
        SegmentGroup(){ for (auto& slot : _segments) slot.store(nullptr, std::memory_order_relaxed); }
    };

    struct FileDirectory{
        std::atomic<SegmentGroup*> _groups[PageAddress::DIRECTORY_SIZE];
        FileDirectory(){
            for (auto& slot : this->_groups)
                slot.store(nullptr, std::memory_order_relaxed);
        }
    };

    struct DatabaseTable{
        FileDirectory _files[MAX_OPEN_FILES];
    };

    class StorageManager final{
        std::atomic<DatabaseTable> _pageTable[MAX_DATABASES];
        UnsignedTinyInt _freeDbStack[MAX_DATABASES];
        UnsignedTinyInt _freeTopDbStack;

        FileManager _fileManager;
        CoreEngine::BufferPoolMemoryManager _memoryManager;

        mutable MultiThreading::Mutex _tableMutex; // protects pageTable_ and frame insertion
        mutable MultiThreading::Mutex _stackMutex; //protects dbStack
        const CoreEngine::Memory::PersistentAllocator _allocator;

        Int _clockHand;

        explicit StorageManager();

        template<FileType Type>
        void EvictPageNoLock();

        void FlushFrameToDiskNoLock(Pages::Frame* framePtr);

        template<FileType Type>
        [[nodiscard]] Pages::Frame* OpenExtentNoLock(
            buffer_pool_index_t slotIndex,
            page_id_t pageId,
            extent_id_t extentId
        );

        template<FileType Type>
        [[nodiscard]] Pages::Frame* CreateFrame(
            buffer_pool_index_t slotIndex,
            page_id_t pageId,
            Constants::PageType type
        );

        template<FileType Type>
        Segment* GetSegmentNoLock(buffer_pool_index_t slotIndex, page_id_t pageId) const;

        template<FileType Type>
        Segment* EnsureSegmentExistsNoLock(buffer_pool_index_t slotIndex, page_id_t pageId) const;

        template<FileType Type>
        Pages::Frame* HandlePageCacheMiss(
            buffer_pool_index_t slotIndex,
            page_id_t pageId
        );

        template<FileType Type>
        Pages::FrameId AcquireFrameId();

        template<FileType Type>
        Pages::Frame* GetFrame(
            buffer_pool_index_t slotIndex,
            page_id_t pageId
        );
        void RemovePage(Pages::Frame* framePtr);

    public:
        static StorageManager& Get();
        ~StorageManager();

        void Initialize(UnsignedBigInt size);

        [[nodiscard]] bool ReserveDbPageTable(UnsignedTinyInt& outIndex);

        void FreeDbPageTable(SmallInt index);

        void CreateFile_(
            FileKey key,
            const DataTypes::StringView& filename,
            const DataTypes::StringView& extension
        );

        void OpenFile(
            FileKey key,
            const DataTypes::StringView& filename
        );

        void CloseAllFiles();

        void FlushAll();

        Pages::HeaderPageView CreateHeaderPage(FileKey fileKey);
        Pages::GlobalAllocationPageView CreateGlobalAllocationMapPage(buffer_pool_index_t slotIndex, gam_index_t gamIndex);
        Pages::AllocationPageView CreateAllocationPage(
            buffer_pool_index_t slotIndex,
            page_id_t pageId,
            page_id_t gamPageId
        );
        Pages::PageFreeSpaceView CreatePageFreeSpacePage(buffer_pool_index_t slotIndex, page_id_t pageId);

        template<typename TView>
        TView CreatePage(
            buffer_pool_index_t fileKey,
            page_id_t pageId
        );
        template<typename TView>
        TView GetPage(
            buffer_pool_index_t slotIndex,
            page_id_t pageId
        );

        template<typename TView>
        inline static constexpr Constants::PageType DeducePageType();
    };

    template <typename TView>
    constexpr Constants::PageType StorageManager::DeducePageType(){
        if constexpr (std::is_same_v<TView, Pages::PageView>)
            return Constants::PageType::DATA;
        else if constexpr (std::is_same_v<TView, Pages::IndexPageView>)
            return Constants::PageType::INDEXED;
        else if constexpr (std::is_same_v<TView, Pages::LobRootView>)
            return Constants::PageType::LOB_ROOT;
        else if constexpr (std::is_same_v<TView, Pages::LobIndexView>)
            return Constants::PageType::LOB_INDEX;
        else if constexpr (std::is_same_v<TView, Pages::LobDataView>)
            return Constants::PageType::LOB_DATA;
        else if constexpr (std::is_same_v<TView, Pages::OverflowPageView>)
            return Constants::PageType::OVERFLOW_TYPE;
        else
            static_assert(DataTypes::AlwaysFalse<TView>, "Invalid Page Type");

        return Constants::PageType::DATA;
    }

    template <typename TView>
    consteval FileType PageFileType(){
        constexpr auto isSystem = std::is_same_v<TView, Pages::GlobalAllocationPageView>
            || std::is_same_v<TView, Pages::HeaderPageView>
            || std::is_same_v<TView, Pages::PageFreeSpaceView>;

        if constexpr (isSystem)
            return FileType::System;
        else
            return FileType::Data;

        std::unreachable();
    }

    template<typename TView>
    TView StorageManager::CreatePage(
        const buffer_pool_index_t fileKey,
        const page_id_t pageId
    ){
        auto* frame = this->CreateFrame<PageFileType<TView>()>(fileKey, pageId, StorageManager::DeducePageType<TView>());
        if constexpr (std::is_same_v<TView, Pages::PageView>)
            frame->Header()->bytesLeft = CoreEngine::StorageLayout::PAGE_SIZE_WITHOUT_HEADER;
        else if constexpr (std::is_same_v<TView, Pages::IndexPageView>){
            frame->Header()->bytesLeft = CoreEngine::StorageLayout::INDEX_PAGE_DEFAULT_SIZE;
            auto* additionalHeader = reinterpret_cast<Pages::IndexPageAdditionalHeader*>(frame->_data + CoreEngine::StorageLayout::PAGE_HEADER_SIZE);
            additionalHeader->nextNode = INVALID_PAGE_ID;
            additionalHeader->previousNode = INVALID_PAGE_ID;
        }
        //LOB Pages don't track available space through pageHeader.
        else if constexpr (
            std::is_same_v<TView, Pages::LobDataView>
            || std::is_same_v<TView, Pages::LobIndexView>
            || std::is_same_v<TView, Pages::LobRootView>
        ) frame->Header()->bytesLeft = 0;
        else if constexpr (std::is_same_v<TView, Pages::OverflowPageView>)
            frame->Header()->bytesLeft = CoreEngine::StorageLayout::PAGE_SIZE_WITHOUT_HEADER;
        else
            static_assert(DataTypes::AlwaysFalse<TView>, "Invalid Page Type");

        return TView(frame);
    }

    template <typename TView>
    TView StorageManager::GetPage(
        const buffer_pool_index_t slotIndex,
        const page_id_t pageId
    ){
        auto* frame = this->GetFrame<PageFileType<TView>()>(slotIndex, pageId);
        return TView(frame);
    }
}
