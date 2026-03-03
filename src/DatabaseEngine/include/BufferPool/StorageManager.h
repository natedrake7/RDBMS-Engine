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

namespace DatabaseEngine {
  class Database;

  namespace StorageTypes {
    class Table;
  }
} // namespace DatabaseEngine

namespace Storage{
    struct PageKey{
        Int databaseId;
        page_id_t pageId;

        bool operator==(const PageKey& other) const{
            return this->databaseId == other.databaseId
                && this->pageId == other.pageId;
        }

        explicit PageKey(const Int databaseId, const page_id_t pageId)
            : databaseId(databaseId), pageId(pageId) {}

        static PageKey Create(const Int databaseId, const page_id_t pageId){
            return PageKey(databaseId, pageId);
        }
    };
}

template<>
struct std::hash<Storage::PageKey> {
    std::size_t operator()(const Storage::PageKey& key) const noexcept{
        return std::hash<page_id_t>()(key.databaseId) ^ std::hash<Int>()(key.pageId);
    }
};

namespace Storage {

    class StorageManager final{
        Int capacity;
        Int clockHand;
        // std::vector<Pages::Frame*> frames;
        Dictionary<PageKey, Int> pageTable; // pageId -> frame index

        DatabaseEngine::BufferPoolMemoryManager* _memoryManager;

        mutable MultiThreading::ReadWriteMutex clockMutex_; // protects eviction sweep
        mutable MultiThreading::ReadWriteMutex tableMutex; // protects pageTable_ and frame insertion

        FileManager fileManager;

    protected:
        explicit StorageManager();

        // static DataTypes::String CreateKey(const DataTypes::StringView& filename, page_id_t pageId);
        Pages::Frame* EvictPage();
        void RemovePageWithoutKeyDeletion(const Pages::Frame* framePtr);
        Pages::Frame* OpenExtent(
            FileKey fileKey,
            page_id_t pageId,
            extent_id_t extentId,
            const DataTypes::StringView& filename,
            const DatabaseEngine::StorageTypes::Table *table
        );
        static void SetReadFilePointerToOffset(std::fstream *file, const std::streampos &offSet);
        static void SetWriteFilePointerToOffset(std::fstream *file, const std::streampos &offSet);
        inline bool IsPageCached(PageKey key) const;
        Pages::Frame* CreateFrame(FileKey fileKey, const DataTypes::StringView& filename, page_id_t pageId, const DatabaseEngine::StorageTypes::Table *table);
        Pages::Frame* GetRawPage(
            FileKey fileKey,
            const DataTypes::StringView& filename,
            page_id_t pageId,
            const DatabaseEngine::StorageTypes::Table *table
        );

    public:
        static StorageManager& Get();
        ~StorageManager();
        void CreateFile(
            FileKey key,
            const DataTypes::StringView& filename,
            const DataTypes::StringView& extension
        );
        Pages::PageView CreatePage(
            FileKey fileKey,
            const DataTypes::StringView& filename,
            const DatabaseEngine::StorageTypes::Table *table,
            page_id_t pageId
        );
        Pages::PageView GetPage(
            FileKey fileKey,
            const DataTypes::StringView& filename,
            page_id_t pageId,
            const DatabaseEngine::StorageTypes::Table *table
        );
        Pages::HeaderPageView GetHeaderPage(
            FileKey fileKey,
            const DataTypes::StringView& filename
        );
        Pages::HeaderPageView CreateHeaderPage(
            FileKey fileKey,
            const DataTypes::StringView& filename
        );
        Pages::LargeObjectView CreateLargeDataPage(
            FileKey fileKey,
            const DataTypes::StringView& filename,
            page_id_t pageId
        );
        Pages::LargeObjectView GetLargeDataPage(
            FileKey fileKey,
            const DataTypes::StringView& filename,
            page_id_t pageId,
            const DatabaseEngine::StorageTypes::Table *table
        );
        Pages::OverflowPageView CreateOverflowPage(
            FileKey fileKey,
            const DataTypes::StringView& filename,
            page_id_t pageId
        );
        Pages::OverflowPageView GetOverflowPage(
            FileKey fileKey,
            const DataTypes::StringView& filename,
            page_id_t pageId,
            const DatabaseEngine::StorageTypes::Table *table
        );
        Pages::GlobalAllocationPageView CreateGlobalAllocationMapPage(
            FileKey fileKey,
            const DataTypes::StringView& filename,
            page_id_t pageId
        );
        Pages::GlobalAllocationPageView GetGlobalAllocationMapPage(
            FileKey fileKey,
            const DataTypes::StringView& filename,
            page_id_t pageId
        );
        Pages::AllocationPageView CreateAllocationPage(
            FileKey fileKey,
            const DataTypes::StringView& filename,
            table_id_t tableId,
            page_id_t pageId,
            extent_id_t startingExtentId
        );
        Pages::AllocationPageView GetAllocationPage(
            FileKey fileKey,
            const DataTypes::StringView& filename,
            page_id_t pageId,
            const DatabaseEngine::StorageTypes::Table *table
        );
        Pages::PageFreeSpaceView CreatePageFreeSpacePage(
            FileKey fileKey,
            const DataTypes::StringView& filename,
            page_id_t pageId
        );
        Pages::PageFreeSpaceView GetPageFreeSpacePage(
            FileKey fileKey,
            const DataTypes::StringView& filename,
            page_id_t pageId
        );
        Pages::IndexPageView CreateIndexPage(
            FileKey fileKey,
            const DataTypes::StringView& filename,
            const DatabaseEngine::StorageTypes::Table* table,
            page_id_t pageId
        );
        Pages::IndexPageView GetIndexPage(
            FileKey fileKey,
            const DataTypes::StringView& filename,
            page_id_t pageId,
            const DatabaseEngine::StorageTypes::Table* table
        );
    };
}
