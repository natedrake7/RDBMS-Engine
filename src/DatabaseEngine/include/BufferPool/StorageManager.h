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

namespace Storage {
    class StorageManager final{
        Int capacity;
        Int clockHand;
        // std::vector<Pages::Frame*> frames;
        Dictionary<std::string, Int> pageTable; // pageId -> frame index

        DatabaseEngine::BufferPoolMemoryManager* _memoryManager;

        mutable MultiThreading::ReadWriteMutex clockMutex_; // protects eviction sweep
        mutable MultiThreading::ReadWriteMutex tableMutex; // protects pageTable_ and frame insertion

        FileManager fileManager;

    protected:
        explicit StorageManager();

        static std::string CreateKey(const std::string& filename, page_id_t pageId);
        Pages::Frame* EvictPage();
        void RemovePageWithoutKeyDeletion(const Pages::Frame* framePtr);
        Pages::Frame* OpenExtent(const page_id_t& pageId, const std::string& filename, extent_id_t extentId, const DatabaseEngine::StorageTypes::Table *table);
        static void SetReadFilePointerToOffset(std::fstream *file, const std::streampos &offSet);
        static void SetWriteFilePointerToOffset(std::fstream *file, const std::streampos &offSet);
        bool IsPageCached(const std::string& filename, page_id_t pageId)const;
        Pages::Frame* CreateFrame(const std::string &filename, page_id_t pageId, const DatabaseEngine::StorageTypes::Table *table);
        Pages::Frame* GetRawPage(const std::string& filename, page_id_t pageId, const DatabaseEngine::StorageTypes::Table *table);

    public:
        static StorageManager& Get();
        ~StorageManager();
        void CreateFile(const std::string& fileName, const std::string& extension)const;
        Pages::PageView CreatePage(const std::string& filename, const DatabaseEngine::StorageTypes::Table *table , page_id_t pageId);
        Pages::PageView GetPage(const std::string& filename, page_id_t pageId, const DatabaseEngine::StorageTypes::Table *table);
        Pages::HeaderPageView GetHeaderPage(const std::string &filename);
        Pages::HeaderPageView CreateHeaderPage(const std::string &filename);
        Pages::LargeObjectView CreateLargeDataPage(const std::string& filename, page_id_t pageId);
        Pages::LargeObjectView GetLargeDataPage(const std::string& filename, page_id_t pageId, const DatabaseEngine::StorageTypes::Table *table);
        Pages::OverflowPageView CreateOverflowPage(
            const std::string& filename,
            page_id_t pageId
        );
        Pages::OverflowPageView GetOverflowPage(
            const std::string& filename,
            page_id_t pageId,
            const DatabaseEngine::StorageTypes::Table *table
        );
        Pages::GlobalAllocationPageView CreateGlobalAllocationMapPage(const std::string &filename, page_id_t pageId);
        Pages::GlobalAllocationPageView GetGlobalAllocationMapPage(const std::string& filename, page_id_t pageId);
        Pages::AllocationPageView CreateAllocationPage(
            const std::string& filename,
            table_id_t tableId,
            page_id_t pageId,
            extent_id_t startingExtentId
        );
        Pages::AllocationPageView GetAllocationPage(
            const std::string& filename,
            page_id_t pageId,
            const DatabaseEngine::StorageTypes::Table *table
        );
        Pages::PageFreeSpaceView CreatePageFreeSpacePage(const std::string &filename, page_id_t pageId);
        Pages::PageFreeSpaceView GetPageFreeSpacePage(const std::string& filename, page_id_t pageId);
        Pages::IndexPageView CreateIndexPage(
            const std::string& filename,
            const DatabaseEngine::StorageTypes::Table* table,
            page_id_t pageId
        );
        Pages::IndexPageView GetIndexPage(
            const std::string& filename,
            page_id_t pageId,
            const DatabaseEngine::StorageTypes::Table* table
        );
    };
}
