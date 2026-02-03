#pragma once
#include "../DatabaseConstants.h"
#include <condition_variable>
#include <string>
#include <vector>

#include "BufferPoolMemory.h"
#include "FileManager.h"
#include "../Pages/OverflowPage.h"
#include "../Pages/PageGuard.h"
#include "Pages/AllocationPageView.h"
#include "Pages/GlobalAllocationPageView.h"
#include "Pages/IndexPageView.h"
#include "Pages/PageFreeSpaceView.h"

namespace DatabaseEngine {
  class Database;

  namespace StorageTypes {
    class Table;
  }
} // namespace DatabaseEngine

namespace Pages {
  class Page;
  class IndexPage;
  class PageFreeSpacePage;
  class IndexAllocationMapPage;
  class GlobalAllocationMapPage;
  class LargeObjectPage;
  class HeaderPage;
  struct PageHeader;
} // namespace Pages

namespace Storage {
    class StorageManager final{
        Int capacity;
        Int clockHand;
        std::vector<Pages::Frame> frames;
        Dictionary<std::string, Int> pageTable; // pageId -> frame index

        DatabaseEngine::BufferPoolMemory memoryPool;

        mutable MultiThreading::ReadWriteMutex clockMutex_; // protects eviction sweep
        mutable MultiThreading::ReadWriteMutex tableMutex; // protects pageTable_ and frame insertion

        FileManager fileManager;

    protected:
        explicit StorageManager();

        static std::string CreateKey(const std::string& filename, page_id_t pageId);
        Pages::Frame* EvictPage();
        void RemovePage(Pages::Page *page);
        void RemovePageWithoutKeyDeletion(const Pages::Frame* framePtr);
        static void AllocateMemoryBasedOnSystemPageType(Pages::Page **page, const Pages::PageHeader &pageHeader);
        static bool AllocateMemoryBasedOnPageType(Pages::Page **page, const Pages::PageHeader &pageHeader);
        Pages::Frame* OpenExtent(const page_id_t& pageId, const std::string& filename, extent_id_t extentId, const DatabaseEngine::StorageTypes::Table *table);
        static void SetReadFilePointerToOffset(fstream *file, const streampos &offSet);
        static void SetWriteFilePointerToOffset(fstream *file, const streampos &offSet);
        static Pages::PageHeader GetPageHeaderFromFile(const vector<char> &data, page_offset_t &offSet);
        bool IsPageCached(const std::string& filename, page_id_t pageId)const;
        Pages::Frame* CreateFrame(const std::string &filename, page_id_t pageId);
        Pages::Frame* GetRawPage(const std::string& filename, page_id_t pageId, const DatabaseEngine::StorageTypes::Table *table);

    public:
        static StorageManager& Get();
        ~StorageManager();
        void CreateFile(const std::string& fileName, const std::string& extension)const;
        Pages::PageView CreatePage(const std::string& filename, const DatabaseEngine::StorageTypes::Table *table , page_id_t pageId);
        Pages::PageView GetPage(const std::string& filename, page_id_t pageId, const DatabaseEngine::StorageTypes::Table *table);
        Pages::PageGuard<Pages::HeaderPage> GetHeaderPage(const std::string &filename);
        Pages::PageGuard<Pages::HeaderPage> CreateHeaderPage(const std::string &filename);
        Pages::PageGuard<Pages::LargeObjectPage> CreateLargeDataPage(const std::string& filename, page_id_t pageId);
        Pages::PageGuard<Pages::LargeObjectPage> GetLargeDataPage(const std::string& filename, page_id_t pageId, const DatabaseEngine::StorageTypes::Table *table);
        Pages::PageGuard<Pages::OverflowPage> CreateOverflowPage(
        const std::string& filename,
        page_id_t pageId
        );
        Pages::PageGuard<Pages::OverflowPage> GetOverflowPage(const std::string& filename, page_id_t pageId, const DatabaseEngine::StorageTypes::Table *table);
        Pages::GlobalAllocationPageView CreateGlobalAllocationMapPage(const std::string &filename, page_id_t pageId);
        Pages::GlobalAllocationPageView GetGlobalAllocationMapPage(const std::string& filename, page_id_t pageId);
        Pages::AllocationPageView CreateAllocationPage(const std::string& filename, table_id_t tableId, page_id_t pageId, extent_id_t startingExtentId);
        Pages::AllocationPageView GetAllocationPage(const std::string& filename, page_id_t pageId, const DatabaseEngine::StorageTypes::Table *table);
        Pages::PageFreeSpaceView CreatePageFreeSpacePage(const std::string &filename, page_id_t pageId);
        Pages::PageFreeSpaceView GetPageFreeSpacePage(const std::string& filename, page_id_t pageId);
        Pages::IndexPageView CreateIndexPage(const std::string& filename, const DatabaseEngine::StorageTypes::Table* table, page_id_t pageId);
        Pages::IndexPageView GetIndexPage(const std::string& filename, page_id_t pageId, const DatabaseEngine::StorageTypes::Table* table);
};

} // namespace Storage
