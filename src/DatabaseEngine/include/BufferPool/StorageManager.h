#pragma once
#include "../PipelineConstants.h"
#include <condition_variable>
#include <string>
#include <vector>
#include "FileManager.h"
#include "../Pages/OverflowPage.h"
#include "../Pages/PageGuard.h"

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
  int capacity;
  int clockHand;
  std::vector<Pages::Page*> frames;
  Dictionary<std::string, int> pageTable; // pageId -> frame index

  mutable MultiThreading::ReadWriteMutex clockMutex_; // protects eviction sweep
  mutable MultiThreading::ReadWriteMutex tableMutex; // protects pageTable_ and frame insertion

  FileManager fileManager;

protected:
  explicit StorageManager();

  static std::string CreateKey(const std::string& filename, const page_id_t &pageId);
  Pages::Page* EvictPage();
  void RemovePage(Pages::Page *page);
  void RemovePageWithoutKeyDeletion(Pages::Page *page);
  static void AllocateMemoryBasedOnSystemPageType(Pages::Page **page, const Pages::PageHeader &pageHeader);
  static bool AllocateMemoryBasedOnPageType(Pages::Page **page, const Pages::PageHeader &pageHeader);
  Pages::Page* OpenExtent(const page_id_t& pageId, const std::string& filename, const extent_id_t &extentId, const DatabaseEngine::StorageTypes::Table *table);
  static void SetReadFilePointerToOffset(fstream *file, const streampos &offSet);
  static void SetWriteFilePointerToOffset(fstream *file, const streampos &offSet);
  static Pages::PageHeader GetPageHeaderFromFile(const vector<char> &data, page_offset_t &offSet);
  bool IsPageCached(const std::string& filename, const page_id_t &pageId)const;
  void InsertPageToCache(Pages::Page *page, const std::string &filename, const page_id_t &pageId);
  Pages::Page* GetRawPage(const std::string& filename, const page_id_t &pageId, const DatabaseEngine::StorageTypes::Table *table);

public:
  static StorageManager& Get();
  ~StorageManager();
  void CreateFile(const std::string& fileName, const std::string& extension)const;
  Pages::PageGuard<Pages::Page> CreatePage(const std::string& filename, const page_id_t &pageId);
  Pages::PageGuard<Pages::Page> GetPage(const std::string& filename, const page_id_t &pageId, const DatabaseEngine::StorageTypes::Table *table);
  Pages::PageGuard<Pages::HeaderPage> GetHeaderPage(const std::string &filename);
  Pages::PageGuard<Pages::HeaderPage> CreateHeaderPage(const std::string &filename);
  Pages::PageGuard<Pages::LargeObjectPage> CreateLargeDataPage(const std::string& filename, const page_id_t &pageId);
  Pages::PageGuard<Pages::LargeObjectPage> GetLargeDataPage(const std::string& filename, const page_id_t &pageId, const DatabaseEngine::StorageTypes::Table *table);
  Pages::PageGuard<Pages::OverflowPage> CreateOverflowPage(const std::string& filename, const page_id_t &pageId);
  Pages::PageGuard<Pages::OverflowPage> GetOverflowPage(const std::string& filename, const page_id_t &pageId, const DatabaseEngine::StorageTypes::Table *table);
  Pages::PageGuard<Pages::GlobalAllocationMapPage> CreateGlobalAllocationMapPage(const std::string &filename, const page_id_t &pageId);
  Pages::PageGuard<Pages::GlobalAllocationMapPage> GetGlobalAllocationMapPage(const std::string& filename, const page_id_t &pageId);
  Pages::PageGuard<Pages::IndexAllocationMapPage> CreateIndexAllocationMapPage(const std::string& filename, const table_id_t &tableId, const page_id_t &pageId,const extent_id_t &startingExtentId);
  Pages::PageGuard<Pages::IndexAllocationMapPage> GetIndexAllocationMapPage(const std::string& filename, const page_id_t &pageId, const DatabaseEngine::StorageTypes::Table *table);
  Pages::PageGuard<Pages::PageFreeSpacePage> CreatePageFreeSpacePage(const std::string &filename, const page_id_t &pageId);
  Pages::PageGuard<Pages::PageFreeSpacePage> GetPageFreeSpacePage(const std::string& filename, const page_id_t &pageId);
  Pages::PageGuard<Pages::IndexPage> CreateIndexPage(const std::string& filename, const page_id_t &pageId);
  Pages::PageGuard<Pages::IndexPage> GetIndexPage(const std::string& filename, const page_id_t &pageId, const DatabaseEngine::StorageTypes::Table* table);
};

} // namespace Storage
