#pragma once
#include "../../Constants.h"
#include <condition_variable>
#include <list>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include "../FileManager/FileManager.h"
#include "../../Pages/OverflowPage/OverflowPage.h"

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
class FileManager;
using namespace std;

typedef list<Pages::Page *>::iterator PageIterator;

class StorageManager final{
  list<Pages::Page *> pageList;
  unordered_map<string, PageIterator> cache;
  list<Pages::Page *> systemPageList;
  unordered_map<string, PageIterator> systemCache;
  mutex pageListMutex;
  mutex systemPageListMutex;
  condition_variable systemConditionVariable;
  condition_variable dataConditionVariable;
  int cacheReaders;
  int cacheWriters;
  int dataReaders;
  int dataWriters;
  FileManager fileManager;

protected:
  explicit StorageManager();
  void RemovePage();
  void RemoveSystemPage();
  static void AllocateMemoryBasedOnSystemPageType(Pages::Page **page, const Pages::PageHeader &pageHeader);
  static bool AllocateMemoryBasedOnPageType(Pages::Page **page, const Pages::PageHeader &pageHeader);
  void OpenExtent(const string& filename, const Constants::extent_id_t &extentId, const DatabaseEngine::StorageTypes::Table *table);
  void OpenSystemExtent(const string& filename, const Constants::extent_id_t &extentId, const DatabaseEngine::StorageTypes::Table* table);
  void OpenSystemPage(const string &filename, const Constants::page_id_t &pageId);
  Pages::Page *GetSystemPage(const string& filename, const Constants::page_id_t &pageId);
  Pages::Page *GetSystemPage(const Constants::page_id_t &pageId, const string &filename);
  Pages::Page *GetSystemPage(const string &filename, const Constants::page_id_t &pageId, const Constants::extent_id_t &extentId, const DatabaseEngine::StorageTypes::Table *table = nullptr);
  static void SetReadFilePointerToOffset(fstream *file, const streampos &offSet);
  static void SetWriteFilePointerToOffset(fstream *file, const streampos &offSet);
  static Pages::PageHeader GetPageHeaderFromFile(const vector<char> &data, Constants::page_offset_t &offSet);
  bool IsPageCached(const string& filename, const Constants::page_id_t &pageId);
  void MovePageToFrontOfSystemList(Pages::Page *page, const Constants::page_id_t &pageId, const string &filename);
  void MovePageToFrontOfList(Pages::Page *page, const Constants::page_id_t &pageId, const string &filename);
  bool IsSystemCacheFull() const;
  void LockSystemPageRead();
  void UnlockSystemPageRead();
  void LockSystemPageWrite();
  void UnlockSystemPageWrite();
  void LockPageRead();
  void UnlockPageRead();
  void LockPageWrite();
  void UnlockPageWrite();
  unordered_map<string, PageIterator>::iterator
  SearchSystemPageInCache(const string& key);
  void MoveSystemPageToStart(const PageIterator &pageIterator);

public:
  static StorageManager& Get();
  ~StorageManager();
  void CreateFile(const string& fileName, const string& extension);
  Pages::Page *CreatePage(const string& filename, const Constants::page_id_t &pageId);
  Pages::Page *GetPage(const string& filename, const Constants::page_id_t &pageId, const Constants::extent_id_t &extentId, const DatabaseEngine::StorageTypes::Table *table);
  Pages::HeaderPage *GetHeaderPage(const string &filename);
  Pages::HeaderPage *CreateHeaderPage(const string &filename);
  Pages::LargeObjectPage *CreateLargeDataPage(const string& filename, const Constants::page_id_t &pageId);
  Pages::LargeObjectPage * GetLargeDataPage(const string& filename, const Constants::page_id_t &pageId, const Constants::extent_id_t &extentId, const DatabaseEngine::StorageTypes::Table *table);
  Pages::OverflowPage *CreateOverflowPage(const string& filename, const Constants::page_id_t &pageId);
  Pages::OverflowPage *GetOverflowPage(const string& filename, const page_id_t &pageId, const extent_id_t &extentId, const DatabaseEngine::StorageTypes::Table *table);
  Pages::GlobalAllocationMapPage *CreateGlobalAllocationMapPage(const string &filename, const Constants::page_id_t &pageId);
  Pages::GlobalAllocationMapPage *GetGlobalAllocationMapPage(const string& filename, const Constants::page_id_t &pageId);
  Pages::IndexAllocationMapPage *CreateIndexAllocationMapPage(const string& filename, const Constants::table_id_t &tableId, const Constants::page_id_t &pageId,const Constants::extent_id_t &startingExtentId);
  Pages::IndexAllocationMapPage *GetIndexAllocationMapPage(const string& filename, const Constants::page_id_t &pageId, const Constants::extent_id_t &extentId, const DatabaseEngine::StorageTypes::Table *table);
  Pages::PageFreeSpacePage *CreatePageFreeSpacePage(const string &filename, const Constants::page_id_t &pageId);
  Pages::PageFreeSpacePage * GetPageFreeSpacePage(const string& filename, const Constants::page_id_t &pageId);
  Pages::IndexPage *CreateIndexPage(const string& filename, const Constants::page_id_t &pageId);
  Pages::IndexPage *GetIndexPage(const string& filename, const Constants::page_id_t &pageId, const Constants::extent_id_t &extentId, const DatabaseEngine::StorageTypes::Table* table);
  [[nodiscard]] bool IsCacheFull() const;
};

} // namespace Storage
