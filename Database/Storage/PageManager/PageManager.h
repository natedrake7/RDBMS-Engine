#pragma once
#include <condition_variable>
#include <list>
#include <mutex>
#include <unordered_map>
#include <string>
#include <vector>
#include "../../Constants.h"


class Table;

namespace DatabaseEngine {
    class Database;
}

namespace Pages {
    class Page;
    class IndexPage;
    class PageFreeSpacePage;
    class IndexAllocationMapPage;
    class GlobalAllocationMapPage;
    class LargeDataPage;
    class HeaderPage;
    struct PageHeader;
}

namespace Storage{
    class FileManager;
    using namespace std;

    typedef list<Pages::Page*>::iterator PageIterator;

    class PageManager {
        FileManager* fileManager;
        list<Pages::Page*> pageList;
        unordered_map<Constants::page_id_t, PageIterator> cache;
        list<Pages::Page*> systemPageList;
        unordered_map<Constants::page_id_t, PageIterator> systemCache;
        const DatabaseEngine::Database* database;
        mutex pageListMutex;
        mutex systemPageListMutex;
        condition_variable systemConditionVariable;
        condition_variable dataConditionVariable;
        int cacheReaders;
        int cacheWriters;
        int dataReaders;
        int dataWriters;
        
        protected:
            void RemovePage();
            void RemoveSystemPage();
            static void AllocateMemoryBasedOnSystemPageType(Pages::Page** page, const Pages::PageHeader& pageHeader);
            static void AllocateMemoryBasedOnPageType(Pages::Page **page, const Pages::PageHeader &pageHeader);
            void OpenExtent(const Constants::extent_id_t& extentId, const Table* table);
            void OpenSystemPage(const Constants::page_id_t& pageId);
            void OpenSystemPage(const Constants::page_id_t &pageId, const string &filename);
            Pages::Page* GetSystemPage(const Constants::page_id_t& pageId);
            Pages::Page* GetSystemPage(const Constants::page_id_t &pageId, const string& filename);
            static void SetReadFilePointerToOffset(fstream* file, const streampos& offSet);
            static void SetWriteFilePointerToOffset(fstream* file, const streampos& offSet);
            static Pages::PageHeader GetPageHeaderFromFile(const vector<char> &data, Constants::page_offset_t &offSet);
            bool IsPageCached(const Constants::page_id_t& pageId);
            void MovePageToFrontOfSystemList(Pages::Page *page, const Constants::page_id_t& pageId, const string& filename);
            void MovePageToFrontOfSystemList(Pages::Page *page, const Constants::page_id_t& pageId);
            void MovePageToFrontOfList(Pages::Page *page, const Constants::page_id_t& pageId, const string& filename);
            void MovePageToFrontOfList(Pages::Page *page, const Constants::page_id_t &pageId);
            void ReadSystemPageFromFile(Pages::Page* page, const vector<char>& buffer, const Constants::page_id_t& pageId, Constants::page_offset_t& offSet, fstream* file);
            void ReadPageFromFile(Pages::Page *page, const vector<char> &buffer, const Table *table, Constants::page_offset_t &offSet, fstream *file);
            bool IsSystemCacheFull() const;
            void LockSystemPageRead();
            void UnlockSystemPageRead();
            void LockSystemPageWrite();
            void UnlockSystemPageWrite();
            void LockPageRead();
            void UnlockPageRead();
            void LockPageWrite();
            void UnlockPageWrite();
            unordered_map<Constants::page_id_t, PageIterator>::iterator SearchSystemPageInCache(const Constants::page_id_t& pageId);
            void MoveSystemPageToStart(const PageIterator& pageIterator);

        public:
            explicit PageManager(FileManager* fileManager);
            ~PageManager();
            void BindDatabase(const DatabaseEngine::Database* database);
            Pages::Page* CreatePage(const Constants::page_id_t& pageId);
            Pages::Page* GetPage(const Constants::page_id_t& pageId, const Constants::extent_id_t& extentId, const Table* table);
            Pages::HeaderPage* GetHeaderPage(const string& filename);
            Pages::HeaderPage* CreateHeaderPage(const string& filename);
            Pages::LargeDataPage* CreateLargeDataPage(const Constants::page_id_t& pageId);
            Pages::LargeDataPage* GetLargeDataPage(const Constants::page_id_t &pageId,const Constants::extent_id_t& extentId, const Table *table);
            Pages::GlobalAllocationMapPage* CreateGlobalAllocationMapPage(const string &filename, const Constants::page_id_t& pageId);
            Pages::GlobalAllocationMapPage* CreateGlobalAllocationMapPage(const Constants::page_id_t& pageId);
            Pages::GlobalAllocationMapPage* GetGlobalAllocationMapPage(const Constants::page_id_t& pageId);
            Pages::IndexAllocationMapPage* CreateIndexAllocationMapPage(const Constants::table_id_t& tableId, const Constants::page_id_t& pageId, const Constants::extent_id_t& startingExtentId);
            Pages::IndexAllocationMapPage* GetIndexAllocationMapPage(const Constants::page_id_t& pageId);
            Pages::PageFreeSpacePage* CreatePageFreeSpacePage(const string &filename, const Constants::page_id_t& pageId);
            Pages::PageFreeSpacePage* CreatePageFreeSpacePage(const Constants::page_id_t& pageId);
            Pages::PageFreeSpacePage* GetPageFreeSpacePage(const Constants::page_id_t& pageId);
            Pages::IndexPage* CreateIndexPage(const Constants::page_id_t& pageId);
            Pages::IndexPage* GetIndexPage(const Constants::page_id_t &pageId);
            bool IsCacheFull() const;
    };
}
