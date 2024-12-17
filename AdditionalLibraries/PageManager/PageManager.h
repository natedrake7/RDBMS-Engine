#pragma once
#include <condition_variable>
#include <list>
#include <mutex>
#include <unordered_map>
#include <string>
#include "../../Database/Pages/Page.h"
#include "../../Database/Pages/Header/HeaderPage.h"
#include "../../Database/Pages/LargeObject/LargeDataPage.h"
#include  "../../Database/Constants.h"
#include "../../Database/Pages/GlobalAllocationMap/GlobalAllocationMapPage.h"
#include "../../Database/Pages/IndexMapAllocation/IndexAllocationMapPage.h"
#include "../../Database/Pages/PageFreeSpace/PageFreeSpacePage.h"
#include "../FileManager/FileManager.h"
#include "../../Database/Pages/IndexPage/IndexPage.h"

class IndexAllocationMapPage;
class GlobalAllocationMapPage;
class PageFreeSpacePage;
class LargeDataPage;
class HeaderPage;
class Page;
class Database;
class Table;
class IndexPage;

using namespace std;

typedef list<Page*>::iterator PageIterator;

class PageManager {
    FileManager* fileManager;
    list<Page*> pageList;
    unordered_map<page_id_t, PageIterator> cache;
    list<Page*> systemPageList;
    unordered_map<page_id_t, PageIterator> systemCache;
    const Database* database;
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
        static void AllocateMemoryBasedOnSystemPageType(Page** page, const PageHeader& pageHeader);
        static void AllocateMemoryBasedOnPageType(Page **page, const PageHeader &pageHeader);
        void OpenExtent(const extent_id_t& extentId, const Table* table);
        void OpenSystemPage(const page_id_t& pageId);
        void OpenSystemPage(const page_id_t &pageId, const string &filename);
        Page* GetSystemPage(const page_id_t& pageId);
        Page* GetSystemPage(const page_id_t &pageId, const string& filename);
        static void SetReadFilePointerToOffset(fstream* file, const streampos& offSet);
        static void SetWriteFilePointerToOffset(fstream* file, const streampos& offSet);
        static PageHeader GetPageHeaderFromFile(const vector<char> &data, page_offset_t &offSet);
        bool IsPageCached(const page_id_t& pageId);
        void MovePageToFrontOfSystemList(Page *page, const page_id_t& pageId, const string& filename);
        void MovePageToFrontOfSystemList(Page *page, const page_id_t& pageId);
        void MovePageToFrontOfList(Page *page, const page_id_t& pageId, const string& filename);
        void MovePageToFrontOfList(Page *page, const page_id_t &pageId);
        void ReadSystemPageFromFile(Page* page, const vector<char>& buffer, const page_id_t& pageId, page_offset_t& offSet, fstream* file);
        void ReadPageFromFile(Page *page, const vector<char> &buffer, const Table *table, page_offset_t &offSet, fstream *file);
        bool IsSystemCacheFull() const;
        void LockSystemPageRead();
        void UnlockSystemPageRead();
        void LockSystemPageWrite();
        void UnlockSystemPageWrite();
        void LockPageRead();
        void UnlockPageRead();
        void LockPageWrite();
        void UnlockPageWrite();
        unordered_map<page_id_t, PageIterator>::iterator SearchSystemPageInCache(const page_id_t& pageId);
        void MoveSystemPageToStart(const PageIterator& pageIterator);

    public:
        explicit PageManager(FileManager* fileManager);
        ~PageManager();
        void BindDatabase(const Database* database);
        Page* CreatePage(const page_id_t& pageId);
        Page* GetPage(const page_id_t& pageId, const extent_id_t& extentId, const Table* table);
        HeaderPage* GetHeaderPage(const string& filename);
        HeaderPage* CreateHeaderPage(const string& filename);
        LargeDataPage* CreateLargeDataPage(const page_id_t& pageId);
        LargeDataPage* GetLargeDataPage(const page_id_t &pageId,const extent_id_t& extentId, const Table *table);
        GlobalAllocationMapPage* CreateGlobalAllocationMapPage(const string &filename, const page_id_t& pageId);
        GlobalAllocationMapPage* CreateGlobalAllocationMapPage(const page_id_t& pageId);
        GlobalAllocationMapPage* GetGlobalAllocationMapPage(const page_id_t& pageId);
        IndexAllocationMapPage* CreateIndexAllocationMapPage(const table_id_t& tableId, const page_id_t& pageId, const extent_id_t& startingExtentId);
        IndexAllocationMapPage* GetIndexAllocationMapPage(const page_id_t& pageId);
        PageFreeSpacePage* CreatePageFreeSpacePage(const string &filename, const page_id_t& pageId);
        PageFreeSpacePage* CreatePageFreeSpacePage(const page_id_t& pageId);
        PageFreeSpacePage* GetPageFreeSpacePage(const page_id_t& pageId);
        IndexPage* CreateIndexPage(const page_id_t& pageId);
        IndexPage* GetIndexPage(const page_id_t &pageId);
        bool IsCacheFull() const;
};