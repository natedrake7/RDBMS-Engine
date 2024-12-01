#pragma once
#include <list>
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

class IndexAllocationMapPage;
class GlobalAllocationMapPage;
class PageFreeSpacePage;
class LargeDataPage;
class HeaderPage;
class Page;
class Database;
class Table;

using namespace std;

typedef list<Page*>::iterator PageIterator;

class PageManager {
    FileManager* fileManager;
    list<Page*> pageList;
    unordered_map<page_id_t, PageIterator> cache;
    list<Page*> systemPageList;
    unordered_map<page_id_t, PageIterator> systemCache;
    const Database* database;
    
    protected:
        void RemovePage();
        void RemoveSystemPage();
        void OpenPage(const page_id_t& pageId, const extent_id_t& extentId, const Table* table);
        HeaderPage* OpenHeaderPage(const string& filename);
        GlobalAllocationMapPage* OpenGlobalAllocationMapPage(const string& filename);
        IndexAllocationMapPage* OpenIndexAllocationMapPage(const page_id_t& pageId);
        PageFreeSpacePage* OpenPageFreeSpacePage(const page_id_t& pageId);
        static void SetReadFilePointerToOffset(fstream* file, const streampos& offSet);
        static void SetWriteFilePointerToOffset(fstream* file, const streampos& offSet);
        static PageHeader GetPageHeaderFromFile(const vector<char> &data, page_offset_t &offSet);

    public:
        explicit PageManager(FileManager* fileManager);
        ~PageManager();
        void BindDatabase(const Database* database);
        Page* CreatePage(const page_id_t& pageId);
        Page* GetPage(const page_id_t& pageId, const extent_id_t& extendId, const Table* table);
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
};