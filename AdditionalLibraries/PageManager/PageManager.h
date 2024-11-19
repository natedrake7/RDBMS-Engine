#pragma once
#include <list>
#include <unordered_map>
#include <string>
#include "../../Database/Page/Page.h"
#include "../FileManager/FileManager.h"

constexpr size_t MAX_NUMBER_OF_PAGES = 100;

class Page;
class Database;
class MetaDataPage;
class Table;

using namespace std;

typedef list<Page*>::iterator PageIterator;

class PageManager {
    FileManager* fileManager;
    list<Page*> pageList;
    unordered_map<int, PageIterator> cache;
    const Database* database;
    
protected:
    void RemovePage();
    Page* OpenPage(const int& pageId, const Table* table);
    MetaDataPage* OpenMetaDataPage(const string& filename);
    static void SetReadFilePointerToOffset(fstream* file, const streampos& offSet);
    static void SetWriteFilePointerToOffset(fstream* file, const streampos& offSet);

public:
    explicit PageManager(FileManager* fileManager);
    ~PageManager();
    void BindDatabase(const Database* database);
    Page* CreatePage(const int& pageId);
    Page* GetPage(const int& pageId, const Table* table);
    MetaDataPage* GetMetaDataPage(const string& filename);
    MetaDataPage* CreateMetaDataPage(const string& filename);
};
