#pragma once
#include <list>
#include <unordered_map>
#include <string>
#include "../../Database/Page/Page.h"
#include  "../../Database/Constants.h"
#include "../FileManager/FileManager.h"

class Page;
class Database;
class MetaDataPage;
class Table;

using namespace std;

typedef list<Page*>::iterator PageIterator;

class PageManager {
    FileManager* fileManager;
    list<Page*> pageList;
    unordered_map<uint16_t, PageIterator> cache;
    const Database* database;
    
protected:
    void RemovePage();
    void OpenPage(const uint16_t& pageId, const Table* table);
    MetaDataPage* OpenMetaDataPage(const string& filename);
    static void SetReadFilePointerToOffset(fstream* file, const streampos& offSet);
    static void SetWriteFilePointerToOffset(fstream* file, const streampos& offSet);

public:
    explicit PageManager(FileManager* fileManager);
    ~PageManager();
    void BindDatabase(const Database* database);
    Page* CreatePage(const uint16_t& pageId);
    Page* GetPage(const uint16_t& pageId, const Table* table);
    MetaDataPage* GetMetaDataPage(const string& filename);
    MetaDataPage* CreateMetaDataPage(const string& filename);
};
