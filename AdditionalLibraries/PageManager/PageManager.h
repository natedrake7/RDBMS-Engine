#pragma once
#include <list>
#include <unordered_map>
#include <string>
#include "../../Database/Page/Page.h"
#include "../FileManager/FileManager.h"

constexpr size_t MAX_NUMBER_OF_PAGES = 100;

class Page;
using namespace std;

typedef list<Page*>::iterator PageIterator;

class PageManager {
    FileManager* fileManager;
    list<Page*> pageList;
    unordered_map<int, PageIterator> cache;
    
protected:
    void RemovePage();
    void OpenPage(const int& pageId, const string& filename);
    
public:
    explicit PageManager(FileManager* fileManager);
    ~PageManager();
    Page* CreatePage(const int& pageId);
    Page* GetPage(const int& pageId, const string& filename);
};
