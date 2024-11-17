#pragma once
#include <vector>
#include "../Row/Row.h"

class Row;
using namespace std;

constexpr size_t PAGE_SIZE = 4096;

typedef struct PageMetadata{
    int pageId;
    int pageSize;
    int nextPageId;
    size_t bytesLeft;

    PageMetadata();
    ~PageMetadata();
}PageMetadata;

class Page {
    PageMetadata metadata;
    bool isDirty;
    vector<Row*> rows;

    public:
        explicit Page(const int& pageId);
        ~Page();
        void InsertRow(Row* row);
        void DeleteRow(Row* row);
        void UpdateRow(Row* row);
        void SetPageDataFromFile(const vector<char>& data);
        const int& GetPageId() const;
        const bool& GetPageDirtyStatus() const;
};



