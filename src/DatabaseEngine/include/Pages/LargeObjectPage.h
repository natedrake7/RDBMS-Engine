#pragma once
#include <fstream>
#include <vector>
#include "../DatabaseConstants.h"
#include "Page.h"

using namespace Constants;
using namespace std;

namespace DatabaseEngine::StorageTypes {
    class Table;
}

namespace Pages {
    struct LargeDataObject {
        object_t* object;
        page_size_t objectSize;
        page_id_t nextPageId;

        LargeDataObject();
        ~LargeDataObject();
    };

    class LargeObjectPage final : public Page{
        LargeDataObject data;

    public:
        explicit LargeObjectPage(page_id_t pageId, bool isPageCreation = false);
        explicit LargeObjectPage();
        explicit LargeObjectPage(const PageHeader& pageHeader);
        ~LargeObjectPage() override;
        void ReadFromDisk(const vector<char>& buffer, const DatabaseEngine::StorageTypes::Table* table, page_offset_t& offSet, fstream* filePtr) override;
        void WriteToDisk(fstream* filePtr) override;
        LargeDataObject* InsertObject(const object_t* object, page_size_t size);
        LargeDataObject* GetObject();
        LargeDataObject* DeleteObject();
    };
}
