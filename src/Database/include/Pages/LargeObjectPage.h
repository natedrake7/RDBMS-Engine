#pragma once
#include <fstream>
#include <vector>
#include "../Constants.h"
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

    struct DataObjectPointer
    {
        page_id_t pageId;

        DataObjectPointer();
        explicit DataObjectPointer(const page_id_t& pageId);
        ~DataObjectPointer();

    };
    
    class LargeObjectPage final : public Page
    {
        LargeDataObject* data;

    public:
        explicit LargeObjectPage(const page_id_t& pageId, const bool& isPageCreation = false);
        explicit LargeObjectPage();
        explicit LargeObjectPage(const PageHeader& pageHeader);
        ~LargeObjectPage() override;
        void ReadFromDisk(const vector<char>& data, const DatabaseEngine::StorageTypes::Table* table, page_offset_t& offSet, fstream* filePtr) override;
        void WriteToDisk(fstream* filePtr) override;
        LargeDataObject* InsertObject(const object_t* object, const page_size_t& size);
        LargeDataObject* GetObject()const;
        LargeDataObject* DeleteObject();
    };
}
