#pragma once
#include <fstream>
#include <vector>
#include "../../Constants.h"
#include "../Page.h"

using namespace Constants;
using namespace std;

namespace DatabaseEngine::StorageTypes {
    class Table;
}

namespace Pages {
    typedef struct DataObject {
        object_t* object;
        page_size_t objectSize;
        page_id_t nextPageId;

        DataObject();
        ~DataObject();
    }DataObject;

    typedef struct DataObjectPointer
    {
        page_id_t pageId;

        DataObjectPointer();
        explicit DataObjectPointer(const page_id_t& pageId);
        ~DataObjectPointer();

    }DataObjectPointer;
    
    class LargeDataPage final : public Page
    {
        DataObject* data;

    public:
        explicit LargeDataPage(const page_id_t& pageId, const bool& isPageCreation = false);
        explicit LargeDataPage();
        explicit LargeDataPage(const PageHeader& pageHeader);
        ~LargeDataPage() override;
        void GetPageDataFromFile(const vector<char>& data, const DatabaseEngine::StorageTypes::Table* table, page_offset_t& offSet, fstream* filePtr) override;
        void WritePageToFile(fstream* filePtr) override;
        DataObject* InsertObject(const object_t* object, const page_size_t& size);
        DataObject* GetObject();
        DataObject* DeleteObject();
    };
}
