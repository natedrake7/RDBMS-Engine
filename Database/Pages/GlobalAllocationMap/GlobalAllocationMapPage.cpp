#include "GlobalAllocationMapPage.h"
#include "../../../AdditionalLibraries/BitMap/BitMap.h"

using namespace ByteMaps;
using namespace DatabaseEngine::StorageTypes;

namespace Pages {
    GlobalAllocationMapPage::GlobalAllocationMapPage(const page_id_t& pageId) : Page(pageId)
    {
        this->header.pageType = PageType::GAM;
        this->extentsMap = new BitMap(EXTENT_BIT_MAP_SIZE, 0xFF);
        this->header.bytesLeft = 0;
        this->isDirty = true;
        this->lastAllocatedExtentId = 0;

    }
    GlobalAllocationMapPage::GlobalAllocationMapPage(const PageHeader& pageHeader) : Page(pageHeader)
    {
        this->extentsMap = new BitMap();
        this->lastAllocatedExtentId = 0;
    }

    GlobalAllocationMapPage::~GlobalAllocationMapPage()
    {
        delete this->extentsMap;
    }

    extent_id_t GlobalAllocationMapPage::AllocateExtent()
    {
        for (extent_id_t extentId = lastAllocatedExtentId; extentId < this->extentsMap->GetSize(); extentId++)
        {
            if (this->extentsMap->Get(extentId))
            {
                this->lastAllocatedExtentId = extentId;
                this->extentsMap->Set(extentId, false);
            
                this->isDirty = true;
                return extentId + (this->header.pageId - 2) * GAM_PAGE_SIZE;
            }
        }

        return 0;
    }

    void GlobalAllocationMapPage::DeallocateExtent(const extent_id_t& extentId)
    {
        this->extentsMap->Set(extentId, true);

        this->isDirty = true;
    }

    void GlobalAllocationMapPage::WritePageToFile(fstream *filePtr)
    {
        this->WritePageHeaderToFile(filePtr);
        this->extentsMap->WriteDataToFile(filePtr);
    }

    void GlobalAllocationMapPage::GetPageDataFromFile(const vector<char> &data, const Table *table, page_offset_t &offSet, fstream *filePtr)
    {
        this->extentsMap->GetDataFromFile(data, offSet);
    }

    bool GlobalAllocationMapPage::IsFull() const { return !this->extentsMap->Get(extentsMap->GetSize() - 1); }

    page_size_t GlobalAllocationMapPage::GetAvailableSize() { return PAGE_SIZE - PageHeader::GetPageHeaderSize(); }
}

