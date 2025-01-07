#include "IndexAllocationMapPage.h"
#include "../../../AdditionalLibraries/BitMap/BitMap.h"
#include "../GlobalAllocationMap/GlobalAllocationMapPage.h"
#include "../../Database.h"

using namespace ByteMaps;
using namespace DatabaseEngine::StorageTypes;
using namespace DatabaseEngine;

namespace Pages {
    IndexAllocationMapPage::IndexAllocationMapPage(const table_id_t& tableId, const page_id_t& pageId, const extent_id_t& startingExtentId) : Page(pageId)
    {
        this->additionalHeader.tableId = tableId;
        this->additionalHeader.startingExtentId = startingExtentId;
        this->header.bytesLeft -= (sizeof(table_id_t) + sizeof(extent_id_t));
        this->ownedExtents = new BitMap(GAM_PAGE_SIZE);
        this->isDirty = true;
        this->header.pageType = PageType::IAM;
        this->header.bytesLeft = 0;
    }

    IndexAllocationMapPage::IndexAllocationMapPage(const PageHeader& pageHeader, const extent_id_t& startingExtentId, const table_id_t& tableId) : Page(pageHeader)
    {
        this->additionalHeader.tableId = tableId;
        this->additionalHeader.startingExtentId = startingExtentId;
        this->ownedExtents = new BitMap();
        this->header.bytesLeft = 0;
    }

    IndexAllocationMapPage::~IndexAllocationMapPage()
    {
        delete this->ownedExtents;
    }

    void IndexAllocationMapPage::SetAllocatedExtent(const extent_id_t &extentId, const GlobalAllocationMapPage* globalAllocationMapPage)
    {
        const extent_id_t bitMapId = extentId - ((globalAllocationMapPage->GetPageId() - 2) * GAM_PAGE_SIZE );
        this->ownedExtents->Set(bitMapId, true);
        this->isDirty = true;
    }

    void IndexAllocationMapPage::SetDeallocatedExtent(const extent_id_t &extentId)
    {
        this->ownedExtents->Set(extentId, false);
        this->isDirty = true;
    }

    void IndexAllocationMapPage::GetAllocatedExtents(vector<extent_id_t>* allocatedExtents) const
    {
        const page_id_t globalAllocationMapPageId = Database::GetGamAssociatedPage(this->header.pageId);

        for (extent_id_t id = 0; id < this->ownedExtents->GetSize(); id++)
        {
            if (this->ownedExtents->Get(id))
                allocatedExtents->push_back(IndexAllocationMapPage::CalculatePageIdOffsetByGamPageId(globalAllocationMapPageId) + id);
        }
    }

    void IndexAllocationMapPage::GetAllocatedExtents(vector<extent_id_t>* allocatedExtents, const extent_id_t& startingExtentIndex) const
    {
        allocatedExtents->clear();

        const page_id_t globalAllocationMapPageId = Database::GetGamAssociatedPage(this->header.pageId);

        if(startingExtentIndex >= this->ownedExtents->GetSize())
            return;
        
        for (extent_id_t id = startingExtentIndex; id < this->ownedExtents->GetSize(); id++)
        {
            if (this->ownedExtents->Get(id))
                allocatedExtents->push_back(IndexAllocationMapPage::CalculatePageIdOffsetByGamPageId(globalAllocationMapPageId) + id);
        }
    }

    extent_id_t IndexAllocationMapPage::GetLastAllocatedExtent() const
    {
        const page_id_t globalAllocationMapPageId = Database::GetGamAssociatedPage(this->header.pageId);

        extent_id_t lastAllocatedExtent = 0;
        for (extent_id_t id = 0; id < this->ownedExtents->GetSize(); id++)
        {
            if (this->ownedExtents->Get(id))
                lastAllocatedExtent = id;
        }

        return (IndexAllocationMapPage::CalculatePageIdOffsetByGamPageId(globalAllocationMapPageId) + lastAllocatedExtent);
    }

    void IndexAllocationMapPage::GetPageDataFromFile(const vector<char> &data, const Table *table, page_offset_t &offSet,fstream *filePtr)
    {
        this->GetAdditionalHeaderFromFile(data, offSet);
        this->ownedExtents->GetDataFromFile(data, offSet);
    }

    void IndexAllocationMapPage::WritePageToFile(fstream *filePtr)
    {
        this->WritePageHeaderToFile(filePtr);

        this->WriteAdditionalHeaderToFile(filePtr);

        this->ownedExtents->WriteDataToFile(filePtr);
    }

    void IndexAllocationMapPage::SetNextPageId(const page_id_t &nextPageId) { this->additionalHeader.nextPageId = nextPageId; }

    IndexAllocationPageAdditionalHeader::IndexAllocationPageAdditionalHeader()
    {
        this->tableId = 0;
        this->startingExtentId = 0;
        this->nextPageId = 0;
    }

    IndexAllocationPageAdditionalHeader::IndexAllocationPageAdditionalHeader(const table_id_t &tableId, const extent_id_t &extentId, const page_id_t& nextPageId)
    {
        this->tableId = tableId;
        this->startingExtentId = extentId;
        this->nextPageId = nextPageId;
    }

    IndexAllocationPageAdditionalHeader::~IndexAllocationPageAdditionalHeader() = default;

    void IndexAllocationMapPage::GetAdditionalHeaderFromFile(const vector<char> &data, page_offset_t &offSet)
    {
        memcpy(&this->additionalHeader, data.data() + offSet, sizeof(IndexAllocationPageAdditionalHeader));
        offSet += sizeof(IndexAllocationPageAdditionalHeader);
    }

    void IndexAllocationMapPage::WriteAdditionalHeaderToFile(fstream* filePtr)
    {
        filePtr->write(reinterpret_cast<const char*>(&this->additionalHeader), sizeof(IndexAllocationPageAdditionalHeader));
    }

    page_id_t IndexAllocationMapPage::CalculatePageIdOffsetByGamPageId(const page_id_t & globalAllocationMapPageId)
    {
        return (globalAllocationMapPageId - 2) * GAM_PAGE_SIZE;
    }
}
