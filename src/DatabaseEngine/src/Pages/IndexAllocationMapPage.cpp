#include "../../include/Pages/IndexAllocationMapPage.h"
#include "../../../Systemic/include/DataStructures/BitMap.h"
#include "../../include/Pages/GlobalAllocationMapPage.h"
#include "../../include/Database.h"
#include "../../../Systemic/include/Guards/ReaderGuard.h"

#include <cstring>

namespace Pages {
    IndexAllocationMapPage::IndexAllocationMapPage(const table_id_t& tableId, const page_id_t& pageId, const extent_id_t& startingExtentId) : Page(pageId){
        this->additionalHeader.tableId = tableId;
        this->additionalHeader.startingExtentId = startingExtentId;
        this->header.bytesLeft -= (sizeof(table_id_t) + sizeof(extent_id_t));
        this->ownedExtents = new ByteMaps::BitMap(GAM_PAGE_SIZE);
        this->isDirty = true;
        this->header.pageType = PageType::IAM;
        this->header.bytesLeft = 0;
        this->priority = Constants::PagePriority::SYSTEM;
        this->lastAllocatedExtentId = 0;
    }

    IndexAllocationMapPage::IndexAllocationMapPage(const PageHeader& pageHeader, const extent_id_t& startingExtentId, const table_id_t& tableId) : Page(pageHeader){
        this->additionalHeader.tableId = tableId;
        this->additionalHeader.startingExtentId = startingExtentId;
        this->lastAllocatedExtentId = 0;
        this->ownedExtents = new ByteMaps::BitMap();
        this->header.bytesLeft = 0;
        this->priority = Constants::PagePriority::SYSTEM;
    }

    IndexAllocationMapPage::~IndexAllocationMapPage(){
        delete this->ownedExtents;
    }

    extent_id_t IndexAllocationMapPage::SetExtentsAllocated(
        const std::vector<extent_id_t>& extentIds,
        const page_id_t& globalAllocationMapPageId
    )
    {
        for (const auto& extentId : extentIds){
            const extent_id_t bitMapId = extentId - IndexAllocationMapPage::CalculatePageIdOffsetByGamPageId(globalAllocationMapPageId);

            if (bitMapId >= this->ownedExtents->GetSize())
                return extentId;

            this->ownedExtents->Set(bitMapId, true);


            this->lastAllocatedExtentId = bitMapId;
            this->isDirty = true;
        }

        return INVALID_EXTENT_ID;
    }

    void IndexAllocationMapPage::SetDeallocatedExtent(const extent_id_t &extentId)
    {
        this->ownedExtents->Set(extentId, false);
        this->isDirty = true;

        //set the lastAllocated accordingly
    }

    void IndexAllocationMapPage::GetAllocatedExtents(std::vector<extent_id_t>* allocatedExtents) const
    {
        const page_id_t globalAllocationMapPageId = DatabaseEngine::Database::GetGamAssociatedPage(this->header.pageId);
        const page_id_t offSet = IndexAllocationMapPage::CalculatePageIdOffsetByGamPageId(globalAllocationMapPageId);

        for (extent_id_t id = 0; id < this->lastAllocatedExtentId; id++)
            if (this->ownedExtents->Get(id))
               allocatedExtents->push_back(offSet + id);
    }

    void IndexAllocationMapPage::GetAllocatedExtents(std::vector<extent_id_t>* allocatedExtents, const extent_id_t& startingExtentIndex) const
    {
        allocatedExtents->clear();

        const page_id_t globalAllocationMapPageId = DatabaseEngine::Database::GetGamAssociatedPage(this->header.pageId);

        if(startingExtentIndex >= this->ownedExtents->GetSize())
            return;

        MultiThreading::ReaderGuard lock(&this->latch);
        for (extent_id_t id = startingExtentIndex; id < this->lastAllocatedExtentId; id++){
            if (this->ownedExtents->Get(id))
                allocatedExtents->push_back(IndexAllocationMapPage::CalculatePageIdOffsetByGamPageId(globalAllocationMapPageId) + id);
        }
    }

    // extent_id_t IndexAllocationMapPage::GetLastAllocatedExtent() const
    // {
    //     const page_id_t globalAllocationMapPageId = DatabaseEngine::Database::GetGamAssociatedPage(this->header.pageId);
    //
    //     extent_id_t lastAllocatedExtent = 0;
    //     for (extent_id_t id = 0; id < this->ownedExtents->GetSize(); id++)
    //     {
    //         if (this->ownedExtents->Get(id))
    //             lastAllocatedExtent = id;
    //     }
    //
    //     return IndexAllocationMapPage::CalculatePageIdOffsetByGamPageId(globalAllocationMapPageId) + lastAllocatedExtent;
    // }

    void IndexAllocationMapPage::ReadFromDisk(const vector<char> &data, const DatabaseEngine::StorageTypes::Table *table, page_offset_t &offSet,fstream *filePtr){
        this->GetAdditionalHeaderFromFile(data, offSet);
        this->ownedExtents->GetDataFromFile(data, offSet);
    }

    void IndexAllocationMapPage::WriteToDisk(fstream *filePtr)
    {
        this->WritePageHeaderToDisk(filePtr);

        this->WriteAdditionalHeaderToFile(filePtr);

        this->ownedExtents->WriteDataToFile(filePtr);
    }

    void IndexAllocationMapPage::SetNextPageId(const page_id_t &nextPageId) { this->additionalHeader.nextPageId = nextPageId; }

    const page_id_t& IndexAllocationMapPage::GetNextPageId() const { return this->additionalHeader.nextPageId; }

    IndexAllocationPageAdditionalHeader::IndexAllocationPageAdditionalHeader()
    {
        this->tableId = 0;
        this->startingExtentId = 0;
        this->nextPageId = INVALID_PAGE_ID;
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

    void IndexAllocationMapPage::WriteAdditionalHeaderToFile(fstream* filePtr)const
    {
        filePtr->write(reinterpret_cast<const char*>(&this->additionalHeader), sizeof(IndexAllocationPageAdditionalHeader));
    }

    page_id_t IndexAllocationMapPage::CalculatePageIdOffsetByGamPageId(const page_id_t & globalAllocationMapPageId)
    {
        return (globalAllocationMapPageId - 2) * GAM_PAGE_SIZE;
    }
}
