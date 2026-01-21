#include "../../include/Pages/GlobalAllocationMapPage.h"
#include "../../../Systemic/include/DataStructures/BitMap.h"
#include "../../../Systemic/include/Guards/ReaderGuard.h"
#include "../../include/Pages/IndexAllocationMapPage.h"
#include "Guards/WriterGuard.h"

namespace Pages {
    GlobalAllocationMapPage::GlobalAllocationMapPage(const page_id_t pageId) : Page(pageId)
    {
        this->header.type = Constants::PageType::GAM;
        this->extentsMap = new ByteMaps::BitMap(Constants::EXTENT_BIT_MAP_SIZE, 0xFF);
        this->header.bytesLeft = 0;
        this->isDirty = true;
        this->lastAllocatedExtentId = 0;
        this->priority = Constants::PagePriority::SYSTEM;

    }
    GlobalAllocationMapPage::GlobalAllocationMapPage(const PageHeader& pageHeader) : Page(pageHeader){
        this->extentsMap = new ByteMaps::BitMap();
        this->lastAllocatedExtentId = 0;
        this->priority = Constants::PagePriority::SYSTEM;
    }

    GlobalAllocationMapPage::~GlobalAllocationMapPage()
    {
        delete this->extentsMap;
    }

    int GlobalAllocationMapPage::AllocateExtentsNoLock(std::vector<extent_id_t>& extents, const Int numberOfExtents){
        int allocatedExtents = 0;

        for (extent_id_t extentId = this->lastAllocatedExtentId; extentId < this->extentsMap->GetSize(); extentId++){
            if (allocatedExtents == numberOfExtents)
                break;

            if (!this->extentsMap ->Get(extentId))
                continue;

            this->lastAllocatedExtentId = extentId;
            this->extentsMap->Set(extentId, false);
            this->isDirty = true;

            allocatedExtents++;

            extents.push_back(IndexAllocationMapPage::CalculatePageIdOffsetByGamPageId(this->header.pageId) + extentId);
        }

        return allocatedExtents;
    }

    void GlobalAllocationMapPage::DeallocateExtent(const extent_id_t extentId){
        this->extentsMap->Set(extentId, true);

        this->isDirty = true;
    }

    void GlobalAllocationMapPage::WriteToDisk(std::fstream *filePtr)
    {
        this->WritePageHeaderToDisk(filePtr);
        this->extentsMap->WriteDataToFile(filePtr);
    }

    void GlobalAllocationMapPage::ReadFromDisk(
        const std::vector<char> &data,
        const DatabaseEngine::StorageTypes::Table *table,
        page_offset_t &offSet,
        std::fstream *filePtr
    ){
        this->extentsMap->GetDataFromFile(data, offSet);
    }

    bool GlobalAllocationMapPage::IsFull() const {
        MultiThreading::ReaderGuard lock(&this->latch);

        return !this->extentsMap->Get(extentsMap->GetSize() - 1);
    }
//Locks Latch
    std::vector<extent_id_t> GlobalAllocationMapPage::GetAllocatedExtents(const extent_id_t startingIndex) const {
        std::vector<extent_id_t> allocatedExtents;

        if(startingIndex >= this->extentsMap->GetSize())
            return allocatedExtents;

        MultiThreading::ReaderGuard lock(&this->latch);

        allocatedExtents.reserve(this->extentsMap->GetSize() - startingIndex);

        for (extent_id_t id = startingIndex; id < this->extentsMap->GetSize(); id++){
            if (!this->extentsMap->Get(id))
                allocatedExtents.push_back(IndexAllocationMapPage::CalculatePageIdOffsetByGamPageId(this->header.pageId) + id);
        }

        return allocatedExtents;
    }
}

