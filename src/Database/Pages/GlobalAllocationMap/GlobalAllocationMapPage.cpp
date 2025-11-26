#include "GlobalAllocationMapPage.h"
#include "../../../Systemic/DataStructures/BitMap/BitMap.h"
#include "../../../Systemic/MultiThreading/Guards/ReaderGuard/ReaderGuard.h"
#include "../IndexMapAllocation/IndexAllocationMapPage.h"

namespace Pages {
    GlobalAllocationMapPage::GlobalAllocationMapPage(const page_id_t& pageId) : Page(pageId)
    {
        this->header.pageType = Constants::PageType::GAM;
        this->extentsMap = new ByteMaps::BitMap(Constants::EXTENT_BIT_MAP_SIZE, 0xFF);
        this->header.bytesLeft = 0;
        this->isDirty = true;
        this->lastAllocatedExtentId = 0;
        this->priority = Constants::PagePriority::SYSTEM;

    }
    GlobalAllocationMapPage::GlobalAllocationMapPage(const PageHeader& pageHeader) : Page(pageHeader)
    {
        this->extentsMap = new ByteMaps::BitMap();
        this->lastAllocatedExtentId = 0;
        this->priority = Constants::PagePriority::SYSTEM;
    }

    GlobalAllocationMapPage::~GlobalAllocationMapPage()
    {
        delete this->extentsMap;
    }

    extent_id_t GlobalAllocationMapPage::AllocateExtent()
    {
        for (extent_id_t extentId = this->lastAllocatedExtentId; extentId < this->extentsMap->GetSize(); extentId++)
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

    void GlobalAllocationMapPage::WriteToDisk(fstream *filePtr)
    {
        this->WritePageHeaderToDisk(filePtr);
        this->extentsMap->WriteDataToFile(filePtr);
    }

    void GlobalAllocationMapPage::ReadFromDisk(const vector<char> &data, const DatabaseEngine::StorageTypes::Table *table, page_offset_t &offSet, fstream *filePtr)
    {
        this->extentsMap->GetDataFromFile(data, offSet);
    }

    bool GlobalAllocationMapPage::IsFull() const {
        MultiThreading::ReaderGuard lock(&this->latch);

        return !this->extentsMap->Get(extentsMap->GetSize() - 1);
    }

//Locks Latch
    std::vector<extent_id_t> GlobalAllocationMapPage::GetAllocatedExtents(const extent_id_t& startingIndex) const {
        std::vector<extent_id_t> allocatedExtents;

        if(startingIndex >= this->extentsMap->GetSize())
            return allocatedExtents;

        MultiThreading::ReaderGuard lock(&this->latch);

        allocatedExtents.reserve(this->extentsMap->GetSize() - startingIndex);

        for (extent_id_t id = startingIndex; id < this->extentsMap->GetSize(); id++)
        {
            if (!this->extentsMap->Get(id))
                allocatedExtents.push_back(IndexAllocationMapPage::CalculatePageIdOffsetByGamPageId(this->header.pageId) + id);
        }

        return allocatedExtents;
    }
}

