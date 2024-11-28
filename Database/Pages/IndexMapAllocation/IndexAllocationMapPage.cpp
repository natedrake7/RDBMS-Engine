#include "IndexAllocationMapPage.h"

IndexAllocationMapPage::IndexAllocationMapPage(const table_id_t& tableId, const page_id_t& pageId) : Page(pageId)
{
    this->tableId = tableId;
    this->startingExtentId = pageId / 8;
    this->metadata.bytesLeft -= (sizeof(table_id_t) + sizeof(extent_id_t));
    this->ownedExtents = new BitMap(this->metadata.bytesLeft * 8);
    this->isDirty = true;
    this->metadata.pageType = PageType::INDEX;
    this->metadata.bytesLeft = 0;
}

IndexAllocationMapPage::IndexAllocationMapPage(const PageMetadata &pageMetaData, const extent_id_t& startingExtentId, const table_id_t& tableId) : Page(pageMetaData)
{
    this->tableId = tableId;
    this->startingExtentId = startingExtentId;
    this->ownedExtents = new BitMap();
    this->metadata.bytesLeft = 0;
}

IndexAllocationMapPage::~IndexAllocationMapPage()
{
    delete this->ownedExtents;
}

void IndexAllocationMapPage::SetAllocatedExtent(const extent_id_t &extentId) { this->ownedExtents->Set(extentId, true); }

void IndexAllocationMapPage::SetDeallocatedExtent(const extent_id_t &extentId) { this->ownedExtents->Set(extentId, false); }

void IndexAllocationMapPage::GetAllocatedExtents(vector<extent_id_t>* allocatedExtents) const
{
    this->ownedExtents->Print();
    for (extent_id_t id = 0; id < this->ownedExtents->GetSize(); id++)
    {
        if (this->ownedExtents->Get(id))
            allocatedExtents->push_back(id);
    }
}

extent_id_t IndexAllocationMapPage::GetLastAllocatedExtent() const
{
    extent_id_t lastAllocatedExtent = 0;
    for (extent_id_t id = 0; id < this->ownedExtents->GetSize(); id++)
    {
        if (this->ownedExtents->Get(id))
            lastAllocatedExtent = id;
    }

    return lastAllocatedExtent;
}

void IndexAllocationMapPage::GetPageDataFromFile(const vector<char> &data, const Table *table, page_offset_t &offSet,fstream *filePtr)
{
    this->ownedExtents->GetDataFromFile(data, offSet);
}

void IndexAllocationMapPage::WritePageToFile(fstream *filePtr)
{
    this->WritePageMetaDataToFile(filePtr);

    this->ownedExtents->WriteDataToFile(filePtr);
}
