#include "IndexAllocationMapPage.h"

IndexAllocationMapPage::IndexAllocationMapPage(const table_id_t& tableId, const page_id_t& pageId) : Page(pageId)
{
    this->additionalHeader.tableId = tableId;
    this->additionalHeader.startingExtentId = pageId / 8;
    this->header.bytesLeft -= (sizeof(table_id_t) + sizeof(extent_id_t));
    this->ownedExtents = new BitMap(EXTENT_BIT_MAP_SIZE);
    this->isDirty = true;
    this->header.pageType = PageType::INDEX;
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

void IndexAllocationMapPage::SetAllocatedExtent(const extent_id_t &extentId)
{
    this->ownedExtents->Set(extentId, true);
    this->isDirty = true;
}

void IndexAllocationMapPage::SetDeallocatedExtent(const extent_id_t &extentId)
{
    this->ownedExtents->Set(extentId, false);
    this->isDirty = true;
}

void IndexAllocationMapPage::GetAllocatedExtents(vector<extent_id_t>* allocatedExtents) const
{
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
    this->GetAdditionalHeaderFromFile(data, offSet);
    this->ownedExtents->GetDataFromFile(data, offSet);
}

void IndexAllocationMapPage::WritePageToFile(fstream *filePtr)
{
    this->WritePageHeaderToFile(filePtr);

    this->WriteAdditionalHeaderToFile(filePtr);

    this->ownedExtents->WriteDataToFile(filePtr);
}

IndexAllocationPageAdditionalHeader::IndexAllocationPageAdditionalHeader()
{
    this->tableId = 0;
    this->startingExtentId = 0;
}

IndexAllocationPageAdditionalHeader::IndexAllocationPageAdditionalHeader(const table_id_t &tableId, const extent_id_t &extentId)
{
    this->tableId = tableId;
    this->startingExtentId = extentId;
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
