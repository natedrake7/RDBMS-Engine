#include "IndexAllocationMapPage.h"

ExtentAvailability::ExtentAvailability()
{
    this->freeExtents = nullptr;
}

ExtentAvailability::~ExtentAvailability()
{
    delete this->freeExtents;    
}

IndexAllocationMapPage::IndexAllocationMapPage(const page_id_t &pageId) : Page(pageId) { }

IndexAllocationMapPage::IndexAllocationMapPage(const PageMetaData &pageMetaData) : Page(pageMetaData) { }

IndexAllocationMapPage::~IndexAllocationMapPage() 
{
    for (const auto& pair: this->allocationMap)
        delete pair.second;
}

void IndexAllocationMapPage::GetPageDataFromFile(const vector<char> &data, const Table *table, page_offset_t &offSet, fstream *filePtr)
{
    for (int i = 0; i < this->metadata.pageSize; i++)
    {
        table_id_t tableId;
        memcpy(&tableId, data.data() + offSet, sizeof(table_id_t));
        offSet += sizeof(table_id_t);

        extent_num_t numberOfExtents;
        memcpy(&numberOfExtents, data.data() + offSet, sizeof(extent_num_t));
        offSet += sizeof(extent_num_t);

        ExtentAvailability* extentAvailability = new ExtentAvailability();

        extentAvailability->freeExtents = new BitMap(numberOfExtents);
        extentAvailability->freeExtents->GetDataFromFile(data, offSet);
        
        for (int j = 0; j < numberOfExtents; j++)
        {
            extent_id_t extentId;
            memcpy(&extentId, data.data() + offSet, sizeof(extent_id_t));
            offSet += sizeof(extent_id_t);

            extentAvailability->extents.push_back(extentId);
        }

        this->allocationMap[tableId] = extentAvailability;
    }
}

void IndexAllocationMapPage::WritePageToFile(fstream *filePtr)
{
    this->WritePageMetaDataToFile(filePtr);
    
    for (const auto& pair: this->allocationMap)
    {
        filePtr->write(reinterpret_cast<const char*>(&pair.first), sizeof(table_id_t));

        const extent_num_t numberOfExtents = pair.second->extents.size();

        filePtr->write(reinterpret_cast<const char*>(&numberOfExtents), sizeof(extent_num_t));

        pair.second->freeExtents->WriteDataToFile(filePtr);

        filePtr->write(reinterpret_cast<const char*>(pair.second->extents.data()), numberOfExtents * sizeof(extent_id_t));
    }
}
