#include "LargeDataPage.h"

DataObject::DataObject()
{
    this->objectSize = 0;
    this->object = nullptr;
}

DataObject::~DataObject()
{
    delete[] this->object;
}

LargeDataPage::LargeDataPage(const uint16_t& pageId) : Page(pageId)
{
    this->isDirty = false;
}

LargeDataPage::~LargeDataPage()
{
    for(const auto& dataObject : this->data)
        delete dataObject;
}

void LargeDataPage::GetPageDataFromFile(const vector<char> &data, const Table *table, uint32_t &offSet)
{
    this->GetPageDataFromFile(data, table, offSet);

    for(int i = 0; i < this->metadata.pageSize; i++)
    {
        DataObject* dataObject = new DataObject();

        memcpy(&dataObject->objectSize, data.data() + offSet, sizeof(uint32_t));
        offSet += sizeof(uint32_t);

        //handle case where object exceeds page read from the next page too

        memcpy(&dataObject->object, data.data() + offSet, dataObject->objectSize);
        offSet += dataObject->objectSize;

        this->data.push_back(dataObject);
    }

}

void LargeDataPage::WritePageToFile(fstream *filePtr)
{
    this->WritePageToFile(filePtr);

    for(const auto& dataObject : this->data)
    {
        //handle case where data exceeds page
        filePtr->write(reinterpret_cast<const char*>(&dataObject->objectSize), sizeof(uint32_t));

        filePtr->write(reinterpret_cast<const char*>(&dataObject->object), dataObject->objectSize);
    }
}

uint16_t LargeDataPage::InsertObject(const unsigned char *object, const uint32_t& size)
{
    DataObject* dataObject = new DataObject();
    dataObject->objectSize = size;

    dataObject->object = new unsigned char[dataObject->objectSize];
    memcpy(dataObject->object, object, dataObject->objectSize);

    //handle case where object is larger than page
    this->data.push_back(dataObject);

    uint16_t objectPosition = PAGE_SIZE - this->metadata.bytesLeft;

    this->metadata.bytesLeft -= (dataObject->objectSize  + sizeof(uint32_t));

    this->isDirty = true;

    return objectPosition;
}
