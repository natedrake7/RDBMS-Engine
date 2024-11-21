#include "LargeDataPage.h"

DataObject::DataObject()
{
    this->objectSize = 0;
    this->nextPageOffset = 0;
    this->nextPageId = 0;
    this->object = nullptr;
}

DataObject::~DataObject()
{
    delete[] this->object;
}

DataObjectPointer::DataObjectPointer()
{
    this->objectSize = 0;
    this->objectOffset = 0;
    this->pageId = 0;
}

DataObjectPointer::DataObjectPointer(const uint16_t& objectSize, const uint16_t& objectOffset, const uint16_t& pageId)
{
    this->objectSize = objectSize;
    this->objectOffset = objectOffset;
    this->pageId = pageId;
}

DataObjectPointer::~DataObjectPointer() = default;

LargeDataPage::LargeDataPage(const uint16_t& pageId) : Page(pageId)
{
    this->metadata.pageId = pageId;
    this->isDirty = false;
}

LargeDataPage::~LargeDataPage()
{
    for(const auto& dataObject : this->data)
        delete dataObject;
}

void LargeDataPage::GetPageDataFromFile(const vector<char> &data, const Table *table, uint16_t &offSet, fstream* filePtr)
{
    this->GetPageMetaDataFromFile(data, table, offSet);

    for(int i = 0; i < this->metadata.pageSize; i++)
    {
        DataObject* dataObject = new DataObject();

        memcpy(&dataObject->objectSize, data.data() + offSet, sizeof(uint16_t));
        offSet += sizeof(uint16_t);

        memcpy(&dataObject->nextPageId, data.data() + offSet, sizeof(uint16_t));
        offSet += sizeof(uint16_t);

        memcpy(&dataObject->nextPageOffset, data.data() + offSet, sizeof(uint16_t));
        offSet += sizeof(uint16_t);

        memcpy(&dataObject->object, data.data() + offSet, dataObject->objectSize);
        offSet += dataObject->objectSize;

        this->data.push_back(dataObject);
    }

}

void LargeDataPage::WritePageToFile(fstream *filePtr)
{
    this->WritePageMetaDataToFile(filePtr);

    for(const auto& dataObject : this->data)
    {
        filePtr->write(reinterpret_cast<const char*>(&dataObject->objectSize), sizeof(uint16_t));
        filePtr->write(reinterpret_cast<const char*>(&dataObject->nextPageId), sizeof(uint16_t));
        filePtr->write(reinterpret_cast<const char*>(&dataObject->nextPageOffset), sizeof(uint16_t));

        filePtr->write(reinterpret_cast<const char*>(&dataObject->object), dataObject->objectSize);
    }
}

DataObject* LargeDataPage::InsertObject(const unsigned char *object, const uint16_t& size, uint16_t* objectPosition)
{
    DataObject* dataObject = new DataObject();
    dataObject->objectSize = size;

    dataObject->object = new unsigned char[dataObject->objectSize];
    memcpy(dataObject->object, object, dataObject->objectSize);

    this->data.push_back(dataObject);

    *objectPosition = PAGE_SIZE - this->metadata.bytesLeft;

    this->metadata.bytesLeft -= (size + 2 * sizeof(uint16_t));
    this->metadata.pageSize = this->data.size();
    this->isDirty = true;

    return dataObject;
}
