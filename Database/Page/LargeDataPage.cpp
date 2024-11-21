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

DataObjectPointer::DataObjectPointer(const page_size_t& objectSize, const page_offset_t& objectOffset, const page_id_t& pageId)
{
    this->objectSize = objectSize;
    this->objectOffset = objectOffset;
    this->pageId = pageId;
}

DataObjectPointer::~DataObjectPointer() = default;

LargeDataPage::LargeDataPage(const page_id_t& pageId) : Page(pageId)
{
    this->metadata.pageId = pageId;
    this->isDirty = false;
}

LargeDataPage::~LargeDataPage()
{
    for(const auto& dataObject : this->data)
        delete dataObject;
}

void LargeDataPage::GetPageDataFromFile(const vector<char> &data, const Table *table, page_offset_t& offSet, fstream* filePtr)
{
    this->GetPageMetaDataFromFile(data, offSet);

    for(int i = 0; i < this->metadata.pageSize; i++)
    {
        DataObject* dataObject = new DataObject();

        memcpy(&dataObject->objectSize, data.data() + offSet, sizeof(page_size_t));
        offSet += sizeof(page_size_t);

        memcpy(&dataObject->nextPageId, data.data() + offSet, sizeof(page_id_t));
        offSet += sizeof(page_id_t);

        memcpy(&dataObject->nextPageOffset, data.data() + offSet, sizeof(page_offset_t));
        offSet += sizeof(page_offset_t);

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
        filePtr->write(reinterpret_cast<const char*>(&dataObject->objectSize), sizeof(page_size_t));
        filePtr->write(reinterpret_cast<const char*>(&dataObject->nextPageId), sizeof(page_id_t));
        filePtr->write(reinterpret_cast<const char*>(&dataObject->nextPageOffset), sizeof(page_offset_t));

        filePtr->write(reinterpret_cast<const char*>(&dataObject->object), dataObject->objectSize);
    }
}

DataObject* LargeDataPage::InsertObject(const unsigned char *object, const page_size_t& size, page_offset_t* objectPosition)
{
    DataObject* dataObject = new DataObject();
    dataObject->objectSize = size;

    dataObject->object = new unsigned char[dataObject->objectSize];
    memcpy(dataObject->object, object, dataObject->objectSize);

    this->data.push_back(dataObject);

    *objectPosition = PAGE_SIZE - this->metadata.bytesLeft;

    this->metadata.bytesLeft -= (size + sizeof(page_size_t) + sizeof(page_offset_t));
    this->metadata.pageSize = this->data.size();
    this->isDirty = true;

    return dataObject;
}
