#include "LargeDataPage.h"
#include "../Page.h"

#include <cstring>

using namespace DatabaseEngine::StorageTypes;

namespace Pages {
    DataObject::DataObject()
    {
        this->objectSize = 0;
        this->nextPageId = 0;
        this->object = nullptr;
    }

    DataObject::~DataObject()
    {
        delete this->object;
    }

    DataObjectPointer::DataObjectPointer()
    {
        this->pageId = 0;
    }

    DataObjectPointer::DataObjectPointer(const page_id_t& pageId)
    {
        this->pageId = pageId;
    }

    DataObjectPointer::~DataObjectPointer() = default;

    LargeDataPage::LargeDataPage(const page_id_t& pageId, const bool& isPageCreation) : Page(pageId, isPageCreation)
    {
        this->header.pageType = PageType::LOB;
        this->data = nullptr;
    }

    LargeDataPage::LargeDataPage() : Page()
    {
        this->isDirty = false;
        this->header.pageType = PageType::LOB;
        this->data = nullptr;
    }

    LargeDataPage::LargeDataPage(const PageHeader& pageHeader) : Page(pageHeader) {
      this->data = nullptr;
    }

    LargeDataPage::~LargeDataPage()
    {
        delete this->data;
    }

    void LargeDataPage::GetPageDataFromFile(const vector<char> &data, const Table *table, page_offset_t& offSet, fstream* filePtr)
    {
      if(this->header.pageSize == 0)
        return;

      this->data = new DataObject();

      memcpy(&this->data->objectSize, data.data() + offSet, sizeof(page_size_t));
      offSet += sizeof(page_size_t);

      memcpy(&this->data->nextPageId, data.data() + offSet, sizeof(page_id_t));
      offSet += sizeof(page_id_t);

      this->data->object = new unsigned char[this->data->objectSize];
      memcpy(this->data->object, data.data() + offSet, this->data->objectSize);
      offSet += this->data->objectSize;
    }

    void LargeDataPage::WritePageToFile(fstream *filePtr)
    {
        this->WritePageHeaderToFile(filePtr);

      if(this->header.pageSize == 0)
        return;

      filePtr->write(reinterpret_cast<const char*>(&this->data->objectSize), sizeof(page_size_t));
      filePtr->write(reinterpret_cast<const char*>(&this->data->nextPageId), sizeof(page_id_t));
      filePtr->write(reinterpret_cast<const char*>(this->data->object), this->data->objectSize);
    }

    DataObject* LargeDataPage::InsertObject(const object_t *object, const page_size_t& size)
    {
        this->data = new DataObject();
        this->data->objectSize = size;

        this->data->object = new object_t[size];
        memcpy(this->data->object, object, this->data->objectSize);

        this->header.bytesLeft -= (size + OBJECT_METADATA_SIZE_T);
        this->header.pageSize = 1;
        this->isDirty = true;

        return this->data;
    }

    DataObject* LargeDataPage::GetObject() { return this->data; }

    DataObject* LargeDataPage::DeleteObject(){
      this->header.bytesLeft = PAGE_SIZE - PageHeader::GetPageHeaderSize();
      this->header.pageSize = 0;

      this->isDirty = true;

      return this->data;
    }
}