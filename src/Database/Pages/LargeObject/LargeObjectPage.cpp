#include "LargeObjectPage.h"
#include "../Page.h"

#include <cstring>

using namespace DatabaseEngine::StorageTypes;

namespace Pages {
    LargeDataObject::LargeDataObject()
    {
        this->objectSize = 0;
        this->nextPageId = 0;
        this->object = nullptr;
    }

    LargeDataObject::~LargeDataObject()
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

    LargeObjectPage::LargeObjectPage(const page_id_t& pageId, const bool& isPageCreation) : Page(pageId, isPageCreation)
    {
        this->header.pageType = PageType::LOB;
        this->data = nullptr;
    }

    LargeObjectPage::LargeObjectPage() : Page()
    {
        this->isDirty = false;
        this->header.pageType = PageType::LOB;
        this->data = nullptr;
    }

    LargeObjectPage::LargeObjectPage(const PageHeader& pageHeader) : Page(pageHeader) {
      this->data = nullptr;
    }

    LargeObjectPage::~LargeObjectPage()
    {
        delete this->data;
    }

    void LargeObjectPage::GetPageDataFromFile(const vector<char> &data, const Table *table, page_offset_t& offSet, fstream* filePtr)
    {
      if(this->header.pageSize == 0)
        return;

      this->data = new LargeDataObject();

      memcpy(&this->data->objectSize, data.data() + offSet, sizeof(page_size_t));
      offSet += sizeof(page_size_t);

      memcpy(&this->data->nextPageId, data.data() + offSet, sizeof(page_id_t));
      offSet += sizeof(page_id_t);

      this->data->object = new unsigned char[this->data->objectSize];
      memcpy(this->data->object, data.data() + offSet, this->data->objectSize);
      offSet += this->data->objectSize;
    }

    void LargeObjectPage::WritePageToFile(fstream *filePtr)
    {
        this->WritePageHeaderToFile(filePtr);

        if(this->header.pageSize == 0)
          return;

        filePtr->write(reinterpret_cast<const char*>(&this->data->objectSize), sizeof(page_size_t));
        filePtr->write(reinterpret_cast<const char*>(&this->data->nextPageId), sizeof(page_id_t));
        filePtr->write(reinterpret_cast<const char*>(this->data->object), this->data->objectSize);
    }

    LargeDataObject* LargeObjectPage::InsertObject(const object_t *object, const page_size_t& size)
    {
        this->data = new LargeDataObject();
        this->data->objectSize = size;

        this->data->object = new object_t[size];
        memcpy(this->data->object, object, this->data->objectSize);

        this->header.bytesLeft -= (size + OBJECT_METADATA_SIZE_T);
        this->header.pageSize = 1;
        this->isDirty = true;

        return this->data;
    }

    LargeDataObject* LargeObjectPage::GetObject() { return this->data; }

    LargeDataObject* LargeObjectPage::DeleteObject(){
      this->header.bytesLeft = PAGE_SIZE - PageHeader::GetPageHeaderSize();
      this->header.pageSize = 0;

      this->isDirty = true;

      return this->data;
    }
}