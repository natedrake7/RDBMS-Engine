#include "../../include/Pages/LargeObjectPage.h"
#include "../../include/Pages/Page.h"

#include <cstring>

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

    DataObjectPointer::DataObjectPointer(const page_id_t pageId)
    {
        this->pageId = pageId;
    }

    DataObjectPointer::~DataObjectPointer() = default;

    LargeObjectPage::LargeObjectPage(const page_id_t pageId, const bool isPageCreation) : Page(pageId, isPageCreation)
    {
        this->header.type = PageType::LOB;
        this->data = nullptr;
    }

    LargeObjectPage::LargeObjectPage() : Page()
    {
        this->isDirty = false;
        this->header.type = PageType::LOB;
        this->data = nullptr;
    }

    LargeObjectPage::LargeObjectPage(const PageHeader& pageHeader) : Page(pageHeader) {
      this->data = nullptr;
    }

    LargeObjectPage::~LargeObjectPage()
    {
        delete this->data;
    }

    void LargeObjectPage::ReadFromDisk(
        const std::vector<char> &buffer,
        const DatabaseEngine::StorageTypes::Table *table,
        page_offset_t& offSet,
        fstream* filePtr
    ){
      if(this->header.size == 0)
        return;

      this->data = new LargeDataObject();

      std::memcpy(&this->data->objectSize, buffer.data() + offSet, sizeof(page_size_t));
      offSet += sizeof(page_size_t);

      std::memcpy(&this->data->nextPageId, buffer.data() + offSet, sizeof(page_id_t));
      offSet += sizeof(page_id_t);

      this->data->object = new unsigned char[this->data->objectSize];
      std::memcpy(this->data->object, buffer.data() + offSet, this->data->objectSize);
      offSet += this->data->objectSize;
    }

    void LargeObjectPage::WriteToDisk(fstream *filePtr)
    {
        this->WritePageHeaderToDisk(filePtr);

        if(this->header.size == 0)
          return;

        filePtr->write(reinterpret_cast<const char*>(&this->data->objectSize), sizeof(page_size_t));
        filePtr->write(reinterpret_cast<const char*>(&this->data->nextPageId), sizeof(page_id_t));
        filePtr->write(reinterpret_cast<const char*>(this->data->object), this->data->objectSize);
    }

    LargeDataObject* LargeObjectPage::InsertObject(const object_t *object, const page_size_t size)
    {
        this->data = new LargeDataObject();
        this->data->objectSize = size;

        this->data->object = new object_t[size];
        memcpy(this->data->object, object, this->data->objectSize);

        this->header.bytesLeft -= (size + OBJECT_METADATA_SIZE_T);
        this->header.size = 1;
        this->isDirty = true;

        return this->data;
    }

    LargeDataObject* LargeObjectPage::GetObject()const { return this->data; }

    LargeDataObject* LargeObjectPage::DeleteObject(){
      this->header.bytesLeft = Constants::PAGE_SIZE_WITHOUT_HEADER;
      this->header.size = 0;

      this->isDirty = true;

      return this->data;
    }
}