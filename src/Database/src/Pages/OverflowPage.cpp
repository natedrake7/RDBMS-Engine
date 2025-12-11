#include <cstring>
#include "../../include/Pages/OverflowPage.h"

namespace Pages {
  OverflowRow::OverflowRow(){
    this->index = 0;
  }

  block_size_t OverflowRow::GetSize()const{
    return this->objectSize + Constants::OVERFLOW_POINTER_SIZE;
  }

  OverflowPage::OverflowPage(const PageHeader & pageHeader): Page(pageHeader){}

  OverflowPage::OverflowPage(){
      this->header.pageType = PageType::OVERFLOW;
  }

  OverflowPage::OverflowPage(const page_id_t & pageId, const bool & isPageCreation): Page(pageId,isPageCreation){
    this->header.pageType = PageType::OVERFLOW;
  }

  OverflowRow* OverflowPage::GetObject(const page_offset_t & index)const{ return this->data.at(index); }

  OverflowRow* OverflowPage::DeleteObject(const page_offset_t& index){
    auto* object = this->data.at(index);

    if(this->data.size() == 1 || index == this->data.size() - 1)
      this->data.erase(this->data.begin() + index);
    else
      this->data.at(index) = nullptr; //mark page to defragment it later (only if there are more items in the page)

    this->UpdateBytesLeft();

    return object;
  }

  OverflowRow* OverflowPage::InsertObject(const object_t *object, const page_size_t & size, int& indexPos){
    auto* row = new OverflowRow();

    row->object = new object_t[size];
    row->objectSize = size;

    memcpy(row->object, object, size);

    this->data.push_back(row);

    indexPos = this->data.size() - 1;
    this->header.pageSize++;
    this->UpdateBytesLeft();

    return row;
  }

  void OverflowPage::UpdateBytesLeft(){
    this->header.bytesLeft = static_cast<page_size_t>(Constants::PAGE_SIZE_WITHOUT_HEADER);

    for(const auto& row : this->data){
      if(row == nullptr) //ignore fragmented parts
        continue;

      this->header.bytesLeft -= row->GetSize();
    }
  }

  void OverflowPage::WriteToDisk(fstream *filePtr){
   this->WritePageHeaderToDisk(filePtr);

    for(const auto& row : this->data){
      if(row == nullptr)
        continue;

      filePtr->write(reinterpret_cast<const char*>(&row->objectSize), sizeof(page_size_t));
      filePtr->write(reinterpret_cast<const char*>(row->object), row->objectSize);
      filePtr->write(reinterpret_cast<const char*>(&row->nextPageId), sizeof(page_id_t));
      filePtr->write(reinterpret_cast<const char*>(&row->index), sizeof(page_offset_t));
    }
  }

  void OverflowPage::ReadFromDisk(const vector<char> & data, const DatabaseEngine::StorageTypes::Table *table, page_offset_t & offSet, fstream *filePtr){
     for(int i = 0;i < this->header.pageSize; i++){
        auto* row = new OverflowRow();

        memcpy(&row->objectSize, data.data() + offSet, sizeof(page_size_t));
        offSet += sizeof(page_size_t);

        row->object = new object_t[row->objectSize];
        memcpy(row->object, data.data() + offSet, row->objectSize);
        offSet += row->objectSize;

        memcpy(&row->nextPageId, data.data() + offSet, sizeof(page_id_t));
        offSet += sizeof(page_id_t);

        memcpy(&row->index, data.data() + offSet, sizeof(page_offset_t));
        offSet += sizeof(page_offset_t);

        this->data.push_back(row);
    }
  }

  OverflowPointer::OverflowPointer(const page_id_t & pageId, const page_offset_t& index): DataObjectPointer(pageId){
    this->index = index;
  }

  OverflowPointer::OverflowPointer(){
    this->index = 0;
    this->pageId = 0;
  }

  OverflowPointer::~OverflowPointer() = default;
} // Pages