//
// Created by natedrake7 on 5/27/25.
//

#include <cstring>
#include "OverflowPage.h"

namespace Pages {
  OverflowRow::OverflowRow() : DataObject(){
    this->index = 0;
  }

  block_size_t OverflowRow::GetSize(){
    return this->objectSize + sizeof(page_offset_t) + sizeof(page_id_t);
  }

  OverflowPage::OverflowPage(const PageHeader & pageHeader): Page(pageHeader){}

  OverflowPage::OverflowPage(){
      this->header.pageType = PageType::OVERFLOW;
  }

  OverflowPage::OverflowPage(const page_id_t & pageId, const bool & isPageCreation): Page(pageId,isPageCreation){
    this->header.pageType = PageType::OVERFLOW;
  }

  OverflowRow* OverflowPage::GetObject(const page_offset_t & index){ return this->data.at(index); }

  OverflowRow* OverflowPage::DeleteObject(const page_offset_t& index){
    auto* object = this->data.at(index);

    this->data.at(index) = nullptr; //mark page to defragment it later

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

    return row;
  }

  void OverflowPage::UpdateBytesLeft(){
    this->header.bytesLeft = PAGE_SIZE - this->header.GetPageHeaderSize();

    for(const auto& row : this->data){
      this->header.bytesLeft -= row->GetSize();
    }
  }

  void OverflowPage::WritePageToFile(fstream *filePtr){
   this->WritePageHeaderToFile(filePtr);

    for(const auto& row : this->data){
      if(row == nullptr)
        continue;

      filePtr->write(reinterpret_cast<const char*>(&row->objectSize), sizeof(page_size_t));
      filePtr->write(reinterpret_cast<const char*>(row->object), row->objectSize);
      filePtr->write(reinterpret_cast<const char*>(&row->nextPageId), sizeof(page_id_t));
      filePtr->write(reinterpret_cast<const char*>(&row->index), sizeof(page_offset_t));
    }
  }

  void OverflowPage::GetPageDataFromFile(const vector<char> & data, const DatabaseEngine::StorageTypes::Table *table, page_offset_t & offSet, fstream *filePtr){
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