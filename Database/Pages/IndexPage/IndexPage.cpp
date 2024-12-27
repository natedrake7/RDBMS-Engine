#include "IndexPage.h"
#include "../../../AdditionalLibraries/B+Tree/BPlusTree.h"
#include "../../Row/Row.h"
#include "../../Table/Table.h"
#include <cstring>

using namespace Indexing;
using namespace DatabaseEngine::StorageTypes;

namespace Pages {
  
IndexPageAdditionalHeader::IndexPageAdditionalHeader() 
{ 
    this->nextPageId = 0;
    this->dataSize = 0;
}

IndexPageAdditionalHeader::IndexPageAdditionalHeader(const page_id_t &nextPageId) 
{
  this->nextPageId = nextPageId;
  this->dataSize = 0;
}

IndexPageAdditionalHeader::~IndexPageAdditionalHeader() = default;

IndexPage::IndexPage(const page_id_t &pageId, const bool &isPageCreation) : Page(pageId, isPageCreation) 
{
  this->treeData = new object_t[PAGE_SIZE - this->header.GetPageHeaderSize() - sizeof(this->additionalHeader)];
  this->header.pageType = PageType::INDEX;
}

IndexPage::IndexPage(const PageHeader &pageHeader) : Page(pageHeader) 
{
  this->treeData = new object_t[PAGE_SIZE - this->header.GetPageHeaderSize() - sizeof(this->additionalHeader)];
}

IndexPage::~IndexPage() { delete this->treeData; }

void IndexPage::GetPageDataFromFile(const vector<char> &data, const Table *table, page_offset_t &offSet, fstream *filePtr) 
{
  this->GetAdditionalHeaderFromFile(data, offSet);

  if (this->additionalHeader.dataSize == 0)
    return;

  this->treeData = new object_t[PAGE_SIZE - this->header.GetPageHeaderSize() - sizeof(this->additionalHeader)];

  memcpy(treeData, data.data() + offSet, this->additionalHeader.dataSize);

  offSet += this->additionalHeader.dataSize;
}

void IndexPage::WritePageToFile(fstream *filePtr) 
{
  this->WritePageHeaderToFile(filePtr);
  this->WriteAdditionalHeaderToFile(filePtr);

  filePtr->write(reinterpret_cast<char *>(this->treeData), this->CalculateTreeDataSize());
}

page_size_t IndexPage::CalculateTreeDataSize() const 
{
  return PAGE_SIZE - this->header.bytesLeft - this->header.GetPageHeaderSize() - sizeof(this->additionalHeader);
}

void IndexPage::WriteTreeDataToPage(Node *node, page_offset_t& offSet) 
{
    const uint16_t numberOfKeys = node->keys.size();

    memcpy(this->treeData + offSet, &node->isLeaf, sizeof(bool));
    offSet += sizeof(bool);

    memcpy(this->treeData + offSet, &numberOfKeys, sizeof(uint16_t));
    offSet += sizeof(uint16_t);

    for (const auto &key : node->keys) 
    {
        memcpy(this->treeData + offSet, &key.size, sizeof(key_size_t));
        offSet += sizeof(key_size_t);

        memcpy(this->treeData + offSet, key.value.data(), key.size);
        offSet += key.size;
    }

    if (node->isLeaf) 
    {
        memcpy(this->treeData + offSet, &node->data,sizeof(BPlusTreeData));
        offSet += sizeof(BPlusTreeData);
    }
    else 
    {
        const uint16_t numberOfChildren = node->children.size();

        memcpy(this->treeData + offSet, &numberOfChildren, sizeof(uint16_t));
        offSet += sizeof(uint16_t);
    }

    this->header.bytesLeft -= node->GetNodeSize();
    this->additionalHeader.dataSize = offSet;

    this->header.pageSize++;
    this->isDirty = true;
}

void IndexPage::GetAdditionalHeaderFromFile(const vector<char> &data, page_offset_t &offSet) 
{
  memcpy(&this->additionalHeader, data.data() + offSet, sizeof(IndexPageAdditionalHeader));
  offSet += sizeof(IndexPageAdditionalHeader);
}

void IndexPage::WriteAdditionalHeaderToFile(fstream *filePtr) 
{
  filePtr->write(reinterpret_cast<char*>(&this->additionalHeader),sizeof(IndexPageAdditionalHeader));
}

void IndexPage::SetNextPageId(const page_id_t &nextPageId) 
{
  this->additionalHeader.nextPageId = nextPageId;
}

const page_id_t &IndexPage::GetNextPageId() const 
{
  return this->additionalHeader.nextPageId;
}

const object_t *IndexPage::GetTreeData() const { return this->treeData; }
} // namespace Pages
