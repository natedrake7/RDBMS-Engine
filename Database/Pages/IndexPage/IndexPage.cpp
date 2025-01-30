#include "IndexPage.h"
#include "../../B+Tree/BPlusTree.h"
#include "../../Table/Table.h"
#include <cstring>
#include <ostream>
#include <stdexcept>

using namespace Indexing;
using namespace DatabaseEngine::StorageTypes;

namespace Pages {

void IndexPage::WriteAdditionalHeaderToFile(fstream * filePtr) const
{
    filePtr->write(reinterpret_cast<const char*>(&this->additionalHeader.treeType), sizeof(TreeType));
    filePtr->write(reinterpret_cast<const char*>(&this->additionalHeader.treeId), sizeof(page_id_t));
    filePtr->write(reinterpret_cast<const char*>(&this->additionalHeader.numberOfSubKeys), sizeof(uint8_t));
}

void IndexPage::ReadAdditionalHeaderFromFile(const vector<char>& data, page_offset_t & offSet)
{
    memcpy(&this->additionalHeader.treeType, data.data() + offSet, sizeof(TreeType));
    offSet += sizeof(TreeType);

    memcpy(&this->additionalHeader.treeId, data.data() + offSet, sizeof(page_id_t));
    offSet += sizeof(page_id_t);

    memcpy(&this->additionalHeader.numberOfSubKeys, data.data() + offSet, sizeof(uint8_t));
    offSet += sizeof(uint8_t);
}

IndexPage::IndexPage(const page_id_t &pageId, const bool &isPageCreation) : Page(pageId, isPageCreation) 
{
  this->header.pageType = PageType::INDEX;
}

IndexPage::IndexPage(const PageHeader &pageHeader) : Page(pageHeader) { }

IndexPage::~IndexPage() 
{ 
    for(const auto& node : this->nodes)
        delete node;
}

void IndexPage::GetPageDataFromFile(const vector<char> &data, const Table *table, page_offset_t &offSet, fstream *filePtr) 
{
    this->ReadAdditionalHeaderFromFile(data, offSet);
    const vector<ColumnType> indexedColumnTypes = table->GetColumnTypeByTreeId(this->additionalHeader.treeId);

    for (int i = 0; i < this->header.pageSize; i++)
    {
        bool isLeaf;
        memcpy(&isLeaf, data.data() + offSet, sizeof(bool));
        offSet += sizeof(bool);

        bool isRoot;
        memcpy(&isRoot, data.data() + offSet, sizeof(bool));
        offSet += sizeof(bool);

        Node* node = new Node(isLeaf, isRoot);

        memcpy(&node->header, data.data() + offSet, NodeHeader::GetNodeHeaderSize());
        offSet += NodeHeader::GetNodeHeaderSize();

        memcpy(&node->parentHeader, data.data() + offSet, NodeHeader::GetNodeHeaderSize());
        offSet += NodeHeader::GetNodeHeaderSize();

        uint16_t numberOfKeys;
        memcpy(&numberOfKeys, data.data() + offSet, sizeof(uint16_t));
        offSet += sizeof(uint16_t);

        for (int j = 0; j < numberOfKeys; j++)
        {
            Key key;
            for (int k = 0; k < this->additionalHeader.numberOfSubKeys; k++)
            {
                key_size_t keySize;
                memcpy(&keySize, data.data() + offSet, sizeof(key_size_t));
                offSet += sizeof(key_size_t);

                vector<object_t> keyValue(keySize);
                memcpy(keyValue.data(), data.data() + offSet, keySize);
                offSet += keySize;

                key.InsertKey(Key(keyValue.data(), keySize, indexedColumnTypes[k]));
            }

            node->keys.push_back(key);
        }

        if (!node->isLeaf)
        {
            uint16_t numberOfChildren;
            memcpy(&numberOfChildren, data.data() + offSet, sizeof(uint16_t));
            offSet += sizeof(uint16_t);

            for (int childPtr = 0; childPtr < numberOfChildren; childPtr++)
            {
                NodeHeader childHeader;
                memcpy(&childHeader, data.data() + offSet, NodeHeader::GetNodeHeaderSize());
                offSet += NodeHeader::GetNodeHeaderSize();

                node->childrenHeaders.push_back(childHeader);
            }

            this->nodes.push_back(node);
            continue;
        }

        if (this->additionalHeader.treeType == TreeType::Clustered)
        {
            memcpy(&node->dataPageId, data.data() + offSet, sizeof(page_id_t));
            offSet += sizeof(page_id_t);
        }
        else
        {
            uint16_t numberOfRows;
            memcpy(&numberOfRows, data.data() + offSet, sizeof(uint16_t));
            offSet += sizeof(uint16_t);

            for (uint16_t rowIndex = 0; rowIndex < numberOfRows; rowIndex++)
            {
                page_id_t pageId = 0;
                memcpy(&pageId, data.data() + offSet, sizeof(page_id_t));
                offSet += sizeof(page_id_t);

                page_offset_t pageIndex = 0;
                memcpy(&pageIndex, data.data() + offSet, sizeof(page_offset_t));
                offSet += sizeof(page_offset_t);

                node->nonClusteredData.emplace_back(pageId, pageIndex);
            }
        }
            
        memcpy(&node->nextNodeHeader, data.data() + offSet, NodeHeader::GetNodeHeaderSize());
        offSet += NodeHeader::GetNodeHeaderSize();

        memcpy(&node->previousNodeHeader, data.data() + offSet, NodeHeader::GetNodeHeaderSize());
        offSet += NodeHeader::GetNodeHeaderSize();
        
        this->nodes.push_back(node);
    }
}

void IndexPage::WritePageToFile(fstream *filePtr) 
{
    if (!this->nodes.empty())
        this->additionalHeader.numberOfSubKeys = this->nodes.front()->keys.front().subKeys.size();
    
  this->WritePageHeaderToFile(filePtr);
  this->WriteAdditionalHeaderToFile(filePtr);

  for (const auto& node : nodes)
  {
      filePtr->write(reinterpret_cast<const char*>(&node->isLeaf), sizeof(bool));
      filePtr->write(reinterpret_cast<const char*>(&node->isRoot), sizeof(bool));

      filePtr->write(reinterpret_cast<const char*>(&node->header), NodeHeader::GetNodeHeaderSize());
      filePtr->write(reinterpret_cast<const char*>(&node->parentHeader), NodeHeader::GetNodeHeaderSize());

      const uint16_t numberOfKeys = node->keys.size();
      filePtr->write(reinterpret_cast<const char*>(&numberOfKeys), sizeof(uint16_t));
      
      for (const auto& key : node->keys)
        for (const auto& subKey: key.subKeys)
        {
          filePtr->write(reinterpret_cast<const char*>(&subKey.size), sizeof(key_size_t));
          filePtr->write(reinterpret_cast<const char*>(subKey.value.data()), subKey.size);
        }

      if (!node->isLeaf)
      {
          const uint16_t numberOfChildren = node->childrenHeaders.size();

          filePtr->write(reinterpret_cast<const char*>(&numberOfChildren), sizeof(uint16_t));
          for (const auto& childHeader : node->childrenHeaders)
              filePtr->write(reinterpret_cast<const char*>(&childHeader), NodeHeader::GetNodeHeaderSize());

          continue;
      }

      //clustered indexed tree
      if (this->additionalHeader.treeType == TreeType::Clustered)
      {
           filePtr->write(reinterpret_cast<const char*>(&node->dataPageId), sizeof(page_id_t));
      }
      else
      {
          //non clustered indexed tree
          const uint16_t numberOfRows = node->nonClusteredData.size();
          filePtr->write(reinterpret_cast<const char*>(&numberOfRows), sizeof(uint16_t));

          for (const auto& nonClusteredData : node->nonClusteredData)
            filePtr->write(reinterpret_cast<const char*>(&nonClusteredData), sizeof(page_id_t) + sizeof(page_offset_t));
      }

      filePtr->write(reinterpret_cast<const char*>(&node->nextNodeHeader),  NodeHeader::GetNodeHeaderSize());
      filePtr->write(reinterpret_cast<const char*>(&node->previousNodeHeader),  NodeHeader::GetNodeHeaderSize());
  }
}

void IndexPage::SetTreeType(const TreeType & treeType) { this->additionalHeader.treeType = treeType; }

void IndexPage::SetTreeId(const page_id_t & treeId) { this->additionalHeader.treeId = treeId; }

const page_id_t & IndexPage::GetTreeId() const { return this->additionalHeader.treeId; }

void IndexPage::InsertNode(Indexing::Node *& node, page_offset_t* indexPosition)
{
    *indexPosition = this->nodes.size();
    this->nodes.push_back(node);

    this->header.bytesLeft -= node->GetNodeSize();

    this->header.pageSize++;
    this->isDirty = true;
}

void IndexPage::DeleteNode(const page_offset_t & indexPosition) 
{
    this->header.bytesLeft += this->nodes.at(indexPosition)->prevNodeSize;

    this->nodes.erase(this->nodes.begin() + indexPosition);

    this->header.pageSize--;
    this->isDirty = true;
}

Indexing::Node * IndexPage::GetLastNode()
{
    return this->nodes.back();
}

void IndexPage::DeleteLastNode()
{
    this->nodes.pop_back();

    this->header.pageSize--;
    this->isDirty = true;
}

void IndexPage::UpdateBytesLeft()
{
    this->header.bytesLeft = PAGE_SIZE - PageHeader::GetPageHeaderSize() - IndexPageAdditionalHeader::GetAdditionalHeaderSize();

    for(const auto& node: this->nodes)
        this->header.bytesLeft -= node->GetNodeSize();

    this->isDirty = true;
}

void IndexPage::UpdateBytesLeft(const page_size_t & prevNodeSize, const page_size_t & currentNodeSize)
{
    this->header.bytesLeft += prevNodeSize - currentNodeSize;

    this->isDirty = true;
}

Node * IndexPage::GetNodeByIndex(const page_offset_t & indexPosition) const
{
    Node* node = this->nodes.at(indexPosition);
    node->isNodeClustered = this->additionalHeader.treeType == TreeType::Clustered;

    return node; 
}

Node* IndexPage::GetRoot() const
{
    for (const auto& node : this->nodes)
        if (node->isRoot)
        {
            node->isNodeClustered = this->additionalHeader.treeType == TreeType::Clustered;
            return node;
        }

    return nullptr;
}

vector<Node*>* IndexPage::GetNodesUnsafe() { return &this->nodes; }

void IndexPage::ResizeNodes(const int& splitFactor)
{
    if(this->header.pageSize <= splitFactor)
        throw invalid_argument("IndexPage::split factor cannot be equal or less to pageSize");

    this->nodes.resize(splitFactor);
    this->header.pageSize = this->nodes.size();

    this->isDirty = true;
}

void IndexPage::UpdatePageSize()
{
    this->header.pageSize = this->nodes.size();

    this->isDirty = true;
}

void IndexPage::UpdateNodeParentHeader(const page_offset_t& indexPosition, const Indexing::NodeHeader & nodeHeader)
{
   this->nodes.at(indexPosition)->parentHeader = nodeHeader;

   this->isDirty = true;
}

void IndexPage::UpdateNodeChildHeader(const page_offset_t & indexPosition, const page_offset_t & childIndexPosition, const Indexing::NodeHeader & nodeHeader)
{
    this->nodes.at(indexPosition)->childrenHeaders[childIndexPosition] = nodeHeader;

    this->isDirty = true;
}

void IndexPage::UpdateNodeNextLeafHeader(const page_offset_t & indexPosition, const Indexing::NodeHeader & nodeHeader)
{
    this->nodes.at(indexPosition)->nextNodeHeader = nodeHeader;

    this->isDirty = true;
}

void IndexPage::UpdateNodePreviousLeafHeader(const page_offset_t & indexPosition, const Indexing::NodeHeader & nodeHeader)
{
    this->nodes.at(indexPosition)->previousNodeHeader = nodeHeader;

    this->isDirty = true;
}

void IndexPage::UpdateNodeHeader(const page_offset_t & indexPosition, const NodeHeader & header)
{
    Node* node = this->nodes.at(indexPosition);

    node->header = header;

    this->isDirty = true;
}

IndexPageAdditionalHeader::IndexPageAdditionalHeader()
{
    this->treeId = 0;
    this->treeType = TreeType::NonClustered;
    this->numberOfSubKeys = 0;
}

IndexPageAdditionalHeader::~IndexPageAdditionalHeader() = default;

page_size_t IndexPageAdditionalHeader::GetAdditionalHeaderSize()
{
    return sizeof(page_id_t) + sizeof(TreeType) + sizeof(uint8_t);
}
} // namespace Pages
