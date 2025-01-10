#include "IndexPage.h"
#include "../../../AdditionalLibraries/B+Tree/BPlusTree.h"
#include "../../Row/Row.h"
#include "../../Table/Table.h"
#include <cstring>

using namespace Indexing;
using namespace DatabaseEngine::StorageTypes;

namespace Pages {
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
    for (int i = 0; i < this->header.pageSize; i++)
    {
        bool isLeaf;
        memcpy(&isLeaf, data.data() + offSet, sizeof(bool));
        offSet += sizeof(bool);

        bool isRoot;
        memcpy(&isRoot, data.data() + offSet, sizeof(bool));
        offSet += sizeof(bool);

        Node* node = new Node(isLeaf);
        node->isRoot = isRoot;

        memcpy(&node->header, data.data() + offSet, NodeHeader::GetNodeHeaderSize());
        offSet += NodeHeader::GetNodeHeaderSize();

        memcpy(&node->parentHeader, data.data() + offSet, NodeHeader::GetNodeHeaderSize());
        offSet += NodeHeader::GetNodeHeaderSize();

        uint16_t numberOfKeys;
        memcpy(&numberOfKeys, data.data() + offSet, sizeof(uint16_t));
        offSet += sizeof(uint16_t);

        for (int j = 0; j < numberOfKeys; j++)
        {
            key_size_t keySize;
            memcpy(&keySize, data.data() + offSet, sizeof(key_size_t));
            offSet += sizeof(key_size_t);

            vector<object_t> keyValue(keySize);
            memcpy(keyValue.data(), data.data() + offSet, keySize);
            offSet += keySize;

            node->keys.emplace_back(keyValue.data(), keySize, KeyType::Int);
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

        if (true)
        {
            memcpy(&node->data, data.data() + offSet, sizeof(BPlusTreeData));
            offSet += sizeof(BPlusTreeData);
        }
        else
        {
            uint16_t numberOfRows;
            memcpy(&numberOfRows, data.data() + offSet, sizeof(uint16_t));
            offSet += sizeof(uint16_t);

            for (uint16_t rowIndex = 0; rowIndex < numberOfRows; rowIndex++)
            {
                BPlusTreeNonClusteredData nonClusteredData;
                memcpy(&nonClusteredData, data.data() + offSet, sizeof(BPlusTreeData) + sizeof(page_offset_t));
                offSet += (sizeof(BPlusTreeData) + sizeof(page_offset_t));

                node->nonClusteredData.push_back(nonClusteredData);
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
  this->WritePageHeaderToFile(filePtr);

  int size = 0;
  for (const auto& node : nodes)
  {
      filePtr->write(reinterpret_cast<const char*>(&node->isLeaf), sizeof(bool));
      filePtr->write(reinterpret_cast<const char*>(&node->isRoot), sizeof(bool));

      filePtr->write(reinterpret_cast<const char*>(&node->header), NodeHeader::GetNodeHeaderSize());
      filePtr->write(reinterpret_cast<const char*>(&node->parentHeader), NodeHeader::GetNodeHeaderSize());

      const uint16_t numberOfKeys = node->keys.size();
      filePtr->write(reinterpret_cast<const char*>(&numberOfKeys), sizeof(uint16_t));
      for (const auto& key : node->keys)
      {
          filePtr->write(reinterpret_cast<const char*>(&key.size), sizeof(key_size_t));
          filePtr->write(reinterpret_cast<const char*>(key.value.data()), key.size);
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
      if (node->nonClusteredData.empty())
           filePtr->write(reinterpret_cast<const char*>(&node->data), sizeof(BPlusTreeData));
      else
      {
          //non clustered indexed tree
          for (const auto& nonClusteredData : node->nonClusteredData)
            filePtr->write(reinterpret_cast<const char*>(&nonClusteredData), sizeof(BPlusTreeData) + sizeof(page_offset_t));
      }

      filePtr->write(reinterpret_cast<const char*>(&node->nextNodeHeader),  NodeHeader::GetNodeHeaderSize());
      filePtr->write(reinterpret_cast<const char*>(&node->previousNodeHeader),  NodeHeader::GetNodeHeaderSize());
  }
}

void IndexPage::InsertNode(Indexing::Node *& node, page_offset_t* indexPosition)
{
    *indexPosition = this->nodes.size();
    this->nodes.push_back(node);

    this->header.bytesLeft -= node->GetNodeSize();

    this->header.pageSize++;
    this->isDirty = true;
}

void IndexPage::DeleteNode(const page_offset_t & indexPosition) { this->nodes.erase(this->nodes.begin() + indexPosition); }

Node *& IndexPage::GetNodeByIndex(const page_offset_t & indexPosition) { return this->nodes.at(indexPosition); }

Node* IndexPage::GetRoot()
{
    for (auto& node : this->nodes)
        if(node->isRoot)
            return node;

    return nullptr;
}

void IndexPage::UpdateNodeParentHeader(const page_offset_t& indexPosition, const Indexing::NodeHeader & nodeHeader)
{
   Node* node = this->nodes.at(indexPosition);

   node->parentHeader = nodeHeader;

   this->isDirty = true;
}

void IndexPage::UpdateNodeChildHeader(const page_offset_t & indexPosition, const page_offset_t & childIndexPosition, const Indexing::NodeHeader & nodeHeader)
{
    Node* node = this->nodes.at(indexPosition);

    node->childrenHeaders[childIndexPosition] = nodeHeader;

    this->isDirty = true;
}

void IndexPage::UpdateNodeNextLeafHeader(const page_offset_t & indexPosition, const Indexing::NodeHeader & nodeHeader)
{
    Node* node = this->nodes.at(indexPosition);

    node->nextNodeHeader = nodeHeader;

    this->isDirty = true;
}

void IndexPage::UpdateNodePreviousLeafHeader(const page_offset_t & indexPosition, const Indexing::NodeHeader & nodeHeader)
{
    Node* node = this->nodes.at(indexPosition);

    node->previousNodeHeader = nodeHeader;

    this->isDirty = true;
}

} // namespace Pages
