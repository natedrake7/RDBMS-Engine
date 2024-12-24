#include "IndexPage.h"
#include "../../../AdditionalLibraries/B+Tree/BPlusTree.h"
#include "../../Table/Table.h"
#include "../../Row/Row.h"
#include "../../Block/Block.h"

using namespace Indexing;
using namespace DatabaseEngine::StorageTypes;

namespace Pages
{
    IndexPage::IndexPage(const page_id_t &pageId, const bool &isPageCreation) : Page(pageId, isPageCreation)
    {
        this->tree = nullptr;
        this->header.pageType = PageType::INDEX;
    }

    IndexPage::IndexPage(const PageHeader &pageHeader) : Page(pageHeader)
    {
        this->tree = nullptr;
    }

    IndexPage::~IndexPage()
    {
        delete this->tree;
    }

    void IndexPage::GetPageDataFromFile(const vector<char> &data, const Table *table, page_offset_t &offSet, fstream *filePtr)
    {
        if (this->header.pageSize == 0)
            return;

        this->tree = new BPlusTree();
        this->tree->ReadTreeHeaderFromFile(data, offSet);

        Node *root = this->GetNodeFromDisk(data, offSet);
        this->tree->SetRoot(root);
    }

    Node *IndexPage::GetNodeFromDisk(const vector<char> &data, page_offset_t &offSet)
    {
        bool isLeaf;
        memcpy(&isLeaf, data.data() + offSet, sizeof(bool));
        offSet += sizeof(bool);

        Node *node = new Node(isLeaf);

        if (node->isLeaf)
        {
            uint16_t dataSize;
            memcpy(&dataSize, data.data() + offSet, sizeof(uint16_t));
            offSet += sizeof(uint16_t);

            page_id_t pageId;
            extent_id_t extentId;

            memcpy(&pageId, data.data() + offSet, sizeof(page_id_t));
            offSet += sizeof(page_id_t);

            memcpy(&extentId, data.data() + offSet, sizeof(extent_id_t));
            offSet += sizeof(extent_id_t);

            node->data.resize(dataSize);

            node->data[0].pageId = new page_id_t(pageId);
            node->data[0].extentId = new extent_id_t(extentId);
        }

        uint16_t numOfKeys;
        memcpy(&numOfKeys, data.data() + offSet, sizeof(uint16_t));
        offSet += sizeof(uint16_t);

        for (int i = 0; i < numOfKeys; i++)
        {
            key_size_t keySize;
            memcpy(&keySize, data.data() + offSet, sizeof(key_size_t));
            offSet += sizeof(key_size_t);

            object_t *keyValue = new object_t[keySize];
            memcpy(keyValue, data.data() + offSet, keySize);

            offSet += keySize;

            node->keys.emplace_back(keyValue, keySize);
        }

        uint16_t numberOfChildren;
        memcpy(&numberOfChildren, data.data() + offSet, sizeof(uint16_t));
        offSet += sizeof(uint16_t);

        node->children.resize(numberOfChildren);

        for (int i = 0; i < numberOfChildren; i++)
            node->children[i] = this->GetNodeFromDisk(data, offSet);

        return node;
    }

    void IndexPage::WritePageToFile(fstream *filePtr)
    {
        this->WritePageHeaderToFile(filePtr);

        if (this->header.pageSize == 0)
            return;

        this->tree->WriteTreeHeaderToFile(filePtr);

        Node *root = this->tree->GetRoot();

        this->WriteNodeToDisk(root, filePtr);
    }

    void IndexPage::WriteNodeToDisk(Node *node, fstream *filePtr)
    {
        this->header.bytesLeft -= sizeof(bool);
        filePtr->write(reinterpret_cast<char *>(&node->isLeaf), sizeof(bool));

        if (node->isLeaf)
        {
            const uint16_t numberOfData = node->data.size();

            filePtr->write(reinterpret_cast<const char *>(&numberOfData), sizeof(uint16_t));
            filePtr->write(reinterpret_cast<char *>(node->data[0].pageId), sizeof(page_id_t));
            filePtr->write(reinterpret_cast<char *>(node->data[0].extentId), sizeof(extent_id_t));
        }

        const uint16_t numberOfKeys = node->keys.size();
        this->header.bytesLeft -= sizeof(uint16_t);
        filePtr->write(reinterpret_cast<const char *>(&numberOfKeys), sizeof(uint16_t));

        for (const auto &key : node->keys)
        {
            filePtr->write(reinterpret_cast<const char *>(&key.size), sizeof(key_size_t));
            filePtr->write(reinterpret_cast<char *>(key.value), key.size);

            this->header.bytesLeft -= (sizeof(key_size_t) + key.size);
        }

        const uint16_t numberOfChildren = node->children.size();
        filePtr->write(reinterpret_cast<const char *>(&numberOfChildren), sizeof(uint16_t));

        for (const auto &child : node->children)
            this->WriteNodeToDisk(child, filePtr);
    }

    Node *IndexPage::FindAppropriateNodeForInsert(const DatabaseEngine::StorageTypes::Table *table, const Key &key, int *indexPosition, vector<pair<Node *, Node *>> *splitLeaves)
    {
        if (!this->tree)
        {
            table->GetIndexedColumnKeys(&this->indexedColumns);
            this->tree = new BPlusTree(table);

            this->header.pageSize = 1;
        }

        return this->tree->FindAppropriateNodeForInsert(key, indexPosition, splitLeaves);
    }

    void IndexPage::RangeQuery(const Key &minKey, const Key &maxKey, vector<QueryData> &result)
    {
        if (!this->tree)
            return;

        this->tree->RangeQuery(minKey, maxKey, result);
    }

    const vector<column_index_t> &IndexPage::GetIndexedColumns() const { return this->indexedColumns; }

    const int &IndexPage::GetBranchingFactor() const { return this->tree->GetBranchingFactor(); }

    void IndexPage::SetRoot(Node *&node) { this->tree->SetRoot(node); }
}
