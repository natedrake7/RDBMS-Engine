#include "BPlusTree.h"
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string.h>
#include "../../Database/Table/Table.h"
#include "../../Database/Pages/IndexPage/IndexPage.h"
#include "../../Database/Storage/StorageManager/StorageManager.h"

using namespace std;
using namespace DatabaseEngine::StorageTypes;
using namespace Pages;
using namespace Storage;

namespace Indexing
{

    static ostream &operator<<(ostream &os, const BPlusTreeData &dataObject)
    {
        os << dataObject.pageId << " " << dataObject.extentId;
        return os;
    }

    Node::Node(const bool &isLeaf)
    {
        this->isLeaf = isLeaf;
        this->isRoot = false;
    }

    vector<Key> &Node::GetKeysData() { return this->keys; }

    BPlusTreeData &Node::GetClusteredData() { return this->data; }

    page_size_t Node::GetNodeSize()
    {
        //included self and parent header
        page_size_t size = 2 * NodeHeader::GetNodeHeaderSize();

        //if node is leaf or not
        size += 2 * sizeof(bool);

        //num of keys to read for node
        size += sizeof(uint16_t);

        for (const auto &key : this->keys)
            size += ( sizeof(key_size_t) + key.size);

        if (this->isLeaf)
        {
            size += this->nonClusteredData.empty() 
                    ? sizeof(BPlusTreeData) 
                    : (this->nonClusteredData.size() * (sizeof(BPlusTreeData) + sizeof(page_offset_t))) + sizeof(uint16_t);

            size += 2 * NodeHeader::GetNodeHeaderSize();
        }
        else
            size += sizeof(uint16_t) + (this->childrenHeaders.size() * NodeHeader::GetNodeHeaderSize());

        return size;
    }

    Node::~Node() = default;

    BPlusTree::BPlusTree(const Table *table, const page_id_t& indexPageId, const TreeType& treeType)
    {
        const auto &tableHeader = table->GetTableHeader();
        
        this->root = nullptr;
        this->t = PAGE_SIZE / table->GetMaximumRowSize();
        this->tableId = tableHeader.tableId;
        this->firstIndexPageId = indexPageId;
        this->type = treeType;
        this->isDirty = false;
        this->database = table->GetDatabase();
    }

    BPlusTree::BPlusTree()
    {
        this->root = nullptr;
        this->t = 0;
        this->tableId = 0;
        this->isDirty = false;
    }

    BPlusTree::~BPlusTree() = default;
    //{
    //    this->DeleteNode(root);
    //}

    void BPlusTree::SplitChild(Node *parent, const int &index, Node *child, vector<pair<Node *, Node *>> *splitLeaves)
    {
        Node *newChild = new Node(child->isLeaf);
        this->InsertNodeToPage(newChild);

        // Insert the new child into the parent's children vector at the correct position
        parent->childrenHeaders.insert(parent->childrenHeaders.begin() + index + 1, newChild->header);

        // Move the middle key from the child to the parent
        parent->keys.insert(parent->keys.begin() + index, child->keys[t - 1]);

        // Assign the second half of the child's keys to the new child
        newChild->keys.assign(child->keys.begin() + t, child->keys.end());

        // Resize the old child to keep only the first half of its keys
        child->keys.resize(t - 1);

        child->parentHeader = parent->header;
        newChild->parentHeader = parent->header;

        if (!child->isLeaf)
        {
            // Assign the second half of the child pointers to the new child
            newChild->childrenHeaders.assign(child->childrenHeaders.begin() + t, child->childrenHeaders.end());

            // Resize the old child's childrenHeaders vector to keep only the first half
            child->childrenHeaders.resize(t);

            return;
        }

        // Maintain the linked list structure for leaf nodes
        newChild->nextNodeHeader = child->nextNodeHeader;
        newChild->previousNodeHeader = child->header;

        child->nextNodeHeader = newChild->header;
        
        splitLeaves->push_back(pair<Node *, Node *>(child, newChild));
    }

    Node *BPlusTree::FindAppropriateNodeForInsert(const Key &key, int *indexPosition, vector<pair<Node *, Node *>> *splitLeaves)
    {
        if (this->root == nullptr)
        {
            this->root = new Node(true);

            this->root->isRoot = true;

            this->InsertNodeToPage(this->root);
        }

        this->isDirty = true;

        if (root->keys.size() == 2 * t - 1) // root is full,
        {
            // create new root
            Node *newRoot = new Node();
            this->InsertNodeToPage(newRoot);

            // add current root as leaf
            newRoot->childrenHeaders.push_back(this->root->header);
            newRoot->isRoot = true;
            this->root->isRoot = false;

            // split the root
            this->SplitChild(newRoot, 0, this->root, splitLeaves);

            // root is the newRoot
            root = newRoot;
        }

        return this->GetNonFullNode(root, key, indexPosition, splitLeaves);
    }

    Node *BPlusTree::GetNonFullNode(Node *node, const Key &key, int *indexPosition, vector<pair<Node *, Node *>> *splitLeaves)
    {
        if (node->isLeaf)
        {
            const auto iterator = ranges::upper_bound(node->keys, key);

            const int indexPos = iterator - node->keys.begin();

            if (!node->keys.empty() && ((node->keys.size() > indexPos 
                && key == node->keys[indexPos]) 
                ||(indexPos > 0 && key == node->keys[indexPos - 1])))
            {
                ostringstream oss;

                oss << "BPlusTree::GetNonFullNode: Key " << node->keys.data() << " already exists";

                throw invalid_argument(oss.str());
            }


            if (indexPosition != nullptr)
                *indexPosition = indexPos;

            return node;
        }

        const auto iterator = ranges::lower_bound(node->keys, key);

        int childIndex = iterator - node->keys.begin();

        Node *child = this->GetNodeFromPage(node->childrenHeaders[childIndex]);

        if (child->keys.size() == 2 * t - 1)
        {
            SplitChild(node, childIndex, child, splitLeaves);

            if (key > node->keys[childIndex])
                childIndex++;
        }

        return GetNonFullNode(this->GetNodeFromPage(node->childrenHeaders[childIndex]), key, indexPosition, splitLeaves);
    }

    void BPlusTree::DeleteNode(Node *node)
    {
        if (!node)
            return;

        //if (!node->isLeaf)
        //    for (const auto &child : node->children)
        //        this->DeleteNode(child);

        delete node;
    }

    void BPlusTree::PrintTree()
    {
        this->PrintTree(root, 0);
    }

    void BPlusTree::RangeQuery(const Key &minKey, const Key &maxKey, vector<QueryData> &result) const
    {
        if (!root)
            return;

        Node *currentNode = this->SearchKey(minKey);
        const Node *previousNode = nullptr;

        while (currentNode)
        {
            if (previousNode && maxKey >= currentNode->keys[0])
            {
                if (maxKey >= previousNode->keys[previousNode->keys.size() - 1])
                    result.emplace_back(previousNode->data, previousNode->keys.size());
            }

            for (int i = 0; i < currentNode->keys.size(); i++)
            {
                const auto &key = currentNode->keys[i];

                if (minKey <= key && maxKey >= key)
                {
                    result.emplace_back(currentNode->data, i);
                    continue;
                }

                if (maxKey < key)
                    return;
            }

            previousNode = currentNode;
            currentNode = (currentNode->nextNodeHeader.pageId != 0) 
                            ? this->GetNodeFromPage(currentNode->nextNodeHeader) 
                            : nullptr;
        }
    }

    void BPlusTree::RangeQuery(const Key & minKey, const Key & maxKey, vector<BPlusTreeNonClusteredData>& result) const
    {
        if (!root)
            return;

        const Node *currentNode = this->SearchKey(minKey);
        const Node *previousNode = nullptr;

        while (currentNode)
        {
            if (previousNode && maxKey >= currentNode->keys[0])
            {
                if (maxKey >= previousNode->keys[previousNode->keys.size() - 1])
                    result.push_back(previousNode->nonClusteredData[previousNode->keys.size()]);
            }

            for (int i = 0; i < currentNode->keys.size(); i++)
            {
                const auto &key = currentNode->keys[i];

                if (minKey <= key && maxKey >= key)
                {
                    result.push_back(currentNode->nonClusteredData[i]);
                    continue;
                }

                if (maxKey < key)
                    return;
            }

            previousNode = currentNode;
            currentNode = (currentNode->nextNodeHeader.pageId != 0) 
                        ? this->GetNodeFromPage(currentNode->nextNodeHeader)
                        : nullptr;
        }
    }

    void BPlusTree::SearchKey(const Key &key, QueryData &result) const
    {
        if (!root)
            return;

        Node *currentNode = root;

        while (!currentNode->isLeaf)
        {
            const auto iterator = ranges::lower_bound(currentNode->keys, key);

            const int index = iterator - currentNode->keys.begin();

            currentNode = this->GetNodeFromPage(currentNode->childrenHeaders[index]);
        }

        const Node *previousNode = nullptr;
        while (currentNode)
        {
            if (previousNode && key <= currentNode->keys[0])
            {
                result.treeData = previousNode->data;
                result.indexPosition = previousNode->keys.size();
                return;
            }

            for (int i = 0; i < currentNode->keys.size(); i++)
            {
                if (key == currentNode->keys[i])
                {

                    result.treeData = currentNode->data;
                    result.indexPosition = i;
                    return;
                }
            }

            previousNode = currentNode;
            currentNode = this->GetNodeFromPage(currentNode->nextNodeHeader);
        }
    }

    page_size_t BPlusTree::GetTreeSize() const
    {
        page_size_t result = 0;

        GetNodeSize(root, result);

        return result;
    }

    Node*& BPlusTree::GetRoot() { return this->root; }

    void BPlusTree::SetRoot(Node *&node) { this->root = node; }

    void BPlusTree::SetBranchingFactor(const int &branchingFactor) { this->t = branchingFactor; }

    const int &BPlusTree::GetBranchingFactor() const { return this->t; }

    void BPlusTree::WriteTreeHeaderToFile(fstream *filePtr)
    {
        filePtr->write(reinterpret_cast<char *>(&this->t), sizeof(int));
        filePtr->write(reinterpret_cast<char *>(&this->tableId), sizeof(table_id_t));
    }

    const bool& BPlusTree::IsTreeDirty() const { return this->isDirty; }

    void BPlusTree::SetTreeType(const TreeType & treeType) { this->type = treeType; }

    void BPlusTree::UpdateRowData(const Key& key, const BPlusTreeNonClusteredData& data)
    {
        Node* currentNode = this->SearchKey(key);

        if(currentNode == nullptr)
            return;

        Node *previousNode = nullptr;
        while (currentNode)
        {
            if (previousNode && key <= currentNode->keys[0])
            {
                previousNode->nonClusteredData[previousNode->keys.size()] = data;
                return;
            }

            for (int i = 0; i < currentNode->keys.size(); i++)
            {
                if (key == currentNode->keys[i])
                {
                    currentNode->nonClusteredData[i] = data;
                    return;
                }
            }

            previousNode = currentNode;
            currentNode = this->GetNodeFromPage(currentNode->nextNodeHeader);
        }
    }

    const page_id_t & BPlusTree::GetFirstIndexPageId() const { return this->firstIndexPageId; }

    void BPlusTree::ReadTreeHeaderFromFile(const vector<char> &data, page_offset_t &offSet)
    {
        memcpy(&this->t, data.data() + offSet, sizeof(int));
        offSet += sizeof(int);

        memcpy(&this->tableId, data.data() + offSet, sizeof(table_id_t));
        offSet += sizeof(table_id_t);
    }

    void BPlusTree::GetNodeSize(const Node *node, page_size_t &size) const
    {
        if (!node)
            return;

        for (const auto &key : node->keys)
            size += sizeof(key);

        if(node->isLeaf)
            size += sizeof(BPlusTreeData); 

        //for (const auto &child : node->children)
        //    GetNodeSize(child, size);
    }

    Node *BPlusTree::SearchKey(const Key &key) const
    {
        Node *currentNode = root;

        while (!currentNode->isLeaf)
        {
            const auto iterator = ranges::lower_bound(currentNode->keys, key);

            const int index = iterator - currentNode->keys.begin();

           currentNode = this->GetNodeFromPage(currentNode->childrenHeaders[index]);
        }

        return currentNode;
    }

    void BPlusTree::InsertNodeToPage(Node*& node)
    {
        IndexPage* indexPage = this->database->FindOrAllocateNextIndexPage(this->tableId, this->firstIndexPageId, node->GetNodeSize());

        //could only be set once and not multiple times but insignificant
        indexPage->SetTreeType(this->type);

        if (this->firstIndexPageId == 0)
            this->firstIndexPageId = indexPage->GetPageId();

         indexPage->InsertNode(node, &node->header.indexPosition);

         node->header.pageId = indexPage->GetPageId();
    }

    Node* BPlusTree::GetNodeFromPage(const NodeHeader & header) const
    {
        IndexPage* indexPage = StorageManager::Get().GetIndexPage(header.pageId);

        return indexPage->GetNodeByIndex(header.indexPosition);
    }

    void BPlusTree::PrintTree(const Node *node, const int &level)
    {
        if (!node)
            return;

        // Indentation for the current level
        for (int i = 0; i < level; ++i)
            cout << "  ";

        // Print node information
        if (node->isLeaf)
            cout << "[Leaf] ";
        else
            cout << "[Internal] ";

        // Print keys in the current node

        cout << "Keys: ";
        for (const auto &key : node->keys)
            cout << key.value.data() << " ";
        if (node->isLeaf)
        {
            cout << "Data: ";
            cout << node->data << " ";
        }

        cout << "\n";

        //// Recursively print children, if any
        //if (!node->isLeaf)
        //    for (const auto &childNode : node->children)
        //        PrintTree(childNode, level + 1);
    }

    Key::Key()
    {
        this->size = 0;
        this->type = KeyType::Int;
    }

    Key::Key(const void *keyValue, const key_size_t &keySize, const KeyType& keyType)
    {
        this->value.resize(keySize);
        memcpy(this->value.data(), keyValue, keySize);

        this->size = keySize;
        this->type = keyType;
    }

    Key::~Key() = default;

    Key::Key(const Key &otherKey)
    {
        this->value = otherKey.value;
        // memcpy(this->value, otherKey.value, otherKey.size);

        this->size = otherKey.size;
        this->type = otherKey.type;
    }

    bool Key::operator>(const Key& otherKey) const
    {
        switch (this->type) 
        {
            case Constants::KeyType::Int:
            {
                int64_t keyVal = 0, otherKeyVal = 0;
                memcpy(&keyVal, this->value.data(), this->value.size());
                memcpy(&otherKeyVal, otherKey.value.data(), otherKey.value.size());

                return keyVal > otherKeyVal;
            }
            case Constants::KeyType::String:
            {
                if (otherKey.size > this->size)
                    return true;

                if (otherKey.size < this->size)
                    return false;

                return memcmp(otherKey.value.data(), this->value.data(), otherKey.size) > 0;
            }
        }

        throw invalid_argument(" > Invalid DataType for Key");
    }

    bool Key::operator<(const Key& otherKey) const
    {
        return !(*this >= otherKey);
    }

    bool Key::operator<=(const Key& otherKey) const
    {
        return !(*this > otherKey);
    }

    bool Key::operator>=(const Key& otherKey) const
    { 
        switch (this->type) 
        {
            case Constants::KeyType::Int:
            {
                int64_t keyVal = 0, otherKeyVal = 0;
                memcpy(&keyVal, this->value.data(), this->value.size());
                memcpy(&otherKeyVal, otherKey.value.data(), otherKey.value.size());

                return keyVal >= otherKeyVal;
            }
            case Constants::KeyType::String:
            {
                if (otherKey.size > this->size)
                    return true;

                if (otherKey.size < this->size)
                    return false;

                return memcmp(otherKey.value.data(), this->value.data(), otherKey.size) >= 0;
            }
        }

        throw invalid_argument(">= Invalid DataType for Key");
    }

    bool Key::operator==(const Key& otherKey) const
    {
        switch (this->type) 
        {
            case Constants::KeyType::Int:
            {
                int64_t keyVal = 0, otherKeyVal = 0;
                memcpy(&keyVal, this->value.data(), this->value.size());
                memcpy(&otherKeyVal, otherKey.value.data(), otherKey.value.size());

                return keyVal == otherKeyVal;
            }
            case Constants::KeyType::String:
                return otherKey.size == this->size && memcmp(otherKey.value.data(), this->value.data(), otherKey.size) == 0;
        }

        throw invalid_argument("== Invalid DataType for Key");
    }

    QueryData::QueryData()
    {
        this->indexPosition = 0;
    }

    QueryData::QueryData(const BPlusTreeData &otherTreeData, const int &otherIndexPosition)
    {
        this->treeData = otherTreeData;
        this->indexPosition = otherIndexPosition;
    }

    QueryData::~QueryData() = default;

    BPlusTreeData::BPlusTreeData()
    {
        this->pageId = 0;
        this->extentId = 0;
    }

    BPlusTreeData::~BPlusTreeData() = default;

    BPlusTreeNonClusteredData::BPlusTreeNonClusteredData() : BPlusTreeData()
    {
        this->index = 0;
    }

    BPlusTreeNonClusteredData::BPlusTreeNonClusteredData(const page_id_t & pageId, const extent_id_t & extentId, const page_offset_t & index)
    {
        this->pageId = pageId;
        this->extentId = extentId;
        this->index = index;
    }

    BPlusTreeNonClusteredData::~BPlusTreeNonClusteredData() = default;

    NodeHeader::NodeHeader()
    {
        this->pageId = 0;
        this->indexPosition = 0;
    }

    NodeHeader::~NodeHeader() = default;

    page_size_t NodeHeader::GetNodeHeaderSize() { return sizeof(page_id_t) + sizeof(page_offset_t); }
}
