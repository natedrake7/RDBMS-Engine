﻿#include "BPlusTree.h"
#include <algorithm>
#include <cstdint>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string.h>
#include "../../Database/Table/Table.h"
#include "../../Database/Storage/PageManager/PageManager.h"

using namespace std;
using namespace DatabaseEngine::StorageTypes;

namespace Indexing
{

    static ostream &operator<<(ostream &os, const BPlusTreeData &dataObject)
    {
        os << dataObject.pageId << " " << dataObject.extentId;
        return os;
    }

    BPlusTreeData::BPlusTreeData()
    {
        this->pageId = 0;
        this->extentId = 0;
    }

    // BPlusTreeData::BPlusTreeData(const page_id_t &pageId, const extent_id_t &extentId)
    // {
    //     this->pageId = new page_id_t(pageId);
    //     this->extentId = new extent_id_t(extentId);
    // }

    // BPlusTreeData::BPlusTreeData(BPlusTreeData &&other) noexcept
    // {
    //     this->pageId = other.pageId;
    //     this->extentId = other.extentId;

    //     other.pageId = nullptr;
    //     other.extentId = nullptr;
    // }

    // BPlusTreeData::BPlusTreeData(const BPlusTreeData &other)
    // {
    //     this->pageId = (other.pageId != nullptr)
    //                        ? new page_id_t(*other.pageId)
    //                        : nullptr;

    //     this->extentId = (other.extentId != nullptr)
    //                          ? new extent_id_t(*other.extentId)
    //                          : nullptr;
    // }

    BPlusTreeData::~BPlusTreeData() = default;
    // {
    //     delete this->pageId;
    //     delete this->extentId;
    // }

    // BPlusTreeData &BPlusTreeData::operator=(const BPlusTreeData &other)
    // {
    //     if (this == &other)
    //         return *this;

    //     this->pageId = (other.pageId != nullptr)
    //                        ? new page_id_t(*other.pageId)
    //                        : nullptr;

    //     this->extentId = (other.extentId != nullptr)
    //                          ? new extent_id_t(*other.extentId)
    //                          : nullptr;

    //     return *this;
    // }

    // BPlusTreeData &BPlusTreeData::operator=(BPlusTreeData &&other)
    // {
    //     if (this == &other)
    //         return *this;

    //     delete pageId;
    //     delete extentId;

    //     pageId = other.pageId;
    //     extentId = other.extentId;

    //     other.pageId = nullptr;
    //     other.extentId = nullptr;

    //     return *this;
    // }

    Node::Node(const bool &isLeaf)
    {
        this->isLeaf = isLeaf;
        this->next = nullptr;
        this->prev = nullptr;
    }

    vector<Key> &Node::GetKeysData() { return this->keys; }

    BPlusTreeData &Node::GetData() { return this->data; }

    page_size_t Node::GetNodeSize()
    {
        page_size_t size = 0;

        for (const auto &key : this->keys)
            size += ( sizeof(key_size_t) + sizeof(key.size) );

        //num of keys to read for node
        size += sizeof(uint16_t);

        if(this->isLeaf)
            size += sizeof(BPlusTreeData);

        //if node is leaf or not
        size += sizeof(bool);

        const uint16_t numberOfChildren = this->children.size();

        size += numberOfChildren;

        return size;
    }

    Node::~Node() = default;

    BPlusTree::BPlusTree(const Table *table)
    {
        const auto &tableHeader = table->GetTableHeader();

        this->root = new Node(true);
        this->t = PAGE_SIZE / table->GetMaximumRowSize();
        this->tableId = tableHeader.tableId;
    }

    BPlusTree::BPlusTree()
    {
        this->root = nullptr;
        this->t = 0;
        this->tableId = 0;
    }

    BPlusTree::~BPlusTree()
    {
        this->DeleteNode(root);
    }

    void BPlusTree::SplitChild(Node *parent, const int &index, Node *child, vector<pair<Node *, Node *>> *splitLeaves) const
    {
        Node *newChild = new Node(child->isLeaf);

        // Insert the new child into the parent's children vector at the correct position
        parent->children.insert(parent->children.begin() + index + 1, newChild);

        // Move the middle key from the child to the parent
        parent->keys.insert(parent->keys.begin() + index, child->keys[t - 1]);

        // Assign the second half of the child's keys to the new child
        newChild->keys.assign(child->keys.begin() + t, child->keys.end());

        // Resize the old child to keep only the first half of its keys
        child->keys.resize(t - 1);

        if (!child->isLeaf)
        {
            // Assign the second half of the child pointers to the new child
            newChild->children.assign(child->children.begin() + t, child->children.end());

            // Resize the old child's children vector to keep only the first half
            child->children.resize(t);

            return;
        }

        // Maintain the linked list structure for leaf nodes
        newChild->next = child->next;
        child->next = newChild;

        // newChild->data.assign(child->data.begin() + t, child->data.end());
        // child->data.resize(t);

        splitLeaves->push_back(pair<Node *, Node *>(child, newChild));
    }

    Node *BPlusTree::FindAppropriateNodeForInsert(const Key &key, int *indexPosition, vector<pair<Node *, Node *>> *splitLeaves)
    {
        if (root->keys.size() == 2 * t - 1) // root is full,
        {
            // create new root
            Node *newRoot = new Node();

            // add current root as leaf
            newRoot->children.push_back(root);

            // split the root
            this->SplitChild(newRoot, 0, root, splitLeaves);

            // root is the newRoot
            root = newRoot;
        }

        return this->GetNonFullNode(root, key, indexPosition, splitLeaves);
    }

    Node *BPlusTree::GetNonFullNode(Node *node, const Key &key, int *indexPosition, vector<pair<Node *, Node *>> *splitLeaves)
    {
        if (node->isLeaf)
        {
            const auto iterator = ranges::upper_bound(node->keys, key, BPlusTree::IsKeyLessThan);

            const int indexPos = iterator - node->keys.begin();

            if (!node->keys.empty() && ((node->keys.size() > indexPos && BPlusTree::IsKeyEqual(key, node->keys[indexPos])) || BPlusTree::IsKeyEqual(key, node->keys[indexPos - 1])))
            {
                ostringstream oss;

                oss << "BPlusTree::GetNonFullNode: Key " << key.value << " already exists";

                throw invalid_argument(oss.str());
            }

            if (indexPosition != nullptr)
                *indexPosition = indexPos;

            return node;
        }

        const auto iterator = ranges::lower_bound(node->keys, key, BPlusTree::IsKeyLessThan);

        int childIndex = iterator - node->keys.begin();

        Node *child = node->children[childIndex];

        if (child->keys.size() == 2 * t - 1)
        {
            SplitChild(node, childIndex, child, splitLeaves);

            if (BPlusTree::IsKeyGreaterThan(key, node->keys[childIndex]))
                childIndex++;
        }

        return GetNonFullNode(node->children[childIndex], key, indexPosition, splitLeaves);
    }

    void BPlusTree::DeleteNode(Node *node)
    {
        if (!node)
            return;

        if (!node->isLeaf)
            for (const auto &child : node->children)
                this->DeleteNode(child);

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

        const Node *currentNode = this->SearchKey(minKey);
        const Node *previousNode = nullptr;

        while (currentNode)
        {
            if (previousNode && BPlusTree::IsKeyGreaterOrEqual(maxKey, currentNode->keys[0]))
            {
                if (BPlusTree::IsKeyGreaterOrEqual(maxKey, previousNode->keys[previousNode->keys.size() - 1]))
                    result.emplace_back(previousNode->data, previousNode->keys.size());
            }

            for (int i = 0; i < currentNode->keys.size(); i++)
            {
                const auto &key = currentNode->keys[i];

                if (BPlusTree::IsKeyLessOrEqual(minKey, key) && BPlusTree::IsKeyGreaterOrEqual(maxKey, key))
                {
                    result.emplace_back(currentNode->data, i);
                    continue;
                }

                if (BPlusTree::IsKeyLessThan(maxKey, key))
                    return;
            }

            previousNode = currentNode;
            currentNode = currentNode->next;
        }
    }

    void BPlusTree::SearchKey(const Key &key, QueryData &result) const
    {
        if (!root)
            return;

        Node *currentNode = root;

        while (!currentNode->isLeaf)
        {
            const auto iterator = ranges::lower_bound(currentNode->keys, key, BPlusTree::IsKeyLessThan);

            const int index = iterator - currentNode->keys.begin();

            currentNode = currentNode->children[index];
        }

        const Node *previousNode = nullptr;
        while (currentNode)
        {
            if (previousNode && BPlusTree::IsKeyLessThan(key, currentNode->keys[0]))
            {
                result.treeData = previousNode->data;
                result.indexPosition = previousNode->keys.size();
                return;
            }

            for (int i = 0; i < currentNode->keys.size(); i++)
            {
                if (BPlusTree::IsKeyEqual(key, currentNode->keys[i]))
                {

                    result.treeData = currentNode->data;
                    result.indexPosition = i;
                    return;
                }

                if (BPlusTree::IsKeyGreaterThan(key, currentNode->keys[i]))
                    return;
            }

            previousNode = currentNode;
            currentNode = currentNode->next;
        }
    }

    page_size_t BPlusTree::GetTreeSize() const
    {
        page_size_t result = 0;

        GetNodeSize(root, result);

        return result;
    }

    Node *BPlusTree::GetRoot() { return this->root; }

    void BPlusTree::SetRoot(Node *&node) { this->root = node; }

    void BPlusTree::SetBranchingFactor(const int &branchingFactor) { this->t = branchingFactor; }

    const int &BPlusTree::GetBranchingFactor() const { return this->t; }

    void BPlusTree::WriteTreeHeaderToFile(fstream *filePtr)
    {
        filePtr->write(reinterpret_cast<char *>(&this->t), sizeof(int));
        filePtr->write(reinterpret_cast<char *>(&this->tableId), sizeof(table_id_t));
    }

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

        for (const auto &child : node->children)
            GetNodeSize(child, size);
    }

    Node *BPlusTree::SearchKey(const Key &key) const
    {
        Node *currentNode = root;

        while (!currentNode->isLeaf)
        {
            const auto iterator = ranges::lower_bound(currentNode->keys, key, BPlusTree::IsKeyLessThan);

            const int index = iterator - currentNode->keys.begin();

            currentNode = currentNode->children[index];
        }

        return currentNode;
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
            cout << key.value << " ";
        if (node->isLeaf)
        {
            cout << "Data: ";
            cout << node->data << " ";
        }

        cout << "\n";

        // Recursively print children, if any
        if (!node->isLeaf)
            for (const auto &childNode : node->children)
                PrintTree(childNode, level + 1);
    }

    bool BPlusTree::IsKeyLessThan(const Key &searchKey, const Key &sortedKey)
    {
        if (searchKey.size < sortedKey.size)
            return true;

        if (searchKey.size > sortedKey.size)
            return false;

        return memcmp(searchKey.value, sortedKey.value, searchKey.size) < 0;
    }

    bool BPlusTree::IsKeyGreaterThan(const Key &searchKey, const Key &sortedKey)
    {
        if (searchKey.size > sortedKey.size)
            return true;

        if (searchKey.size < sortedKey.size)
            return false;

        return memcmp(searchKey.value, sortedKey.value, searchKey.size) > 0;
    }

    bool BPlusTree::IsKeyEqual(const Key &searchKey, const Key &sortedKey)
    {
        return searchKey.size == sortedKey.size && memcmp(searchKey.value, sortedKey.value, searchKey.size) == 0;
    }

    bool BPlusTree::IsKeyGreaterOrEqual(const Key &searchKey, const Key &sortedKey)
    {
        if (searchKey.size > sortedKey.size)
            return true;

        if (searchKey.size < sortedKey.size)
            return false;

        return memcmp(searchKey.value, sortedKey.value, searchKey.size) >= 0;
    }

    bool BPlusTree::IsKeyLessOrEqual(const Key &searchKey, const Key &sortedKey)
    {
        if (searchKey.size < sortedKey.size)
            return true;

        if (searchKey.size > sortedKey.size)
            return false;

        return memcmp(searchKey.value, sortedKey.value, searchKey.size) <= 0;
    }

    Key::Key()
    {
        this->value = nullptr;
        this->size = 0;
    }

    Key::Key(const void *keyValue, const key_size_t &keySize)
    {
        this->value = new object_t[keySize];

        memcpy(this->value, keyValue, keySize);

        this->size = keySize;
    }

    Key::~Key()
    {
        delete this->value;
    }

    Key::Key(const Key &otherKey)
    {
        this->value = new object_t[otherKey.size];
        memcpy(this->value, otherKey.value, otherKey.size);

        this->size = otherKey.size;
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
}
