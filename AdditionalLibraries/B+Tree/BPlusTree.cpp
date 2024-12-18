#include "BPlusTree.h"
#include <algorithm>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include "../../Database/Table/Table.h"

using namespace std;

namespace Indexing {
    
    static ostream& operator<<(ostream& os, const BPlusTreeData& dataObject)
    {
        os << dataObject.pageId << " " << dataObject.rowIndex;
        return os;
    }

    BPlusTreeData::BPlusTreeData()
    {
        this->pageId = 0;
        this->rowIndex = 0;
    }

    BPlusTreeData::BPlusTreeData(const page_id_t &pageId, const page_offset_t &rowIndex)
    {
        this->pageId = pageId;
        this->rowIndex = rowIndex;
    }

    BPlusTreeData::~BPlusTreeData() = default;

    BPlusTree::Node::Node(const bool& isLeaf)
    {
        this->isLeaf = isLeaf;
        this->next = nullptr;
        this->prev = nullptr;
    }

    BPlusTree::BPlusTree(const Table* table)
    {
        const auto& tableHeader = table->GetTableHeader();
        
        this->root = new Node(true);
        this->t = PAGE_SIZE / table->GetMaximumRowSize();
        this->tableId = tableHeader.tableId;
    }

    BPlusTree::~BPlusTree()
    {
        this->DeleteNode(root);   
    }

    void BPlusTree::SplitChild(Node* parent, const int& index, Node* child) const
    {
        Node* newChild = new Node(child->isLeaf);

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
        
        //new page should be created to store half the new values
        
        // Maintain the linked list structure for leaf nodes
        newChild->next = child->next;
        child->next = newChild;

        newChild->data.assign(child->data.begin() + t, child->data.end());
        child->data.resize(t);
    }

    void BPlusTree::Insert(const int& key, const BPlusTreeData& value)
    {
        if (root->keys.size() == 2 * t - 1) //root is full,
        {
            //create new root
            Node* newRoot = new Node();

            //add current root as leaf
            newRoot->children.push_back(root);

            //split the root
            this->SplitChild(newRoot, 0, root);

            //root is the newRoot
            root = newRoot;
        }
        
        this->InsertToNonFullNode(root, key, value);
    }

    void BPlusTree::InsertToNonFullNode(Node* node, const int& key, const BPlusTreeData& value)
    {
        if (node->isLeaf)
        {
            const auto iterator = upper_bound(node->keys.begin(), node->keys.end(), key);

            const int indexPosition = iterator - node->keys.begin();

            if (!node->keys.empty() && node->keys.size() > indexPosition 
                && node->keys[indexPosition] == key)
            {
                ostringstream oss;

                oss << "BPlusTree::InsertToNonFullNode: Key " << key << " already exists";
                
                throw invalid_argument(oss.str());
            }

            node->keys.insert(iterator, key);

            node->data.insert(node->data.begin() + indexPosition, value);
            
            return;
        }

        const auto iterator = lower_bound(node->keys.begin(), node->keys.end(), key);

        int childIndex = iterator - node->keys.begin();

        Node* child = node->children[childIndex];

        if (child->keys.size() == 2 * t - 1)
        {
            SplitChild(node, childIndex, child);

            if (key > node->keys[childIndex])
                childIndex++;
        }

        InsertToNonFullNode(node->children[childIndex], key, value);
    }

    void BPlusTree::DeleteNode(Node *node)
    {
        if (!node)
            return;

        if (!node->isLeaf)
            for (const auto& child : node->children)
                this->DeleteNode(child);

        delete node;
    }

    void BPlusTree::PrintTree()
    {
        this->PrintTree(root, 0);
    }

    void BPlusTree::RangeQuery(const int &minKey, const int &maxKey, vector<BPlusTreeData> &result) const
    {
        if (!root)
            return;

        const Node* currentNode = this->SearchKey(minKey);
        const Node* previousNode = nullptr;
        
        while (currentNode)
        {
            if (previousNode && currentNode->keys[0] <= maxKey)
            {
                const auto& lastDataObject = previousNode->data.back();
                
                if (previousNode->keys[previousNode->keys.size() - 1] <= maxKey)
                    result.push_back(lastDataObject);
            }
            
            for (int i = 0; i < currentNode->keys.size(); i++)
            {
                const int& key = currentNode->keys[i];

                if (key >= minKey && key <= maxKey)
                {
                    result.push_back(currentNode->data[i]);
                    continue;
                }

                if (key > maxKey)
                    return;
                
            }
            
            previousNode = currentNode;
            currentNode = currentNode->next;
        }
    }

    void BPlusTree::SearchKey(const int &key, BPlusTreeData &result) const
    {
        if (!root)
            return;
        
        Node* currentNode = root;

        while (!currentNode->isLeaf)
        {
            const auto iterator = lower_bound(currentNode->keys.begin(), currentNode->keys.end(), key);
            const int index = iterator - currentNode->keys.begin();

            currentNode = currentNode->children[index];
        }

        const Node* previousNode = nullptr;
        while (currentNode)
        {
            if (previousNode && currentNode->keys[0] > key)
            {
                result = previousNode->data[previousNode->keys.size()];
                return;
            }
            
            for (int i = 0; i < currentNode->keys.size(); i++)
            {
                if (key == currentNode->keys[i])
                {
                    result = currentNode->data[i];
                    return;
                }
                
                if (key < currentNode->keys[i])
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

    void BPlusTree::SetBranchingFactor(const int& branchingFractor) { this->t = branchingFractor; }

    void BPlusTree::GetNodeSize(Node *node, page_size_t &size) const
    {
        if (!node)
            return;

        for (const auto& data : node->data)
            size+= sizeof(BPlusTreeData);
        
        for (const auto& key : node->keys)
            size += sizeof(key);
        
        for (const auto& child : node->children)
            GetNodeSize(child, size);
    }


    BPlusTree::Node * BPlusTree::SearchKey(const int &key) const
    {
        Node* currentNode = root;

        while (!currentNode->isLeaf)
        {
            const auto iterator = lower_bound(currentNode->keys.begin(), currentNode->keys.end(), key);
            const int index = iterator - currentNode->keys.begin();

            currentNode = currentNode->children[index];
        }

        return currentNode;
    }

    void BPlusTree::PrintTree(Node* node, const int& level)
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
        for (const int& key : node->keys)
            cout << key << " ";
        if (node->isLeaf)
        {
            cout << "Data: ";
            for (const auto& dataObject : node->data)
                cout << dataObject << " ";
        }

        cout << "\n";

        // Recursively print children, if any
        if (!node->isLeaf)
            for (const auto& childNode : node->children)
                PrintTree(childNode, level + 1);
    }
}
