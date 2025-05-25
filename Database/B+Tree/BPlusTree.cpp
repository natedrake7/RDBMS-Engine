#include "BPlusTree.h"
#include <algorithm>
#include <cstring>
#include <ctime>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <cstring>
#include "../../Database/Table/Table.h"
#include "../../Database/Pages/IndexPage/IndexPage.h"
#include "../../Database/Storage/StorageManager/StorageManager.h"
#include "../../Database/Column/Column.h"
#include "../../AdditionalLibraries/AdditionalDataTypes/Decimal/Decimal.h"
#include "../Database.h"
#include "../Row/Row.h"

using namespace std;
using namespace DatabaseEngine::StorageTypes;
using namespace Pages;
using namespace Storage;
using namespace DataTypes;

namespace Indexing
{

    Node::Node(const bool &isLeaf, const bool& isRoot, const bool& isNodeClustered)
    {
        this->isLeaf = isLeaf;
        this->isRoot = isRoot;
        this->isNodeClustered = isNodeClustered;
        this->currentNodeSize = this->GetNodeSize();
        this->prevNodeSize = this->GetNodeSize();
        this->dataPageId = 0;
    }

    page_size_t Node::GetNodeSize() const
    {
        //included self and parent header + prev and next
        page_size_t size = 2 * NodeHeader::GetNodeHeaderSize();

        //if node is leaf or not
        size += 2 * sizeof(bool);

        //num of keys to read for node
        size += sizeof(uint16_t);
        size += sizeof(uint8_t);

        for (const auto &key : this->keys)
            size += key.GetKeySize();

        if (this->isLeaf)
        {
            size += this->isNodeClustered 
                    ? sizeof(page_id_t) 
                    : ((this->nonClusteredData.size() * BPlusTreeNonClusteredData::GetNonClusteredDataSize())+ sizeof(uint16_t));

            size += 2 * NodeHeader::GetNodeHeaderSize();
        }
        else
            size += sizeof(uint16_t) + (this->childrenHeaders.size() * NodeHeader::GetNodeHeaderSize());

        return size;
    }

    Node::~Node() = default;

    BPlusTree::BPlusTree(Table *table, const page_id_t& indexPageId, const TreeType& treeType, const int& nonClusteredIndexId)
    {
        const auto &tableHeader = table->GetTableHeader();

        //handle degree here correctly based on indexed columns
        this->t = BPlusTree::CalculateTreeDegree(table, treeType, nonClusteredIndexId);
        this->root = nullptr;
        this->tableId = tableHeader.tableId;
        this->firstIndexPageId = indexPageId;
        this->type = treeType;
        this->database = table->GetDatabase();
        this->nonClusteredIndexId = nonClusteredIndexId;
        this->table = table;
    }

    BPlusTree::BPlusTree()
    {
        this->root = nullptr;
        this->t = 0;
        this->tableId = 0;
    }

    BPlusTree::~BPlusTree() = default;
    //{
    //    this->DeleteNode(root);
    //}

    int BPlusTree::CalculateTreeDegree(const Table* table, const TreeType& treeType, const int& nonClusteredIndexId)
    {
        if(treeType == TreeType::Clustered)
            return (PAGE_SIZE - PageHeader::GetPageHeaderSize()) / (table->GetMaximumRowSize() * 2);
        
        const vector<Column*>& columns = table->GetColumns();

        vector<vector<column_index_t>> nonClusteredIndexes;
        table->GetNonClusteredIndexedColumnKeys(&nonClusteredIndexes);

        int keySize = 0;
        for(const auto& key: nonClusteredIndexes[nonClusteredIndexId])
        {
            const Column* column = columns[key];

            keySize += column->GetColumnSize();
        }

        return (PAGE_SIZE - PageHeader::GetPageHeaderSize() - IndexPageAdditionalHeader::GetAdditionalHeaderSize()) 
                    / ((keySize + BPlusTreeNonClusteredData::GetNonClusteredDataSize() + 4 * NodeHeader::GetNodeHeaderSize()) * 2);
    }

    void BPlusTree::SplitChild(Node *parent, const int &index, Node *child)
    {
        Node *newChild = new Node(child->isLeaf, false, this->type == TreeType::Clustered);

        // Move the middle key from the child to the parent
        parent->keys.insert(parent->keys.begin() + index, child->keys[t - 1]);

        // Assign the second half of the child's keys to the new child
        newChild->keys.assign(child->keys.begin() + t, child->keys.end());

        // Resize the old child to keep only the first half of its keys
        child->keys.resize(t - 1);

        newChild->parentHeader = parent->header;
        
        this->InsertNodeToPage(newChild, parent->header.pageId);
        parent->childrenHeaders.insert(parent->childrenHeaders.begin() + index + 1, newChild->header);

        if (child->isLeaf)
        {
            if (this->type == TreeType::Clustered)
                this->database->SplitPage(child, newChild, this->t, this->tableId);
            else
            {
                newChild->nonClusteredData.assign(child->nonClusteredData.begin() + this->t, child->nonClusteredData.end());
                child->nonClusteredData.resize(this->t);
            }

            // Maintain the linked list structure for leaf nodes
            newChild->nextNodeHeader = child->nextNodeHeader;
            newChild->previousNodeHeader = child->header;
            child->nextNodeHeader = newChild->header;
        }
        else
        {
            // Assign the second half of the child pointers to the new child
            newChild->childrenHeaders.assign(child->childrenHeaders.begin() + t, child->childrenHeaders.end());

            // Resize the old child's childrenHeaders vector to keep only the first half
            child->childrenHeaders.resize(t);
        }

        this->database->UpdateNodeConnections(parent);
        this->database->UpdateNodeConnections(newChild);
        this->database->UpdateNodeConnections(child);

        // this->database->UpdateNodeConnections(newChild);
        //Check if pages are overflowed for parent, for child it will simply update the available bytes of the page
        this->database->SplitNodeFromIndexPage(tableId, parent, nonClusteredIndexId);
        this->database->SplitNodeFromIndexPage(tableId, child, nonClusteredIndexId);
        this->database->SplitNodeFromIndexPage(tableId, newChild, nonClusteredIndexId);
    }

    Node *BPlusTree::FindAppropriateNodeForInsert(const Key &key, int *indexPosition, AdditionalDataTypes::ResultStatus& status)
    {
        if (this->root == nullptr)
        {
            //maybe root page is removed and need to be reopened
            this->root = new Node(true, true, this->type == TreeType::Clustered);
            this->InsertNodeToPage(this->root, 0);
        }

        if (root->keys.size() == 2 * t - 1) // root is full,
        {
            // create new root
            Node *newRoot = new Node(false, true, this->type == TreeType::Clustered);
            this->InsertNodeToPage(newRoot, root->header.pageId);

            // add current root as leaf
            newRoot->childrenHeaders.push_back(this->root->header);
            this->root->isRoot = false;

            // split the root
            this->SplitChild(newRoot, 0, this->root);

            // root is the newRoot
            root = newRoot;
        }

        Node *node = this->GetNonFullNode(root, key, indexPosition, status);

        if (status.code != AdditionalDataTypes::ResultCode::Ok)
            return nullptr;

        return node;
    }

    Node *BPlusTree::GetNonFullNode(Node *node, const Key &key, int *indexPosition, AdditionalDataTypes::ResultStatus& status)
    {
        if (node->isLeaf)
        {
            const auto iterator = ranges::upper_bound(node->keys, key);

            const int indexPos = iterator - node->keys.begin();

            if (!node->keys.empty()
                && ((node->keys.size() > indexPos && key == node->keys[indexPos]) 
                || (indexPos > 0 && key == node->keys[indexPos - 1])))
            {
                const ostringstream oss;

                cerr << "BPlusTree::GetNonFullNode: Key " << key << " already exists";

                status.code = AdditionalDataTypes::ResultCode::DuplicateKey;
                status.message = oss.str();
                return nullptr;
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
            this->SplitChild(node, childIndex, child);

            if (key > node->keys[childIndex])
                childIndex++;
        }

        Node* returnedNode = this->GetNonFullNode(this->GetNodeFromPage(node->childrenHeaders[childIndex]), key, indexPosition, status);

        if (status.code != AdditionalDataTypes::ResultCode::Ok)
            return nullptr;
        
        return returnedNode;
    }

    void BPlusTree::DeleteNode(const Node *node)
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

    void BPlusTree::IndexScan(const Key &minKey, const Key &maxKey, vector<QueryData> &result) const
    {
        if (!root)
            return;

        Node *currentNode = this->SearchLeftMostLeafNode();
        const Node *previousNode = nullptr;

        while (currentNode)
        {
            if (previousNode && maxKey >= currentNode->keys[0])
            {
                if (maxKey >= previousNode->keys[previousNode->keys.size() - 1])
                    result.emplace_back(previousNode->dataPageId, previousNode->keys.size());
            }

            for (int i = 0; i < currentNode->keys.size(); i++)
            {
                const auto &key = currentNode->keys[i];

                if (minKey <= key && maxKey >= key)
                {
                    result.emplace_back(currentNode->dataPageId, i);
                    continue;
                }

                // if (maxKey < key)
                //     return;
            }

            if(currentNode->nextNodeHeader.pageId == 0)
                return;

            previousNode = currentNode;
            currentNode = this->GetNodeFromPage(currentNode->nextNodeHeader);
        }
    }

    void BPlusTree::IndexScan(vector<QueryData> &result) const
    {
        if (!root)
            return;

        Node *currentNode = this->SearchLeftMostLeafNode();
        const Node *previousNode = nullptr;

        while (currentNode)
        {
            for (int i = 0; i < currentNode->keys.size(); i++)
                result.emplace_back(currentNode->dataPageId, i);

            if(currentNode->nextNodeHeader.pageId == 0)
                return;

            previousNode = currentNode;
            currentNode = this->GetNodeFromPage(currentNode->nextNodeHeader);
        }
    }

    void BPlusTree::IndexSeek(const Key &minKey, const Key &maxKey, vector<QueryData> &result) const
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
                    result.emplace_back(previousNode->dataPageId, previousNode->keys.size());
            }

            for (int i = 0; i < currentNode->keys.size(); i++)
            {
                const auto &key = currentNode->keys[i];

                if (minKey <= key && maxKey >= key)
                {
                    result.emplace_back(currentNode->dataPageId, i);
                    continue;
                }

                if (maxKey < key)
                    return;
            }

            if(currentNode->nextNodeHeader.pageId == 0)
                return;

            previousNode = currentNode;
            currentNode = this->GetNodeFromPage(currentNode->nextNodeHeader);
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
                result.pageId = previousNode->dataPageId;
                result.indexPosition = previousNode->keys.size();
                return;
            }

            for (int i = 0; i < currentNode->keys.size(); i++)
            {
                if (key == currentNode->keys[i])
                {

                    result.pageId = currentNode->dataPageId;
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

    void BPlusTree::Remove(const Key &key){

        if (!this->root)
            return;

        Node *currentNode = this->SearchKey(key);

        int keyIndex = -1;
        for (int i = 0; i < currentNode->keys.size(); i++) {
            if (currentNode->keys[i] == key) {
                keyIndex = i;
                break;
            }
        }

        if (keyIndex == -1)
            return;

        currentNode->keys.erase(currentNode->keys.begin() + keyIndex);

        if (this->type == TreeType::NonClustered)
            currentNode->nonClusteredData.erase(currentNode->nonClusteredData.begin() + keyIndex);

        //get the index page and update it
        auto* indexPage = StorageManager::Get().GetIndexPage(this->database->GetFileName(), currentNode->header.pageId);
        indexPage->UpdateBytesLeft();
        indexPage->SetDirty();

        if (currentNode->keys.size() >= (t - 1 ) / 2)
            return;

        this->HandleUnderflow(currentNode);
    }

    void BPlusTree::HandleUnderflow(Node* node) {
        if (node->isRoot) {
            this->HandleRootUnderflow();
            return;
        }

        Node* parent = this->GetNodeFromPage(node->parentHeader);
        int index = -1;
        for (int i = 0; i < parent->childrenHeaders.size(); i++) {
            if (parent->childrenHeaders[i].pageId == node->header.pageId
                && parent->childrenHeaders[i].indexPosition == node->header.indexPosition) {
                index = i;
                break;
            }
        }

        if (index > 0 && this->TryBorrowFromLeftSibling(node, parent, index))
            return;

        if (index < parent->childrenHeaders.size() - 1
            && this->TryBorrowFromRightSibling(node, parent, index))
            return;

        //if borrowing failed merge nodes
        if (index > 0) {
            Node* leftSibling = this->GetNodeFromPage(parent->childrenHeaders[index - 1]);
            this->MergeNodes(leftSibling, node, parent, index - 1);

            return;
        }

        Node* rightSibling = this->GetNodeFromPage(parent->childrenHeaders[index + 1]);
        this->MergeNodes(node, rightSibling, parent, index);
    }

    void BPlusTree::HandleRootUnderflow() {
        if (this->root->keys.empty() && !this->root->childrenHeaders.empty()) {
            const Node* oldRoot = this->root;

            Node* newRoot = this->GetNodeFromPage(root->childrenHeaders[0]);

            newRoot->isRoot = true;
            newRoot->parentHeader = NodeHeader();

            auto* indexPage = StorageManager::Get().GetIndexPage(this->database->GetFileName(),
                                                                oldRoot->header.pageId);

            auto* nodes = indexPage->GetNodesUnsafe();

            indexPage->DeleteNode(oldRoot->header.indexPosition);

            for (int i = oldRoot->header.indexPosition; i < nodes->size(); i++)
                this->database->UpdateNodeConnections((*nodes)[i]);

            delete root;
            root = newRoot;

            indexPage->UpdateBytesLeft();
            indexPage->SetDirty();

            return;
        }

        if (!this->root->keys.empty())
            return;

        //else root is empty and delete it (no more index items should be available but just to be sure
        auto* indexPage = StorageManager::Get().GetIndexPage(this->database->GetFileName(),
                                                     root->header.pageId);

        auto* nodes = indexPage->GetNodesUnsafe();

        indexPage->DeleteNode(this->root->header.indexPosition);

        for (int i = root->header.indexPosition; i < nodes->size(); i++)
            this->database->UpdateNodeConnections((*nodes)[i]);

        delete root;
        this->root = nullptr; // Tree is now empty

        indexPage->SetDirty();
        indexPage->UpdateBytesLeft();
        indexPage->UpdatePageSize();
    }

    bool BPlusTree::TryBorrowFromLeftSibling(Node* node, Node* parent, const int& index)const{
        Node* sibling = this->GetNodeFromPage(parent->childrenHeaders[index - 1]);

        if (sibling->keys.size() <= (t - 1) / 2)
            return false;

        if (node->isLeaf) {
            node->keys.insert(node->keys.begin(), sibling->keys.back());

            if (this->type == TreeType::Clustered) {

                //insert last child from left sibling to the current page

                const auto nodeExtentId = DatabaseEngine::Database::CalculateExtentIdByPageId(node->dataPageId);
                const auto siblingExtentId = DatabaseEngine::Database::CalculateExtentIdByPageId(sibling->dataPageId);

                //update page
                auto* nodeDataPage = StorageManager::Get().GetPage(this->database->GetFileName(), node->dataPageId, nodeExtentId, this->table);
                auto* siblingDataPage = StorageManager::Get().GetPage(this->database->GetFileName(), sibling->dataPageId, siblingExtentId, this->table);

                vector<Row*>* nodeRows = nodeDataPage->GetDataRowsUnsafe();
                vector<Row*>* siblingRows = siblingDataPage->GetDataRowsUnsafe();

                if (!siblingRows->empty()) {
                    nodeRows->push_back(siblingRows->back());
                    siblingRows->pop_back();
                }

                siblingDataPage->UpdatePageSize();
                siblingDataPage->UpdateBytesLeft();
                siblingDataPage->SetDirty();

                nodeDataPage->UpdatePageSize();
                nodeDataPage->SetDirty();
                nodeDataPage->UpdateBytesLeft();
            }
            else {
                node->nonClusteredData.insert(node->nonClusteredData.begin(), sibling->nonClusteredData.back());
                sibling->nonClusteredData.pop_back();
            }

            sibling->keys.pop_back();

            parent->keys[index - 1] = node->keys[0];
        }
        else {
            // Move parent key down to node
            node->keys.insert(node->keys.begin(), parent->keys[index - 1]);
            // Move last key from left sibling up to parent
            parent->keys[index - 1] = sibling->keys.back();
            sibling->keys.pop_back();

            const auto& childHeader = sibling->childrenHeaders.back();

            // Move last child pointer
            node->childrenHeaders.insert(node->childrenHeaders.begin(),
                                      childHeader);

            auto* childIndexPage = StorageManager::Get().GetIndexPage(this->database->GetFileName(), childHeader.pageId);

            auto* childNode = childIndexPage->GetNodeByIndex(childHeader.indexPosition);

            childNode->parentHeader = node->header;

            childIndexPage->SetDirty();

            sibling->childrenHeaders.pop_back();
        }

        this->database->SplitNodeFromIndexPage(tableId, parent, nonClusteredIndexId);
        this->database->SplitNodeFromIndexPage(tableId, node, nonClusteredIndexId);
        this->database->SplitNodeFromIndexPage(tableId, sibling, nonClusteredIndexId);

        return true;
    }

    bool BPlusTree::TryBorrowFromRightSibling(Node *node, Node *parent, const int &index) const{
        Node* sibling = this->GetNodeFromPage(parent->childrenHeaders[index + 1]);

        if (sibling->keys.size() <= (t - 1) / 2)
            return false;

        if (node->isLeaf) {
            node->keys.insert(node->keys.begin(), sibling->keys.front());

            if (this->type == TreeType::Clustered) {
                const auto nodeExtentId = DatabaseEngine::Database::CalculateExtentIdByPageId(node->dataPageId);
                const auto siblingExtentId = DatabaseEngine::Database::CalculateExtentIdByPageId(sibling->dataPageId);

                //update page
                auto* nodeDataPage = StorageManager::Get().GetPage(this->database->GetFileName(), node->dataPageId, nodeExtentId, this->table);
                auto* siblingDataPage = StorageManager::Get().GetPage(this->database->GetFileName(), sibling->dataPageId, siblingExtentId, this->table);

                vector<Row*>* nodeRows = nodeDataPage->GetDataRowsUnsafe();
                vector<Row*>* siblingRows = siblingDataPage->GetDataRowsUnsafe();

                if (!siblingRows->empty()) {
                    nodeRows->push_back(siblingRows->front());
                    siblingRows->erase(siblingRows->begin());
                }

                siblingDataPage->UpdatePageSize();
                siblingDataPage->UpdateBytesLeft();
                siblingDataPage->SetDirty();

                nodeDataPage->UpdatePageSize();
                nodeDataPage->SetDirty();
                nodeDataPage->UpdateBytesLeft();
            }
            else {
                node->nonClusteredData.push_back(sibling->nonClusteredData.front());
                sibling->nonClusteredData.erase(sibling->nonClusteredData.begin());
            }

            sibling->keys.erase(sibling->keys.begin());

            parent->keys[index] = sibling->keys[0];
        }
        else {
            node->keys.push_back(parent->keys[index]);


            // Move first key from right sibling up to parent
            parent->keys[index] = sibling->keys.front();
            sibling->keys.erase(sibling->keys.begin());

            const auto& childHeader = sibling->childrenHeaders.front();

            // Move first child pointer
            node->childrenHeaders.push_back(childHeader);

            auto* childIndexPage = StorageManager::Get().GetIndexPage(this->database->GetFileName(), childHeader.pageId);

            auto* childNode = childIndexPage->GetNodeByIndex(childHeader.indexPosition);

            childNode->parentHeader = node->header;

            childIndexPage->SetDirty();

            sibling->childrenHeaders.erase(sibling->childrenHeaders.begin());
        }

        this->database->SplitNodeFromIndexPage(tableId, parent, nonClusteredIndexId);
        this->database->SplitNodeFromIndexPage(tableId, node, nonClusteredIndexId);
        this->database->SplitNodeFromIndexPage(tableId, sibling, nonClusteredIndexId);

        return true;
    }

    void BPlusTree::MergeNodes(Node *leftNode, Node *rightNode, Node *parent, int parentKeyIndex){
        if (leftNode->isLeaf) {
            leftNode->keys.insert(leftNode->keys.end(), rightNode->keys.begin(), rightNode->keys.end());

            if (this->type == TreeType::Clustered) {

                const auto leftPageExtentId = DatabaseEngine::Database::CalculateExtentIdByPageId(leftNode->dataPageId);
                const auto rightPageExtentId = DatabaseEngine::Database::CalculateExtentIdByPageId(rightNode->dataPageId);

                //update page
                auto* leftPage = StorageManager::Get().GetPage(this->database->GetFileName(), leftNode->dataPageId, leftPageExtentId, this->table);
                auto* rightPage = StorageManager::Get().GetPage(this->database->GetFileName(), rightNode->dataPageId, rightPageExtentId, this->table);

                vector<Row*>* leftRows = leftPage->GetDataRowsUnsafe();
                vector<Row*>* rightRows = rightPage->GetDataRowsUnsafe();

                leftRows->insert(leftRows->end(), rightRows->begin(), rightRows->end());

                rightRows->clear();

                leftPage->UpdatePageSize();
                leftPage->UpdateBytesLeft();
                leftPage->SetDirty();

                rightPage->UpdatePageSize();
                rightPage->SetDirty();
                rightPage->UpdateBytesLeft();
            }
            else {
                leftNode->nonClusteredData.insert(leftNode->nonClusteredData.end(), rightNode->nonClusteredData.begin(), rightNode->nonClusteredData.end());
                rightNode->nonClusteredData.clear();
            }

            leftNode->nextNodeHeader = rightNode->nextNodeHeader;
            if (rightNode->nextNodeHeader.pageId != 0) {
                Node* nextNode = this->GetNodeFromPage(rightNode->nextNodeHeader);
                nextNode->previousNodeHeader = leftNode->header;
            }
        }
        else {
            // Merge internal nodes
            leftNode->keys.push_back(parent->keys[parentKeyIndex]);
            leftNode->keys.insert(leftNode->keys.end(), rightNode->keys.begin(), rightNode->keys.end());

            leftNode->childrenHeaders.insert(leftNode->childrenHeaders.end(),
                                           rightNode->childrenHeaders.begin(),
                                           rightNode->childrenHeaders.end());

            for (const auto& childHeader : rightNode->childrenHeaders) {
                auto* childIndexPage = StorageManager::Get().GetIndexPage(this->database->GetFileName(), childHeader.pageId);

                auto* childNode = childIndexPage->GetNodeByIndex(childHeader.indexPosition);

                childNode->parentHeader = leftNode->header;

                childIndexPage->SetDirty();
            }
        }

        // Remove the parent key and right node pointer
        parent->keys.erase(parent->keys.begin() + parentKeyIndex);
        parent->childrenHeaders.erase(parent->childrenHeaders.begin() + parentKeyIndex + 1);

        // Remove the right node from its page
        auto* indexPage = StorageManager::Get().GetIndexPage(this->database->GetFileName(),
                                                            rightNode->header.pageId);
        auto* nodes = indexPage->GetNodesUnsafe();
        nodes->Remove(rightNode->header.indexPosition);

        // Update page connections

        // auto leftNodeHeader = leftNode->header;
        //
        // if ( rightNode->header.pageId == leftNode->header.pageId &&
        //     rightNode->header.indexPosition < leftNode->header.indexPosition)
        //     leftNodeHeader.indexPosition--;
        //
        // auto parentNodeHeader = parent->header;
        //
        // if ( rightNode->header.pageId == parent->header.pageId &&
        //     rightNode->header.indexPosition < parent->header.indexPosition)
        //     parentNodeHeader.indexPosition--;
        //
        // this->database->UpdateNodeConnectionsOnDelete(leftNode, rightNode, leftNodeHeader);
        // this->database->UpdateNodeConnectionsOnDelete(parent, rightNode, parentNodeHeader);

        // for (int i = rightNode->header.indexPosition; i < nodes->size(); i++) {
        //     auto* node = (*nodes)[i];
        //
        //     // if (node == leftNode
        //     //     || node == parent)
        //     //     continue;
        //
        //     const auto nodeHeader = NodeHeader(node->header.pageId, i);
        //
        //     this->database->UpdateNodeConnectionsOnDelete(node, rightNode, nodeHeader);
        //
        //     node->header = nodeHeader;
        // }

        indexPage->SetDirty();
        indexPage->UpdateBytesLeft();
        indexPage->UpdatePageSize();

        this->database->SplitNodeFromIndexPage(tableId, leftNode, nonClusteredIndexId);
        this->database->SplitNodeFromIndexPage(tableId, parent, nonClusteredIndexId);

        // Handle parent underflow if necessary
        if (parent->keys.size() < (t - 1) / 2 && !parent->isRoot)
            this->HandleUnderflow(parent);

        delete rightNode;
    }

    Node*& BPlusTree::GetRoot() { return this->root; }

    void BPlusTree::SetRoot(Node *&node) { this->root = node; }

    void BPlusTree::SetBranchingFactor(const int &branchingFactor) { this->t = branchingFactor; }

    const int &BPlusTree::GetBranchingFactor() const { return this->t; }

    void BPlusTree::WriteTreeHeaderToFile(fstream *filePtr) const
    {
        filePtr->write(reinterpret_cast<const char *>(&this->t), sizeof(int));
        filePtr->write(reinterpret_cast<const char *>(&this->tableId), sizeof(table_id_t));
    }

    void BPlusTree::SetTreeType(const TreeType & treeType) { this->type = treeType; }

    void BPlusTree::UpdateRowData(const Key& key, const BPlusTreeNonClusteredData& data) const
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
            size += sizeof(page_id_t); 

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

    Node *BPlusTree::SearchLeftMostLeafNode() const
    {
        Node *currentNode = root;
        
        while (!currentNode->isLeaf)
            currentNode = this->GetNodeFromPage(currentNode->childrenHeaders[0]);

        return currentNode;
    }

    void BPlusTree::InsertNodeToPage(Node*& node, const page_id_t& parentPageId)
    {
        IndexPage* indexPage = parentPageId == 0 
                                ? this->database->FindOrAllocateNextIndexPage(this->tableId, parentPageId, node->currentNodeSize, this->nonClusteredIndexId)
                                : StorageManager::Get().GetIndexPage(this->database->GetFileName(), parentPageId);

        //could only be set once and not multiple times but insignificant
        indexPage->InsertNode(node, &node->header.indexPosition);
        node->header.pageId = indexPage->GetPageId();
        
        this->firstIndexPageId = root->header.pageId;
        this->table->SetClusteredIndexPageId(root->header.pageId);
    }

    Node* BPlusTree::GetNodeFromPage(const NodeHeader & header) const
    {
        const extent_id_t extentId = DatabaseEngine::Database::CalculateExtentIdByPageId(header.pageId);

        const IndexPage* indexPage = StorageManager::Get().GetIndexPage(this->database->GetFileName(), header.pageId, extentId, this->table);

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
            cout << node->dataPageId << " ";
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
        this->type = Constants::ColumnType::Int;
        this->indexKeyPosition = -1;
        this->currentSearchKeyPosition = -1;
    }

    Key::Key(const void *keyValue, const key_size_t &keySize, const Constants::ColumnType& keyType)
    {
        this->value.resize(keySize);
        memcpy(this->value.data(), keyValue, keySize);

        this->size = keySize;
        this->type = keyType;
        this->indexKeyPosition = -1;
        this->currentSearchKeyPosition = -1;
    }

    Key::Key(const Field &field){
        const auto& keySize = field.GetSize();

        this->value.resize(keySize);
        memcpy(this->value.data(), field.GetRawData(), keySize);

        this->size = keySize;
        this->type = field.GetType();
        this->indexKeyPosition = -1;
        this->currentSearchKeyPosition = -1;
    }

    Key::Key(const vector<Key> &subKeys)
    {
        this->size = 0;
        for (const auto &key : subKeys)
        {
            this->subKeys.push_back(key);
            this->size += key.size;
        }
        this->type = Constants::ColumnType::Int;
        this->indexKeyPosition = -1;
        this->currentSearchKeyPosition = -1;
    }

    Key::~Key() = default;

    Key::Key(const Key &otherKey)
    {
        this->type = otherKey.type;
        this->size = otherKey.size;

        if(otherKey.subKeys.empty())
        {
            this->value = otherKey.value;
            return;
        }

        this->subKeys = otherKey.subKeys;
        this->indexKeyPosition = -1;
        this->currentSearchKeyPosition = -1;

        //key is not composite
        // memcpy(this->value, otherKey.value, otherKey.size);

    }

    void Key::InsertKey(const Key &otherKey)
    {
        this->size += (otherKey.size + sizeof(key_size_t));

        this->subKeys.push_back(otherKey);
    }

    bool Key::operator>(const Key& otherKey) const
    {
        if(!this->subKeys.empty())
            return this->CompareCompositeKeys(otherKey) > 0;
        
        switch (this->type) 
        {
            case Constants::ColumnType::TinyInt:
                return *reinterpret_cast<const int8_t*>(this->value.data()) > *reinterpret_cast<const int8_t*>(otherKey.value.data());
            case Constants::ColumnType::SmallInt:
                return *reinterpret_cast<const int16_t*>(this->value.data()) > *reinterpret_cast<const int16_t*>(otherKey.value.data());
            case Constants::ColumnType::Int:
                return *reinterpret_cast<const int32_t*>(this->value.data()) > *reinterpret_cast<const int32_t*>(otherKey.value.data());
            case Constants::ColumnType::BigInt:
                return *reinterpret_cast<const int64_t*>(this->value.data()) > *reinterpret_cast<const int64_t*>(otherKey.value.data());
            case Constants::ColumnType::String:
            case Constants::ColumnType::UnicodeString:
            {
                if (otherKey.size > this->size)
                    return true;

                if (otherKey.size < this->size)
                    return false;

                return memcmp(otherKey.value.data(), this->value.data(), otherKey.size) > 0;
            }
            case Constants::ColumnType::Decimal:
                return Decimal(this->value.data(), this->size) > Decimal(otherKey.value.data(), otherKey.size);
            case Constants::ColumnType::Bool:
                return *reinterpret_cast<const bool*>(this->value.data()) > *reinterpret_cast<const bool*>(otherKey.value.data());
            case Constants::ColumnType::DateTime:
                return *reinterpret_cast<const time_t*>(this->value.data()) > *reinterpret_cast<const time_t*>(otherKey.value.data());
            case Constants::ColumnType::ColumnTypeCount: 
            default:
                throw invalid_argument("> Invalid DataType for Key");
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
        if(!this->subKeys.empty())
            return this->CompareCompositeKeys(otherKey) >= 0;
        
        switch (this->type) 
        {
            case Constants::ColumnType::TinyInt:
                return *reinterpret_cast<const int8_t*>(this->value.data()) >= *reinterpret_cast<const int8_t*>(otherKey.value.data());
            case Constants::ColumnType::SmallInt:
                return *reinterpret_cast<const int16_t*>(this->value.data()) >= *reinterpret_cast<const int16_t*>(otherKey.value.data());
            case Constants::ColumnType::Int:
                return *reinterpret_cast<const int32_t*>(this->value.data()) >= *reinterpret_cast<const int32_t*>(otherKey.value.data());
            case Constants::ColumnType::BigInt:
                return *reinterpret_cast<const int64_t*>(this->value.data()) >= *reinterpret_cast<const int64_t*>(otherKey.value.data());
            case Constants::ColumnType::String:
            case Constants::ColumnType::UnicodeString:
            {
                if (otherKey.size > this->size)
                    return true;

                if (otherKey.size < this->size)
                    return false;

                return memcmp(otherKey.value.data(), this->value.data(), otherKey.size) >= 0;
            }
            case Constants::ColumnType::Decimal:
                return Decimal(this->value.data(), this->size) >= Decimal(otherKey.value.data(), otherKey.size);
            case Constants::ColumnType::Bool:
                return *reinterpret_cast<const bool*>(this->value.data()) >= *reinterpret_cast<const bool*>(otherKey.value.data());
            case Constants::ColumnType::DateTime:
                return *reinterpret_cast<const time_t*>(this->value.data()) >= *reinterpret_cast<const time_t*>(otherKey.value.data());
            case Constants::ColumnType::ColumnTypeCount: 
            default:
                throw invalid_argument(">= Invalid DataType for Key");
        }
    }

    int Key::GetKeySize() const
    {
        return this->size;
    }

    int Key::CompareCompositeKeys(const Key& otherKey) const
    {
        // if (this->subKeys.size() != otherKey.subKeys.size())
        //     throw std::invalid_argument("Key::CompareCompositeKeys: Size mismatch");

        if(this->indexKeyPosition != -1)
            return Key::CompareSubKeys(this->subKeys[this->currentSearchKeyPosition], otherKey.subKeys[this->indexKeyPosition]);
        
        for (int i = 0; i < this->subKeys.size(); i++)
        {
            if (this->subKeys[i] == otherKey.subKeys[i])
                continue;

            if (this->subKeys[i] < otherKey.subKeys[i])
                return -1;

            return 1;
        }

        return 0;
    }

    int Key::CompareSubKeys(const Key& firstKey, const Key& otherKey)
    {
        if (firstKey == otherKey)
            return 0;

        if (firstKey < otherKey)
            return -1;

        return 1;
    }

    std::ostream & operator<<(std::ostream &os, const Key &key){
        if(!key.subKeys.empty())
        {
            os << "(";

            for (int i = 0; i < key.subKeys.size(); i++) {
                const auto& subKey = key.subKeys[i];
            
                os << subKey;
            
                if (i != key.subKeys.size() - 1)
                    os << ", ";
            }

            os << ")";
            
            return os;
        }

        switch (key.type) 
        {
            case Constants::ColumnType::TinyInt:
                os << *reinterpret_cast<const int8_t*>(key.value.data());
                break;
            case Constants::ColumnType::SmallInt:
                os << *reinterpret_cast<const int16_t*>(key.value.data());
                break;
            case Constants::ColumnType::Int:
                os << *reinterpret_cast<const int32_t*>(key.value.data());
                break;
            case Constants::ColumnType::BigInt:
                os << *reinterpret_cast<const int64_t*>(key.value.data());
                break;
            case Constants::ColumnType::String:
            case Constants::ColumnType::UnicodeString:
                os << reinterpret_cast<const char*>(key.value.data());
                break;
            case Constants::ColumnType::Decimal:
                os << Decimal(key.value.data(), key.size).ToString();
                break;
            case Constants::ColumnType::Bool:
                os << *reinterpret_cast<const bool*>(key.value.data());
                break;
            case Constants::ColumnType::DateTime:
                os << DateTime(*reinterpret_cast<const time_t*>(key.value.data())).ToString();
                break;
            case Constants::ColumnType::ColumnTypeCount: 
            default:
                throw invalid_argument("Invalid DataType for Key");
        }

        return os;
    }

    bool Key::operator==(const Key& otherKey) const
    {
        if(!this->subKeys.empty())
            return this->CompareCompositeKeys(otherKey) == 0;
        
        switch (this->type) 
        {
            case Constants::ColumnType::TinyInt:
                return *reinterpret_cast<const int8_t*>(this->value.data()) == *reinterpret_cast<const int8_t*>(otherKey.value.data());
            case Constants::ColumnType::SmallInt:
                return *reinterpret_cast<const int16_t*>(this->value.data()) == *reinterpret_cast<const int16_t*>(otherKey.value.data());
            case Constants::ColumnType::Int:
                return *reinterpret_cast<const int32_t*>(this->value.data()) == *reinterpret_cast<const int32_t*>(otherKey.value.data());
            case Constants::ColumnType::BigInt:
                return *reinterpret_cast<const int64_t*>(this->value.data()) == *reinterpret_cast<const int64_t*>(otherKey.value.data());
            case Constants::ColumnType::String:
            case Constants::ColumnType::UnicodeString:
                return otherKey.size == this->size && memcmp(otherKey.value.data(), this->value.data(), otherKey.size) == 0;
            case Constants::ColumnType::Decimal:
                return Decimal(this->value.data(), this->size) == Decimal(otherKey.value.data(), otherKey.size);
            case Constants::ColumnType::Bool:
                return *reinterpret_cast<const bool*>(this->value.data()) == *reinterpret_cast<const bool*>(otherKey.value.data());
            case Constants::ColumnType::DateTime:
                return *reinterpret_cast<const time_t*>(this->value.data()) == *reinterpret_cast<const time_t*>(otherKey.value.data());
            case Constants::ColumnType::ColumnTypeCount: 
            default:
                throw invalid_argument("== Invalid DataType for Key");
        }

    }

    QueryData::QueryData()
    {
        this->indexPosition = 0;
        this->pageId = 0;
    }

    QueryData::QueryData(const page_id_t &pageId, const page_offset_t &otherIndexPosition)
    {
        this->pageId = pageId;
        this->indexPosition = otherIndexPosition;
    }

    QueryData::~QueryData() = default;

    BPlusTreeNonClusteredData::BPlusTreeNonClusteredData()
    {
        this->index = 0;
        this->pageId = 0;
    }

    BPlusTreeNonClusteredData::BPlusTreeNonClusteredData(const page_id_t & pageId, const page_offset_t & index)
    {
        this->pageId = pageId;
        this->index = index;
    }

    BPlusTreeNonClusteredData::~BPlusTreeNonClusteredData() = default;

    page_size_t BPlusTreeNonClusteredData::GetNonClusteredDataSize() { return sizeof(page_id_t) + sizeof(page_offset_t); }

    NodeHeader::NodeHeader()
    {
        this->pageId = 0;
        this->indexPosition = 0;
    }

    NodeHeader::NodeHeader(const page_id_t & pageId, const page_offset_t & indexPosition)
    {
        this->pageId = pageId;
        this->indexPosition = indexPosition;
    }

    NodeHeader::NodeHeader(const NodeHeader & otherHeader)
    {
        this->pageId = otherHeader.pageId;
        this->indexPosition = otherHeader.indexPosition;
    }

    NodeHeader::~NodeHeader() = default;

    page_size_t NodeHeader::GetNodeHeaderSize() { return sizeof(page_id_t) + sizeof(page_offset_t); }
}
