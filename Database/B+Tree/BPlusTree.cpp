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
#include "../Block/Block.h"
#include "../../AdditionalLibraries/BitMap/BitMap.h"

using namespace std;
using namespace DatabaseEngine::StorageTypes;
using namespace Pages;
using namespace Storage;
using namespace DataTypes;

namespace Indexing
{
    BPlusTree::BPlusTree(Table *table, const page_id_t& indexPageId, const TreeType& treeType, const int& nonClusteredIndexId)
    {
        const auto &tableHeader = table->GetTableHeader();

        //handle degree here correctly based on indexed columns
        this->keySize = table->CalculateIndexKeySize();
        this->t = BPlusTree::CalculateTreeDegree(table, treeType, nonClusteredIndexId);
        this->root = nullptr;
        this->tableId = tableHeader.tableId;
        this->tablePosition = tableHeader.ordinalPosition;
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
        this->keySize = 0;
    }

    BPlusTree::~BPlusTree() = default;
    //{
    //    this->DeleteNode(root);
    //}

    int BPlusTree::CalculateTreeDegree(const Table* table, const TreeType& treeType, const int& nonClusteredIndexId)
    {
        if(treeType == TreeType::Clustered){
          const uint32_t pageSize = PAGE_SIZE - PageHeader::GetPageHeaderSize() - IndexPageAdditionalHeader::GetAdditionalHeaderSize();

          auto rowSize = table->GetMaximumRowSize();
          int degree = static_cast<int>(pageSize / ((this->keySize + rowSize) * 2));

          while(degree < 2){
            rowSize = table->ReduceMaximumRowSize();

            degree = static_cast<int>(pageSize / ((this->keySize + rowSize) * 2));
          }

          return degree;
        }

//        const vector<Column*>& columns = table->GetColumns();
//
//        vector<vector<column_index_t>> nonClusteredIndexes;
//        table->GetNonClusteredIndexedColumnKeys(&nonClusteredIndexes);
//
//        int keySize = 0;
//        for(const auto& key: nonClusteredIndexes[nonClusteredIndexId])
//        {
//            const Column* column = columns[key];
//
//            keySize += column->GetColumnSize();
//        }
//
//        return (PAGE_SIZE - PageHeader::GetPageHeaderSize() - IndexPageAdditionalHeader::GetAdditionalHeaderSize())
//                    / (keySize + BPlusTreeNonClusteredData::GetNonClusteredDataSize());
    }

    void BPlusTree::SplitChild(IndexPage *parent, const int &index, IndexPage *child)
    {
        auto* newChild = this->AllocateNewPage(parent->GetPageId());

        newChild->SetIsLeaf(child->IsLeaf());
        newChild->SetIsRoot(false);
        newChild->SetTreeType(this->type);

        auto* childKeys = child->GetKeysUnsafe();

        auto* parentKeys = parent->GetKeysUnsafe();

        // Move the middle key from the child to the parent
        parentKeys->insert(parentKeys->begin() + index, (*childKeys)[t - 1]);

        auto* newChildKeys = newChild->GetKeysUnsafe();

        // Assign the second half of the child's keys to the new child
        newChildKeys->assign(childKeys->begin() + t, childKeys->end());

        // Resize the old child to keep only the first half of its keys
        childKeys->resize(t - 1);

        auto* parentChildren = parent->GetChildren();

        parentChildren->insert(parentChildren->begin() + index + 1, newChild->GetPageId());

        if (child->IsLeaf())
        {
            if (this->type == TreeType::Clustered) {
                auto* childRows = child->GetDataRowsUnsafe();

                auto* newChildRows = newChild->GetDataRowsUnsafe();

                newChildRows->assign(childRows->begin() + t, childRows->end());

                childRows->resize(t);
            }
            else
            {
                auto* childRows = child->GetNonClusteredDataUnsafe();

                auto* newChildRows = newChild->GetNonClusteredDataUnsafe();

                newChildRows->assign(childRows->begin() + t, childRows->end());

                childRows->resize(t);
            }

            newChild->SetNextPage(child->GetNextPage());
            newChild->SetPreviousPage(child->GetPageId());

            child->SetNextPage(newChild->GetPageId());

            child->UpdatePageSize();
            newChild->UpdatePageSize();
        }
        else
        {
            auto* newChildChildren = newChild->GetChildren();

            auto* childChildren = child->GetChildren();

            // Assign the second half of the child pointers to the new child
            newChildChildren->assign(childChildren->begin() + t, childChildren->end());

            // Resize the old child's childrenHeaders vector to keep only the first half
            childChildren->resize(t);
        }

        parent->UpdateBytesLeft();
        child->UpdateBytesLeft();
        newChild->UpdateBytesLeft();
    }

    Pages::IndexPage* BPlusTree::FindAppropriateNodeForInsert(const Key &key, int *indexPosition, AdditionalDataTypes::ResultStatus& status)
    {
        if (this->root == nullptr)
        {
            //maybe root page is removed and need to be reopened
            this->root = this->AllocateNewPage(INVALID_PAGE_ID);

            this->root->SetIsRoot(true);
            this->root->SetIsLeaf(true);
            this->root->SetTreeType(this->type);

            this->firstIndexPageId = this->root->GetPageId();

            if(this->nonClusteredIndexId != -1)
              this->table->SetNonClusteredIndexPageId(this->firstIndexPageId, this->nonClusteredIndexId);
            else
              this->table->SetClusteredIndexPageId(this->firstIndexPageId);

            // this->InsertNodeToPage(this->root, 0);
        }

        if (this->root->GetKeysUnsafe()->size() == 2 * t - 1) // root is full,
        {
            auto* newRoot = this->AllocateNewPage(this->firstIndexPageId);

            newRoot->SetIsRoot(true);
            newRoot->SetIsLeaf(false);
            newRoot->SetTreeType(this->type);

            newRoot->InsertChild(this->root->GetPageId());

            this->root->SetIsRoot(false);
            this->firstIndexPageId = newRoot->GetPageId();

            if(this->nonClusteredIndexId != -1)
              this->table->SetNonClusteredIndexPageId(this->firstIndexPageId, this->nonClusteredIndexId);
            else
              this->table->SetClusteredIndexPageId(this->firstIndexPageId);

            // split the root
            this->SplitChild(newRoot, 0, this->root);

            // root is the newRoot
            this->root = newRoot;
        }

        auto* node = this->GetNonFullNode(this->root, key, indexPosition, status);

        if (status.code != AdditionalDataTypes::ResultCode::Ok)
            return nullptr;

        return node;
    }

    Pages::IndexPage *BPlusTree::GetNonFullNode(Pages::IndexPage *node, const Key &key, int *indexPosition, AdditionalDataTypes::ResultStatus& status)
    {
        auto* keys = node->GetKeysUnsafe();

        if (node->IsLeaf())
        {
            const auto iterator = std::upper_bound(keys->begin(), keys->end(), &key);

            const int indexPos = iterator - keys->begin();

            if (!keys->empty()
                && ((keys->size() > indexPos && key == *keys->at(indexPos))
                || (indexPos > 0 && key == *keys->at(indexPos - 1))))
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
        
        const auto iterator = std::lower_bound(keys->begin(), keys->end(), &key);

        int childIndex = iterator - keys->begin();

        const auto* children = node->GetChildren();

        auto *child = this->GetNode(children->at(childIndex));

        auto* childKeys = child->GetKeysUnsafe();

        if (childKeys->size() == 2 * t - 1)
        {
            this->SplitChild(node, childIndex, child);

            if (key > *keys->at(childIndex))
                childIndex++;
        }

        auto* returnedNode = this->GetNonFullNode(this->GetNode(children->at(childIndex)), key, indexPosition, status);

        if (status.code != AdditionalDataTypes::ResultCode::Ok)
            return nullptr;
        
        return returnedNode;
    }

    void BPlusTree::IndexScan(const Key &minKey, const Key &maxKey, vector<QueryData> &result)
    {
        // if (!root)
        //     return;

        // auto *currentNode = this->SearchLeftMostLeafNode();
        // IndexPage *previousNode = nullptr;

        // while (currentNode)
        // {
        //     auto* keys = currentNode->GetKeysUnsafe();

        //     if (previousNode && maxKey >= *keys->at(0))
        //     {
        //         auto* previousKeys = previousNode->GetKeysUnsafe();

        //         if (maxKey >= *previousKeys->at(previousKeys->size() - 1)){

        //             previousNode->GetNonClusteredDataUnsafe()

        //             result.emplace_back(previousNode->dataPageId, previousNode->keys.size());

        //         }
        //     }

        //     for (int i = 0; i < currentNode->keys.size(); i++)
        //     {
        //         const auto &key = currentNode->keys[i];

        //         if (minKey <= key && maxKey >= key)
        //         {
        //             result.emplace_back(currentNode->dataPageId, i);
        //             continue;
        //         }

        //         // if (maxKey < key)
        //         //     return;
        //     }

        //     if(currentNode->nextNodeHeader.pageId == 0)
        //         return;

        //     previousNode = currentNode;
        //     currentNode = this->GetNodeFromPage(currentNode->nextNodeHeader);
        // }
    }

    void BPlusTree::IndexScan(vector<QueryData> &result)
    {
        if (!root)
            return;

        auto *currentNode = this->SearchLeftMostLeafNode();
        IndexPage *previousNode = nullptr;

        while (currentNode)
        {
            auto* keys = currentNode->GetKeysUnsafe();

            // for (int i = 0; i < keys->size(); i++)
            //     result.emplace_back(currentNode->dataPageId, i);

            if(currentNode->GetNextPage() == 0)
                return;

            previousNode = currentNode;
            currentNode = this->GetNode(currentNode->GetNextPage());
        }
    }

    void BPlusTree::IndexScan(vector<Row>* result){
        this->root = this->GetNode(this->firstIndexPageId);

        if (!this->root)
          return;

        auto *currentNode = this->SearchLeftMostLeafNode();

        int count = 0;
        while (currentNode)
        {
          for(auto* row: *currentNode->GetDataRowsUnsafe()){
            const RowHeader *rowHeader = row->GetHeader();

            vector<Block *> copyBlocks = row->GetBlockCopies();

            result->emplace_back(*table, copyBlocks, rowHeader->nullBitMap);
          }

          if(currentNode->GetNextPage() == 0)
              return;

          currentNode = this->GetNode(currentNode->GetNextPage());
        }

    }

    void BPlusTree::IndexScan(vector<DatabaseEngine::StorageTypes::Row> *result, Expressions::Expression *expression){
        this->root = this->GetNode(this->firstIndexPageId);

        if (!this->root)
          return;

        auto *currentNode = this->SearchLeftMostLeafNode();

        while (currentNode)
        {

          for(auto* row: *currentNode->GetDataRowsUnsafe()){
            if(!row->Evaluate(expression))
              continue;

            const RowHeader *rowHeader = row->GetHeader();

            vector<Block *> copyBlocks = row->GetBlockCopies();

            result->emplace_back(*table, copyBlocks, rowHeader->nullBitMap);
          }

          if(currentNode->GetNextPage() == 0)
              return;

          currentNode = this->GetNode(currentNode->GetNextPage());
      }
    }

    void BPlusTree::IndexScanUpdate(const Expressions::Expression *expression, const vector<Field> & updates){
        this->root = this->GetNode(this->firstIndexPageId);

        if (!this->root)
          return;

        HashSet<column_index_t> updatedColumns;

        for(const auto& update : updates)
          updatedColumns.Add(update.GetColumnIndex());

        auto *currentNode = this->SearchLeftMostLeafNode();

        while (currentNode)
        {
          for(auto* row: *currentNode->GetDataRowsUnsafe()){
            if(!row->Evaluate(expression))
              continue;

            this->table->HandleRowUpdate(currentNode, row, updates, updatedColumns, false);
          }

          if(currentNode->GetNextPage() == 0)
            return;

          currentNode = this->GetNode(currentNode->GetNextPage());
        }
    }

    void BPlusTree::IndexSeekUpdate(Expressions::Expression* expression, const Key* minKey, const Key* maxKey, const vector<Field> & updates){
        this->root = this->GetNode(this->firstIndexPageId);

        if (!this->root)
          return;

        HashSet<column_index_t> updatedColumns;
        auto *currentNode = this->SearchKey(*minKey);
        IndexPage *previousNode = nullptr;

        while (currentNode)
        {
          auto* keys = currentNode->GetKeysUnsafe();

          if (previousNode && maxKey >= keys->at(0))
          {
            auto* previousKeys = previousNode->GetKeysUnsafe();

            // Check if the last key in the previous node is within the range
            if (maxKey >= previousKeys->at(previousKeys->size() - 1)) {
                auto* previousRows = previousNode->GetDataRowsUnsafe();

                auto* row = previousRows->at(previousRows->size() - 1);

                if(row->Evaluate(expression))
                  this->table->HandleRowUpdate(previousNode, previousRows->at(previousRows->size() - 1), updates, updatedColumns, false);
            }
          }
          else if(maxKey < keys->at(0))
               return;

          auto* rows = currentNode->GetDataRowsUnsafe();

          for (int i = 0; i < keys->size(); i++)
          {
            const auto &key = keys->at(i);

            if (minKey <= key && maxKey >= key && rows->at(i)->Evaluate(expression))
            {
                this->table->HandleRowUpdate(previousNode, rows->at(i), updates, updatedColumns, false);
                continue;
            }

//            if (maxKey < *key && !previousNode)
//                return;
          }

          if(currentNode->GetNextPage() == 0)
            return;

          previousNode = currentNode;
          currentNode = this->GetNode(currentNode->GetNextPage());
        }
    }


    void BPlusTree::IndexSeek(const Key &minKey, const Key &maxKey, vector<QueryData> &result) const
    {
        if (!this->root)
            return;

        auto *currentNode = this->SearchKey(minKey);
        IndexPage *previousNode = nullptr;

        while (currentNode)
        {
            auto* keys = currentNode->GetKeysUnsafe();

            if (previousNode && maxKey >= *keys->at(0))
            {
                auto* previousKeys = previousNode->GetKeysUnsafe();

                // Check if the last key in the previous node is within the range
                // if (maxKey >= *previousKeys->at(previousKeys->size() - 1))
                //     result.emplace_back(previousNode->dataPageId, previousNode->keys.size());
            }

            for (auto key : *keys)
            {
                if (minKey <= *key && maxKey >= *key)
                {
                    // result.emplace_back(currentNode->dataPageId, i);
                    continue;
                }

                if (maxKey < *key)
                    return;
            }

            if(currentNode->GetNextPage() == 0)
                return;

            previousNode = currentNode;
            currentNode = this->GetNode(currentNode->GetNextPage());
        }
    }

    void BPlusTree::IndexSeek(const Key &minKey, const Key &maxKey, vector<DatabaseEngine::StorageTypes::Row> *result){
        this->root = this->GetNode(this->firstIndexPageId);

        if (!this->root)
            return;

        auto *currentNode = this->SearchKey(minKey);
        IndexPage *previousNode = nullptr;

        while (currentNode)
        {
            auto* keys = currentNode->GetKeysUnsafe();

            if (previousNode && maxKey >= *keys->at(0))
            {
                auto* previousKeys = previousNode->GetKeysUnsafe();

                // Check if the last key in the previous node is within the range
                if (maxKey >= *previousKeys->at(previousKeys->size() - 1)) {
                    previousNode->GetRowByIndex(result, *table, previousKeys->size() - 1);
                }
            }

            for (int i = 0; i < keys->size(); i++)
            {
                const auto &key = keys->at(i);

                if (minKey <= *key && maxKey >= *key)
                {
                    currentNode->GetRowByIndex(result, *table, i);
                    continue;
                }

//                if (maxKey < *key)
//                    return;
            }

            if(currentNode->GetNextPage() == 0)
                return;

            previousNode = currentNode;
            currentNode = this->GetNode(currentNode->GetNextPage());
        }
    }

    void BPlusTree::SearchKey(const Key &key, QueryData &result) const
    {
        if (!root)
            return;

        auto *currentNode = this->root;

        while (!currentNode->IsLeaf())
        {
            auto* keys = currentNode->GetKeysUnsafe();

            const auto iterator = std::lower_bound(keys->begin(), keys->end(), &key);

            const int index = iterator - keys->begin();

            auto* children = currentNode->GetChildren();

            currentNode = this->GetNode(children->at(index));
        }
        
        IndexPage *previousNode = nullptr;
        while (currentNode)
        {
            auto* keys = currentNode->GetKeysUnsafe();

            if (previousNode && key <= *keys->at(0))
            {
                // result.pageId = previousNode->dataPageId;
                // result.indexPosition = previousNode->keys.size();
                return;
            }

            for (int i = 0; i < keys->size(); i++)
            {
                if (key == *keys->at(i))
                {

                    // result.pageId = currentNode->dataPageId;
                    // result.indexPosition = i;
                    return;
                }
            }

            previousNode = currentNode;
            currentNode = this->GetNode(currentNode->GetNextPage());
        }
    }

    void BPlusTree::Remove(const Key &key){

      if (!this->root)
        return;

      std::vector<Pages::IndexPage*> ancestors;
      auto *currentNode = this->SearchKeyWithAncestors(key, ancestors);

      auto* keys = currentNode->GetKeysUnsafe();

      int keyIndex = -1;

      for (int i = 0; i < keys->size(); i++) {
        if (*keys->at(i) == key) {
          keyIndex = i;
          break;
        }
      }

      if (keyIndex == -1)
        return;

      keys->erase(keys->begin() + keyIndex);

      if (this->type == TreeType::NonClustered){
        auto* nonClusteredData = currentNode->GetNonClusteredDataUnsafe();

        nonClusteredData->erase(nonClusteredData->begin() + keyIndex);
      }
      else{
            auto* rows = currentNode->GetDataRowsUnsafe();
            Row* row = rows->at(keyIndex);

            rows->erase(rows->begin() + keyIndex);

            delete row;
      }

      currentNode->UpdatePageSize();
      currentNode->UpdateBytesLeft();

      if (keys->size() >= (t - 1 ) / 2)
        return;

      int parentIndex = ancestors.size() - 1;
      this->HandleUnderflow(currentNode, ancestors, parentIndex);
   }

   void BPlusTree::HandleUnderflow(Pages::IndexPage* node, const std::vector<Pages::IndexPage*>& ancestors, int& parentIndex) {
        if (node->IsRoot()) {
           this->HandleRootUnderflow();
           return;
        }

        auto* parent = ancestors.at(parentIndex);

        int index = -1;

        auto* children = parent->GetChildren();

        for (int i = 0; i < children->size(); i++) {
         if (children->at(i) == node->GetPageId()) {
             index = i;
             break;
         }
       }

       if (index > 0 && this->TryBorrowFromLeftSibling(node, parent, index))
           return;

       if (index < children->size() - 1
           && this->TryBorrowFromRightSibling(node, parent, index))
           return;

       //if borrowing failed merge nodes
       if (index > 0) {
           auto* leftSibling = this->GetNode(children->at(index - 1));
           this->MergeNodes(leftSibling, node, parent, index - 1, ancestors, parentIndex);

           return;
       }

      auto* rightSibling = this->GetNode(children->at(index + 1));
      this->MergeNodes(node, rightSibling, parent, index, ancestors, parentIndex);
  }

    void BPlusTree::HandleRootUnderflow() {
        auto* keys = this->root->GetKeysUnsafe();
        auto* children = this->root->GetChildren();

        //root only has one child, delete current root and make child root
         if (keys->empty() && !children->empty()) {
              const auto* oldRoot = this->root;

              auto* newRoot = this->GetNode(children->at(0));

              newRoot->SetIsRoot(true);

              this->root->MarkEmpty();

              this->root = newRoot;

             return;
         }

         if (!keys->empty())
             return;

         //else root is empty and delete it (no more index items should be available but just to be sure

        this->root->MarkEmpty();
        this->root = nullptr; // Tree is now empty
    }

    bool BPlusTree::TryBorrowFromLeftSibling(IndexPage* node, IndexPage* parent, const int& index)const{
        auto* children = parent->GetChildren();

        auto* sibling = this->GetNode(children->at(index - 1));

        auto* siblingKeys = sibling->GetKeysUnsafe();

         if (siblingKeys->size() <= (t - 1) / 2)
             return false;

        auto* parentKeys = parent->GetKeysUnsafe();
        auto* nodeKeys = node->GetKeysUnsafe();

         if (node->IsLeaf()) {
            //get from left sibling the last key
            nodeKeys->insert(nodeKeys->begin(), siblingKeys->back());

             if (this->type == TreeType::Clustered) {

              //insert last child from left sibling to the current page
              auto* nodeRows = node->GetDataRowsUnsafe();
              auto* siblingRows = sibling->GetDataRowsUnsafe();

              if (!siblingRows->empty()) {
                   nodeRows->push_back(siblingRows->back());
                   siblingRows->pop_back();
               }
             }
             else {
              auto* nodeNonClusteredData = node->GetNonClusteredDataUnsafe();
              auto* siblingNonClusteredData = sibling->GetNonClusteredDataUnsafe();

              if(!siblingNonClusteredData->empty()){
                nodeNonClusteredData->insert(nodeNonClusteredData->begin(), siblingNonClusteredData->back());
                siblingNonClusteredData->pop_back();
              }
             }

            siblingKeys->pop_back();

            parentKeys->at(index - 1) = nodeKeys->front();
         }
         else {
            // Move parent key down to node
            nodeKeys->insert(nodeKeys->begin(), parentKeys->at(index - 1));

            // Move last key from left sibling up to parent
            parentKeys->at(index - 1) = siblingKeys->back();
            siblingKeys->pop_back();
         }

        node->UpdatePageSize();
        node->UpdateBytesLeft();

        sibling->UpdatePageSize();
        sibling->UpdateBytesLeft();

        parent->UpdatePageSize();
        parent->UpdateBytesLeft();

        return true;
    }

    bool BPlusTree::TryBorrowFromRightSibling(IndexPage *node, IndexPage *parent, const int &index) const{

          auto* children = parent->GetChildren();

          auto* sibling = this->GetNode(children->at(index + 1));

          auto* siblingKeys = sibling->GetKeysUnsafe();

         if (siblingKeys->size() <= (t - 1) / 2)
             return false;

          auto* nodeKeys = node->GetKeysUnsafe();
          auto* parentKeys = parent->GetKeysUnsafe();

         if (node->IsLeaf()) {
             nodeKeys->insert(nodeKeys->begin(), siblingKeys->front());

             if (this->type == TreeType::Clustered) {
                 vector<Row*>* nodeRows = node->GetDataRowsUnsafe();
                 vector<Row*>* siblingRows = sibling->GetDataRowsUnsafe();

                 if (!siblingRows->empty()) {
                     nodeRows->push_back(siblingRows->front());
                     siblingRows->erase(siblingRows->begin());
                 }
             }
             else {
              auto* nodeNonClusteredData = node->GetNonClusteredDataUnsafe();
              auto* siblingNonClusteredData = sibling->GetNonClusteredDataUnsafe();

              if(!siblingNonClusteredData->empty()){
                nodeNonClusteredData->push_back(siblingNonClusteredData->front());
                siblingNonClusteredData->erase(siblingNonClusteredData->begin());
              }
             }

              siblingKeys->erase(siblingKeys->begin());

              parentKeys->at(index) = nodeKeys->front();
         }
         else {

            nodeKeys->push_back(parentKeys->at(index));
            parentKeys->at(index) = siblingKeys->front();

             // Move first key from right sibling up to parent
            siblingKeys->erase(siblingKeys->begin());
         }

        node->UpdatePageSize();
        node->UpdateBytesLeft();

        sibling->UpdatePageSize();
        sibling->UpdateBytesLeft();

        parent->UpdatePageSize();
        parent->UpdateBytesLeft();

        return true;
    }

    void BPlusTree::MergeNodes(
        IndexPage *leftNode,
        IndexPage *rightNode,
        IndexPage *parent,
        int parentKeyIndex,
        const std::vector<Pages::IndexPage*>& ancestors,
        int& parentIndex){
          auto* leftNodeKeys = leftNode->GetKeysUnsafe();
          auto* rightNodeKeys = rightNode->GetKeysUnsafe();

          auto* parentKeys = parent->GetKeysUnsafe();
         if (leftNode->IsLeaf()) {
              leftNodeKeys->insert(leftNodeKeys->end(), rightNodeKeys->begin(), rightNodeKeys->end());

             if (this->type == TreeType::Clustered) {
                auto* leftNodeRows = leftNode->GetDataRowsUnsafe();
                auto* rightNodeRows = rightNode->GetDataRowsUnsafe();

                leftNodeRows->insert(leftNodeRows->end(), rightNodeRows->begin(), rightNodeRows->end());
                rightNodeRows->clear();
             }
             else {
                auto* leftNodeNonClusteredData = leftNode->GetNonClusteredDataUnsafe();
                auto* rightNodeNonClusteredData = rightNode->GetNonClusteredDataUnsafe();

                leftNodeNonClusteredData->insert(leftNodeNonClusteredData->end(), rightNodeNonClusteredData->begin(), rightNodeNonClusteredData->end());
                leftNodeNonClusteredData->clear();
             }

              leftNode->SetNextPage(rightNode->GetNextPage());


              if(rightNode->GetNextPage() != 0){
                auto* nextNode = this->GetNode(rightNode->GetNextPage());
                nextNode->SetPreviousPage(leftNode->GetPageId());
              }
         }
         else {
             // Merge internal nodes
              leftNodeKeys->push_back(parentKeys->at(parentKeyIndex));

              parentKeys->erase(parentKeys->begin() + parentKeyIndex);

              leftNodeKeys->insert(leftNodeKeys->end(), rightNodeKeys->begin(), rightNodeKeys->end());

              auto* leftNodeChildren = leftNode->GetChildren();
              auto* rightNodeChildren = rightNode->GetChildren();

              leftNodeChildren->insert(leftNodeChildren->end(), rightNodeChildren->begin(), rightNodeChildren->end());
         }

         // Remove the parent key and right node pointer
          auto* parentChildrenHeaders = parent->GetChildren();
          parentChildrenHeaders->erase(parentChildrenHeaders->begin() + parentKeyIndex + 1);

          rightNode->MarkEmpty();

          leftNode->UpdatePageSize();
          leftNode->UpdateBytesLeft();

          parent->UpdatePageSize();
          parent->UpdateBytesLeft();

         // Handle parent underflow if necessary
         if (parentKeys->size() < (t - 1) / 2 && !parent->IsRoot()){
            parentIndex--;
            this->HandleUnderflow(parent, ancestors, parentIndex);
          }

         delete rightNode;
    }

    IndexPage*& BPlusTree::GetRoot() { return this->root; }

    void BPlusTree::SetRoot(IndexPage *&node) { this->root = node; }

    void BPlusTree::SetBranchingFactor(const int &branchingFactor) { this->t = branchingFactor; }

    const int &BPlusTree::GetBranchingFactor() const { return this->t; }

    void BPlusTree::SetTreeType(const TreeType & treeType) { this->type = treeType; }

    void BPlusTree::UpdateRowData(const Key& key, const Headers::RowIdentifier& data) const
    {
        // auto* currentNode = this->SearchKey(key);

        // if(currentNode == nullptr)
        //     return;

        // IndexPage *previousNode = nullptr;
        // while (currentNode)
        // {

        //     auto* keys = currentNode->GetKeysUnsafe();

        //     if (previousNode && key <= currentNode->keys[0])
        //     {
        //         previousNode->nonClusteredData[previousNode->keys.size()] = data;
        //         return;
        //     }

        //     for (int i = 0; i < currentNode->keys.size(); i++)
        //     {
        //         if (key == currentNode->keys[i])
        //         {
        //             currentNode->nonClusteredData[i] = data;
        //             return;
        //         }
        //     }

        //     previousNode = currentNode;
        //     currentNode = this->GetNodeFromPage(currentNode->nextNodeHeader);
        // }
    }

    const page_id_t & BPlusTree::GetFirstIndexPageId() const { return this->firstIndexPageId; }

    Pages::IndexPage *BPlusTree::SearchKey(const Key &key) const
    {
        auto *currentNode = this->root;

        while (!currentNode->IsLeaf())
        {
            auto* keys = currentNode->GetKeysUnsafe();

            const auto iterator = std::lower_bound(keys->begin(), keys->end(), &key);

            const int index = iterator - keys->begin();

            currentNode = this->GetNode(currentNode->GetChildren()->at(index));
        }

        return currentNode;
    }

    Pages::IndexPage* BPlusTree::SearchKeyWithAncestors(const Key & key, vector<Pages::IndexPage *> & ancestors) const{
      auto *currentNode = this->root;

      while (!currentNode->IsLeaf())
      {
        auto* keys = currentNode->GetKeysUnsafe();

        const auto iterator = std::lower_bound(keys->begin(), keys->end(), &key);

        const int index = iterator - keys->begin();

        ancestors.push_back(currentNode);

        currentNode = this->GetNode(currentNode->GetChildren()->at(index));
      }

      return currentNode;
    }

   Pages::IndexPage *BPlusTree::SearchLeftMostLeafNode() const
    {
        auto *currentNode = this->root;
        
        while (!currentNode->IsLeaf())
            currentNode = this->GetNode(currentNode->GetChildren()->at(0));

        return currentNode;
    }

    Pages::IndexPage * BPlusTree::AllocateNewPage(const page_id_t& parentPageId)const{
        return this->database->FindOrAllocateNextIndexPage(this->tablePosition, parentPageId, this->nonClusteredIndexId);
    }

    Pages::IndexPage * BPlusTree::GetNode(const page_id_t& pageId) const
    {
        const extent_id_t extentId = DatabaseEngine::Database::CalculateExtentIdByPageId(pageId);

        return StorageManager::Get().GetIndexPage(this->database->GetFileName(), pageId, extentId, this->table);
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
            case Constants::ColumnType::Guid:
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
            case Constants::ColumnType::Guid:
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
            case Constants::ColumnType::Guid:
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
            case Constants::ColumnType::Guid:
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
}