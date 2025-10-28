#include "BPlusTree.h"
#include <algorithm>
#include <iostream>
#include <sstream>
#include "../../Database/Table/Table.h"
#include "../../Database/Pages/IndexPage/IndexPage.h"
#include "../../Database/Storage/StorageManager/StorageManager.h"
#include "../../Database/Column/Column.h"
#include "../Database.h"
#include "../Row/Row.h"
#include "../Block/Block.h"

namespace Indexing
{
    BPlusTree::BPlusTree(DatabaseEngine::StorageTypes::Table *table, const page_id_t& indexPageId, const TreeType& treeType, const int& nonClusteredIndexId)
    {
        const auto &tableHeader = table->GetTableHeader();

        //handle degree here correctly based on indexed columns
        this->keySize = table->CalculateIndexKeySize(nonClusteredIndexId);
        this->t = BPlusTree::CalculateTreeDegree(table, treeType, nonClusteredIndexId);
        this->root = nullptr;
        this->tableId = tableHeader.tableId;
        this->tablePosition = tableHeader.ordinalPosition;
        this->indexPageId = indexPageId;
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

    int BPlusTree::CalculateTreeDegree(const DatabaseEngine::StorageTypes::Table* table, const TreeType& treeType, const int& nonClusteredIndexId)const
    {
        if(treeType == TreeType::Clustered){
          const uint32_t pageSize = PAGE_SIZE - Pages::PageHeader::GetPageHeaderSize() - Pages::IndexPageAdditionalHeader::GetAdditionalHeaderSize();

          auto rowSize = table->GetMaximumRowSize();
          int degree = static_cast<int>(pageSize / ((this->keySize + rowSize) * 2));

          while(degree < 2){
            rowSize = table->ReduceMaximumRowSize();

            degree = static_cast<int>(pageSize / ((this->keySize + rowSize) * 2));
          }

          return degree;
        }

        const vector<DatabaseEngine::StorageTypes::Column*>& columns = table->GetColumns();

        const auto& index = table->GetNonClusteredIndexes(nonClusteredIndexId);

        int keySize = 0;
        for(const auto& columnPos: index.columns)
        {
            const DatabaseEngine::StorageTypes::Column* column = columns.at(columnPos);

            keySize += column->GetColumnSize();
        }

        const auto pageSize = (PAGE_SIZE - Pages::PageHeader::GetPageHeaderSize() - Pages::IndexPageAdditionalHeader::GetAdditionalHeaderSize());

        const auto degree = static_cast<int>(pageSize / ((this->keySize + Constants::ROW_ID_SIZE) * 2));

        return degree;
    }

    void BPlusTree::SplitChild(Pages::IndexPage *parent, const int &index, Pages::IndexPage *child)const
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

    Pages::IndexPage* BPlusTree::FindAppropriateNodeForInsert(const DataTypes::Indexing::Key &key, int *indexPosition, Errors::ResultStatus& status)
    {
        if (this->root == nullptr)
        {
            //maybe root page is removed and need to be reopened
            this->root = this->AllocateNewPage(INVALID_PAGE_ID);

            this->root->SetIsRoot(true);
            this->root->SetIsLeaf(true);
            this->root->SetTreeType(this->type);

            this->indexPageId = this->root->GetPageId();

            if(this->nonClusteredIndexId != -1)
              this->table->SetNonClusteredIndexPageId(this->indexPageId, this->nonClusteredIndexId);
            else
              this->table->SetClusteredIndexPageId(this->indexPageId);

            // this->InsertNodeToPage(this->root, 0);
        }

        if (this->root->GetKeysUnsafe()->size() == 2 * t - 1) // root is full,
        {
            auto* newRoot = this->AllocateNewPage(this->indexPageId);

            newRoot->SetIsRoot(true);
            newRoot->SetIsLeaf(false);
            newRoot->SetTreeType(this->type);

            newRoot->InsertChild(this->root->GetPageId());

            this->root->SetIsRoot(false);
            this->indexPageId = newRoot->GetPageId();

            if(this->nonClusteredIndexId != -1)
              this->table->SetNonClusteredIndexPageId(this->indexPageId, this->nonClusteredIndexId);
            else
              this->table->SetClusteredIndexPageId(this->indexPageId);

            // split the root
            this->SplitChild(newRoot, 0, this->root);

            // root is the newRoot
            this->root = newRoot;
        }

        auto* node = this->GetNonFullNode(this->root, key, indexPosition, status);

        if (status.code != Errors::ResultCode::Ok)
            return nullptr;

        return node;
    }

    Pages::IndexPage *BPlusTree::GetNonFullNode(Pages::IndexPage *node, const DataTypes::Indexing::Key &key, int *indexPosition, Errors::ResultStatus& status)
    {
        auto* keys = node->GetKeysUnsafe();

        if (node->IsLeaf())
        {
            const auto iterator = ranges::upper_bound(*keys, &key);

            const int indexPos = iterator - keys->begin();

            if (!keys->empty()
                && ((keys->size() > indexPos && key == *keys->at(indexPos))
                || (indexPos > 0 && key == *keys->at(indexPos - 1))))
            {
                const ostringstream oss;

                std::cerr << "BPlusTree::GetNonFullNode: Key " << key << " already exists" << std::endl;

                status.code = Errors::ResultCode::DuplicateKey;
                status.message = oss.str();
                return nullptr;
            }


            if (indexPosition != nullptr)
                *indexPosition = indexPos;

            return node;
        }
        
        const auto iterator = ranges::lower_bound(*keys, &key);

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

        if (status.code != Errors::ResultCode::Ok)
            return nullptr;
        
        return returnedNode;
    }

    void BPlusTree::IndexScan(vector<DataTypes::Indexing::QueryData> &result)const
    {
        if (!root)
            return;

        auto *currentNode = this->SearchLeftMostLeafNode();
        Pages::IndexPage *previousNode = nullptr;

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

    void BPlusTree::IndexScan(
        std::vector<const DatabaseEngine::StorageTypes::Row*> *result,
        QueryPipeline::PhysicalPlan::IndexState& state,
        const int& rowsToSelect){
        if (this->indexPageId == Constants::INVALID_PAGE_ID)
            return;

        this->root = this->GetNode(this->indexPageId);

        auto *currentNode = state.pageId == INVALID_PAGE_ID
                                ? this->SearchLeftMostLeafNode()
                                : this->GetNode(state.pageId);

        while (currentNode)
        {
            const auto* rows = currentNode->GetDataRowsUnsafe();

            for (int i = state.GetNextKeyIndex(); i < rows->size(); i++) {
                result->push_back(rows->at(i));

                if (result->size() == rowsToSelect) {
                    state.pageId = currentNode->GetPageId();
                    state.lastFetchedKeyIndex = i;

                    return;
                }
            }

            if(currentNode->GetNextPage() == 0)
                return;

            currentNode = this->GetNode(currentNode->GetNextPage());
        }
    }

    void BPlusTree::IndexScan(
        std::vector<const DatabaseEngine::StorageTypes::Row*> *result,
        QueryPipeline::PhysicalPlan::IndexState& state,
        const Expressions::Expression *expression,
        const int& rowsToSelect
    ){
        if (this->indexPageId == Constants::INVALID_PAGE_ID)
            return;

        this->root = this->GetNode(this->indexPageId);

        auto *currentNode = state.pageId == INVALID_PAGE_ID
                                ? this->SearchLeftMostLeafNode()
                                : this->GetNode(state.pageId);


        while (currentNode)
        {
            const auto* rows = currentNode->GetDataRowsUnsafe();

            for (int i = state.GetNextKeyIndex(); i < rows->size(); i++) {
                const auto* row = rows->at(i);

                if(!expression->Evaluate(row).GetBool())
                    continue;

                result->push_back(row);

                if (result->size() == rowsToSelect) {
                    state.lastFetchedKeyIndex = i;
                    state.pageId = currentNode->GetPageId();

                    return;
                }
            }

            if(currentNode->GetNextPage() == 0) {
              return;
            }

            currentNode = this->GetNode(currentNode->GetNextPage());
      }
    }

    void BPlusTree::IndexScan(std::vector<const DatabaseEngine::StorageTypes::Row*> *result, const Expressions::Expression *expression){
        if (this->indexPageId == Constants::INVALID_PAGE_ID)
            return;

        this->root = this->GetNode(this->indexPageId);

        auto *currentNode = this->SearchLeftMostLeafNode();

        while (currentNode)
        {
            for (const auto* row : *currentNode->GetDataRowsUnsafe()) {
                if(!expression->Evaluate(row).GetBool())
                    continue;

                result->push_back(row);
            }

            if(currentNode->GetNextPage() == 0) {
                return;
            }

            currentNode = this->GetNode(currentNode->GetNextPage());
        }
    }

    void BPlusTree::IndexScan(std::vector<const DatabaseEngine::StorageTypes::Row*> *result){
        this->root = this->GetNode(this->indexPageId);

        if (!this->root)
            return;

        auto *currentNode = this->SearchLeftMostLeafNode();

        while (currentNode)
        {
            for (const auto* row : *currentNode->GetDataRowsUnsafe()) {
                result->push_back(row);
            }

            if(currentNode->GetNextPage() == 0) {
                return;
            }

            currentNode = this->GetNode(currentNode->GetNextPage());
        }
    }

    void BPlusTree::IndexScan(
        vector<Headers::RowIdentifier> *result,
        QueryPipeline::PhysicalPlan::IndexState& state,
        const int& rowsToSelect){

        if (this->indexPageId == Constants::INVALID_PAGE_ID)
            return;

        this->root = this->GetNode(this->indexPageId);

        auto *currentNode = state.pageId == INVALID_PAGE_ID
                        ? this->SearchLeftMostLeafNode()
                        : this->GetNode(state.pageId);

        const int startingPosition = state.lastFetchedKeyIndex == -1 ? 0 : state.lastFetchedKeyIndex + 1;

        while (currentNode)
        {
            const auto* rowIds = currentNode->GetNonClusteredDataUnsafe();

            for (int i = startingPosition; i < rowIds->size(); i++) {
                const auto* rowId = rowIds->at(i);

                result->emplace_back(rowId->pageId, rowId->indexId);

                state.pageId = rowId->pageId;
                state.lastFetchedKeyIndex = i;

                if (result->size() == rowsToSelect)
                    return;
            }



            // for(auto* row: *currentNode->GetDataRowsUnsafe()){
            //     if(!row->Evaluate(expression))
            //         continue;
            //
            //     const RowHeader *rowHeader = row->GetHeader();
            //
            //     vector<Block *> copyBlocks = row->GetBlockCopies();
            //
            //     result->emplace_back(*table, copyBlocks, rowHeader->nullBitMap);
            // }

            if(currentNode->GetNextPage() == 0)
                return;

            currentNode = this->GetNode(currentNode->GetNextPage());
        }
    }

    void BPlusTree::IndexScan(vector<Headers::RowIdentifier> *result, const Expressions::Expression *expression){
        if (this->indexPageId == Constants::INVALID_PAGE_ID)
            return;

        this->root = this->GetNode(this->indexPageId);

        const auto *currentNode = this->SearchLeftMostLeafNode();

        while (currentNode)
        {
            // for(auto* row: *currentNode->GetDataRowsUnsafe()){
            //     if(!row->Evaluate(expression))
            //         continue;
            //
            //     const RowHeader *rowHeader = row->GetHeader();
            //
            //     vector<Block *> copyBlocks = row->GetBlockCopies();
            //
            //     result->emplace_back(*table, copyBlocks, rowHeader->nullBitMap);
            // }

            if(currentNode->GetNextPage() == 0)
                return;

            currentNode = this->GetNode(currentNode->GetNextPage());
        }
    }

    void BPlusTree::IndexScanUpdate(const Expressions::Expression *expression, const vector<Value> & updates){
        if (this->indexPageId == Constants::INVALID_PAGE_ID)
            return;

        this->root = this->GetNode(this->indexPageId);

        HashSet<column_index_t> updatedColumns;

        for(const auto& update : updates)
          updatedColumns.Add(update.GetColumnIndex());

        auto *currentNode = this->SearchLeftMostLeafNode();

        while (currentNode)
        {
          for(auto* row: *currentNode->GetDataRowsUnsafe()){
              const auto value = expression->Evaluate(row);
              if(!value.GetBool())
                  continue;

            const auto result = this->table->HandleRowUpdate(currentNode, row, updates, updatedColumns, false);
              if (result.code != Errors::ResultCode::Ok)
                  return;
          }

          if(currentNode->GetNextPage() == 0)
            return;

          currentNode = this->GetNode(currentNode->GetNextPage());
        }
    }

    Errors::ResultStatus BPlusTree::IndexScanUpdate(const Expressions::Expression *expression, const vector<QueryPipeline::Statements::UpdateColumn *> &updates){
        this->root = this->GetNode(this->indexPageId);

        if (!this->root)
            return {};

        HashSet<column_index_t> updatedColumns;

        for(const auto& update : updates)
            updatedColumns.Add(update->name.index);

        auto *currentNode = this->SearchLeftMostLeafNode();

        while (currentNode)
        {
            for(auto* row: *currentNode->GetDataRowsUnsafe()){
                const auto value = expression->Evaluate(row);
                if(!value.GetBool())
                    continue;

                const auto result = this->table->HandleRowUpdate(currentNode, row, updates, updatedColumns, false);

                if (result.code != Errors::ResultCode::Ok)
                    return result;
            }

            if(currentNode->GetNextPage() == 0)
                return {};

            currentNode = this->GetNode(currentNode->GetNextPage());
        }

        return {};
    }

    Errors::ResultStatus BPlusTree::IndexScanUpdate(const vector<QueryPipeline::Statements::UpdateColumn *> &updates){
        this->root = this->GetNode(this->indexPageId);

        if (!this->root)
            return {};

        HashSet<column_index_t> updatedColumns;

        for(const auto& update : updates)
            updatedColumns.Add(update->name.index);

        auto *currentNode = this->SearchLeftMostLeafNode();

        while (currentNode)
        {
            for(auto* row: *currentNode->GetDataRowsUnsafe()) {
                const auto result = this->table->HandleRowUpdate(currentNode, row, updates, updatedColumns, false);

                if (result.code != Errors::ResultCode::Ok)
                    return result;
            }

            if(currentNode->GetNextPage() == 0)
                return {};

            currentNode = this->GetNode(currentNode->GetNextPage());
        }

        return {};
    }

    void BPlusTree::InsertRowsToOtherTree(const int& indexPos){
        if (this->indexPageId == INVALID_PAGE_ID)
            return;

        this->root = this->GetNode(this->indexPageId);

        if (!this->root)
            return;

        auto *currentNode = this->SearchLeftMostLeafNode();

        while (currentNode)
        {
            const auto* rows = currentNode->GetDataRowsUnsafe();

            for (int i = 0;i < rows->size(); i++) {
                const auto* row = rows->at(i);

                this->table->NonClusteredIndexInsert(row, indexPos, Headers::RowIdentifier(currentNode->GetPageId(), i));
            }

            if(currentNode->GetNextPage() == 0)
                return;

            currentNode = this->GetNode(currentNode->GetNextPage());
        }
    }

    void BPlusTree::InsertColumnToRow(const Constants::column_index_t& index, const Value &defaultValue){
        this->root = this->GetNode(this->indexPageId);

        if (!this->root)
            return;

        auto *currentNode = this->SearchLeftMostLeafNode();

        while (currentNode)
        {
            for(auto* row: *currentNode->GetDataRowsUnsafe())
                this->table->HandleAddColumn(currentNode, row, index, defaultValue);

            if(currentNode->GetNextPage() == 0
                || currentNode->GetNextPage() == INVALID_PAGE_ID)
                return;

            currentNode = this->GetNode(currentNode->GetNextPage());
        }
    }

    void BPlusTree::RemoveColumnFromRow(const Constants::column_index_t &index){
        this->root = this->GetNode(this->indexPageId);

        if (!this->root)
            return;

        auto *currentNode = this->SearchLeftMostLeafNode();

        while (currentNode)
        {
            for(auto* row: *currentNode->GetDataRowsUnsafe())
                DatabaseEngine::StorageTypes::Table::HandleRemoveColumn(currentNode, row, index);

            if(currentNode->GetNextPage() == 0
                || currentNode->GetNextPage() == INVALID_PAGE_ID)
                return;

            currentNode = this->GetNode(currentNode->GetNextPage());
        }

    }

    Errors::ResultStatus BPlusTree::IndexSeekUpdate(const Expressions::Expression* expression, const DataTypes::Indexing::Key* minKey, const DataTypes::Indexing::Key* maxKey, const vector<Value> & updates){
        this->root = this->GetNode(this->indexPageId);

        if (!this->root)
          return {};

        HashSet<column_index_t> updatedColumns;
        for(const auto& update : updates)
            updatedColumns.Add(update.GetColumnIndex());

        auto *currentNode = this->SearchKey(*minKey);
        Pages::IndexPage *previousNode = nullptr;

        while (currentNode)
        {
          const auto* keys = currentNode->GetKeysUnsafe();

          if (previousNode && maxKey >= keys->at(0))
          {
            const auto* previousKeys = previousNode->GetKeysUnsafe();

            // Check if the last key in the previous node is within the range
            if (maxKey >= previousKeys->at(previousKeys->size() - 1)) {
                const auto* previousRows = previousNode->GetDataRowsUnsafe();

                const auto* row = previousRows->at(previousRows->size() - 1);

                const auto value = expression->Evaluate(row);
                if(value.GetBool()) {
                    const auto result = this->table->HandleRowUpdate(previousNode, previousRows->at(previousRows->size() - 1), updates, updatedColumns, false);

                    if (result.code != Errors::ResultCode::Ok)
                        return result;
                }
            }
          }
          else if(maxKey < keys->at(0))
               return {};

          const auto* rows = currentNode->GetDataRowsUnsafe();

          for (int i = 0; i < keys->size(); i++)
          {
            const auto &key = keys->at(i);

            if (*minKey > *key)
                continue;

            if (*maxKey < *key)
                break;

              const auto value = expression->Evaluate(rows->at(i));
              if(!value.GetBool())
                continue;

            const auto result = this->table->HandleRowUpdate(currentNode, rows->at(i), updates, updatedColumns, false);

            if (result.code != Errors::ResultCode::Ok)
              return result;

//            if (maxKey < *key && !previousNode)
//                return;
          }

          if(currentNode->GetNextPage() == 0)
            return {};

          previousNode = currentNode;
          currentNode = this->GetNode(currentNode->GetNextPage());
        }

        return {};
    }

    Errors::ResultStatus BPlusTree::IndexSeekUpdate(const DataTypes::Indexing::Key *minKey, const DataTypes::Indexing::Key *maxKey, const vector<Value> &updates){
                this->root = this->GetNode(this->indexPageId);

        if (!this->root)
          return {};

        HashSet<column_index_t> updatedColumns;
        for(const auto& update : updates)
            updatedColumns.Add(update.GetColumnIndex());

        auto *currentNode = this->SearchKey(*minKey);
        Pages::IndexPage *previousNode = nullptr;

        while (currentNode)
        {
          const auto* keys = currentNode->GetKeysUnsafe();

          if (previousNode && maxKey >= keys->at(0))
          {
            const auto* previousKeys = previousNode->GetKeysUnsafe();

            // Check if the last key in the previous node is within the range
            if (maxKey >= previousKeys->at(previousKeys->size() - 1)) {
                const auto* previousRows = previousNode->GetDataRowsUnsafe();

                const auto result = this->table->HandleRowUpdate(previousNode, previousRows->at(previousRows->size() - 1), updates, updatedColumns, false);

                if (result.code != Errors::ResultCode::Ok)
                    return result;
            }
          }
          else if(maxKey < keys->at(0))
               return {};

          const auto* rows = currentNode->GetDataRowsUnsafe();

          for (int i = 0; i < keys->size(); i++)
          {
            const auto &key = keys->at(i);

            if (*minKey > *key)
                continue;

            if (*maxKey < *key)
                break;

            const auto result = this->table->HandleRowUpdate(currentNode, rows->at(i), updates, updatedColumns, false);

            if (result.code != Errors::ResultCode::Ok)
              return result;

//            if (maxKey < *key && !previousNode)
//                return;
          }

          if(currentNode->GetNextPage() == 0)
            return {};

          previousNode = currentNode;
          currentNode = this->GetNode(currentNode->GetNextPage());
        }

        return {};
    }


    void BPlusTree::IndexSeek(const DataTypes::Indexing::Key &minKey, const DataTypes::Indexing::Key &maxKey, vector<DataTypes::Indexing::QueryData> &result) const
    {
        if (!this->root)
            return;

        auto *currentNode = this->SearchKey(minKey);
        Pages::IndexPage *previousNode = nullptr;

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

            for (const auto* key : *keys)
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

    void BPlusTree::IndexSeek(const DataTypes::Indexing::Key &minKey, const DataTypes::Indexing::Key &maxKey, std::vector<const DatabaseEngine::StorageTypes::Row*> *result){
        if (this->indexPageId == INVALID_PAGE_ID)
            return;

        this->root = this->GetNode(this->indexPageId);

        if (!this->root)
            return;

        auto *currentNode = this->SearchKey(minKey);
        Pages::IndexPage *previousNode = nullptr;

        while (currentNode)
        {
            const auto* keys = currentNode->GetKeysUnsafe();

            if (previousNode && maxKey >= *keys->at(0))
            {
                const auto* previousKeys = previousNode->GetKeysUnsafe();

                // Check if the last key in the previous node is within the range
                if (maxKey >= *previousKeys->at(previousKeys->size() - 1)) {
                    result->push_back(previousNode->GetRow(previousKeys->size() - 1));
                }
            }

            for (int i = 0; i < keys->size(); i++)
            {
                const auto &key = keys->at(i);

                if (minKey <= *key && maxKey >= *key)
                {
                    result->push_back(currentNode->GetRow(i));
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

    void BPlusTree::SearchKey(const DataTypes::Indexing::Key &key, DataTypes::Indexing::QueryData &result) const
    {
        if (!root)
            return;

        auto *currentNode = this->root;

        while (!currentNode->IsLeaf())
        {
            auto* keys = currentNode->GetKeysUnsafe();

            const auto iterator = ranges::lower_bound(*keys, &key);

            const int index = iterator - keys->begin();

            auto* children = currentNode->GetChildren();

            currentNode = this->GetNode(children->at(index));
        }
        
        Pages::IndexPage *previousNode = nullptr;
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

    void BPlusTree::Remove(const DataTypes::Indexing::Key &key){

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
            const auto* row = rows->at(keyIndex);

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
        const auto* keys = this->root->GetKeysUnsafe();
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

    bool BPlusTree::TryBorrowFromLeftSibling(Pages::IndexPage* node, Pages::IndexPage* parent, const int& index)const{
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

    bool BPlusTree::TryBorrowFromRightSibling(Pages::IndexPage *node, Pages::IndexPage *parent, const int &index) const{

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
                 vector<DatabaseEngine::StorageTypes::Row*>* nodeRows = node->GetDataRowsUnsafe();
                 vector<DatabaseEngine::StorageTypes::Row*>* siblingRows = sibling->GetDataRowsUnsafe();

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
        Pages::IndexPage *leftNode,
        Pages::IndexPage *rightNode,
        Pages::IndexPage *parent,
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

    Pages::IndexPage*& BPlusTree::GetRoot() { return this->root; }

    void BPlusTree::SetRoot(Pages::IndexPage *&node) { this->root = node; }

    void BPlusTree::SetBranchingFactor(const int &branchingFactor) { this->t = branchingFactor; }

    const int &BPlusTree::GetBranchingFactor() const { return this->t; }

    void BPlusTree::SetTreeType(const TreeType & treeType) { this->type = treeType; }

    const page_id_t & BPlusTree::GetFirstIndexPageId() const { return this->indexPageId; }

    Pages::IndexPage *BPlusTree::SearchKey(const DataTypes::Indexing::Key &key) const
    {
        auto *currentNode = this->root;

        while (!currentNode->IsLeaf())
        {
            auto* keys = currentNode->GetKeysUnsafe();

            const auto iterator = ranges::lower_bound(*keys, &key);

            const int index = iterator - keys->begin();

            currentNode = this->GetNode(currentNode->GetChildren()->at(index));
        }

        return currentNode;
    }

    Pages::IndexPage* BPlusTree::SearchKeyWithAncestors(const DataTypes::Indexing::Key& key, vector<Pages::IndexPage *> & ancestors) const{
      auto *currentNode = this->root;

      while (!currentNode->IsLeaf())
      {
        auto* keys = currentNode->GetKeysUnsafe();

        const auto iterator = ranges::lower_bound(*keys, &key);

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

        return Storage::StorageManager::Get().GetIndexPage(this->database->GetFileName(), pageId, extentId, this->table);
    }
}