#include "../include/BTree.h"
#include <algorithm>
#include <iostream>
#include <sstream>
#include "../include/DataStorage/Block.h"
#include "../include/DataStorage/Column.h"
#include "../include/DataStorage/Row.h"
#include "../include/DataStorage/Table.h"
#include "../include/Pages/IndexPage.h"
#include "../include/BufferPool/StorageManager.h"
#include "../include/Database.h"
#include "../../Systemic/include/Guards/ReaderGuard.h"
#include "../../Systemic/include/Guards/WriterGuard.h"

namespace Indexing
{
    BTree::BTree(DatabaseEngine::StorageTypes::Table *table, const page_id_t& indexPageId, const TreeType& treeType, const int& nonClusteredIndexId)
    {
        //handle degree here correctly based on indexed columns
        this->keySize = table->CalculateIndexKeySize(nonClusteredIndexId);
        this->degree = BTree::CalculateTreeDegree(table, treeType, nonClusteredIndexId);
        this->indexPageId = indexPageId;
        this->type = treeType;
        this->database = table->GetDatabase();
        this->nonClusteredIndexId = nonClusteredIndexId;
        this->table = table;
    }

    BTree::BTree()
    {
        this->degree = 0;
        this->keySize = 0;
        this->nonClusteredIndexId = -1;
        this->indexPageId = Constants::INVALID_PAGE_INDEX_ID;
        this->table = nullptr;
        this->database = nullptr;
        this->type = TreeType::NonClustered;
    }

    BTree::~BTree() = default;

    int BTree::CalculateTreeDegree(const DatabaseEngine::StorageTypes::Table* otherTable, const TreeType& treeType, const int& nonClusteredId)const
    {
        if(treeType == TreeType::Clustered){
          auto rowSize = otherTable->GetMaximumRowSize();
          int calculatedDegree = static_cast<int>(Constants::INDEX_PAGE_DEFAULT_SIZE / ((this->keySize + rowSize) * 2));

          while(calculatedDegree < 2){
            rowSize = otherTable->ReduceMaximumRowSize();

            calculatedDegree = static_cast<int>(Constants::INDEX_PAGE_DEFAULT_SIZE / ((this->keySize + rowSize) * 2));
          }

          return calculatedDegree;
        }

        const vector<DatabaseEngine::StorageTypes::Column*>& columns = otherTable->GetColumns();

        const auto& index = otherTable->GetNonClusteredIndexes(nonClusteredId);

        int computedKeySize = 0;
        for(const auto& columnPos: index.columns)
        {
            const DatabaseEngine::StorageTypes::Column* column = columns.at(columnPos);

            computedKeySize += column->GetColumnSize();
        }

        return static_cast<int>(Constants::INDEX_PAGE_DEFAULT_SIZE / ((this->keySize + Constants::ROW_ID_SIZE) * 2));
    }

    int BTree::LowerBound(
        const std::vector<DataTypes::Indexing::Key *> *keys,
        const DataTypes::Indexing::Key &key
    )  {
        for (int i = 0; i < keys->size(); i++) {
            const auto &currentKey = (*keys)[i];

            if (*currentKey >= key)
                return i;
        }

        return keys->size();
    }

    int BTree::PartialLowerBound(const std::vector<DataTypes::Indexing::Key *> *keys, const DataTypes::Indexing::Key &key) {
        for (int i = 0; i < keys->size(); i++) {
            const auto &currentKey = (*keys)[i];

            if (currentKey->PartialGreaterThan(key))
                return i;
        }

        return keys->size();
    }

    bool BTree::IsDuplicateKey(
        const std::vector<DataTypes::Indexing::Key *> *keys,
        const DataTypes::Indexing::Key &key,
        const int &indexPos
    ) {
        return !keys->empty() && (
            (keys->size() > indexPos && key == *keys->at(indexPos))
            || (indexPos > 0 && key == *keys->at(indexPos - 1))
        );
    }

    void BTree::CreateDuplicateKeyError(Errors::RuntimeStatus& status, const DataTypes::Indexing::Key &key) {
        ostringstream os;

        os << "BPlusTree::GetNonFullNode: Key " << key << " already exists" << std::endl;

        status.code = Errors::RuntimeError::DuplicateKey;
        status.message = os.str();
    }

    Pages::PageGuard<Pages::IndexPage> BTree::CreateRootPage(int& indexPosition) {
        //maybe root page is removed and need to be reopened
        auto root = this->AllocateNewPage(Constants::INVALID_PAGE_ID);

        {
            MultiThreading::WriterGuard lock(&root->GetLatch());

            root->SetIsRoot(true);
            root->SetIsLeaf(true);
            root->SetTreeType(this->type);
        }

        this->indexPageId = root->GetPageId();
        indexPosition = 0;

        return root;
    }

    void BTree::SplitRoot(Pages::PageGuard<Pages::IndexPage>& root, MultiThreading::ReaderGuard& rootLock) {
        {
            auto newRoot = this->AllocateNewPage(this->indexPageId);

            MultiThreading::WriterGuard newRootLock(&newRoot->GetLatch());

            newRoot->SetIsRoot(true);
            newRoot->SetIsLeaf(false);
            newRoot->SetTreeType(this->type);

            auto promotedRootLock = MultiThreading::WriterGuard::Promote(&root->GetLatch(), rootLock);

            newRoot->InsertChild(root->GetPageId());
            root->SetIsRoot(false);
            this->indexPageId = newRoot->GetPageId();

            this->SplitChildNoLock(newRoot, 0, root);
            root = newRoot;
        }

        //let table mutexes handle this
        if(this->nonClusteredIndexId != -1)
            this->table->SetNonClusteredIndexPageId(this->indexPageId, this->nonClusteredIndexId);
        else
            this->table->SetClusteredIndexPageId(this->indexPageId);
    }

    void BTree::SplitChild(
        Pages::PageGuard<Pages::IndexPage>& parent,
        MultiThreading::ReaderGuard& parentReadLock,
        const int &index,
        Pages::PageGuard<Pages::IndexPage>& child,
        MultiThreading::ReaderGuard& childReadLock
    ){
        auto parentLock = MultiThreading::WriterGuard::Promote(&parent->GetLatch(), parentReadLock);
        auto childLock = MultiThreading::WriterGuard::Promote(&child->GetLatch(), childReadLock);

        this->SplitChildNoLock(parent, index, child);
    }

    void BTree::SplitLeafNoLock(
        Pages::PageGuard<Pages::IndexPage> &parent,
        Pages::PageGuard<Pages::IndexPage> &child,
        Pages::PageGuard<Pages::IndexPage> &newChild,
        const int& index
    )const {
        auto* childKeys = child->GetKeysUnsafe();
        auto* newChildKeys = newChild->GetKeysUnsafe();
        auto* parentKeys = parent->GetKeysUnsafe();

        const auto* upgradedKey = (*childKeys)[this->degree - 1];
        auto* newKey = new DataTypes::Indexing::Key(upgradedKey);

        auto* parentChildren = parent->GetChildren();

        // Move the middle key from the child to the parent
        parentKeys->insert(parentKeys->begin() + index, newKey);
        parentChildren->insert(parentChildren->begin() + index + 1, newChild->GetPageId());

        // Assign the second half of the child's keys to the new child
        newChildKeys->assign(childKeys->begin() + this->degree, childKeys->end());
        childKeys->resize(this->degree);

        if (this->type == TreeType::Clustered) {
            auto* childRows = child->GetDataRowsUnsafe();

            auto* newChildRows = newChild->GetDataRowsUnsafe();

            newChildRows->assign(childRows->begin() + this->degree, childRows->end());
            childRows->resize(this->degree);
        }
        else{
            auto* childRows = child->GetNonClusteredDataUnsafe();

            auto* newChildRows = newChild->GetNonClusteredDataUnsafe();

            newChildRows->assign(childRows->begin() + this->degree, childRows->end());
            childRows->resize(this->degree);
        }

        newChild->SetNextPage(child->GetNextPage());
        newChild->SetPreviousPage(child->GetPageId());

        child->SetNextPage(newChild->GetPageId());
    }

    void BTree::SplitInternalNodeNoLock(
        Pages::PageGuard<Pages::IndexPage> &parent,
        Pages::PageGuard<Pages::IndexPage> &child,
        Pages::PageGuard<Pages::IndexPage> &newChild,
        const int& index
    ) const {
        auto* childKeys = child->GetKeysUnsafe();
        auto* newChildKeys = newChild->GetKeysUnsafe();
        auto* parentKeys = parent->GetKeysUnsafe();

        // Move the middle key from the child to the parent
        parentKeys->insert(parentKeys->begin() + index, (*childKeys)[this->degree - 1]);

        // Assign the second half of the child's keys to the new child
        newChildKeys->assign(childKeys->begin() + this->degree, childKeys->end());

        auto* parentChildren = parent->GetChildren();

        parentChildren->insert(parentChildren->begin() + index + 1, newChild->GetPageId());

        childKeys->resize(this->degree - 1);

        auto* newChildChildren = newChild->GetChildren();
        auto* childChildren = child->GetChildren();

        // Assign the second half of the child pointers to the new child
        newChildChildren->assign(childChildren->begin() + this->degree, childChildren->end());

        // Resize the old child's childrenHeaders vector to keep only the first half
        childChildren->resize(this->degree);
    }

    void BTree::SplitChildNoLock(
        Pages::PageGuard<Pages::IndexPage> &parent,
        const int &index,
        Pages::PageGuard<Pages::IndexPage> &child
    ) {
        auto newChild = this->AllocateNewPage(parent->GetPageId());

        MultiThreading::WriterGuard newChildLock(&newChild->GetLatch());

        newChild->SetIsLeaf(child->IsLeaf());
        newChild->SetIsRoot(false);
        newChild->SetTreeType(this->type);

        if (child->IsLeaf())
            this->SplitLeafNoLock(parent, child, newChild, index);
        else
            this->SplitInternalNodeNoLock(parent, child, newChild, index);

        parent->UpdateBytesLeft();
        child->UpdateBytesLeft();
        newChild->UpdateBytesLeft();

        parent->UpdatePageSize();
        child->UpdatePageSize();
        newChild->UpdatePageSize();
    }

    Pages::PageGuard<Pages::IndexPage> BTree::FindInsertNode(
        const DataTypes::Indexing::Key &key,
        int &indexPosition,
        Errors::RuntimeStatus& status
    ){
        //base case scenario
        if (this->indexPageId == Constants::INVALID_PAGE_ID) {
            auto root =  this->CreateRootPage(indexPosition);

            if(this->nonClusteredIndexId != -1)
                this->table->SetNonClusteredIndexPageId(this->indexPageId, this->nonClusteredIndexId);
            else
                this->table->SetClusteredIndexPageId(this->indexPageId);

            return root;
        }

        auto root = this->GetNode(this->indexPageId);

        {
            MultiThreading::ReaderGuard rootLock(&root->GetLatch());

            if (root->GetKeysUnsafe()->size() == 2 * this->degree - 1) // root is full,
                this->SplitRoot(root, rootLock);
        }

        return this->GetNonFullNode(root, key, indexPosition, status);
    }

    Pages::PageGuard<Pages::IndexPage> BTree::GetNonFullNode(
        Pages::PageGuard<Pages::IndexPage>& parent,
        const DataTypes::Indexing::Key &key,
        int& indexPosition,
        Errors::RuntimeStatus& status
    ){
        Pages::PageGuard<Pages::IndexPage> intermediateNode;
        {
            MultiThreading::ReaderGuard parentLock(&parent->GetLatch());

            const auto* parentKeys = parent->GetKeysUnsafe();
            if (parent->IsLeaf())
                return BTree::GetNonFullLeafNode(parent, parentKeys, key, indexPosition, status);

            auto childIndex = BTree::LowerBound(parentKeys, key);

            const auto* parentChildren = parent->GetChildren();

            const auto childId = parentChildren->at(childIndex);

            auto child = this->GetNode(childId);

            MultiThreading::ReaderGuard childLock(&child->GetLatch());

            const auto* childKeys = child->GetKeysUnsafe();

            if (childKeys->size() == 2 * this->degree - 1){
                this->SplitChild(parent, parentLock, childIndex, child, childLock);

                //split child will break the lock and we need to reacquire it
                MultiThreading::ReaderGuard newParentLock(&parent->GetLatch());

                if (key > *parentKeys->at(childIndex))
                    childIndex++;

                intermediateNode = this->GetNode(parentChildren->at(childIndex));
            }
            else
                intermediateNode = this->GetNode(parentChildren->at(childIndex));
        }

        return this->GetNonFullNode(intermediateNode, key, indexPosition, status);
    }

    Pages::PageGuard<Pages::IndexPage> BTree::GetNonFullLeafNode(
        Pages::PageGuard<Pages::IndexPage> &node,
        const std::vector<DataTypes::Indexing::Key*>*& parentKeys,
        const DataTypes::Indexing::Key &key,
        int &indexPosition,
        Errors::RuntimeStatus &status
    ) {
        const auto indexPos = BTree::LowerBound(parentKeys, key);

        if (BTree::IsDuplicateKey(parentKeys, key, indexPos))
        {
            BTree::CreateDuplicateKeyError(status, key);
            return node;
        }

        indexPosition = indexPos;
        return node;
    }

    void BTree::IndexScan(vector<DataTypes::Indexing::QueryData> &result)const
    {
        if (this->indexPageId == INVALID_PAGE_ID)
            return;

        auto currentNode = this->SearchLeftMostLeafNode();
        Pages::PageGuard<Pages::IndexPage> previousNode;

        while (currentNode.Get())
        {
            // auto* keys = currentNode->GetKeysUnsafe();

            // for (int i = 0; i < keys->size(); i++)
            //     result.emplace_back(currentNode->dataPageId, i);

            if(currentNode->GetNextPage() == Constants::INVALID_PAGE_ID)
                return;

            // previousNode = currentNode;
            currentNode = this->GetNode(currentNode->GetNextPage());
        }
    }

    void BTree::IndexScan(
        const QueryPipeline::PhysicalPlan::ExecutionProperties& properties,
        std::vector<const DatabaseEngine::StorageTypes::Row*> *result,
        QueryPipeline::PhysicalPlan::IndexState& state
    )const{
        if (this->indexPageId == Constants::INVALID_PAGE_ID)
            return;

        auto currentNode = state.pageId == Constants::INVALID_PAGE_ID
                                ? this->SearchLeftMostLeafNode()
                                : this->GetNode(state.pageId);

        while (currentNode.Get())
        {
            MultiThreading::ReaderGuard lock(&currentNode->GetLatch());

            const auto* rows = currentNode->GetDataRowsUnsafe();

            for (int i = state.GetNextKeyIndex(); i < rows->size(); i++) {
                const auto* row = rows->at(i)->GetVisibleVersionForTransaction(properties.snapshot);

                if (!row)
                    continue;

                result->push_back(row);

                if (result->size() == properties.batchSize) {
                    state.pageId = currentNode->GetPageId();
                    state.lastFetchedKeyIndex = i;

                    return;
                }
            }

            if(currentNode->GetNextPage() == Constants::INVALID_PAGE_ID)
                return;

            currentNode = this->GetNode(currentNode->GetNextPage());
        }
    }

    void BTree::IndexScan(
        const QueryPipeline::PhysicalPlan::ExecutionProperties& properties,
        std::vector<const DatabaseEngine::StorageTypes::Row*> *result,
        QueryPipeline::PhysicalPlan::IndexState& state,
        const Expressions::Expression *expression
    )const{
        if (this->indexPageId == Constants::INVALID_PAGE_ID)
            return;

        auto currentNode = state.pageId == INVALID_PAGE_ID
                                ? this->SearchLeftMostLeafNode()
                                : this->GetNode(state.pageId);

        Expressions::EvaluationContext context(Expressions::EvaluationContext::EvaluationContextType::SingleRow, properties.variables);

        while (currentNode.Get())
        {
            MultiThreading::ReaderGuard lock(&currentNode->GetLatch());

            const auto* rows = currentNode->GetDataRowsUnsafe();

            for (int i = state.GetNextKeyIndex(); i < rows->size(); i++) {
                const auto* row = rows->at(i)->GetVisibleVersionForTransaction(properties.snapshot);

                if (!row)
                    continue;

                context.row = row;
                if(!expression->Evaluate(context).GetBool())
                    continue;

                result->push_back(row);

                if (result->size() == properties.batchSize) {
                    state.lastFetchedKeyIndex = i;
                    state.pageId = currentNode->GetPageId();

                    return;
                }
            }

            if(currentNode->GetNextPage() == Constants::INVALID_PAGE_ID) {
              return;
            }

            currentNode = this->GetNode(currentNode->GetNextPage());
      }
    }

    void BTree::IndexScan(
        const QueryPipeline::PhysicalPlan::ExecutionProperties& properties,
        std::vector<const DatabaseEngine::StorageTypes::Row*> *result,
        const Expressions::Expression *expression
    )const{
        if (this->indexPageId == Constants::INVALID_PAGE_ID)
            return;

        auto currentNode = this->SearchLeftMostLeafNode();
        Expressions::EvaluationContext context(Expressions::EvaluationContext::EvaluationContextType::SingleRow, properties.variables);

        while (currentNode.Get())
        {
            MultiThreading::ReaderGuard lock(&currentNode->GetLatch());

            for (const auto* pageRow : *currentNode->GetDataRowsUnsafe()) {
                auto* row = pageRow->GetVisibleVersionForTransaction(properties.snapshot);

                context.row = row;
                if(!row || !expression->Evaluate(context).GetBool())
                    continue;

                result->push_back(row);
            }

            if(currentNode->GetNextPage() == Constants::INVALID_PAGE_ID)
                return;

            currentNode = this->GetNode(currentNode->GetNextPage());
        }
    }

    void BTree::IndexScan(
        const QueryPipeline::PhysicalPlan::ExecutionProperties& properties,
        std::vector<const DatabaseEngine::StorageTypes::Row*> *result
    )const{
        if (this->indexPageId == Constants::INVALID_PAGE_ID)
            return;

        auto currentNode = this->SearchLeftMostLeafNode();

        while (currentNode.Get())
        {
            MultiThreading::ReaderGuard lock(&currentNode->GetLatch());

            for (const auto& pageRow : *currentNode->GetDataRowsUnsafe()) {
                auto* row = pageRow->GetVisibleVersionForTransaction(properties.snapshot);

                if (!row)
                    continue;

                result->push_back(row);
            }

            if(currentNode->GetNextPage() == Constants::INVALID_PAGE_ID)
                return;

            currentNode = this->GetNode(currentNode->GetNextPage());
        }
    }

    void BTree::IndexScan(
        vector<Headers::RowIdentifier> *result,
        QueryPipeline::PhysicalPlan::IndexState& state,
        const int& rowsToSelect
    )const{

        if (this->indexPageId == Constants::INVALID_PAGE_ID)
            return;

        auto currentNode = state.pageId == INVALID_PAGE_ID
                        ? this->SearchLeftMostLeafNode()
                        : this->GetNode(state.pageId);

        const int startingPosition = state.GetNextKeyIndex();

        while (currentNode.Get())
        {
            MultiThreading::ReaderGuard lock(&currentNode->GetLatch());

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

            if(currentNode->GetNextPage() == Constants::INVALID_PAGE_ID)
                return;

            currentNode = this->GetNode(currentNode->GetNextPage());
        }
    }

    void BTree::IndexScan(vector<Headers::RowIdentifier> *result, const Expressions::Expression *expression)const{
        if (this->indexPageId == Constants::INVALID_PAGE_ID)
            return;

        auto currentNode = this->SearchLeftMostLeafNode();

        while (currentNode.Get())
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

            if(currentNode->GetNextPage() == Constants::INVALID_PAGE_ID)
                return;

            currentNode = this->GetNode(currentNode->GetNextPage());
        }
    }

    void BTree::IndexScanUpdate(
        const QueryPipeline::PhysicalPlan::ExecutionProperties& properties,
        const Expressions::Expression *expression,
        const vector<Value> & updates
    )const{
        if (this->indexPageId == INVALID_PAGE_ID)
            return;

        HashSet<column_index_t> updatedColumns;

        for(const auto& update : updates)
          updatedColumns.Add(update.GetColumnIndex());

        auto currentNode = this->SearchLeftMostLeafNode();
        Expressions::EvaluationContext context(Expressions::EvaluationContext::EvaluationContextType::SingleRow, properties.variables);

        while (currentNode.Get())
        {
            MultiThreading::WriterGuard lock(&currentNode->GetLatch());

            for(auto* row: *currentNode->GetDataRowsUnsafe()){

                context.row = row;
                const auto value = expression->Evaluate(context);
                if(!value.GetBool())
                  continue;

            const auto result = this->table->HandleRowUpdate(currentNode.Get(), row, properties, updates, updatedColumns, false);

              if (result.code != Errors::RuntimeError::Ok)
                  return;
          }

          if(currentNode->GetNextPage() == Constants::INVALID_PAGE_ID)
            return;

          currentNode = this->GetNode(currentNode->GetNextPage());
        }
    }

    Errors::RuntimeStatus BTree::IndexScanUpdate(
        const QueryPipeline::PhysicalPlan::ExecutionProperties& properties,
        const Expressions::Expression *expression,
        const std::vector<QueryPipeline::Statements::UpdateColumn *> &updates
    )const{
        if (this->indexPageId == Constants::INVALID_PAGE_ID)
            return {};

        HashSet<column_index_t> updatedColumns;
        for(const auto& update : updates)
            updatedColumns.Add(update->name.index);

        auto currentNode = this->SearchLeftMostLeafNode();
        Expressions::EvaluationContext context(Expressions::EvaluationContext::EvaluationContextType::SingleRow, properties.variables);

        while (currentNode.Get())
        {
            MultiThreading::WriterGuard lock(&currentNode->GetLatch());

            for(auto* row: *currentNode->GetDataRowsUnsafe()){
                context.row = row;

                const auto value = expression->Evaluate(context);
                if(!value.GetBool())
                    continue;

                const auto result = this->table->HandleRowUpdate(currentNode.Get(), row, properties, updates, updatedColumns, false);

                if (result.code != Errors::RuntimeError::Ok)
                    return result;
            }

            if(currentNode->GetNextPage() == Constants::INVALID_PAGE_ID)
                return {};

            currentNode = this->GetNode(currentNode->GetNextPage());
        }

        return {};
    }

    Errors::RuntimeStatus BTree::IndexScanUpdate(const QueryPipeline::PhysicalPlan::ExecutionProperties& properties, const vector<QueryPipeline::Statements::UpdateColumn *> &updates)const{
        if (this->indexPageId == INVALID_PAGE_ID)
            return {};

        HashSet<column_index_t> updatedColumns;

        for(const auto& update : updates)
            updatedColumns.Add(update->name.index);

        auto currentNode = this->SearchLeftMostLeafNode();

        while (currentNode.Get())
        {
            MultiThreading::WriterGuard lock(&currentNode->GetLatch());

            for(auto* row: *currentNode->GetDataRowsUnsafe()) {
                const auto result = this->table->HandleRowUpdate(currentNode.Get(), row, properties, updates, updatedColumns, false);

                if (result.code != Errors::RuntimeError::Ok)
                    return result;
            }

            if(currentNode->GetNextPage() == Constants::INVALID_PAGE_ID)
                return {};

            currentNode = this->GetNode(currentNode->GetNextPage());
        }

        return {};
    }

//escalate to table lock
    void BTree::InsertRowsToOtherTree(const int& indexPos)const{
        if (this->indexPageId == INVALID_PAGE_ID)
            return;

        auto currentNode = this->SearchLeftMostLeafNode();

        while (currentNode.Get())
        {
            const auto* rows = currentNode->GetDataRowsUnsafe();

            for (int i = 0;i < rows->size(); i++) {
                const auto* row = rows->at(i);

                this->table->NonClusteredIndexInsert(row, indexPos, Headers::RowIdentifier(currentNode->GetPageId(), i));
            }

            if(currentNode->GetNextPage() == Constants::INVALID_PAGE_ID)
                return;

            currentNode = this->GetNode(currentNode->GetNextPage());
        }
    }

    void BTree::InsertColumnToRow(const Constants::column_index_t& index, const Value &defaultValue)const{
        if (this->indexPageId == Constants::INVALID_PAGE_ID)
            return;

        auto currentNode = this->SearchLeftMostLeafNode();

        while (currentNode.Get())
        {
            for(auto* row: *currentNode->GetDataRowsUnsafe())
                this->table->HandleAddColumn(currentNode.Get(), row, index, defaultValue);

            if(currentNode->GetNextPage() == Constants::INVALID_PAGE_ID)
                return;

            currentNode = this->GetNode(currentNode->GetNextPage());
        }
    }

    void BTree::RemoveColumnFromRow(const Constants::column_index_t &index)const{
        if (this->indexPageId == Constants::INVALID_PAGE_ID)
            return;

        auto root = this->GetNode(this->indexPageId);

        auto currentNode = this->SearchLeftMostLeafNode();

        while (currentNode.Get())
        {
            for(auto* row: *currentNode->GetDataRowsUnsafe())
                DatabaseEngine::StorageTypes::Table::HandleRemoveColumn(currentNode.Get(), row, index);

            if(currentNode->GetNextPage() == Constants::INVALID_PAGE_ID)
                return;

            currentNode = this->GetNode(currentNode->GetNextPage());
        }

    }

    Errors::RuntimeStatus BTree::IndexSeekUpdate(
        const QueryPipeline::PhysicalPlan::ExecutionProperties& properties,
        const Expressions::Expression* expression,
        const DataTypes::Indexing::Key* minKey,
        const DataTypes::Indexing::Key* maxKey,
        const vector<Value> & updates
    )const{
        if (this->indexPageId == Constants::INVALID_PAGE_ID)
            return {};

        HashSet<column_index_t> updatedColumns;
        for(const auto& update : updates)
            updatedColumns.Add(update.GetColumnIndex());

        auto currentNode = this->SearchKey(*minKey);
        Pages::PageGuard<Pages::IndexPage> previousNode;

        Expressions::EvaluationContext context(Expressions::EvaluationContext::EvaluationContextType::SingleRow, properties.variables);
        while (true)
        {
            if (currentNode.Get() == nullptr)
                break;

            MultiThreading::WriterGuard lock(&currentNode->GetLatch());

            const auto* keys = currentNode->GetKeysUnsafe();

            if (previousNode.Get() && maxKey >= keys->at(0))
            {
                MultiThreading::WriterGuard previousNodeLock(&previousNode->GetLatch());

                const auto* previousKeys = previousNode->GetKeysUnsafe();

                // Check if the last key in the previous node is within the range
                if (maxKey >= previousKeys->at(previousKeys->size() - 1)) {
                    const auto* previousRows = previousNode->GetDataRowsUnsafe();

                    const auto* row = previousRows->at(previousRows->size() - 1);

                    context.row = row;
                    const auto value = expression->Evaluate(context);
                    if(value.GetBool()) {
                        const auto result = this->table->HandleRowUpdate(previousNode.Get(), previousRows->at(previousRows->size() - 1), properties, updates, updatedColumns, false);

                        if (result.code != Errors::RuntimeError::Ok)
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

              context.row = rows->at(i);
              const auto value = expression->Evaluate(context);
              if(!value.GetBool())
                continue;

            const auto result = this->table->HandleRowUpdate(currentNode.Get(), rows->at(i), properties, updates, updatedColumns, false);

            if (result.code != Errors::RuntimeError::Ok)
              return result;

//            if (maxKey < *key && !previousNode)
//                return;
          }

          if(currentNode->GetNextPage() == Constants::INVALID_PAGE_ID)
            return {};

          previousNode = currentNode;
          currentNode = this->GetNode(currentNode->GetNextPage());
        }

        return {};
    }

    Errors::RuntimeStatus BTree::IndexSeekUpdate(
        const QueryPipeline::PhysicalPlan::ExecutionProperties& properties,
        const DataTypes::Indexing::Key *minKey,
        const DataTypes::Indexing::Key *maxKey,
        const vector<Value> &updates
    )const{
        if (this->indexPageId == Constants::INVALID_PAGE_ID)
            return {};

        HashSet<column_index_t> updatedColumns;
        for(const auto& update : updates)
            updatedColumns.Add(update.GetColumnIndex());

        auto currentNode = this->SearchKey(*minKey);
        Pages::PageGuard<Pages::IndexPage> previousNode;

        while (currentNode.Get()){
            if (currentNode.Get() == nullptr)
                break;

            MultiThreading::WriterGuard lock(&currentNode->GetLatch());

            const auto* keys = currentNode->GetKeysUnsafe();

            if (previousNode.Get())
            {
                if (maxKey >= keys->at(0)) {
                    MultiThreading::WriterGuard previousNodeLock(&previousNode->GetLatch());

                    const auto* previousKeys = previousNode->GetKeysUnsafe();

                    // Check if the last key in the previous node is within the range
                    if (maxKey >= previousKeys->at(previousKeys->size() - 1)) {
                        const auto* previousRows = previousNode->GetDataRowsUnsafe();

                        const auto result = this->table->HandleRowUpdate(previousNode.Get(), previousRows->at(previousRows->size() - 1), properties, updates, updatedColumns, false);

                        if (result.code != Errors::RuntimeError::Ok)
                            return result;
                    }
                }
                else if(maxKey < keys->at(0))
                    return {};
            }

            const auto* rows = currentNode->GetDataRowsUnsafe();

            for (int i = 0; i < keys->size(); i++){
                const auto &key = keys->at(i);

                if (*minKey > *key)
                    continue;

                if (*maxKey < *key)
                    break;

                const auto result = this->table->HandleRowUpdate(currentNode.Get(), rows->at(i), properties, updates, updatedColumns, false);

                if (result.code != Errors::RuntimeError::Ok)
                  return result;
            }

            if(currentNode->GetNextPage() == Constants::INVALID_PAGE_ID)
                return {};

            previousNode = currentNode;
            currentNode = this->GetNode(currentNode->GetNextPage());
        }

        return {};
    }


    //TODO fix non clusteredIndex Seek
    void BTree::IndexSeekRange(const DataTypes::Indexing::Key &minKey, const DataTypes::Indexing::Key &maxKey, vector<DataTypes::Indexing::QueryData> &result) const{
        if (this->indexPageId == Constants::INVALID_PAGE_ID)
            return;

        auto currentNode = this->SearchKey(minKey);
        Pages::PageGuard<Pages::IndexPage> previousNode;

        while (currentNode.Get())
        {
            auto* keys = currentNode->GetKeysUnsafe();

            if (previousNode.Get() && maxKey >= *keys->at(0))
            {
                // auto* previousKeys = previousNode->GetKeysUnsafe();

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

            if(currentNode->GetNextPage() == Constants::INVALID_PAGE_ID)
                return;

            previousNode = currentNode;
            currentNode = this->GetNode(currentNode->GetNextPage());
        }
    }

    void BTree::IndexSeekRange(
        const QueryPipeline::PhysicalPlan::ExecutionProperties& properties,
        const DataTypes::Indexing::Key &minKey,
        const DataTypes::Indexing::Key &maxKey,
        std::vector<const DatabaseEngine::StorageTypes::Row*> *result
    )const{
        if (this->indexPageId == INVALID_PAGE_ID)
            return;

        auto currentNode = this->SearchKey(minKey);

        while (true){
            if (!currentNode.Get())
                break;

            MultiThreading::ReaderGuard lock(&currentNode->GetLatch());

            const auto* keys = currentNode->GetKeysUnsafe();

            for (int i = 0; i < keys->size(); i++){
                const auto &key = keys->at(i);

                if (key->InClosedRange(minKey, maxKey)){
                    const auto* visibleRow = currentNode->GetRow(i)->GetVisibleVersionForTransaction(properties.snapshot);

                    if (!visibleRow)
                        continue;

                    result->push_back(visibleRow);
                    continue;
                }

                if (maxKey < *key)
                    return;
            }

            if(currentNode->GetNextPage() == Constants::INVALID_PAGE_ID)
                return;

            currentNode = this->GetNode(currentNode->GetNextPage());
        }
    }

    void BTree::IndexSeek(
        const QueryPipeline::PhysicalPlan::ExecutionProperties &properties,
        const DataTypes::Indexing::Key &key,
        std::vector<const DatabaseEngine::StorageTypes::Row *> *result
    ) const {
        if (this->indexPageId == INVALID_PAGE_ID)
            return;

        auto currentNode = this->SearchKey(key);

        while (true){
            if (!currentNode.Get())
                break;

            MultiThreading::ReaderGuard lock(&currentNode->GetLatch());

            const auto* keys = currentNode->GetKeysUnsafe();

            for (int i = 0; i < keys->size(); i++){
                const auto& rowKey = keys->at(i);

                if (key == *rowKey) {
                    const auto* visibleRow = currentNode->GetRow(i)->GetVisibleVersionForTransaction(properties.snapshot);

                    if (!visibleRow)
                        continue;

                    result->push_back(visibleRow);
                }

                if (key < *rowKey)
                    return;
            }

            const auto& nextNodeId = currentNode->GetNextPage();
            if(nextNodeId == Constants::INVALID_PAGE_ID)
                return;

            currentNode = this->GetNode(nextNodeId);
        }
    }

    void BTree::IndexSeek(
        const QueryPipeline::PhysicalPlan::ExecutionProperties &properties,
        const DataTypes::Indexing::Key &key,
        std::vector<const DatabaseEngine::StorageTypes::Row *> *result,
        const Expressions::Expression *expression
    ) const {
        if (this->indexPageId == INVALID_PAGE_ID)
            return;

        auto currentNode = this->SearchKey(key);

        auto context = Expressions::EvaluationContext(Expressions::EvaluationContext::EvaluationContextType::SingleRow, properties.variables);

        while (true){
            if (!currentNode.Get())
                break;

            MultiThreading::ReaderGuard lock(&currentNode->GetLatch());

            const auto* keys = currentNode->GetKeysUnsafe();

            for (int i = 0; i < keys->size(); i++){
                const auto& rowKey = keys->at(i);

                if (key == *rowKey) {
                    const auto* visibleRow = currentNode->GetRow(i)->GetVisibleVersionForTransaction(properties.snapshot);

                    context.row = visibleRow;
                    if (!visibleRow || !expression->Evaluate(context).GetBool())
                        continue;

                    result->push_back(visibleRow);
                }

                if (key < *rowKey)
                    return;
            }

            const auto& nextNodeId = currentNode->GetNextPage();
            if(nextNodeId == Constants::INVALID_PAGE_ID)
                return;

            currentNode = this->GetNode(nextNodeId);
        }
    }

    void BTree::SearchKey(const DataTypes::Indexing::Key &key, DataTypes::Indexing::QueryData &result) const
    {
        if (this->indexPageId == Constants::INVALID_PAGE_ID)
            return;

        // auto currentNode = this->GetNode(this->indexPageId);
        //
        // while (!currentNode->IsLeaf())
        // {
        //     auto* keys = currentNode->GetKeysUnsafe();
        //
        //     const auto iterator = ranges::lower_bound(*keys, &key);
        //
        //     const int index = iterator - keys->begin();
        //
        //     auto* children = currentNode->GetChildren();
        //
        //     currentNode = this->GetNode(children->at(index));
        // }

        auto currentNode = this->SearchKey(key);

        Pages::PageGuard<Pages::IndexPage> previousNode;
        while (true)
        {
            if (currentNode.Get() == nullptr)
                return;

            // MultiThreading::ReaderGuard
            auto* keys = currentNode->GetKeysUnsafe();

            if (previousNode.Get() && key <= *keys->at(0))
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

    void BTree::Remove(const DataTypes::Indexing::Key &key){

      if (this->indexPageId == Constants::INVALID_PAGE_ID)
        return;

      vector<Pages::PageGuard<Pages::IndexPage>> ancestors;
      auto currentNode = std::move(this->SearchKeyWithAncestors(key, ancestors));

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

      if (keys->size() >= (degree - 1 ) / 2)
        return;

      int parentIndex = ancestors.size() - 1;
      this->HandleUnderflow(currentNode, ancestors, parentIndex);
   }

   void BTree::HandleUnderflow(Pages::PageGuard<Pages::IndexPage>& node, std::vector<Pages::PageGuard<Pages::IndexPage>>& ancestors, int& parentIndex) {
        if (node->IsRoot()) {
           this->HandleRootUnderflow();
           return;
        }

        auto& parent = ancestors.at(parentIndex);

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
           auto leftSibling = this->GetNode(children->at(index - 1));
           this->MergeNodes(leftSibling, node, parent, index - 1, ancestors, parentIndex);

           return;
       }

      auto rightSibling = this->GetNode(children->at(index + 1));
      this->MergeNodes(node, rightSibling, parent, index, ancestors, parentIndex);
  }

    void BTree::HandleRootUnderflow() {
        auto root = this->GetNode(this->indexPageId);

        const auto* keys = root->GetKeysUnsafe();
        auto* children = root->GetChildren();

        //root only has one child, delete current root and make child root
         if (keys->empty() && !children->empty()) {
            auto oldRoot = std::move(root);

            auto newRoot = std::move(this->GetNode(children->at(0)));

            newRoot->SetIsRoot(true);

            root->MarkEmpty();

            this->indexPageId = newRoot->GetPageId();

            return;
         }

         if (!keys->empty())
             return;

         //else root is empty and delete it (no more index items should be available but just to be sure

        root->MarkEmpty();
    }

    bool BTree::TryBorrowFromLeftSibling(Pages::PageGuard<Pages::IndexPage>& node, Pages::PageGuard<Pages::IndexPage>& parent, const int& index)const{
        auto* children = parent->GetChildren();

        auto sibling = std::move(this->GetNode(children->at(index - 1)));

        auto* siblingKeys = sibling->GetKeysUnsafe();

         if (siblingKeys->size() <= (degree - 1) / 2)
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

    bool BTree::TryBorrowFromRightSibling(Pages::PageGuard<Pages::IndexPage>& node, Pages::PageGuard<Pages::IndexPage>& parent, const int &index) const{

          auto* children = parent->GetChildren();

          auto sibling = this->GetNode(children->at(index + 1));

          auto* siblingKeys = sibling->GetKeysUnsafe();

         if (siblingKeys->size() <= (degree - 1) / 2)
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

    void BTree::MergeNodes(
       Pages::PageGuard<Pages::IndexPage>& leftNode,
       Pages::PageGuard<Pages::IndexPage>& rightNode,
       Pages::PageGuard<Pages::IndexPage>& parent,
        int parentKeyIndex,
        std::vector<Pages::PageGuard<Pages::IndexPage>>& ancestors,
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


              if(rightNode->GetNextPage() != Constants::INVALID_PAGE_ID){
                auto nextNode = this->GetNode(rightNode->GetNextPage());
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
         if (parentKeys->size() < (degree - 1) / 2 && !parent->IsRoot()){
            parentIndex--;
            this->HandleUnderflow(parent, ancestors, parentIndex);
          }

         delete rightNode.Get();
    }

    void BTree::SetBranchingFactor(const int &branchingFactor) { this->degree = branchingFactor; }

    const int &BTree::GetBranchingFactor() const { return this->degree; }

    void BTree::SetTreeType(const TreeType & treeType) { this->type = treeType; }

    const page_id_t & BTree::GetFirstIndexPageId() const { return this->indexPageId; }

    Pages::PageGuard<Pages::IndexPage> BTree::SearchKey(const DataTypes::Indexing::Key &key) const
    {
        auto currentNode = this->GetNode(this->indexPageId);

        while (true) {
            MultiThreading::ReaderGuard lock(&currentNode->GetLatch());

            if (currentNode->IsLeaf())
                return currentNode;

            const auto* keys = currentNode->GetKeysUnsafe();

            const auto index = BTree::PartialLowerBound(keys, key);

            currentNode = this->GetNode(currentNode->GetChildren()->at(index));
        }
    }

    Pages::PageGuard<Pages::IndexPage> BTree::SearchKeyWithAncestors(const DataTypes::Indexing::Key& key, vector<Pages::PageGuard<Pages::IndexPage>> & ancestors) const{
      auto currentNode = this->GetNode(this->indexPageId);

      while (!currentNode->IsLeaf())
      {
        const auto* keys = currentNode->GetKeysUnsafe();

        const auto iterator = ranges::lower_bound(*keys, &key);
        //
        // const int index = iterator - keys->begin();
        const auto index = BTree::LowerBound(keys, key);

        ancestors.push_back(std::move(currentNode));

        currentNode = std::move(this->GetNode(currentNode->GetChildren()->at(index)));
      }

      return currentNode;
    }

  Pages::PageGuard<Pages::IndexPage> BTree::SearchLeftMostLeafNode() const{
        auto currentNode = this->GetNode(this->indexPageId);

        while (true) {
            MultiThreading::ReaderGuard lock(&currentNode->GetLatch());

            if (currentNode->IsLeaf())
                return currentNode;

            currentNode = this->GetNode(currentNode->GetChildren()->at(0));
        }
    }

    Pages::PageGuard<Pages::IndexPage> BTree::AllocateNewPage(const page_id_t& parentPageId){
        return this->database->FindOrAllocateNextIndexPage(this->table, parentPageId, this->nonClusteredIndexId);
    }

    Pages::PageGuard<Pages::IndexPage> BTree::GetNode(const page_id_t& pageId) const{
        return Storage::StorageManager::Get().GetIndexPage(this->database->GetFileName(), pageId, this->table);
    }
}