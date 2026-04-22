#include "../include/BTree.h"
#include <algorithm>
#include <cassert>
#include <iostream>
#include "../include/DataStorage/Column.h"
#include "../include/DataStorage/Table.h"
#include "../include/BufferPool/StorageManager.h"
#include "../include/Database.h"
#include "../../Systemic/include/Guards/ReaderGuard.h"
#include "../../Systemic/include/Guards/WriterGuard.h"
#include "Schedulers/StatisticsScheduler.h"
#include <cmath>

#include "Contexts/ExecutionContext.h"
#include "Memory/Allocator.h"

namespace Indexing{
    bool BTree::ShouldSplit(const Pages::IndexPageView& node) const{
        return node.Keys() == 2 * this->degree - 1;
    }

    void BTree::AssignLeavesConnections(
        const Pages::IndexPageView& child,
        const Pages::IndexPageView& newChild
    ){
        newChild.SetRightSibling(child.RightSibling());
        newChild.SetLeftSibling(child.PageId());

        child.SetRightSibling(newChild.PageId());
    }

    Int BTree::LeafLowerBound(
        const ::Memory::IAllocator* allocator,
        const Pages::IndexPageView& page,
        const DataTypes::Indexing::Key& key
    ){
        const auto numberOfKeys = page.Keys();

        for (Int i = 0; i < numberOfKeys; i++) {
            const auto tupleKey = page.GetKeyByIndex(allocator, i);

            if (tupleKey == key){
                std::cout   << "Found duplicate key: "
                            << key << " and: "
                            << tupleKey << " at position " << i
                            << " in page " << page.PageId()
                            << std::endl;
                return -1;
            }

            if (tupleKey > key)
                return i;
        }

        return numberOfKeys;
    }

    Int BTree::LeafPartialLowerBound(
        const ::Memory::IAllocator* allocator,
        const Pages::IndexPageView& page,
        const DataTypes::Indexing::Key& key
    ){
        const auto numberOfKeys = page.Keys();

        for (Int i = 0; i < numberOfKeys; i++) {
            const auto pageKey = page.GetKeyByIndex(allocator, i);

            if (key <= pageKey)
                return i;
        }

        return numberOfKeys;
    }

    Int BTree::InternalNodeLowerBound(
        const ::Memory::IAllocator* allocator,
        const Pages::IndexPageView& page,
        const DataTypes::Indexing::Key& key
    ){
        const auto numberOfKeys = page.Keys();

        for (Int i = 0; i < numberOfKeys; i++) {
            const auto tupleKey = page.GetKeyByIndex(allocator, i + 1);

            // if (tupleKey == key){
            //     std::cout
            //         << "Found duplicate key: "
            //         << key << " and: " << tupleKey << " at position "
            //         << i << " in page " << page.PageId() << std::endl;
            //     return -1;
            // }

            if (tupleKey >= key)
                return i;
        }

        return numberOfKeys;
    }

    Int BTree::InternalNodePartialLowerBound(
        const ::Memory::IAllocator* allocator,
        const Pages::IndexPageView& page,
        const DataTypes::Indexing::Key& key
    ){
        const auto numberOfKeys = page.Keys();

        for (Int i = 0; i < numberOfKeys; i++) {
            const auto pageKey = page.GetKeyByIndex(allocator, i + 1);

            if (key <= pageKey)
                return i;
        }

        return numberOfKeys;
    }

    Errors::RuntimeStatus BTree::CreateDuplicateKeyError(
        const DataTypes::Indexing::Key &key,
        const ::Memory::IAllocator* allocator
    ) {
        auto str = DataTypes::String::Concat(allocator, "BTree::CreateDuplicateKeyError: Key ", key.ToString(allocator), " already exists");
        return Errors::RuntimeStatus(Errors::RuntimeError::DuplicateKey, std::move(str));
    }

    Pages::IndexPageView BTree::CreateRootPage(Int& indexPosition, const Int pagesToAllocate) {
        //maybe root page is removed and need to be reopened
        auto root = this->AllocateNewPage(INVALID_PAGE_ID, INVALID_PAGE_ID, pagesToAllocate);

        {
            MultiThreading::WriterGuard lock(&root.Latch());

            root.SetIsRoot(true);
            root.SetIsLeaf(true);
            root.SetTreeType(this->type);
        }

        this->rootPageId = root.PageId();
        indexPosition = 0;

        return root;
    }

    void BTree::SplitRoot(
        const CoreEngine::ExecutionContext& context,
        Pages::IndexPageView& root,
        MultiThreading::ReaderGuard& rootLock,
        const Int pagesToAllocate
    ){
        {
            auto newRoot = this->AllocateNewPage(this->rootPageId, INVALID_PAGE_ID, pagesToAllocate);

            MultiThreading::WriterGuard newRootLock(&newRoot.Latch());

            newRoot.SetIsRoot(true);
            newRoot.SetIsLeaf(false);
            newRoot.SetTreeType(this->type);

            auto promotedRootLock = MultiThreading::WriterGuard::Promote(&root.Latch(), rootLock);

            newRoot.InsertFirstChild(root.PageId());

            root.SetIsRoot(false);
            this->rootPageId = newRoot.PageId();

            this->SplitChildNoLock(context, newRoot, 0, root, pagesToAllocate);
            root = std::move(newRoot);
        }

        //let table mutexes handle this
        if(this->nonClusteredIndexId != -1)
            this->table->SetNonClusteredIndexPageId(this->rootPageId, this->nonClusteredIndexId);
        else
            this->table->SetClusteredIndexPageId(this->rootPageId);
    }

    void BTree::SplitChild(
        const CoreEngine::ExecutionContext& context,
        const Pages::IndexPageView& parent,
        MultiThreading::ReaderGuard& parentReadLock,
        const Int index,
        const Pages::IndexPageView& child,
        MultiThreading::ReaderGuard& childReadLock,
        const Int pagesToAllocate
    ){
        auto parentLock = MultiThreading::WriterGuard::Promote(&parent.Latch(), parentReadLock);
        auto childLock = MultiThreading::WriterGuard::Promote(&child.Latch(), childReadLock);

        this->SplitChildNoLock(context, parent, index, child, pagesToAllocate);
    }

    void BTree::SplitLeafNoLock(
        const CoreEngine::ExecutionContext& context,
        const Pages::IndexPageView &parent,
        const Pages::IndexPageView &child,
        const Pages::IndexPageView &newChild,
        const Int index
    ){
        const auto* allocator = context.GetAllocator();
        const auto mid = child.PageSize() / 2;

        // move right half
        //mid = 14, keys = 29 -> donor new size = 15
        newChild.DistributeFromPage(allocator, &child, mid, mid);

        // promote first key of new child
        const auto childKey = newChild.GetKeyByIndex(allocator, 0);
        parent.InsertChild(newChild.PageId(), &childKey, index + 1);
        BTree::AssignLeavesConnections(child, newChild);
    }

    //TODO maybe optimize further
    void BTree::SplitInternalNodeNoLock(
        const CoreEngine::ExecutionContext& context,
        const Pages::IndexPageView &parent,
        const Pages::IndexPageView &child,
        const Pages::IndexPageView &newChild,
        const Int index
    ){
        const auto* allocator = context.GetAllocator();
        const auto mid = child.Keys() / 2;

        //get middle child and insert it as first child(no key is moved)
        //size = 29, mid = 14 -> moveIndex = 15 so no row is duplicated
        // 1. promote
        const auto promotedKey = child.GetKeyByIndex(allocator, mid);
        // 2. fix first child of right node
        const auto firstChild = child.GetChild(allocator, mid);
        newChild.InsertFirstChild(firstChild);
        // 3. move remaining
        newChild.DistributeFromPage(allocator, &child, mid + 1, mid);
        // 5. insert into parent
        parent.InsertChild(newChild.PageId(), &promotedKey, index + 1);
    }

    void BTree::SplitChildNoLock(
        const CoreEngine::ExecutionContext& context,
        const Pages::IndexPageView &parent,
        const Int index,
        const Pages::IndexPageView &child,
        const Int pagesToAllocate
    ) {
        const auto newChild = this->AllocateNewPage(parent.PageId(), child.PageId(), pagesToAllocate);

        MultiThreading::WriterGuard newChildLock(&newChild.Latch());

        newChild.SetIsLeaf(child.IsLeaf());
        newChild.SetIsRoot(false);
        newChild.SetTreeType(this->type);

        if (child.IsLeaf()){
            Indexing::BTree::SplitLeafNoLock(context, parent, child, newChild, index);
            return;
        }

        Indexing::BTree::SplitInternalNodeNoLock(context, parent, child, newChild, index);
    }

    Errors::RuntimeStatus BTree::InsertToNonFullNode(
            const CoreEngine::ExecutionContext& context,
            const Pages::IndexPageView& parent,
            const Pages::IndexInsertTuple& tuple,
            const Int pagesToAllocate,
            Int& indexPosition
    ){
        Pages::IndexPageView intermediateNode;
        {
            MultiThreading::ReaderGuard parentLock(&parent.Latch());

            if (parent.IsLeaf())
                return BTree::InsertToNode(context, parent, tuple, indexPosition);

            auto childIndex = BTree::InternalNodeLowerBound(context.GetAllocator(), parent, tuple.key);

            auto childId = parent.GetChild(context.GetAllocator(), childIndex);

            auto child = this->GetNode(childId);

            MultiThreading::ReaderGuard childLock(&child.Latch());

            if (this->ShouldSplit(child)){
                // if (!this->TryRedistributeLeaf(parent, parentLock, child, childLock, childIndex)) {
                // Redistribution failed, must split
                    this->SplitChild(
                        context, parent,
                        parentLock, childIndex,
                        child, childLock,
                        pagesToAllocate
                    );

                    //split child will break the lock and we need to reacquire it
                    MultiThreading::ReaderGuard newParentLock(&parent.Latch());

                    // After split, check which child the key belongs to
                    if (tuple.key > parent.GetKeyByIndex(context.GetAllocator(), childIndex))
                        childIndex++;

                    childId = parent.GetChild(context.GetAllocator(), childIndex);
                    intermediateNode = this->GetNode(childId);
                // }
                // else
                //     IntermediateNode = std::move(parent);
            }
            else
                intermediateNode = std::move(child);
        }  // All locks released here

        return this->InsertToNonFullNode(context, intermediateNode, tuple, pagesToAllocate, indexPosition);
    }

    Errors::RuntimeStatus BTree::InsertToNode(
        const CoreEngine::ExecutionContext& context,
        const Pages::IndexPageView &parent,
        const Pages::IndexInsertTuple& tuple,
        Int& indexPosition
    ){
        indexPosition = BTree::LeafLowerBound(context.GetAllocator(), parent, tuple.key);
        if (indexPosition == -1)
            return BTree::CreateDuplicateKeyError(tuple.key, context.GetAllocator());

        parent.InsertTuple(tuple, indexPosition);
        return {};
    }

    Pages::IndexPageView BTree::SearchKey(
        const ::Memory::IAllocator* allocator,
        const DataTypes::Indexing::Key &key
    ) const{
        auto currentNode = this->GetNode(this->rootPageId);

        while (true) {
            MultiThreading::ReaderGuard lock(&currentNode.Latch());

            if (currentNode.IsLeaf())
                return currentNode;

            const auto index = BTree::InternalNodePartialLowerBound(allocator, currentNode, key);
            const auto childId = currentNode.GetChild(allocator, index);
            currentNode = this->GetNode(childId);
        }
    }

    Pages::IndexPageView BTree::SearchKeyWithAncestors(const DataTypes::Indexing::Key& key, DataStructures::Array<Pages::IndexPageView> & ancestors) const{
      auto currentNode = this->GetNode(this->rootPageId);

    //   while (!currentNode.IsLeaf()){
    //     const auto index = BTree::InternalNodePartialLowerBound(currentNode, key);

    //     ancestors.push_back(std::move(currentNode));

    //     currentNode = std::move(this->GetNode(currentNode.GetChild(index)));
    //   }

      return currentNode;
    }

    Pages::IndexPageView BTree::SearchLeftMostLeafNode(const ::Memory::IAllocator* allocator) const{
        auto currentNode = this->GetNode(this->rootPageId);

        while (!currentNode.IsLeaf()) {
            MultiThreading::ReaderGuard lock(&currentNode.Latch());
            currentNode = this->GetNode(currentNode.GetChild(allocator, 0));
        }

        return currentNode;
    }

    Pages::IndexPageView BTree::SearchLeftMostLeafNode(
        const ::Memory::IAllocator* allocator,
        TinyInt &depth
    ) const{
        auto currentNode = this->GetNode(this->rootPageId);

        while (!currentNode.IsLeaf()){
            MultiThreading::ReaderGuard lock(&currentNode.Latch());
            depth++;
            currentNode = this->GetNode(currentNode.GetChild(allocator, 0));
        }

        return currentNode;
    }

    Pages::IndexPageView BTree::GetNode(const page_id_t pageId) const{
        return Storage::StorageManager::Get().GetIndexPage(
            this->database->GetDataFileKey(),
            this->database->GetFileName(),
            pageId,
            this->table
        );
    }

    Int BTree::CalculateTreeDegree(
        const CoreEngine::StorageTypes::Table* otherTable,
        const Constants::TreeType treeType,
        const Int nonClusteredId
    )const{
        if(treeType == Constants::TreeType::Clustered){
          auto rowSize = otherTable->GetMaximumRowSize();
          Int calculatedDegree = static_cast<Int>(Constants::INDEX_PAGE_DEFAULT_SIZE / ((this->keySize + rowSize) * 2));

          while(calculatedDegree < 2){
            rowSize = otherTable->ReduceMaximumRowSize();

            calculatedDegree = static_cast<Int>(Constants::INDEX_PAGE_DEFAULT_SIZE / ((this->keySize + rowSize) * 2));
          }

          return calculatedDegree;
        }

        const auto& columns = otherTable->GetColumns();

        const auto& index = otherTable->GetNonClusteredIndexes(nonClusteredId);

        Int computedKeySize = 0;
        for(const auto& columnPos: index.columns){
            const auto* column = columns[columnPos];
            computedKeySize += column->Size();
        }

        return static_cast<Int>(Constants::INDEX_PAGE_DEFAULT_SIZE / ((this->keySize + ROW_ID_SIZE) * 2));
    }

    Pages::IndexPageView BTree::AllocateNewPage(const page_id_t parentPageId, const page_id_t splitChildPageId, const Int pagesToAllocate){
        return this->database->FindOrAllocateNextIndexPage(
            this->table,
            parentPageId,
            splitChildPageId,
            pagesToAllocate,
            this->nonClusteredIndexId
        );
    }

    void BTree::HandleUnderflow(const Pages::IndexPageView& node, DataStructures::Array<Pages::IndexPageView>& ancestors, Int& parentIndex) {
        if (node.IsRoot()) {
           this->HandleRootUnderflow();
           return;
        }

      //   auto& parent = ancestors.at(parentIndex);
      //
      //   Int index = -1;
      //
      //   const auto* children = parent.GetChildren();
      //
      //   for (Int i = 0; i < children->size(); i++) {
      //    if (children->at(i) == node->GetPageId()) {
      //        index = i;
      //        break;
      //    }
      //  }
      //
      //  if (index > 0 && this->TryBorrowFromLeftSibling(node, parent, index))
      //      return;
      //
      //  if (index < children->size() - 1
      //      && this->TryBorrowFromRightSibling(node, parent, index))
      //      return;
      //
      //  //if borrowing failed merge nodes
      //  if (index > 0) {
      //      auto leftSibling = this->GetNode(children->at(index - 1));
      //      this->MergeNodes(leftSibling, node, parent, index - 1, ancestors, parentIndex);
      //
      //      return;
      //  }
      //
      // auto rightSibling = this->GetNode(children->at(index + 1));
      // this->MergeNodes(node, rightSibling, parent, index, ancestors, parentIndex);
  }

    void BTree::HandleRootUnderflow() {
        auto root = this->GetNode(this->rootPageId);

        // const auto* keys = root->GetKeysUnsafe();
        // auto* children = root->GetChildren();
        //
        // //root only has one child, delete current root and make child root
        //  if (keys->empty() && !children->empty()) {
        //     auto oldRoot = std::move(root);
        //
        //     auto newRoot = std::move(this->GetNode(children->at(0)));
        //
        //     newRoot->SetIsRoot(true);
        //
        //     root->MarkEmpty();
        //
        //     this->rootPageId = newRoot->GetPageId();
        //
        //     return;
        //  }
        //
        //  if (!keys->empty())
        //      return;
        //
        //  //else root is empty and delete it (no more index items should be available but just to be sure
        //
        // root->MarkEmpty();
    }

    bool BTree::TryBorrowFromLeftSibling(Pages::IndexPageView& node, Pages::IndexPageView& parent, const Int index)const{
        // auto* children = parent.GetChildren();
        //
        // auto sibling = std::move(this->GetNode(children->at(index - 1)));
        //
        // auto* siblingKeys = sibling->GetKeysUnsafe();
        //
        //  if (siblingKeys->size() <= (degree - 1) / 2)
        //      return false;
        //
        // auto* parentKeys = parent.GetKeysUnsafe();
        // auto* nodeKeys = node->GetKeysUnsafe();
        //
        //  if (node->IsLeaf()) {
        //     //get from left sibling the last key
        //     nodeKeys->insert(nodeKeys->begin(), siblingKeys->back());
        //
        //      if (this->type == TreeType::Clustered) {
        //
        //       //insert last child from left sibling to the current page
        //       auto nodeRows = node->DataRowsNoLock(this->table);
        //       auto siblingRows = sibling->DataRowsNoLock(this->table);
        //
        //       if (!siblingRows.empty()) {
        //            nodeRows.push_back(siblingRows.back());
        //            siblingRows.pop_back();
        //        }
        //      }
        //      else {
        //       auto* nodeNonClusteredData = node->NonClusteredDataNoLock();
        //       auto* siblingNonClusteredData = sibling->NonClusteredDataNoLock();
        //
        //       if(!siblingNonClusteredData->empty()){
        //         nodeNonClusteredData->insert(nodeNonClusteredData->begin(), siblingNonClusteredData->back());
        //         siblingNonClusteredData->pop_back();
        //       }
        //      }
        //
        //     siblingKeys->pop_back();
        //
        //     parentKeys->at(index - 1) = nodeKeys->front();
        //  }
        //  else {
        //     // Move parent key down to node
        //     nodeKeys->insert(nodeKeys->begin(), parentKeys->at(index - 1));
        //
        //     // Move last key from left sibling up to parent
        //     parentKeys->at(index - 1) = siblingKeys->back();
        //     siblingKeys->pop_back();
        //  }
        //
        // node->UpdatePageSize();
        // node->UpdateBytesLeft();
        //
        // sibling->UpdatePageSize();
        // sibling->UpdateBytesLeft();
        //
        // parent.UpdatePageSize();
        // parent.UpdateBytesLeft();

        return true;
    }

    bool BTree::TryBorrowFromRightSibling(Pages::IndexPageView& node, Pages::IndexPageView& parent, const Int index) const{

        //   auto* children = parent.GetChildren();
        //
        //   auto sibling = this->GetNode(children->at(index + 1));
        //
        //   auto* siblingKeys = sibling->GetKeysUnsafe();
        //
        //  if (siblingKeys->size() <= (degree - 1) / 2)
        //      return false;
        //
        //   auto* nodeKeys = node->GetKeysUnsafe();
        //   auto* parentKeys = parent.GetKeysUnsafe();
        //
        //  if (node->IsLeaf()) {
        //      nodeKeys->insert(nodeKeys->begin(), siblingKeys->front());
        //
        //      if (this->type == TreeType::Clustered) {
        //          auto nodeRows = node->DataRowsNoLock(this->table);
        //          auto siblingRows = sibling->DataRowsNoLock(this->table);
        //
        //          if (!siblingRows.empty()) {
        //              nodeRows.push_back(siblingRows.front());
        //              siblingRows.erase(siblingRows.begin());
        //          }
        //      }
        //      else {
        //       auto* nodeNonClusteredData = node->NonClusteredDataNoLock();
        //       auto* siblingNonClusteredData = sibling->NonClusteredDataNoLock();
        //
        //       if(!siblingNonClusteredData->empty()){
        //         nodeNonClusteredData->push_back(siblingNonClusteredData->front());
        //         siblingNonClusteredData->erase(siblingNonClusteredData->begin());
        //       }
        //      }
        //
        //       siblingKeys->erase(siblingKeys->begin());
        //
        //       parentKeys->at(index) = nodeKeys->front();
        //  }
        //  else {
        //
        //     nodeKeys->push_back(parentKeys->at(index));
        //     parentKeys->at(index) = siblingKeys->front();
        //
        //      // Move first key from right sibling up to parent
        //     siblingKeys->erase(siblingKeys->begin());
        //  }
        //
        // node->UpdatePageSize();
        // node->UpdateBytesLeft();
        //
        // sibling->UpdatePageSize();
        // sibling->UpdateBytesLeft();
        //
        // parent.UpdatePageSize();
        // parent.UpdateBytesLeft();

        return true;
    }

    bool BTree::TryRedistributeLeaf(
        const CoreEngine::ExecutionContext& context,
        const Pages::IndexPageView &parent,
        MultiThreading::ReaderGuard &parentLock,
        Pages::IndexPageView &child,
        MultiThreading::ReaderGuard &childLock,
        const Int childIndex
    )const {
        if (!child.IsLeaf())
            return {};

        if (child.HasLeftSibling()) {
            auto sibling = this->GetNode(child.LeftSibling());

            MultiThreading::ReaderGuard siblingLock(&sibling.Latch());

            if (sibling.Keys() < 2 * this->degree - 1
                && this->TryRedistributeLeafWithLeftSibling(child, sibling, childLock, siblingLock)) {

                // Update parent separator key between left sibling and child
                auto parentWriteLock = MultiThreading::WriterGuard::Promote(&parent.Latch(), parentLock);

                if (childIndex > 0) {
                    const auto childKey = child.GetKeyByIndex(context.GetAllocator(), childIndex - 1);
                    parent.InsertKey(childKey, childIndex - 1);
                }

                return true;
            }
        }

        if (child.HasRightSibling()) {
            auto sibling = this->GetNode(child.RightSibling());

            MultiThreading::ReaderGuard siblingLock(&sibling.Latch());
            //
            // if (sibling->GetKeysUnsafe()->size() < 2 * this->degree - 1
            //     && this->TryRedistributeLeafWithRightSibling(child, sibling, childLock, siblingLock)) {
            //
            //     // Update parent separator key between child and right sibling
            //     auto parentWriteLock = MultiThreading::WriterGuard::Promote(&parent.Latch(), parentLock);
            //     auto* parentKeys = parent.GetKeysUnsafe();
            //     auto* siblingKeys = sibling->GetKeysUnsafe();
            //
            //     if (childIndex > 0) {
            //         delete (*parentKeys)[childIndex];
            //         const auto* firstSiblingKey = siblingKeys->at(0);
            //
            //         parentKeys->at(childIndex) = new DataTypes::Indexing::Key(firstSiblingKey);
            //     }
            //
            //     return true;
            // }
        }

        return false;
    }

    bool BTree::TryRedistributeLeafWithLeftSibling(
        Pages::IndexPageView &child,
        Pages::IndexPageView &sibling,
        MultiThreading::ReaderGuard &childLock,
        MultiThreading::ReaderGuard &siblingLock
    )const {
        // Calculate balanced distribution
        const Int targetSiblingKeys = (sibling.Keys() + child.Keys()) / 2;
        const Int keysToMove = targetSiblingKeys - sibling.Keys();

        // Only redistribute if we actually need to move keys
        if (keysToMove <= 0)
            return false;

        MultiThreading::WriterGuard::Promote(&child.Latch(), childLock);
        MultiThreading::WriterGuard::Promote(&sibling.Latch(), siblingLock);

        // Move exactly keysToMove keys from child to sibling
        // const auto srcKeyEnd = childKeys->begin() + keysToMove;
        //
        // siblingKeys->insert(siblingKeys->end(), childKeys->begin(), srcKeyEnd);
        // childKeys->erase(childKeys->begin(), srcKeyEnd);
        //
        // if (this->type == TreeType::Clustered) {
        //     sibling->DistributeFromPage(child.Get(), keysToMove, 0, );
        //
        //
        //     const auto srcEnd = childRows.begin() + keysToMove;
        //     //
        //     // siblingRows.insert(siblingRows.end(), childRows.begin(), srcEnd);
        //
        //     for (Int i = 0; i < keysToMove; i++)
        //         siblingRows.push_back(std::move(childRows.at(i)));
        //
        //     childRows.erase(childRows.begin(), srcEnd);
        // }
        // else {
        //     const auto srcEnd = childNonClusteredData->begin() + keysToMove;
        //
        //     siblingNonClusteredData->insert(siblingNonClusteredData->end(), childNonClusteredData->begin(), srcEnd);
        //     childNonClusteredData->erase(childNonClusteredData->begin(), srcEnd);
        // }
        //
        // child.UpdateBytesLeft();
        // sibling->UpdateBytesLeft();
        //
        // child.UpdatePageSize();
        // sibling->UpdatePageSize();

        return true;
    }

    bool BTree::TryRedistributeLeafWithRightSibling(
        Pages::IndexPageView &child,
        Pages::IndexPageView &sibling,
        MultiThreading::ReaderGuard &childLock,
        MultiThreading::ReaderGuard &siblingLock
    )const {
        // auto* siblingKeys = sibling->GetKeysUnsafe();
        // auto* childKeys = child.GetKeysUnsafe();
        //
        // // Calculate balanced distribution
        // const Int totalKeys = static_cast<Int>(siblingKeys->size() + childKeys->size());
        // const Int targetChildKeys = totalKeys / 2;
        // const Int keysToMove = static_cast<Int>(childKeys->size()) - targetChildKeys;
        //
        // // Only redistribute if we actually need to move keys
        // if (keysToMove <= 0)
        //     return false;
        //
        // MultiThreading::WriterGuard::Promote(&child.Latch(), childLock);
        // MultiThreading::WriterGuard::Promote(&sibling->Latch(), siblingLock);
        //
        // auto childRows = child.DataRowsNoLock(this->table);
        // auto siblingRows = sibling->DataRowsNoLock(this->table);
        //
        // auto* childNonClusteredData = child.NonClusteredDataNoLock();
        // auto* siblingNonClusteredData = sibling->NonClusteredDataNoLock();
        //
        // if (this->type == TreeType::Clustered) {
        //     const auto srcBegin = childRows.end() - keysToMove;
        //     const auto index = childRows.size() - keysToMove;
        //
        //     for (Int i = index; i < childRows.size(); i++)
        //         siblingRows.insert(siblingRows.begin(), std::move(childRows.at(i)));
        //
        //     // siblingRows.insert(siblingRows.begin(), srcBegin, childRows.end());
        //     childRows.erase(srcBegin, childRows.end());
        // }
        // else {
        //     const auto srcBegin = childNonClusteredData->end() - keysToMove;
        //
        //     siblingNonClusteredData->insert(siblingNonClusteredData->begin(), srcBegin, childNonClusteredData->end());
        //     childNonClusteredData->erase(srcBegin, childNonClusteredData->end());
        // }
        //
        // const auto keySrcBegin = childKeys->end() - keysToMove;
        //
        // siblingKeys->insert(siblingKeys->begin(), keySrcBegin, childKeys->end());
        // childKeys->erase(keySrcBegin, childKeys->end());
        //
        // child.UpdateBytesLeft();
        // sibling->UpdateBytesLeft();
        //
        // child.UpdatePageSize();
        // sibling->UpdatePageSize();

        return true;
    }

    void BTree::MergeNodes(
       Pages::IndexPageView& leftNode,
       Pages::IndexPageView& rightNode,
       Pages::IndexPageView& parent,
        Int parentKeyIndex, DataStructures::Array<Pages::IndexPageView>& ancestors,
        Int& parentIndex){
         //  auto* leftNodeKeys = leftNode->GetKeysUnsafe();
         //  auto* rightNodeKeys = rightNode->GetKeysUnsafe();
         //
         //  auto* parentKeys = parent.GetKeysUnsafe();
         // if (leftNode->IsLeaf()) {
         //      leftNodeKeys->insert(leftNodeKeys->end(), rightNodeKeys->begin(), rightNodeKeys->end());
         //
         //     if (this->type == TreeType::Clustered) {
         //        auto leftNodeRows = leftNode->DataRowsNoLock(this->table);
         //        auto rightNodeRows = rightNode->DataRowsNoLock(this->table);
         //
         //        leftNodeRows.insert(leftNodeRows.end(), rightNodeRows.begin(), rightNodeRows.end());
         //        rightNodeRows.clear();
         //     }
         //     else {
         //        auto* leftNodeNonClusteredData = leftNode->NonClusteredDataNoLock();
         //        auto* rightNodeNonClusteredData = rightNode->NonClusteredDataNoLock();
         //
         //        leftNodeNonClusteredData->insert(leftNodeNonClusteredData->end(), rightNodeNonClusteredData->begin(), rightNodeNonClusteredData->end());
         //        leftNodeNonClusteredData->clear();
         //     }
         //
         //      leftNode->SetRightSibling(rightNode.GetRightSibling());
         //
         //
         //      if(rightNode.GetRightSibling() != INVALID_PAGE_ID){
         //        auto nextNode = this->GetNode(rightNode.GetRightSibling());
         //        nextNode->SetLeftSibling(leftNode->GetPageId());
         //      }
         // }
         // else {
         //     // Merge Internal nodes
         //      leftNodeKeys->push_back(parentKeys->at(parentKeyIndex));
         //
         //      parentKeys->erase(parentKeys->begin() + parentKeyIndex);
         //
         //      leftNodeKeys->insert(leftNodeKeys->end(), rightNodeKeys->begin(), rightNodeKeys->end());
         //
         //      auto* leftNodeChildren = leftNode->GetChildren();
         //      auto* rightNodeChildren = rightNode->GetChildren();
         //
         //      leftNodeChildren->insert(leftNodeChildren->end(), rightNodeChildren->begin(), rightNodeChildren->end());
         // }
         //
         // // Remove the parent key and right node poInter
         //  auto* parentChildrenHeaders = parent.GetChildren();
         //  parentChildrenHeaders->erase(parentChildrenHeaders->begin() + parentKeyIndex + 1);
         //
         //  rightNode->MarkEmpty();
         //
         //  leftNode->UpdatePageSize();
         //  leftNode->UpdateBytesLeft();
         //
         //  parent.UpdatePageSize();
         //  parent.UpdateBytesLeft();
         //
         // // Handle parent underflow if necessary
         // if (parentKeys->size() < (degree - 1) / 2 && !parent.IsRoot()){
         //    parentIndex--;
         //    this->HandleUnderflow(parent, ancestors, parentIndex);
         //  }
         //
         // delete rightNode.Get();
    }

    void BTree::CalculateClusteredStatistics(
        const ::Memory::IAllocator* allocator,
        Pages::IndexPageView& currentNode,
        Headers::IndexStatistics& indexStatistics,
        Headers::TableStatistics& tableStatistics, DataStructures::Array<Headers::ColumnStatistics>& columnStatistics,
        Dictionary<Int, SortedDictionary<Value, BigInt, ValueComparator>>& sortedValues
    ) const{

        while (true) {
            MultiThreading::ReaderGuard lock(&currentNode.Latch());

            tableStatistics.pageCount++;
            indexStatistics.leafPages++;

            const auto numOfRows = currentNode.PageSize();
            tableStatistics.rowCount += static_cast<Int>(numOfRows);

            for (Int i = 0;i < numOfRows; i++){
                auto [_, rowPtr] = currentNode.PeekLeafTuple(allocator, i);

                auto materializedRow = rowPtr.Materialize(allocator);
                tableStatistics.averageRowSize += rowPtr.Size();

                for (Int j = 0; j < columnStatistics.Size(); j++) {
                    auto& columnStats = columnStatistics[j];

                    CoreEngine::StatisticsScheduler::UpdateColumnStatistics(
                        columnStats,
                        materializedRow.GetColumnReferenceAt(j),
                        sortedValues[columnStats.columnId]
                    );
                }
            }

            if(!currentNode.HasRightSibling())
                break;

            currentNode = this->GetNode(currentNode.RightSibling());
        }

        tableStatistics.averageRowSize = static_cast<Int>(std::ceil(static_cast<float>(tableStatistics.averageRowSize) / static_cast<float>(tableStatistics.rowCount)));
    }

    void BTree::UpdatePfsPage(const Pages::IndexPageView& node) const{
        const auto pageFreeSpacePage = CoreEngine::Database::GetAssociatedPfsPage(
            this->database->GetSystemFileKey(),
            this->database->GetSystemFilename(),
            node.PageId()
        );

        MultiThreading::WriterGuard pfsPageLock(&pageFreeSpacePage.Latch());
        MultiThreading::WriterGuard pageLock(&node.Latch());

        pageFreeSpacePage.SetPageMetaData(&node);
    }

    BTree::BTree(
        CoreEngine::StorageTypes::Table *table,
        const page_id_t indexPageId,
        const Constants::TreeType treeType,
        const Int nonClusteredIndexId
    ){
        //handle degree here correctly based on indexed columns
        this->keySize = table->CalculateIndexKeySize(nonClusteredIndexId);
        this->degree = BTree::CalculateTreeDegree(table, treeType, nonClusteredIndexId);
        this->rootPageId = indexPageId;
        this->type = treeType;
        this->database = table->GetDatabase();
        this->nonClusteredIndexId = nonClusteredIndexId;
        this->table = table;
    }

    BTree::BTree(){
        this->degree = 0;
        this->keySize = 0;
        this->nonClusteredIndexId = -1;
        this->rootPageId = INVALID_PAGE_INDEX_ID;
        this->table = nullptr;
        this->database = nullptr;
        this->type = Constants::TreeType::NonClustered;
    }

    BTree::~BTree() = default;

    Errors::RuntimeStatus BTree::InsertRow(
        const CoreEngine::ExecutionContext& context,
        const Pages::IndexInsertTuple& tuple,
        const Int pagesToAllocate,
        Int &indexPosition
    ){
        if (pagesToAllocate == 56)
        {
            int val = 0;
        }
        //base case scenario
        if (this->IsEmpty()) {
            const auto root =  this->CreateRootPage(indexPosition, pagesToAllocate);

            if(this->nonClusteredIndexId != -1)
                this->table->SetNonClusteredIndexPageId(this->rootPageId, this->nonClusteredIndexId);
            else
                this->table->SetClusteredIndexPageId(this->rootPageId);

            root.InsertTuple(tuple);
            this->UpdatePfsPage(root);
            return {};
        }

        auto root = this->GetNode(this->rootPageId);

        {
            MultiThreading::ReaderGuard rootLock(&root.Latch());

            if (root.Keys() == 2 * this->degree - 1) // root is full,
                this->SplitRoot(context, root, rootLock, pagesToAllocate);
        }

        return this->InsertToNonFullNode(context, root, tuple, pagesToAllocate, indexPosition);
    }

    //TODO fix non clusteredIndex Seek
    void BTree::IndexSeekRange(
        const DataTypes::Indexing::Key &minKey,
        const DataTypes::Indexing::Key &maxKey, DataStructures::Array<DataTypes::Indexing::QueryData> &result
    ) const{
        if (this->IsEmpty())
            return;

        // auto currentNode = this->SearchKey(minKey);
        Pages::IndexPageView previousNode;

        while (true)
        {
            // auto* keys = currentNode->GetKeysUnsafe();

            // if (previousNode.Get() && maxKey >= *keys->at(0))
            // {
            //     // auto* previousKeys = previousNode->GetKeysUnsafe();
            //
            //     // Check if the last key in the previous node is within the range
            //     // if (maxKey >= *previousKeys->at(previousKeys->size() - 1))
            //     //     result.emplace_back(previousNode->dataPageId, previousNode->keys.size());
            // }
            //
            // for (const auto* key : *keys)
            // {
            //     if (minKey <= *key && maxKey >= *key)
            //     {
            //         // result.emplace_back(currentNode->dataPageId, i);
            //         continue;
            //     }
            //
            //     if (maxKey < *key)
            //         return;
            // }

            // if(!currentNode.HasRightSibling())
            //     return;
            //
            // previousNode = std::move(currentNode);
            // currentNode = this->GetNode(currentNode.RightSibling());
        }
    }

    void BTree::IndexSeekRange(
        const CoreEngine::ExecutionContext& context,
        const DataTypes::Indexing::Key &minKey,
        const DataTypes::Indexing::Key &maxKey,
        DataStructures::Array<Pages::RowReference>* result
    )const{
        if (this->IsEmpty())
            return;

        const auto& allocator = context.GetAllocator();
        auto currentNode = this->SearchKey(allocator, minKey);

        while (true){
            MultiThreading::ReaderGuard lock(&currentNode.Latch());

            for (Int i = 0; i < currentNode.Keys(); i++){
                auto [key, row] = currentNode.PeekLeafTuple(allocator, i);

                if (key.InClosedRange(minKey, maxKey)){
                    currentNode.AppendRowToBuffer(allocator, result, context.GetSnapshot(), i);
                    continue;
                }

                if (maxKey < key)
                    return;
            }

            if(!currentNode.HasRightSibling())
                return;

            currentNode = this->GetNode(currentNode.RightSibling());
        }
    }

    void BTree::IndexSeekRange(
        const CoreEngine::ExecutionContext& context,
        const DataTypes::Indexing::Key& minKey,
        const DataTypes::Indexing::Key& maxKey,
        DataStructures::Array<Pages::RowReference>* result,
        const Expressions::Expression* expression
    ) const{
        if (this->IsEmpty())
            return;

        const auto& allocator = context.GetAllocator();
        auto currentNode = this->SearchKey(allocator, minKey);

        while (true){
            MultiThreading::ReaderGuard lock(&currentNode.Latch());

            Expressions::EvaluationContext evaluationContext(
                Expressions::EvaluationContext::EvaluationContextType::SingleRow,
                context
            );

            for (Int i = 0; i < currentNode.Keys(); i++){
                auto [key, row] = currentNode.PeekLeafTuple(allocator, i);

                if (key.InClosedRange(minKey, maxKey)){
                    evaluationContext.row = &row;
                    if (expression->Evaluate(evaluationContext).AsBool())
                        currentNode.AppendRowToBuffer(allocator, result, context.GetSnapshot(), i);
                    continue;
                }

                if (maxKey < key)
                    return;
            }

            if(!currentNode.HasRightSibling())
                return;

            currentNode = this->GetNode(currentNode.RightSibling());
        }
    }

    void BTree::IndexSeek(
        const CoreEngine::ExecutionContext& context,
        const DataTypes::Indexing::Key &key,
        DataStructures::Array<Pages::RowReference>* result
    ) const {
        if (this->IsEmpty())
            return;

        const auto& allocator = context.GetAllocator();
        auto currentNode = this->SearchKey(allocator, key);

        while (true){
            MultiThreading::ReaderGuard lock(&currentNode.Latch());
            for (Int i = 0; i < currentNode.Keys(); i++){
                auto [tupleKey, row] = currentNode.PeekLeafTuple(allocator, i);

                // std::cout << "Comparing keys: " << tupleKey << " and " << key << std::endl;
                // std::cout << "Row: " << row << std::endl;
                if (key == tupleKey){
                    currentNode.AppendRowToBuffer(allocator, result, context.GetSnapshot(), i);
                    continue;
                }

                if (key < tupleKey)
                    return;
            }

            const auto rightSibling = currentNode.RightSibling();
            if(rightSibling == INVALID_PAGE_ID)
                return;

            currentNode = this->GetNode(rightSibling);
        }
    }

    void BTree::IndexSeek(
        const CoreEngine::ExecutionContext& context,
        const DataTypes::Indexing::Key &key,
        DataStructures::Array<Pages::RowReference>* result,
        const Expressions::Expression *expression
    ) const {
        if (this->IsEmpty())
            return;

        const auto& allocator = context.GetAllocator();
        auto currentNode = this->SearchKey(allocator, key);

        auto evaluationContext = Expressions::EvaluationContext(
            Expressions::EvaluationContext::EvaluationContextType::SingleRow,
            context
        );

        while (true){
            MultiThreading::ReaderGuard lock(&currentNode.Latch());

            for (Int i = 0; i < currentNode.Keys(); i++){
                auto [tupleKey, row] = currentNode.PeekLeafTuple(allocator, i);

                if (key == tupleKey ){
                    evaluationContext.row = &row;

                    if (expression->Evaluate(evaluationContext).AsBool())
                        currentNode.AppendRowToBuffer(allocator, result, context.GetSnapshot(), i);

                    continue;
                }

                if (key < tupleKey)
                    return;
            }

            const auto rightSibling = currentNode.RightSibling();
            if(rightSibling == INVALID_PAGE_ID)
                return;

            currentNode = this->GetNode(rightSibling);
        }
    }

    void BTree::SystemIndexSeek(
        const ::Memory::IAllocator* allocator,
        const DataTypes::Indexing::Key& key,
        DataStructures::Array<Pages::RowReference>* result
    ) const{
        if (this->IsEmpty())
            return;

        auto currentNode = this->SearchKey(allocator, key);

        while (true){
            MultiThreading::ReaderGuard lock(&currentNode.Latch());
            for (Int i = 0; i < currentNode.Keys(); i++){
                auto [tupleKey, row] = currentNode.PeekLeafTuple(allocator, i);

                // std::cout << "Comparing keys: " << tupleKey << " and " << key << std::endl;
                // std::cout << "Row: " << row << std::endl;
                if (key == tupleKey ){
                    currentNode.AppendRowToBuffer(allocator, result, i);
                    continue;
                }

                if (key < tupleKey)
                    return;
            }

            const auto rightSibling = currentNode.RightSibling();
            if(rightSibling == INVALID_PAGE_ID)
                return;

            currentNode = this->GetNode(rightSibling);
        }
    }

    void BTree::SystemIndexSeek(
        const ::Memory::IAllocator* allocator,
        const DataTypes::Indexing::Key& key,
        DataStructures::Array<Pages::RowReference>* result,
        const Expressions::Expression* expression
    ) const{
        if (this->IsEmpty())
            return;

        auto currentNode = this->SearchKey(allocator, key);

        auto evaluationContext = Expressions::EvaluationContext(
            Expressions::EvaluationContext::EvaluationContextType::SingleRow,
            allocator
        );

        while (true){
            MultiThreading::ReaderGuard lock(&currentNode.Latch());
            for (Int i = 0; i < currentNode.Keys(); i++){
                auto [tupleKey, row] = currentNode.PeekLeafTuple(allocator, i);

                // std::cout << "Comparing keys: " << tupleKey << " and " << key << std::endl;
                // std::cout << "Row: " << row << std::endl;
                if (key == tupleKey ){
                    evaluationContext.row = &row;
                    if (expression->Evaluate(evaluationContext).AsBool())
                        currentNode.AppendRowToBuffer(allocator, result, i);

                    continue;
                }

                if (key < tupleKey)
                    return;
            }

            const auto rightSibling = currentNode.RightSibling();
            if(rightSibling == INVALID_PAGE_ID)
                return;

            currentNode = this->GetNode(rightSibling);
        }
    }

    void BTree::IndexScan(DataStructures::Array<DataTypes::Indexing::QueryData> &result)const
    {
        if (this->IsEmpty())
            return;

        // auto currentNode = this->SearchLeftMostLeafNode();
        // Pages::IndexPageView previousNode;
        //
        // while (true)
        // {
        //     // auto* keys = currentNode->GetKeysUnsafe();
        //
        //     // for (Int i = 0; i < keys->size(); i++)
        //     //     result.emplace_back(currentNode->dataPageId, i);
        //
        //     if(!currentNode.HasRightSibling())
        //         return;
        //
        //     // previousNode = currentNode;
        //     currentNode = this->GetNode(currentNode.RightSibling());
        // }
    }

    void BTree::IndexScan(
        const CoreEngine::ExecutionContext& context,
        DataStructures::Array<Pages::RowReference>* result,
        CoreEngine::IndexState& state
    )const{
        if (this->IsEmpty())
            return;

        const auto& allocator = context.GetAllocator();

        result->Reserve(context.GetBatchSize());
        auto currentNode = state.pageId == INVALID_PAGE_ID
                                ? this->SearchLeftMostLeafNode(allocator)
                                : this->GetNode(state.pageId);

        state.canFetchMore = false;
        while (true){
            MultiThreading::ReaderGuard lock(&currentNode.Latch());

            for (Int i = state.GetNextKeyIndex(); i < currentNode.PageSize(); i++)
                currentNode.AppendRowToBuffer(allocator, result, context.GetSnapshot(), i);

            if(!currentNode.HasRightSibling()) {
                state.canFetchMore = false;
                return;
            }

            if (result->Size() >= context.GetBatchSize()) {
                state.pageId = currentNode.RightSibling();
                state.lastFetchedKeyIndex = INVALID_PAGE_INDEX_ID;
                state.canFetchMore = true;
                return;
            }

            currentNode = this->GetNode(currentNode.RightSibling());
        }
    }

    void BTree::IndexScan(
        const CoreEngine::ExecutionContext& context,
        DataStructures::Array<Pages::RowReference>* result,
        CoreEngine::IndexState& state,
        const Expressions::Expression *expression
    )const{
        if (this->IsEmpty())
            return;

        const auto& allocator = context.GetAllocator();
        auto currentNode = state.pageId == INVALID_PAGE_ID
                                ? this->SearchLeftMostLeafNode(allocator)
                                : this->GetNode(state.pageId);

        Expressions::EvaluationContext evaluationContext(
            Expressions::EvaluationContext::EvaluationContextType::SingleRow,
            context
        );

        state.canFetchMore = false;
        while (true)
        {
            MultiThreading::ReaderGuard lock(&currentNode.Latch());

            for (Int i = state.GetNextKeyIndex(); i < currentNode.Keys(); i++) {
                auto [key, row] = currentNode.PeekLeafTuple(allocator, i);
                evaluationContext.row = &row;
                if (!expression->Evaluate(evaluationContext).AsBool())
                    continue;

                currentNode.AppendRowToBuffer(allocator, result, context.GetSnapshot(), i);

                if (result->Size() == context.GetBatchSize()) {
                    state.lastFetchedKeyIndex = i;
                    state.pageId = currentNode.PageId();
                    state.canFetchMore = true;

                    return;
                }
            }

            if(!currentNode.HasRightSibling()) {
                state.canFetchMore = false;
                return;
            }

            currentNode = this->GetNode(currentNode.RightSibling());
      }
    }


    void BTree::IndexScan(
        const CoreEngine::ExecutionContext& context,
        DataStructures::Array<Pages::RowReference>* result,
        const Expressions::Expression *expression
    )const{
        if (this->IsEmpty())
            return;

        const auto& allocator = context.GetAllocator();
        auto currentNode = this->SearchLeftMostLeafNode(allocator);
        Expressions::EvaluationContext evaluationContext(
            Expressions::EvaluationContext::EvaluationContextType::SingleRow,
            context
        );

        while (true)
        {
            MultiThreading::ReaderGuard lock(&currentNode.Latch());

            for (Int i = 0;i < currentNode.PageSize();i++){

                auto [key, row] = currentNode.PeekLeafTuple(allocator, i);
                evaluationContext.row = &row;
                if(!expression->Evaluate(evaluationContext).AsBool())
                    continue;

                currentNode.AppendRowToBuffer(allocator, result, context.GetSnapshot(), i);
            }

            if(!currentNode.HasRightSibling())
                return;

            currentNode = this->GetNode(currentNode.RightSibling());
        }
    }

    void BTree::SystemIndexScan(
        const ::Memory::IAllocator* allocator,
        DataStructures::Array<Pages::RowReference>* result,
        const Expressions::Expression* expression
    ) const{
        if (this->IsEmpty())
            return;

        auto currentNode = this->SearchLeftMostLeafNode(allocator);
        Expressions::EvaluationContext evaluationContext(
            Expressions::EvaluationContext::EvaluationContextType::SingleRow,
            allocator
        );

        while (true){
            MultiThreading::ReaderGuard lock(&currentNode.Latch());

            for (Int i = 0;i < currentNode.PageSize();i++){

                auto [key, row] = currentNode.PeekLeafTuple(allocator, i);
                evaluationContext.row = &row;

                if(!expression->Evaluate(evaluationContext).AsBool())
                    continue;

                currentNode.AppendRowToBuffer(allocator, result, i);
            }

            if(!currentNode.HasRightSibling())
                return;

            currentNode = this->GetNode(currentNode.RightSibling());
        }
    }

    void BTree::SystemIndexScan(
        const ::Memory::IAllocator* allocator,
        DataStructures::Array<Pages::RowReference>* result
    ) const{
        if (this->IsEmpty())
            return;

        auto currentNode = this->SearchLeftMostLeafNode(allocator);

        while (true){
            MultiThreading::ReaderGuard lock(&currentNode.Latch());

            for (Int i = 0;i < currentNode.PageSize();i++){
                auto [key, row] = currentNode.PeekLeafTuple(allocator, i);
                currentNode.AppendRowToBuffer(allocator, result, i);
            }

            if(!currentNode.HasRightSibling())
                return;

            currentNode = this->GetNode(currentNode.RightSibling());
        }
    }

    void BTree::IndexScan(
        const CoreEngine::ExecutionContext& context,
        DataStructures::Array<Pages::RowReference>* result
    )const{
        if (this->IsEmpty())
            return;

        const auto& allocator = context.GetAllocator();
        auto currentNode = this->SearchLeftMostLeafNode(allocator);

        while (true)
        {
            MultiThreading::ReaderGuard lock(&currentNode.Latch());

            for (Int i = 0;i < currentNode.PageSize();i++){
                currentNode.AppendRowToBuffer(allocator, result, context.GetSnapshot(), i);
            }

            if(!currentNode.HasRightSibling())
                return;

            currentNode = this->GetNode(currentNode.RightSibling());
        }
    }

    void BTree::IndexScan(
     DataStructures::Array<DataTypes::RowIdentifier> *result,
        CoreEngine::IndexState& state,
        const Int rowsToSelect
    )const{

        if (this->IsEmpty())
            return;
        //
        // auto currentNode = state.pageId == INVALID_PAGE_ID
        //                 ? this->SearchLeftMostLeafNode()
        //                 : this->GetNode(state.pageId);
        //
        // const Int startingPosition = state.GetNextKeyIndex();
        //
        // while (true)
        // {
        //     MultiThreading::ReaderGuard lock(&currentNode.Latch());

            // const auto* rowIds = currentNode->NonClusteredDataNoLock();
            //
            // for (Int i = startingPosition; i < rowIds->size(); i++) {
            //     const auto& rowId = rowIds->at(i);
            //
            //     result->emplace_back(rowId.pageId, rowId.indexId);
            //
            //     state.pageId = rowId.pageId;
            //     state.lastFetchedKeyIndex = i;
            //
            //     if (result->size() == rowsToSelect)
            //         return;
            // }



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

        //     if(!currentNode.HasRightSibling())
        //         return;
        //
        //     currentNode = this->GetNode(currentNode.RightSibling());
        // }
    }

    void BTree::IndexScan DataStructures::Array<DataTypes::RowIdentifier> *result, const Expressions::Expression *expression)const{
        if (this->IsEmpty())
            return;

        // auto currentNode = this->SearchLeftMostLeafNode();
        //
        // while (true){
        //     // for(auto* row: *currentNode->GetDataRowsUnsafe()){
        //     //     if(!row->Evaluate(expression))
        //     //         continue;
        //     //
        //     //     const RowHeader *rowHeader = row->GetHeader();
        //     //
        //     //     vector<Block *> copyBlocks = row->GetBlockCopies();
        //     //
        //     //     result->emplace_back(*table, copyBlocks, rowHeader->nullBitMap);
        //     // }
        //
        //     if(!currentNode.HasRightSibling())
        //         return;
        //
        //     currentNode = this->GetNode(currentNode.RightSibling());
        // }
    }

    void BTree::IndexScanUpdate(
        const CoreEngine::ExecutionContext& context,
        const Expressions::Expression *expression,
        const DataStructures::Array<Value> &updates
    )const{
        if (this->IsEmpty())
            return;

        const auto& allocator = context.GetAllocator();
        auto currentNode = this->SearchLeftMostLeafNode(allocator);
        Expressions::EvaluationContext evaluationContext(Expressions::EvaluationContext::EvaluationContextType::SingleRow, context);

        while (true){
            MultiThreading::WriterGuard lock(&currentNode.Latch());

            for (Int indexPosition = 0; indexPosition < currentNode.PageSize();indexPosition++){
                auto tuple = currentNode.PeekLeafTuple(context.GetAllocator(), indexPosition);

                evaluationContext.row = &tuple.row;
                const auto value = expression->Evaluate(evaluationContext);
                if(!value.AsBool())
                    continue;

                const auto result = this->table->UpdateRowNoLock(
                    &currentNode,
                    tuple.row,
                    context,
                    updates
                );

                if (!result.IsOk())
                    return;
            }

          if(!currentNode.HasRightSibling())
            return;

          currentNode = this->GetNode(currentNode.RightSibling());
        }
    }

    Errors::RuntimeStatus BTree::IndexScanUpdate(
        const CoreEngine::ExecutionContext& context,
        const Expressions::Expression *expression,
        const DataStructures::Array<Expressions::Expression*>& updates
    )const{
        if (this->IsEmpty())
            return {};

        const auto& allocator = context.GetAllocator();
        auto currentNode = this->SearchLeftMostLeafNode(allocator);
        Expressions::EvaluationContext evaluationContext(
            Expressions::EvaluationContext::EvaluationContextType::SingleRow,
            context
        );

        while (true){
            MultiThreading::WriterGuard lock(&currentNode.Latch());

            for (Int i = 0;i < currentNode.PageSize();i++){
                auto tuple = currentNode.PeekLeafTuple(allocator, i);

                evaluationContext.row = &tuple.row;

                const auto value = expression->Evaluate(evaluationContext);
                if(!value.AsBool())
                    continue;

                auto result = this->table->UpdateRowNoLock(
                    &currentNode,
                    tuple.row,
                    context,
                    updates
                );

                if (!result.IsOk())
                    return result;
            }

            if(!currentNode.HasRightSibling())
                return {};

            currentNode = this->GetNode(currentNode.RightSibling());
        }

        return {};
    }

   Errors::RuntimeStatus BTree::IndexScanUpdate(
       const CoreEngine::ExecutionContext& context,
       const DataStructures::Array<Expressions::Expression*>& updates
    )const{
        if (this->IsEmpty())
            return {};

        const auto& allocator = context.GetAllocator();
        auto currentNode = this->SearchLeftMostLeafNode(allocator);

        while (true){
            MultiThreading::WriterGuard lock(&currentNode.Latch());

            for (Int indexPosition = 0;indexPosition < currentNode.PageSize();indexPosition++){
                auto tuple = currentNode.PeekLeafTuple(allocator, indexPosition);

                auto result = this->table->UpdateRowNoLock(
                    &currentNode,
                    tuple.row,
                    context,
                    updates
                );

                if (!result.IsOk())
                  return result;
            }

            if(!currentNode.HasRightSibling())
                return {};

            currentNode = this->GetNode(currentNode.RightSibling());
        }

        return {};
    }

    Errors::RuntimeStatus BTree::IndexSeekUpdate(
        const CoreEngine::ExecutionContext& context,
        const DataTypes::Indexing::Key &key,
        const DataStructures::Array<Value> &updates
    ) const {
        if (this->IsEmpty())
            return {};

        auto currentNode = this->SearchKey(context.GetAllocator(), key);

        while (true) {
            MultiThreading::WriterGuard lock(&currentNode.Latch());

            for (Int indexPosition = 0; indexPosition < currentNode.Keys(); indexPosition++){
                auto tuple = currentNode.PeekLeafTuple(context.GetAllocator(), indexPosition);
                if (key != tuple.key || key < tuple.key)
                    continue;

                auto result = this->table->UpdateRowNoLock(
                    &currentNode,
                    tuple.row,
                    context,
                    updates
                );

                if (!result.IsOk())
                    return result;
            }

            if(!currentNode.HasRightSibling())
                return {};

            currentNode = this->GetNode(currentNode.RightSibling());
        }
    }

    Errors::RuntimeStatus BTree::IndexSeekUpdate(
        const CoreEngine::ExecutionContext& context,
        const Expressions::Expression* expression,
        const DataTypes::Indexing::Key* minKey,
        const DataTypes::Indexing::Key* maxKey,
        const DataStructures::Array<Value>& updates
    )const{
        if (this->IsEmpty())
            return {};

        const auto& allocator = context.GetAllocator();
        auto currentNode = this->SearchKey(allocator, *minKey);

        Expressions::EvaluationContext evaluationContext(
            Expressions::EvaluationContext::EvaluationContextType::SingleRow,
            context
        );

        while (true)
        {
            MultiThreading::WriterGuard lock(&currentNode.Latch());

              for (Int indexPosition = 0; indexPosition < currentNode.Keys(); indexPosition++){
                auto tuple = currentNode.PeekLeafTuple(allocator, indexPosition);

                if (*minKey > tuple.key)
                    continue;

                if (*maxKey < tuple.key)
                    break;

                  evaluationContext.row = &tuple.row;
                  const auto value = expression->Evaluate(evaluationContext);
                  if(!value.AsBool())
                    continue;

                auto result = this->table->UpdateRowNoLock(
                    &currentNode,
                    tuple.row,
                    context,
                    updates
                );

                if (!result.IsOk())
                  return result;
              }

          if(!currentNode.HasRightSibling())
            return {};

          currentNode = this->GetNode(currentNode.RightSibling());
        }

        return {};
    }

    Errors::RuntimeStatus BTree::IndexSeekUpdate(
        const CoreEngine::ExecutionContext& context,
        const DataTypes::Indexing::Key *minKey,
        const DataTypes::Indexing::Key *maxKey,
        const DataStructures::Array<Value> &updates
    )const{
        if (this->IsEmpty())
            return {};

        auto currentNode = this->SearchKey(context.GetAllocator(), *minKey);

        while (true){
            MultiThreading::WriterGuard lock(&currentNode.Latch());

            for (Int indexPosition = 0; indexPosition < currentNode.Keys(); indexPosition++){
                auto tuple = currentNode.PeekLeafTuple(context.GetAllocator(), indexPosition);

                if (*minKey > tuple.key)
                    continue;

                if (*maxKey < tuple.key)
                    break;

                auto result = this->table->UpdateRowNoLock(
                    &currentNode,
                    tuple.row,
                    context,
                    updates
                );

                if (!result.IsOk())
                  return result;
            }

            if(!currentNode.HasRightSibling())
                return {};

            currentNode = this->GetNode(currentNode.RightSibling());
        }
    }

    Errors::RuntimeStatus BTree::SystemIndexSeekUpdate(
        const Memory::IAllocator* allocator,
        const DataTypes::Indexing::Key& key,
        const DataStructures::Array<Value>& updates
    ) const{
        if (this->IsEmpty())
            return {};

        auto currentNode = this->SearchKey(allocator, key);

        while (true) {
            MultiThreading::WriterGuard lock(&currentNode.Latch());

            for (Int indexPosition = 0; indexPosition < currentNode.Keys(); indexPosition++){
                auto tuple = currentNode.PeekLeafTuple(allocator, indexPosition);
                if (key != tuple.key || key < tuple.key)
                    continue;

                auto result = this->table->SystemUpdateRowNoLock(
                    &currentNode,
                    tuple.row,
                    allocator,
                    updates
                );

                if (!result.IsOk())
                    return result;
            }

            if(!currentNode.HasRightSibling())
                return {};

            currentNode = this->GetNode(currentNode.RightSibling());
        }
    }

    void BTree::SearchKey(const DataTypes::Indexing::Key &key, DataTypes::Indexing::QueryData &result) const
    {
        if (this->IsEmpty())
            return;

        // auto currentNode = this->GetNode(this->indexPageId);
        //
        // while (!currentNode->IsLeaf())
        // {
        //     auto* keys = currentNode->GetKeysUnsafe();
        //
        //     const auto iterator = ranges::lower_bound(*keys, &key);
        //
        //     const Int index = iterator - keys->begin();
        //
        //     auto* children = currentNode->GetChildren();
        //
        //     currentNode = this->GetNode(children->at(index));
        // }

        // auto currentNode = this->SearchKey(key);
        //
        // Pages::IndexPageView previousNode;
        // while (true)
        // {
        //     if (currentNode == nullptr)
        //         return;
        //
        //     // MultiThreading::ReaderGuard
        //     auto* keys = currentNode->GetKeysUnsafe();
        //
        //     if (previousNode.Get() && key <= *keys->at(0))
        //     {
        //         // result.pageId = previousNode->dataPageId;
        //         // result.indexPosition = previousNode->keys.size();
        //         return;
        //     }
        //
        //     for (Int i = 0; i < keys->size(); i++)
        //     {
        //         if (key == *keys->at(i))
        //         {
        //
        //             // result.pageId = currentNode->dataPageId;
        //             // result.indexPosition = i;
        //             return;
        //         }
        //     }
        //
        //     previousNode = currentNode;
        //     currentNode = this->GetNode(currentNode.GetRightSibling());
        // }
    }

    void BTree::Remove(const DataTypes::Indexing::Key &key){

      if (this->IsEmpty())
        return;

      // vector<Pages::IndexPageView> ancestors;
      // auto currentNode = std::move(this->SearchKeyWithAncestors(key, ancestors));
      //
      // auto* keys = currentNode->GetKeysUnsafe();
      //
      // Int keyIndex = -1;
      //
      // for (Int i = 0; i < keys->size(); i++) {
      //   if (*keys->at(i) == key) {
      //     keyIndex = i;
      //     break;
      //   }
      // }
      //
      // if (keyIndex == -1)
      //   return;
      //
      // keys->erase(keys->begin() + keyIndex);
      //
      // if (this->type == TreeType::NonClustered){
      //   auto* nonClusteredData = currentNode->NonClusteredDataNoLock();
      //
      //   nonClusteredData->erase(nonClusteredData->begin() + keyIndex);
      // }
      // else{
      //       auto rows = currentNode->DataRowsNoLock(this->table);
      //       rows.erase(rows.begin() + keyIndex);
      // }
      //
      // currentNode->UpdatePageSize();
      // currentNode->UpdateBytesLeft();
      //
      // if (keys->size() >= (degree - 1 ) / 2)
      //   return;
      //
      // Int parentIndex = ancestors.size() - 1;
      // this->HandleUnderflow(currentNode, ancestors, parentIndex);
   }

    void BTree::SetBranchingFactor(const Int branchingFactor) { this->degree = branchingFactor; }

    Int BTree::GetBranchingFactor() const { return this->degree; }

    void BTree::SetTreeType(const Constants::TreeType treeType) { this->type = treeType; }

    page_id_t BTree::GetFirstIndexPageId() const { return this->rootPageId; }

    //escalate to table lock
    void BTree::InsertRowsToOtherTree(const Int indexPos, const Int pagesToAllocate)const{
        if (this->IsEmpty())
            return;

        // auto currentNode = this->SearchLeftMostLeafNode();

        // while (currentNode){
        //     const auto rows = currentNode->DataRowsNoLock(this->table);
        //
        //     for (Int i = 0;i < rows.size(); i++) {
        //         const auto& row = rows.at(i);
        //
        //         this->table->NonClusteredIndexInsert(&row, indexPos, pagesToAllocate, Headers::RowIdentifier(currentNode->GetPageId(), i));
        //     }
        //
        //     if(!currentNode->HasRightSibling())
        //         return;
        //
        //     currentNode = this->GetNode(currentNode.GetRightSibling());
        // }
    }

  void BTree::InsertColumnToRow(const column_index_t index, const Value &defaultValue)const{
        if (this->IsEmpty())
            return;
        //
        // auto currentNode = this->SearchLeftMostLeafNode();

        // while (currentNode)
        // {
        //     for(auto& row: currentNode->DataRowsNoLock(this->table))
        //         this->table->HandleAddColumn(currentNode, &row, index, defaultValue);
        //
        //     if(!currentNode->HasRightSibling())
        //         return;
        //
        //     currentNode = this->GetNode(currentNode.GetRightSibling());
        // }
    }

    void BTree::RemoveColumnFromRow(const column_index_t index)const{
        if (this->IsEmpty())
            return;
        //
        // auto root = this->GetNode(this->rootPageId);
        //
        // auto currentNode = this->SearchLeftMostLeafNode();
        //
        // while (true)
        // {
        //     // for(auto& row: currentNode->DataRowsNoLock(this->table))
        //     //     DatabaseEngine::StorageTypes::Table::HandleRemoveColumn(currentNode, &row, index);
        //
        //     if(!currentNode.HasRightSibling())
        //         return;
        //
        //     currentNode = this->GetNode(currentNode.RightSibling());
        // }

    }

    bool BTree::IsEmpty() const{ return this->rootPageId == INVALID_PAGE_ID; }

    void BTree::CalculateIndexStatistics(
        Headers::IndexStatistics& indexStatistics,
        Headers::TableStatistics& tableStatistics, DataStructures::Array<Headers::ColumnStatistics>& columnStatistics,
        Dictionary<Int, SortedDictionary<Value, BigInt, ValueComparator>>& sortedValues
    ) const {
        if (this->IsEmpty())
            return;

        const CoreEngine::Memory::Allocator allocator;
        auto currentNode = this->SearchLeftMostLeafNode(&allocator, indexStatistics.depth);

        if (this->type == Constants::TreeType::Clustered){
            this->CalculateClusteredStatistics(
                &allocator,
                currentNode,
                indexStatistics,
                tableStatistics,
                columnStatistics,
                sortedValues
            );
            return;
        }
    }
}
