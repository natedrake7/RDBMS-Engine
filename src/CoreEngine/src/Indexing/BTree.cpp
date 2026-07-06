#include "../../include/Indexing/BTree.h"
#include <algorithm>
#include <cassert>
#include "../../include/DataStorage/Row.h"
#include "../../include/DataStorage/Column.h"
#include "../../include/DataStorage/Table.h"
#include "../../include/BufferPool/StorageManager.h"
#include "../../include/Database.h"
#include "../../../Systemic/include/Guards/ReaderGuard.h"
#include "../../../Systemic/include/Guards/WriterGuard.h"
#include "Schedulers/StatisticsScheduler.h"
#include <cmath>

#include "ScanState.h"
#include "Contexts/ExecutionContext.h"
#include "Evaluators/Expression.h"
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

    Int BTree::ScanLeafUpperBound(
        const Pages::IndexPageView& page,
        const DataTypes::Indexing::Key& key,
        Int left
    ){
        Int right = page.Keys();

        while (left < right){
            const Int mid = left + (right - left) / 2;

            const auto comparison = page.PartialComparePageKeyAgainst(key, mid);
            if (comparison == Comparators::Comparator::Greater){
                right = mid;
                continue;
            }

            left = mid + 1;
        }

        return left;
    }

    Int BTree::ScanLeafLowerBound(
        const Pages::IndexPageView& page,
        const DataTypes::Indexing::Key& key
    ){
        Int left = 0;
        Int right = page.Keys();

        while (left < right){
            const Int mid = left + (right - left) / 2;
            if (page.PartialComparePageKeyAgainst(key, mid) == Comparators::Comparator::Less){
                left = mid + 1;
                continue;
            }

            right = mid;
        }

        return left;
    }

    Int BTree::LeafLowerBound(
        const Pages::IndexPageView& page,
        const DataTypes::Indexing::Key& key
    ){
        Int left = 0;
        Int right = page.Keys();

        while (left < right){
            const Int mid = left + (right - left) / 2;
            if (page.ComparePageKeyAgainst(key, mid) == Comparators::Comparator::Less){
                left = mid + 1;
                continue;
            }

            right = mid;
        }

        if (left < page.Keys()){
            if (page.ComparePageKeyAgainst(key, left) == Comparators::Comparator::Equal)
                return -1;  // duplicate key
        }

        return left;
    }

    Int BTree::InternalNodeLowerBound(
        const Pages::IndexPageView& page,
        const DataTypes::Indexing::Key& key
    ){
        Int left = 0;
        Int right = page.Keys();

        while (left < right) {
            const Int mid = left + (right - left) / 2;

            if (page.ComparePageKeyAgainst(key, mid + 1) == Comparators::Comparator::Less)
                left = mid + 1;
            else
                right = mid;
        }

        return left;
    }

    Errors::RuntimeStatus BTree::CreateDuplicateKeyError(
        const DataTypes::Indexing::Key &key,
        const ::Memory::IAllocator* allocator
    ) {
        auto str = DataTypes::String::Concat(allocator, "BTree::CreateDuplicateKeyError: Key ", key.ToString(allocator), " already exists");
        return Errors::RuntimeStatus(Errors::RuntimeError::DuplicateKey, std::move(str));
    }

    Pages::IndexPageView BTree::CreateRootPage(
        CoreEngine::StorageTypes::ExtentReservation& extentReservation,
        Int& indexPosition
    ) {
        //maybe root page is removed and need to be reopened
        auto root = extentReservation.Next<Pages::IndexPageView>();

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
        CoreEngine::StorageTypes::ExtentReservation& extentReservation
    ){
        {
            auto newRoot = extentReservation.Next<Pages::IndexPageView>();

            auto promotedRootLock = MultiThreading::WriterGuard::Promote(&root.Latch(), rootLock);

            MultiThreading::WriterGuard newRootLock(&newRoot.Latch());

            newRoot.SetIsRoot(true);
            newRoot.SetIsLeaf(false);
            newRoot.SetTreeType(this->type);

            newRoot.InsertFirstChild(root.PageId());

            root.SetIsRoot(false);
            this->rootPageId = newRoot.PageId();

            this->SplitChildNoLock(context, newRoot, 0, root, extentReservation);
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
        CoreEngine::StorageTypes::ExtentReservation& extentReservation
    ){
        auto parentLock = MultiThreading::WriterGuard::Promote(&parent.Latch(), parentReadLock);
        auto childLock = MultiThreading::WriterGuard::Promote(&child.Latch(), childReadLock);

        this->SplitChildNoLock(context, parent, index, child, extentReservation);
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
        const auto childKey = newChild.GetKeyByIndex(0);
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
        const auto promotedKey = child.GetKeyByIndex(mid);
        // 2. fix first child of right node
        const auto firstChild = child.GetChild(mid);
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
        CoreEngine::StorageTypes::ExtentReservation& extentReservation
    ) const{
        const auto newChild = extentReservation.Next<Pages::IndexPageView>();

        MultiThreading::WriterGuard newChildLock(&newChild.Latch());

        newChild.SetIsLeaf(child.IsLeaf());
        newChild.SetIsRoot(false);
        newChild.SetTreeType(this->type);

        if (child.IsLeaf()){
            BTree::SplitLeafNoLock(context, parent, child, newChild, index);
            return;
        }

        BTree::SplitInternalNodeNoLock(context, parent, child, newChild, index);
    }

    Errors::RuntimeStatus BTree::InsertToNonFullNode(
        const CoreEngine::ExecutionContext& context,
        Pages::IndexPageView& root,
        const Pages::IndexInsertTuple& tuple,
        CoreEngine::StorageTypes::ExtentReservation& extentReservation,
        Int& indexPosition
    ){
        auto node = std::move(root);
        while (true){
            MultiThreading::ReaderGuard nodeLock(&node.Latch());

            // Leaf node => insert here
            if (node.IsLeaf())
                return BTree::InsertToNode(
                    context,
                    node, nodeLock,
                    tuple, indexPosition
                );

            auto childIndex = BTree::InternalNodeLowerBound(node, tuple.key);
            auto childPageId = node.GetChild(childIndex);
            auto child = this->GetNode(childPageId);

            MultiThreading::ReaderGuard childLock(&child.Latch());

            if (this->ShouldSplit(child)){
                this->SplitChild(
                    context,node,
                    nodeLock,
                    childIndex, child,
                    childLock,
                    extentReservation
                );

                MultiThreading::ReaderGuard newNodeLock(&node.Latch());

                childIndex = BTree::InternalNodeLowerBound(node, tuple.key);
                childPageId = node.GetChild(childIndex);
                child = this->GetNode(childPageId);
            }

            // descend iteratively
            node = std::move(child);
            // locks released automatically here
        }
    }

    Errors::RuntimeStatus BTree::InsertToNode(
        const CoreEngine::ExecutionContext& context,
        const Pages::IndexPageView &node,
        MultiThreading::ReaderGuard& readGuard,
        const Pages::IndexInsertTuple& tuple,
        Int& indexPosition
    ){
        indexPosition = BTree::LeafLowerBound(node, tuple.key);
        if (indexPosition == -1)
            return BTree::CreateDuplicateKeyError(tuple.key, context.GetAllocator());

        auto writerGuard = MultiThreading::WriterGuard::Promote(&node.Latch(), readGuard);
        node.InsertTuple(tuple, indexPosition);
        return {};
    }

    Pages::IndexPageView BTree::SearchKey(const DataTypes::Indexing::Key &key) const{
        auto currentNode = this->GetNode(this->rootPageId);

        while (true) {
            MultiThreading::ReaderGuard lock(&currentNode.Latch());

            if (currentNode.IsLeaf())
                return currentNode;

            const auto index = BTree::InternalNodeLowerBound(currentNode, key);
            const auto childId = currentNode.GetChild(index);
            currentNode = this->GetNode(childId);
        }
    }

    Pages::IndexPageView BTree::SearchKeyWithAncestors(const DataTypes::Indexing::Key& key, DataStructures::PolymorphicArray<Pages::IndexPageView> & ancestors) const{
      auto currentNode = this->GetNode(this->rootPageId);

    //   while (!currentNode.IsLeaf()){
    //     const auto index = BTree::InternalNodePartialLowerBound(currentNode, key);

    //     ancestors.push_back(std::move(currentNode));

    //     currentNode = std::move(this->GetNode(currentNode.GetChild(index)));
    //   }

      return currentNode;
    }

    Pages::IndexPageView BTree::SearchLeftMostLeafNode() const{
        auto currentNode = this->GetNode(this->rootPageId);

        while (!currentNode.IsLeaf()) {
            MultiThreading::ReaderGuard lock(&currentNode.Latch());
            currentNode = this->GetNode(currentNode.GetChild(0));
        }

        return currentNode;
    }

    Pages::IndexPageView BTree::SearchLeftMostLeafNode(TinyInt &depth) const{
        auto currentNode = this->GetNode(this->rootPageId);

        while (!currentNode.IsLeaf()){
            MultiThreading::ReaderGuard lock(&currentNode.Latch());
            depth++;
            currentNode = this->GetNode(currentNode.GetChild(0));
        }

        return currentNode;
    }

    Pages::IndexPageView BTree::GetNode(const page_id_t pageId) const{
        return Storage::StorageManager::Get().GetPage<Pages::IndexPageView>(
            this->database->DataFileKey(),
            pageId
        );
    }

    Int BTree::CalculateTreeDegree(
        const CoreEngine::StorageTypes::Table* otherTable,
        const Constants::TreeType treeType,
        const Int nonClusteredId
    )const{
        if(treeType == Constants::TreeType::Clustered){
          auto rowSize = otherTable->GetMaximumRowSize();
          Int calculatedDegree = Constants::INDEX_PAGE_DEFAULT_SIZE / ((this->keySize + rowSize) * 2);

          while(calculatedDegree < 2){
            rowSize = otherTable->ReduceMaximumRowSize();

            calculatedDegree = Constants::INDEX_PAGE_DEFAULT_SIZE / ((this->keySize + rowSize) * 2);
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

    void BTree::HandleUnderflow(const Pages::IndexPageView& node, DataStructures::PolymorphicArray<Pages::IndexPageView>& ancestors, Int& parentIndex) {
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
                    const auto childKey = child.GetKeyByIndex(childIndex - 1);
                    // parent.InsertKey(childKey, childIndex - 1);
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
        Int parentKeyIndex, DataStructures::PolymorphicArray<Pages::IndexPageView>& ancestors,
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
        Headers::TableStatistics& tableStatistics, DataStructures::PolymorphicArray<Headers::ColumnStatistics>& columnStatistics,
        Dictionary<Int, SortedDictionary<Value, BigInt, ValueComparator>>& sortedValues
    ) const{

        while (true) {
            MultiThreading::ReaderGuard lock(&currentNode.Latch());

            tableStatistics.pageCount++;
            indexStatistics.leafPages++;

            const auto numOfRows = currentNode.PageSize();
            tableStatistics.rowCount += static_cast<Int>(numOfRows);

            for (Int i = 0;i < numOfRows; i++){
                // auto [_, rowPtr] = currentNode.PeekLeafTuple(allocator, i);

                // auto materializedRow = rowPtr.Materialize(allocator);
                // tableStatistics.averageRowSize += rowPtr.Size();
                //
                // for (Int j = 0; j < columnStatistics.Size(); j++) {
                //     auto& columnStats = columnStatistics[j];
                //
                //     CoreEngine::StatisticsScheduler::UpdateColumnStatistics(
                //         columnStats,
                //         materializedRow.GetColumnReferenceAt(j),
                //         sortedValues[columnStats.columnId]
                //     );
                // }
            }

            if(!currentNode.HasRightSibling())
                break;

            currentNode = this->GetNode(currentNode.RightSibling());
        }

        tableStatistics.averageRowSize = static_cast<Int>(std::ceil(static_cast<float>(tableStatistics.averageRowSize) / static_cast<float>(tableStatistics.rowCount)));
    }

    void BTree::UpdatePfsPage(const Pages::IndexPageView& page) const{
        const auto pageFreeSpacePage = CoreEngine::Database::GetAssociatedPfsPage(
            this->database->SystemFileKey(),
            page.PageId()
        );

        MultiThreading::WriterGuard pfsPageLock(&pageFreeSpacePage.Latch());
        MultiThreading::WriterGuard pageLock(&page.Latch());

        pageFreeSpacePage.SetPageMetaData(&page);
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
        CoreEngine::StorageTypes::ExtentReservation& extentReservation,
        Int &indexPosition
    ){
        //base case scenario
        if (this->IsEmpty()) {
            const auto root =  this->CreateRootPage(extentReservation, indexPosition);

            if(this->nonClusteredIndexId != -1)
                this->table->SetNonClusteredIndexPageId(this->rootPageId, this->nonClusteredIndexId);
            else
                this->table->SetClusteredIndexPageId(this->rootPageId);

            root.InsertTuple(tuple);
            this->UpdatePfsPage(root);
            return Errors::RuntimeStatus();
        }

        auto root = this->GetNode(this->rootPageId);

        {
            MultiThreading::ReaderGuard rootLock(&root.Latch());

            if (root.Keys() == 2 * this->degree - 1) // root is full,
                this->SplitRoot(context, root, rootLock, extentReservation);
        }

        return this->InsertToNonFullNode(context, root, tuple, extentReservation, indexPosition);
    }

    void BTree::IndexSeekRange(
        const CoreEngine::ExecutionContext& context,
        const DataTypes::Indexing::Key &minKey,
        const DataTypes::Indexing::Key &maxKey,
        DataStructures::PolymorphicArray<CoreEngine::StorageTypes::RID>* result
    )const{
        if (this->IsEmpty())
            return;

        auto currentNode = this->SearchKey(minKey);

        auto startingIndex = BTree::ScanLeafLowerBound(currentNode, minKey);

        while (true){
            MultiThreading::ReaderGuard lock(&currentNode.Latch());
            const auto endingIndex = BTree::ScanLeafUpperBound(currentNode, maxKey, startingIndex);

            for (Int i = startingIndex; i < endingIndex; i++){
                if (!currentNode.IsRowVisible(context.GetSnapshot(), i))
                    continue;

                result->Push(CoreEngine::StorageTypes::RID(currentNode.PageId(), i));
            }

            if(!currentNode.HasRightSibling())
                return;

            currentNode = this->GetNode(currentNode.RightSibling());
            startingIndex = 0; // Reset starting index for subsequent nodes
        }
    }

    void BTree::IndexSeekRange(
        const CoreEngine::ExecutionContext& context,
        const DataTypes::Indexing::Key& minKey,
        const DataTypes::Indexing::Key& maxKey,
        DataStructures::PolymorphicArray<CoreEngine::StorageTypes::RID>* result,
        const Expressions::Expression* expression
    ) const{
        if (this->IsEmpty())
            return;

        auto currentNode = this->SearchKey(minKey);
        auto startingIndex = BTree::ScanLeafLowerBound(currentNode, minKey);

        while (true){
            MultiThreading::ReaderGuard lock(&currentNode.Latch());

            Expressions::EvaluationContext evaluationContext(
                Expressions::EvaluationContext::EvaluationContextType::SingleRow,
                context
            );

            const auto endingIndex = BTree::ScanLeafUpperBound(currentNode, maxKey, startingIndex);
            for (Int i = startingIndex; i < endingIndex; i++){
                if (!currentNode.IsRowVisible(context.GetSnapshot(), i))
                    continue;

                auto row = CoreEngine::StorageTypes::RID(currentNode.PageId(), i);
                evaluationContext.row = &row;

                if (Expressions::RowModeFilter(expression, evaluationContext))
                    result->Push(row);
            }

            if(!currentNode.HasRightSibling())
                return;

            currentNode = this->GetNode(currentNode.RightSibling());
            startingIndex = 0; // Reset starting index for subsequent nodes
        }
    }

    void BTree::IndexSeek(
        const CoreEngine::ExecutionContext& context,
        const DataTypes::Indexing::Key &key,
        DataStructures::PolymorphicArray<CoreEngine::StorageTypes::RID>* result
    ) const {
        if (this->IsEmpty())
            return;

        auto currentNode = this->SearchKey(key);

        auto startingIndex = BTree::ScanLeafLowerBound(currentNode, key);
        while (true){

            MultiThreading::ReaderGuard lock(&currentNode.Latch());

            const auto endingIndex = BTree::ScanLeafUpperBound(currentNode, key, startingIndex);
            for (Int i = startingIndex; i < endingIndex; i++){
                if (!currentNode.IsRowVisible(context.GetSnapshot(), i))
                    continue;

                result->Push(CoreEngine::StorageTypes::RID(currentNode.PageId(), i));
            }

            const auto rightSibling = currentNode.RightSibling();
            if(rightSibling == INVALID_PAGE_ID)
                return;

            currentNode = this->GetNode(rightSibling);
            startingIndex = 0; // Reset starting index for subsequent nodes
        }
    }

    void BTree::IndexSeek(
        const CoreEngine::ExecutionContext& context,
        const DataTypes::Indexing::Key &key,
        DataStructures::PolymorphicArray<CoreEngine::StorageTypes::RID>* result,
        const Expressions::Expression *expression
    ) const {
        if (this->IsEmpty())
            return;

        auto currentNode = this->SearchKey(key);

        auto evaluationContext = Expressions::EvaluationContext(
            Expressions::EvaluationContext::EvaluationContextType::SingleRow,
            context
        );

        auto startingIndex = BTree::ScanLeafLowerBound(currentNode, key);
        while (true){
            MultiThreading::ReaderGuard lock(&currentNode.Latch());

            evaluationContext.page = &currentNode;
            const auto endingIndex = BTree::ScanLeafUpperBound(currentNode, key, startingIndex);
            for (Int i = startingIndex; i < endingIndex; i++){
                if (!currentNode.IsRowVisible(context.GetSnapshot(), i))
                    continue;

                auto rid = CoreEngine::StorageTypes::RID(currentNode.PageId(), i);
                evaluationContext.row = &rid;
                if (Expressions::RowModeFilter(expression, evaluationContext))
                    result->Push(rid);
            }

            const auto rightSibling = currentNode.RightSibling();
            if(rightSibling == INVALID_PAGE_ID)
                return;

            currentNode = this->GetNode(rightSibling);
            startingIndex = 0; // Reset starting index for subsequent nodes
        }
    }

    void BTree::SystemIndexSeek(
        const DataTypes::Indexing::Key& key,
        DataStructures::PolymorphicArray<CoreEngine::StorageTypes::RID>* result
    ) const{
        if (this->IsEmpty())
            return;

        auto currentNode = this->SearchKey(key);
        auto startingIndex = BTree::ScanLeafLowerBound(currentNode, key);

        while (true){
            MultiThreading::ReaderGuard lock(&currentNode.Latch());

            const auto endingIndex = BTree::ScanLeafUpperBound(currentNode, key, startingIndex);
            for (Int i = startingIndex; i < endingIndex; i++)
                result->Push(CoreEngine::StorageTypes::RID(currentNode.PageId(), i));

            const auto rightSibling = currentNode.RightSibling();
            if(rightSibling == INVALID_PAGE_ID)
                return;

            currentNode = this->GetNode(rightSibling);
            startingIndex = 0; // Reset starting index for subsequent nodes
        }
    }

    void BTree::SystemIndexSeek(
        const ::Memory::IAllocator* allocator,
        const DataTypes::Indexing::Key& key,
        DataStructures::PolymorphicArray<CoreEngine::StorageTypes::RID>* result,
        const Expressions::Expression* expression
    ) const{
        if (this->IsEmpty())
            return;

        auto currentNode = this->SearchKey(key);

        auto evaluationContext = Expressions::EvaluationContext(
            Expressions::EvaluationContext::EvaluationContextType::SingleRow,
            allocator,
            this->table
        );

        auto startingIndex = BTree::ScanLeafLowerBound(currentNode, key);
        while (true){
            MultiThreading::ReaderGuard lock(&currentNode.Latch());

            evaluationContext.page = &currentNode;
            const auto endingIndex = BTree::ScanLeafUpperBound(currentNode, key, startingIndex);
            for (Int i = startingIndex; i < endingIndex; i++){
                auto rid = CoreEngine::StorageTypes::RID(currentNode.PageId(), i);
                evaluationContext.row = &rid;

                if (Expressions::RowModeFilter(expression, evaluationContext))
                    result->Push(rid);
            }

            const auto rightSibling = currentNode.RightSibling();
            if(rightSibling == INVALID_PAGE_ID)
                return;

            currentNode = this->GetNode(rightSibling);
            startingIndex = 0; // Reset starting index for subsequent nodes
        }
    }

    void BTree::IndexScan(
        const CoreEngine::ExecutionContext& context,
        DataStructures::PolymorphicArray<CoreEngine::StorageTypes::RID>* result,
        CoreEngine::IndexState& state
    )const{
        if (this->IsEmpty())
            return;

        result->Reserve(context.GetBatchSize());
        auto currentNode = state.pageId == INVALID_PAGE_ID
                                ? this->SearchLeftMostLeafNode()
                                : this->GetNode(state.pageId);

        state.canFetchMore = false;
        while (true){
            MultiThreading::ReaderGuard lock(&currentNode.Latch());

            for (Int i = state.GetNextKeyIndex(); i < currentNode.PageSize(); i++){
                if (!currentNode.IsRowVisible(context.GetSnapshot(), i))
                    continue;
                result->Push(CoreEngine::StorageTypes::RID(currentNode.PageId(), i));
            }

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
        DataStructures::PolymorphicArray<CoreEngine::StorageTypes::RID>* result,
        CoreEngine::IndexState& state,
        const Expressions::Expression *expression
    )const{
        if (this->IsEmpty())
            return;

        auto currentNode = state.pageId == INVALID_PAGE_ID
                                ? this->SearchLeftMostLeafNode()
                                : this->GetNode(state.pageId);

        Expressions::EvaluationContext evaluationContext(
            Expressions::EvaluationContext::EvaluationContextType::SingleRow,
            context
        );

        state.canFetchMore = false;
        bool exprResult = false, exprNull = false;
        while (true){
            MultiThreading::ReaderGuard lock(&currentNode.Latch());

            evaluationContext.page = &currentNode;
            for (Int i = state.GetNextKeyIndex(); i < currentNode.PageSize(); i++) {
                if (!currentNode.IsRowVisible(context.GetSnapshot(), i))
                    continue;

                auto rid = CoreEngine::StorageTypes::RID(currentNode.PageId(), i);
                evaluationContext.row = &rid;
                Expressions::EvaluateExpression(expression, evaluationContext, &exprResult, &exprNull);
                if (exprResult)
                    result->Push(rid);

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
        DataStructures::PolymorphicArray<CoreEngine::StorageTypes::RID>* result,
        const Expressions::Expression *expression
    )const{
        if (this->IsEmpty())
            return;

        auto currentNode = this->SearchLeftMostLeafNode();
        Expressions::EvaluationContext evaluationContext(
            Expressions::EvaluationContext::EvaluationContextType::SingleRow,
            context
        );

        bool exprResult = false, exprNull = false;
        while (true){
            MultiThreading::ReaderGuard lock(&currentNode.Latch());

            evaluationContext.page = &currentNode;
            for (Int i = 0;i < currentNode.PageSize();i++){
                if (!currentNode.IsRowVisible(context.GetSnapshot(), i))
                    continue;

                auto rid = CoreEngine::StorageTypes::RID(currentNode.PageId(), i);
                evaluationContext.row = &rid;
                Expressions::EvaluateExpression(expression, evaluationContext, &exprResult, &exprNull);

                if(exprResult)
                    result->Push(rid);
            }

            if(!currentNode.HasRightSibling())
                return;

            currentNode = this->GetNode(currentNode.RightSibling());
        }
    }

    void BTree::SystemIndexScan(
        const ::Memory::IAllocator* allocator,
        DataStructures::PolymorphicArray<CoreEngine::StorageTypes::RID>* result,
        const Expressions::Expression* expression
    ) const{
        if (this->IsEmpty())
            return;

        auto currentNode = this->SearchLeftMostLeafNode();
        Expressions::EvaluationContext evaluationContext(
            Expressions::EvaluationContext::EvaluationContextType::SingleRow,
            allocator,
            this->table
        );

        while (true){
            MultiThreading::ReaderGuard lock(&currentNode.Latch());

            evaluationContext.page = &currentNode;
            for (Int i = 0;i < currentNode.PageSize();i++){
                auto rid = CoreEngine::StorageTypes::RID(currentNode.PageId(), i);

                evaluationContext.row = &rid;
                if (!Expressions::RowModeFilter(expression, evaluationContext))
                    continue;

                result->Push(rid);
            }

            if(!currentNode.HasRightSibling())
                return;

            currentNode = this->GetNode(currentNode.RightSibling());
        }
    }

    void BTree::SystemIndexScan(DataStructures::PolymorphicArray<CoreEngine::StorageTypes::RID>* result) const{
        if (this->IsEmpty())
            return;

        auto currentNode = this->SearchLeftMostLeafNode();
        while (true){
            MultiThreading::ReaderGuard lock(&currentNode.Latch());

            for(Int i = 0;i < currentNode.PageSize();i++)
                result->Push(CoreEngine::StorageTypes::RID(currentNode.PageId(), i));

            if(!currentNode.HasRightSibling())
                return;

            currentNode = this->GetNode(currentNode.RightSibling());
        }
    }

    void BTree::IndexScan(
        const CoreEngine::ExecutionContext& context,
        DataStructures::PolymorphicArray<CoreEngine::StorageTypes::RID>* result
    )const{
        if (this->IsEmpty())
            return;

        auto currentNode = this->SearchLeftMostLeafNode();
        while (true){
            MultiThreading::ReaderGuard lock(&currentNode.Latch());

            for(Int i = 0;i < currentNode.PageSize();i++){
                if (!currentNode.IsRowVisible(context.GetSnapshot(), i))
                    continue;
                result->Push(CoreEngine::StorageTypes::RID(currentNode.PageId(), i));
            }

            if(!currentNode.HasRightSibling())
                return;

            currentNode = this->GetNode(currentNode.RightSibling());
        }
    }

    void BTree::IndexScan(
     DataStructures::PolymorphicArray<DataTypes::RowIdentifier> *result,
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

    void BTree::IndexScan(DataStructures::PolymorphicArray<DataTypes::RowIdentifier> *result, const Expressions::Expression *expression)const{
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
        const DataStructures::PolymorphicArray<Value> &updates
    )const{
        if (this->IsEmpty())
            return;

        auto currentNode = this->SearchLeftMostLeafNode();
        Expressions::EvaluationContext evaluationContext(
            Expressions::EvaluationContext::EvaluationContextType::SingleRow,
            context
        );

        bool exprResult = false, exprNull = false;
        while (true){
            MultiThreading::WriterGuard lock(&currentNode.Latch());

            evaluationContext.page = &currentNode;
            for (Int i = 0; i < currentNode.PageSize();i++){
                if (!currentNode.IsRowVisible(context.GetSnapshot(), i))
                    continue;
                const auto row = CoreEngine::StorageTypes::RID(currentNode.PageId(), i);

                evaluationContext.row = &row;
                Expressions::EvaluateExpression(expression, evaluationContext, &exprResult, &exprNull);
                if (!exprResult) continue;

                const auto result = this->table->UpdateRowNoLock(
                    &currentNode,
                    &row,
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
        const DataStructures::PolymorphicArray<Expressions::Expression*>& updates
    )const{
        if (this->IsEmpty())
            return {};

        auto currentNode = this->SearchLeftMostLeafNode();
        Expressions::EvaluationContext evaluationContext(
            Expressions::EvaluationContext::EvaluationContextType::SingleRow,
            context
        );

        bool exprResult = false, exprNull = false;
        while (true){
            MultiThreading::WriterGuard lock(&currentNode.Latch());

            evaluationContext.page = &currentNode;
            for (Int i = 0;i < currentNode.PageSize();i++){
                if (!currentNode.IsRowVisible(context.GetSnapshot(), i))
                    continue;

                auto rid = CoreEngine::StorageTypes::RID(currentNode.PageId(), i);
                evaluationContext.row = &rid;

                Expressions::EvaluateExpression(expression, evaluationContext, &exprResult, &exprNull);
                if (!exprResult) continue;

                auto result = this->table->UpdateRowNoLock(
                    &currentNode,
                    &rid,
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
       const DataStructures::PolymorphicArray<Expressions::Expression*>& updates
    )const{
        if (this->IsEmpty())
            return {};

        auto currentNode = this->SearchLeftMostLeafNode();

        while (true){
            MultiThreading::WriterGuard lock(&currentNode.Latch());

            for (Int i = 0;i < currentNode.PageSize();i++){
                if (!currentNode.IsRowVisible(context.GetSnapshot(), i))
                    continue;

                auto rid = CoreEngine::StorageTypes::RID(currentNode.PageId(), i);
                auto result = this->table->UpdateRowNoLock(
                    &currentNode,
                    &rid,
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
        const DataStructures::PolymorphicArray<Value> &updates
    ) const {
        if (this->IsEmpty())
            return {};

        auto currentNode = this->SearchKey(key);

        auto startingIndex = BTree::ScanLeafLowerBound(currentNode, key);
        while (true) {
            MultiThreading::WriterGuard lock(&currentNode.Latch());

            const auto endingIndex = BTree::ScanLeafUpperBound(currentNode, key, startingIndex);
            for (Int i = startingIndex; i < endingIndex; i++){
                if (!currentNode.IsRowVisible(context.GetSnapshot(), i))
                    continue;

                auto rid = CoreEngine::StorageTypes::RID(currentNode.PageId(), i);
                auto result = this->table->UpdateRowNoLock(
                    &currentNode,
                    &rid,
                    context,
                    updates
                );

                if (!result.IsOk())
                    return result;
            }

            if(!currentNode.HasRightSibling())
                return {};

            currentNode = this->GetNode(currentNode.RightSibling());
            startingIndex = 0;
        }
    }

    Errors::RuntimeStatus BTree::IndexSeekUpdate(
        const CoreEngine::ExecutionContext& context,
        const Expressions::Expression* expression,
        const DataTypes::Indexing::Key* minKey,
        const DataTypes::Indexing::Key* maxKey,
        const DataStructures::PolymorphicArray<Value>& updates
    )const{
        if (this->IsEmpty())
            return {};

        Expressions::EvaluationContext evaluationContext(
            Expressions::EvaluationContext::EvaluationContextType::SingleRow,
            context
        );

        auto currentNode = this->SearchKey(*minKey);
        auto startingIndex = BTree::ScanLeafLowerBound(currentNode, *minKey);

        while (true){
            MultiThreading::WriterGuard lock(&currentNode.Latch());

            evaluationContext.page = &currentNode;
            const auto endingIndex = BTree::ScanLeafUpperBound(currentNode, *maxKey, startingIndex);
            for (Int i = startingIndex; i < endingIndex; i++){
                if (!currentNode.IsRowVisible(context.GetSnapshot(), i))
                    continue;

                auto rid = CoreEngine::StorageTypes::RID(currentNode.PageId(), i);
                evaluationContext.row = &rid;
                if(!Expressions::RowModeFilter(expression, evaluationContext))
                    continue;

                auto result = this->table->UpdateRowNoLock(
                    &currentNode,
                    &rid,
                    context,
                    updates
                );

                if (!result.IsOk())
                    return result;
            }

            if(!currentNode.HasRightSibling())
                return {};

            currentNode = this->GetNode(currentNode.RightSibling());
            startingIndex = 0;
        }
    }

    Errors::RuntimeStatus BTree::IndexSeekUpdate(
        const CoreEngine::ExecutionContext& context,
        const DataTypes::Indexing::Key *minKey,
        const DataTypes::Indexing::Key *maxKey,
        const DataStructures::PolymorphicArray<Value> &updates
    )const{
        if (this->IsEmpty())
            return {};

        auto currentNode = this->SearchKey(*minKey);
        auto startingIndex = BTree::ScanLeafLowerBound(currentNode, *minKey);

        while (true){
            MultiThreading::WriterGuard lock(&currentNode.Latch());

            const auto endingIndex = BTree::ScanLeafUpperBound(currentNode, *maxKey, startingIndex);
            for (Int i = startingIndex; i < endingIndex; i++){
                if (!currentNode.IsRowVisible(context.GetSnapshot(), i))
                    continue;

                auto rid = CoreEngine::StorageTypes::RID(currentNode.PageId(), i);
                auto result = this->table->UpdateRowNoLock(
                    &currentNode,
                    &rid,
                    context,
                    updates
                );

                if (!result.IsOk())
                    return result;
            }

            if(!currentNode.HasRightSibling())
                return {};

            currentNode = this->GetNode(currentNode.RightSibling());
            startingIndex = 0;
        }
    }

    Errors::RuntimeStatus BTree::SystemIndexSeekUpdate(
        const Memory::IAllocator* allocator,
        const DataTypes::Indexing::Key& key,
        const DataStructures::PolymorphicArray<Value>& updates
    ) const{
        if (this->IsEmpty())
            return {};

        auto currentNode = this->SearchKey(key);
        auto startingIndex = BTree::ScanLeafLowerBound(currentNode, key);

        while (true) {
            MultiThreading::WriterGuard lock(&currentNode.Latch());

            const auto endingIndex = BTree::ScanLeafUpperBound(currentNode, key, startingIndex);
            for (Int i = startingIndex; i < endingIndex; i++){
                auto rid = CoreEngine::StorageTypes::RID(currentNode.PageId(), i);
                auto result = this->table->SystemUpdateRowNoLock(
                    &currentNode,
                    &rid,
                    allocator,
                    updates
                );

                if (!result.IsOk())
                    return result;
            }

            if(!currentNode.HasRightSibling())
                return {};

            currentNode = this->GetNode(currentNode.RightSibling());
            startingIndex = 0;
        }
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
    void BTree::InsertRowsToOtherTree(CoreEngine::StorageTypes::ExtentReservation& extentReservation, const Int indexPos)const{
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
        Headers::TableStatistics& tableStatistics, DataStructures::PolymorphicArray<Headers::ColumnStatistics>& columnStatistics,
        Dictionary<Int, SortedDictionary<Value, BigInt, ValueComparator>>& sortedValues
    ) const {
        if (this->IsEmpty())
            return;

        const CoreEngine::Memory::Allocator allocator;
        auto currentNode = this->SearchLeftMostLeafNode(indexStatistics.depth);

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
