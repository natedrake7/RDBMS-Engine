#include "../include/BTree.h"
#include <algorithm>
#include <iostream>
#include <sstream>
#include "../include/DataStorage/Column.h"
#include "../include/DataStorage/Row.h"
#include "../include/DataStorage/Table.h"
#include "../include/Pages/IndexPage.h"
#include "../include/BufferPool/StorageManager.h"
#include "../include/Database.h"
#include "../../Systemic/include/Guards/ReaderGuard.h"
#include "../../Systemic/include/Guards/WriterGuard.h"
#include "Schedulers/StatisticsScheduler.h"
#include <cmath>

#include "Pages/PageFreeSpacePage.h"

namespace Indexing{
    void BTree::AssignLeavesConnections(
        Pages::IndexPageView& child,
        Pages::IndexPageView& newChild
    ){
        newChild.SetRightSibling(child.RightSibling());
        newChild.SetLeftSibling(child.PageId());

        child.SetRightSibling(newChild.PageId());
    }

    Int BTree::LeafLowerBound(
        const Pages::IndexPageView& page,
        const DataTypes::Indexing::Key& key
    ){
        const auto numberOfKeys = page.SubKeys();

        for (Int i = 0; i < numberOfKeys; i++) {
            const auto tupleKey = page.GetKey(i);

            if (tupleKey == key){
                std::cout << "Found duplicate key: " << key << " and: " << tupleKey << " at position " << i << " in page " << page.PageId() << std::endl;
                return -1;
            }

            if (tupleKey > key)
                return i;
        }

        return numberOfKeys;
    }

    Int BTree::LeafPartialLowerBound(
        const Pages::IndexPageView& page,
        const DataTypes::Indexing::Key& key
    ){
        const auto numberOfKeys = page.SubKeys();

        for (Int i = 0; i < numberOfKeys; i++) {
            const auto pageKey = page.GetKey(i);

            if (key <= pageKey)
                return i;
        }

        return numberOfKeys;
    }

    Int BTree::InternalNodeLowerBound(
        const Pages::IndexPageView& page,
        const DataTypes::Indexing::Key& key
    ){
        const auto numberOfKeys = page.SubKeys();

        for (Int i = 0; i < numberOfKeys; i++) {
            const auto tupleKey = page.GetKey(i + 1);

            if (tupleKey == key){
                std::cout << "Found duplicate key: " << key << " and: " << tupleKey << " at position " << i << " in page " << page.PageId() << std::endl;
                return -1;
            }

            if (tupleKey > key)
                return i;
        }

        return numberOfKeys;
    }

    Int BTree::InternalNodePartialLowerBound(
        const Pages::IndexPageView& page,
        const DataTypes::Indexing::Key& key
    ){
        const auto numberOfKeys = page.SubKeys();

        for (Int i = 0; i < numberOfKeys; i++) {
            const auto pageKey = page.GetKey(i + 1);

            if (key <= pageKey)
                return i;
        }

        return numberOfKeys;
    }

    Errors::RuntimeStatus BTree::CreateDuplicateKeyError(const DataTypes::Indexing::Key &key) {
        ostringstream os;
        os << "BTree::CreateDuplicateKeyError: Key " << key << " already exists" << std::endl;
        return {Errors::RuntimeError::DuplicateKey, os.str()};
    }

    Pages::IndexPageView BTree::CreateRootPage(Int& indexPosition, const Int pagesToAllocate) {
        //maybe root page is removed and need to be reopened
        auto root = this->AllocateNewPage(INVALID_PAGE_ID, pagesToAllocate);

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
        Pages::IndexPageView& root,
        MultiThreading::ReaderGuard& rootLock,
        const Int pagesToAllocate
    ){
        {
            auto newRoot = this->AllocateNewPage(this->rootPageId, pagesToAllocate);

            MultiThreading::WriterGuard newRootLock(&newRoot.Latch());

            newRoot.SetIsRoot(true);
            newRoot.SetIsLeaf(false);
            newRoot.SetTreeType(this->type);

            auto promotedRootLock = MultiThreading::WriterGuard::Promote(&root.Latch(), rootLock);

            newRoot.InsertChild(root.PageId());
            root.SetIsRoot(false);
            this->rootPageId = newRoot.PageId();

            this->SplitChildNoLock(newRoot, 0, root, pagesToAllocate);
            root = newRoot;
        }

        //let table mutexes handle this
        if(this->nonClusteredIndexId != -1)
            this->table->SetNonClusteredIndexPageId(this->rootPageId, this->nonClusteredIndexId);
        else
            this->table->SetClusteredIndexPageId(this->rootPageId);
    }

    void BTree::SplitChild(
        Pages::IndexPageView& parent,
        MultiThreading::ReaderGuard& parentReadLock,
        const Int index,
        Pages::IndexPageView& child,
        MultiThreading::ReaderGuard& childReadLock,
        const Int pagesToAllocate
    ){
        auto parentLock = MultiThreading::WriterGuard::Promote(&parent.Latch(), parentReadLock);
        auto childLock = MultiThreading::WriterGuard::Promote(&child.Latch(), childReadLock);

        this->SplitChildNoLock(parent, index, child, pagesToAllocate);
    }

    void BTree::SplitLeafNoLock(
        Pages::IndexPageView &parent,
        Pages::IndexPageView &child,
        Pages::IndexPageView &newChild,
        const Int index
    )const {
        // Move the middle key from the child to the parent
        const auto childKey = child.GetKey(this->degree);
        parent.InsertChild(newChild.PageId(), &childKey, index + 1);

        // Assign the second half of the child's keys to the new child
        if (this->type == TreeType::Clustered) {
            newChild.DistributeFromPage(&child, this->degree, this->degree);
            BTree::AssignLeavesConnections(child, newChild);
            return;
        }

        // for (Int i = this->degree; i < child.GetPageSize(); i++){
        //     auto rowId = child.GetLeafTuple(i).row;
        //     newChild.InsertTuple(LeafNodeTuple{child.GetKey(i), rowId});
        // }
        // auto* childRows = child.NonClusteredDataNoLock();
        //
        // auto* newChildRows = newChild.NonClusteredDataNoLock();
        //
        // newChildRows->assign(childRows->begin() + this->degree, childRows->end());
        // childRows->resize(this->degree);

        BTree::AssignLeavesConnections(child, newChild);
    }

    //TODO maybe optimize further
    void BTree::SplitInternalNodeNoLock(
        const Pages::IndexPageView &parent,
        const Pages::IndexPageView &child,
        const Pages::IndexPageView &newChild,
        const Int index
    ) const {
        const auto childKey = child.GetKey(this->degree - 1);
        parent.InsertChild(newChild.PageId(), &childKey, index + 1);

        const auto middleChild = child.GetChild(this->degree);
        newChild.InsertChild(middleChild);

        newChild.DistributeFromPage(&child, this->degree, this->degree - 1);
    }

    void BTree::SplitChildNoLock(
        Pages::IndexPageView &parent,
        const Int index,
        Pages::IndexPageView &child,
        const Int pagesToAllocate
    ) {
        auto newChild = this->AllocateNewPage(parent.PageId(), pagesToAllocate);

        MultiThreading::WriterGuard newChildLock(&newChild.Latch());

        newChild.SetIsLeaf(child.IsLeaf());
        newChild.SetIsRoot(false);
        newChild.SetTreeType(this->type);

        if (child.IsLeaf()){
            this->SplitLeafNoLock(parent, child, newChild, index);
            return;
        }

        this->SplitInternalNodeNoLock(parent, child, newChild, index);
    }

    Errors::RuntimeStatus BTree::InsertToNonFullNode(
            Pages::IndexPageView& parent,
            const Pages::IndexInsertTuple& tuple,
            const Int pagesToAllocate,
            Int& indexPosition
    ){
        Pages::IndexPageView IntermediateNode;
        {
            MultiThreading::ReaderGuard parentLock(&parent.Latch());

            if (parent.IsLeaf())
                return BTree::InsertToNode(parent, tuple, indexPosition);

            auto childIndex = BTree::InternalNodeLowerBound(parent, tuple.key);

            auto childId = parent.GetChild(childIndex);

            auto child = this->GetNode(childId);

            MultiThreading::ReaderGuard childLock(&child.Latch());

            if (child.SubKeys() == 2 * this->degree - 1){
                // if (!this->TryRedistributeLeaf(parent, parentLock, child, childLock, childIndex)) {
                // Redistribution failed, must split
                    this->SplitChild(parent, parentLock, childIndex, child, childLock, pagesToAllocate);

                    //split child will break the lock and we need to reacquire it
                    MultiThreading::ReaderGuard newParentLock(&parent.Latch());

                    // After split, check which child the key belongs to
                    if (tuple.key > parent.GetKey(childIndex))
                        childIndex++;

                    childId = parent.GetChild(childIndex);

                    IntermediateNode = this->GetNode(childId);
                // }
                // else
                //     IntermediateNode = std::move(parent);
            }
            else
                IntermediateNode = std::move(child);
        }  // All locks released here

        return this->InsertToNonFullNode(IntermediateNode, tuple, pagesToAllocate, indexPosition);
    }

    Errors::RuntimeStatus BTree::InsertToNode(
        const Pages::IndexPageView &parent,
        const Pages::IndexInsertTuple& tuple,
        Int& indexPosition
    )
    {
        indexPosition = BTree::LeafLowerBound(parent, tuple.key);
        if (indexPosition == -1)
            return BTree::CreateDuplicateKeyError(tuple.key);

        parent.InsertTuple(tuple, indexPosition);

        return {};
    }

    Pages::IndexPageView BTree::SearchKey(const DataTypes::Indexing::Key &key) const{
        auto currentNode = this->GetNode(this->rootPageId);

        while (true) {
            MultiThreading::ReaderGuard lock(&currentNode.Latch());

            if (currentNode.IsLeaf())
                return currentNode;

            const auto index = BTree::InternalNodePartialLowerBound(currentNode, key);
            currentNode = this->GetNode(currentNode.GetChild(index));
        }
    }

    Pages::IndexPageView BTree::SearchKeyWithAncestors(const DataTypes::Indexing::Key& key, vector<Pages::IndexPageView> & ancestors) const{
      auto currentNode = this->GetNode(this->rootPageId);

      while (!currentNode.IsLeaf()){
        const auto index = BTree::InternalNodePartialLowerBound(currentNode, key);

        ancestors.push_back(std::move(currentNode));

        currentNode = std::move(this->GetNode(currentNode.GetChild(index)));
      }

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
        return Storage::StorageManager::Get().GetIndexPage(this->database->GetFileName(), pageId, this->table);
    }

    Int BTree::CalculateTreeDegree(
        const DatabaseEngine::StorageTypes::Table* otherTable,
        const TreeType treeType,
        const Int nonClusteredId
    )const{
        if(treeType == TreeType::Clustered){
          auto rowSize = otherTable->GetMaximumRowSize();
          Int calculatedDegree = static_cast<Int>(Constants::INDEX_PAGE_DEFAULT_SIZE / ((this->keySize + rowSize) * 2));

          while(calculatedDegree < 2){
            rowSize = otherTable->ReduceMaximumRowSize();

            calculatedDegree = static_cast<Int>(Constants::INDEX_PAGE_DEFAULT_SIZE / ((this->keySize + rowSize) * 2));
          }

          return calculatedDegree;
        }

        const vector<DatabaseEngine::StorageTypes::Column*>& columns = otherTable->GetColumns();

        const auto& index = otherTable->GetNonClusteredIndexes(nonClusteredId);

        Int computedKeySize = 0;
        for(const auto& columnPos: index.columns)
        {
            const DatabaseEngine::StorageTypes::Column* column = columns.at(columnPos);

            computedKeySize += column->Size();
        }

        return static_cast<Int>(Constants::INDEX_PAGE_DEFAULT_SIZE / ((this->keySize + ROW_ID_SIZE) * 2));
    }

    Pages::IndexPageView BTree::AllocateNewPage(const page_id_t parentPageId, const Int pagesToAllocate){
        return this->database->FindOrAllocateNextIndexPage(
            this->table,
            parentPageId,
            pagesToAllocate,
            this->nonClusteredIndexId
        );
    }

    void BTree::HandleUnderflow(const Pages::IndexPageView& node, std::vector<Pages::IndexPageView>& ancestors, Int& parentIndex) {
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

            if (sibling.SubKeys() < 2 * this->degree - 1
                && this->TryRedistributeLeafWithLeftSibling(child, sibling, childLock, siblingLock)) {

                // Update parent separator key between left sibling and child
                auto parentWriteLock = MultiThreading::WriterGuard::Promote(&parent.Latch(), parentLock);

                if (childIndex > 0) {
                    const auto childKey = child.GetKey(childIndex - 1);
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
        const Int targetSiblingKeys = (sibling.SubKeys() + child.SubKeys()) / 2;
        const Int keysToMove = targetSiblingKeys - sibling.SubKeys();

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
        Int parentKeyIndex,
        std::vector<Pages::IndexPageView>& ancestors,
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
        Pages::IndexPageView& currentNode,
        Headers::IndexStatistics& indexStatistics,
        Headers::TableStatistics& tableStatistics,
        std::vector<Headers::ColumnStatistics>& columnStatistics,
        Dictionary<Int, SortedDictionary<Value, BigInt, ValueComparator>>& sortedValues
    ) const{
        while (true) {
            MultiThreading::ReaderGuard lock(&currentNode.Latch());

            tableStatistics.pageCount++;
            indexStatistics.leafPages++;

            const auto numOfRows = currentNode.PageSize();
            tableStatistics.rowCount += static_cast<Int>(numOfRows);

            for (Int i = 0;i < numOfRows; i++){
                auto tuple = currentNode.PeekLeafTuple(i);

                // tableStatistics.averageRowSize += static_cast<Int>(tuple.row.TotalSize());
                //
                // for (Int j = 0; j < columnStatistics.size(); j++) {
                //     auto& columnStats = columnStatistics[j];
                //     const auto& value = tuple.row.GetColumnByIndex(j);
                //
                //     DatabaseEngine::StatisticsScheduler::UpdateColumnStatistics(
                //         columnStats,
                //         value,
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

    void BTree::UpdatePfsPage(const Pages::IndexPageView& node) const{
        const auto pageFreeSpacePage = DatabaseEngine::Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), node.PageId());

        MultiThreading::WriterGuard pfsPageLock(&pageFreeSpacePage.Latch());
        MultiThreading::WriterGuard pageLock(&node.Latch());

        pageFreeSpacePage.SetPageMetaData(&node);
    }

    BTree::BTree(DatabaseEngine::StorageTypes::Table *table, const page_id_t indexPageId, const TreeType treeType, const Int nonClusteredIndexId)
    {
        //handle degree here correctly based on indexed columns
        this->keySize = table->CalculateIndexKeySize(nonClusteredIndexId);
        this->degree = BTree::CalculateTreeDegree(table, treeType, nonClusteredIndexId);
        this->rootPageId = indexPageId;
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
        this->rootPageId = INVALID_PAGE_INDEX_ID;
        this->table = nullptr;
        this->database = nullptr;
        this->type = TreeType::NonClustered;
    }

    BTree::~BTree() = default;

    Errors::RuntimeStatus BTree::InsertRow(
        const Pages::IndexInsertTuple& tuple,
        const Int pagesToAllocate,
        Int &indexPosition
    ){
        //base case scenario
        if (this->IsEmpty()) {
            auto root =  this->CreateRootPage(indexPosition, pagesToAllocate);

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

            if (root.SubKeys() == 2 * this->degree - 1) // root is full,
                this->SplitRoot(root, rootLock, pagesToAllocate);
        }

        return this->InsertToNonFullNode(root, tuple, pagesToAllocate, indexPosition);
    }

    //TODO fix non clusteredIndex Seek
    void BTree::IndexSeekRange(const DataTypes::Indexing::Key &minKey, const DataTypes::Indexing::Key &maxKey, vector<DataTypes::Indexing::QueryData> &result) const{
        if (this->IsEmpty())
            return;

        auto currentNode = this->SearchKey(minKey);
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

            if(!currentNode.HasRightSibling())
                return;

            previousNode = currentNode;
            currentNode = this->GetNode(currentNode.RightSibling());
        }
    }

    void BTree::IndexSeekRange(
        const DatabaseEngine::ExecutionProperties& properties,
        const DataTypes::Indexing::Key &minKey,
        const DataTypes::Indexing::Key &maxKey,
        std::vector<Pages::RowReference> *result
    )const{
        if (this->IsEmpty())
            return;

        auto currentNode = this->SearchKey(minKey);

        while (true){
            MultiThreading::ReaderGuard lock(&currentNode.Latch());

            for (Int i = 0; i < currentNode.SubKeys(); i++){
                auto [key, row] = currentNode.PeekLeafTuple(i);

                if (key.InClosedRange(minKey, maxKey)){
                    currentNode.AppendRowToBuffer(result, table, properties.snapshot, i);
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
        const DatabaseEngine::ExecutionProperties& properties,
        const DataTypes::Indexing::Key& minKey,
        const DataTypes::Indexing::Key& maxKey,
        std::vector<Pages::RowReference> *result,
        const Expressions::Expression* expression
    ) const{
        if (this->IsEmpty())
            return;

        auto currentNode = this->SearchKey(minKey);

        while (true){
            MultiThreading::ReaderGuard lock(&currentNode.Latch());

            Expressions::EvaluationContext context(Expressions::EvaluationContext::EvaluationContextType::SingleRow, properties.variables);

            for (Int i = 0; i < currentNode.SubKeys(); i++){
                auto [key, row] = currentNode.PeekLeafTuple(i);

                if (key.InClosedRange(minKey, maxKey)){
                    context.row = &row;
                    if (expression->Evaluate(context).AsBool())
                        currentNode.AppendRowToBuffer(result, table, properties.snapshot, i);
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
        const DatabaseEngine::ExecutionProperties &properties,
        const DataTypes::Indexing::Key &key,
        std::vector<Pages::RowReference> *result
    ) const {
        if (this->IsEmpty())
            return;

        auto currentNode = this->SearchKey(key);

        while (true){
            MultiThreading::ReaderGuard lock(&currentNode.Latch());
            for (Int i = 0; i < currentNode.SubKeys(); i++){
                auto [tupleKey, row] = currentNode.PeekLeafTuple(i);

                // std::cout << "Comparing keys: " << tupleKey << " and " << key << std::endl;
                // std::cout << "Row: " << row << std::endl;
                if (key == tupleKey){
                    currentNode.AppendRowToBuffer(result, table, properties.snapshot, i);
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
        const DatabaseEngine::ExecutionProperties &properties,
        const DataTypes::Indexing::Key &key,
        std::vector<Pages::RowReference> *result,
        const Expressions::Expression *expression
    ) const {
        if (this->IsEmpty())
            return;

        auto currentNode = this->SearchKey(key);

        auto context = Expressions::EvaluationContext(Expressions::EvaluationContext::EvaluationContextType::SingleRow, properties.variables);

        while (true){
            MultiThreading::ReaderGuard lock(&currentNode.Latch());

            for (Int i = 0; i < currentNode.SubKeys(); i++){
                auto [tupleKey, row] = currentNode.PeekLeafTuple(i);

                if (key == tupleKey ){
                    context.row = &row;

                    if (expression->Evaluate(context).AsBool())
                        currentNode.AppendRowToBuffer(result, table, properties.snapshot, i);

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

    void BTree::IndexScan(vector<DataTypes::Indexing::QueryData> &result)const
    {
        if (this->IsEmpty())
            return;

        auto currentNode = this->SearchLeftMostLeafNode();
        Pages::IndexPageView previousNode;

        while (true)
        {
            // auto* keys = currentNode->GetKeysUnsafe();

            // for (Int i = 0; i < keys->size(); i++)
            //     result.emplace_back(currentNode->dataPageId, i);

            if(!currentNode.HasRightSibling())
                return;

            // previousNode = currentNode;
            currentNode = this->GetNode(currentNode.RightSibling());
        }
    }

    void BTree::IndexScan(
        const DatabaseEngine::ExecutionProperties& properties,
        std::vector<Pages::RowReference> *result,
        DatabaseEngine::IndexState& state
    )const{
        if (this->IsEmpty())
            return;

        result->reserve(properties.batchSize);
        auto currentNode = state.pageId == INVALID_PAGE_ID
                                ? this->SearchLeftMostLeafNode()
                                : this->GetNode(state.pageId);

        state.canFetchMore = false;
        while (true){
            MultiThreading::ReaderGuard lock(&currentNode.Latch());

            for (Int i = state.GetNextKeyIndex(); i < currentNode.PageSize(); i++)
                currentNode.AppendRowToBuffer(result, this->table, properties.snapshot, i);

            if(!currentNode.HasRightSibling()) {
                state.canFetchMore = false;
                return;
            }

            if (result->size() >= properties.batchSize) {
                state.pageId = currentNode.RightSibling();
                state.lastFetchedKeyIndex = INVALID_PAGE_INDEX_ID;
                state.canFetchMore = true;
                return;
            }

            currentNode = this->GetNode(currentNode.RightSibling());
        }
    }

    void BTree::IndexScan(
        const DatabaseEngine::ExecutionProperties& properties,
        std::vector<Pages::RowReference> *result,
        DatabaseEngine::IndexState& state,
        const Expressions::Expression *expression
    )const{
        if (this->IsEmpty())
            return;

        auto currentNode = state.pageId == INVALID_PAGE_ID
                                ? this->SearchLeftMostLeafNode()
                                : this->GetNode(state.pageId);

        Expressions::EvaluationContext context(Expressions::EvaluationContext::EvaluationContextType::SingleRow, properties.variables);

        state.canFetchMore = false;
        while (true)
        {
            MultiThreading::ReaderGuard lock(&currentNode.Latch());

            for (Int i = state.GetNextKeyIndex(); i < currentNode.SubKeys(); i++) {
                auto [key, row] = currentNode.PeekLeafTuple(i);
                context.row = &row;
                if (!expression->Evaluate(context).AsBool())
                    continue;

                currentNode.AppendRowToBuffer(result, table, properties.snapshot, i);

                if (result->size() == properties.batchSize) {
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
        const DatabaseEngine::ExecutionProperties& properties,
        std::vector<Pages::RowReference> *result,
        const Expressions::Expression *expression
    )const{
        if (this->IsEmpty())
            return;

        auto currentNode = this->SearchLeftMostLeafNode();
        Expressions::EvaluationContext context(Expressions::EvaluationContext::EvaluationContextType::SingleRow, properties.variables);

        while (true)
        {
            MultiThreading::ReaderGuard lock(&currentNode.Latch());

            for (Int i = 0;i < currentNode.PageSize();i++){

                auto [key, row] = currentNode.PeekLeafTuple(i);
                context.row = &row;
                if(!expression->Evaluate(context).AsBool())
                    continue;

                currentNode.AppendRowToBuffer(result, table, properties.snapshot, i);
            }

            if(!currentNode.HasRightSibling())
                return;

            currentNode = this->GetNode(currentNode.RightSibling());
        }
    }

    void BTree::IndexScan(
        const DatabaseEngine::ExecutionProperties& properties,
        std::vector<Pages::RowReference> *result
    )const{
        if (this->IsEmpty())
            return;

        auto currentNode = this->SearchLeftMostLeafNode();

        while (true)
        {
            MultiThreading::ReaderGuard lock(&currentNode.Latch());

            for (Int i = 0;i < currentNode.PageSize();i++){
                currentNode.AppendRowToBuffer(result, table, properties.snapshot, i);
            }

            if(!currentNode.HasRightSibling())
                return;

            currentNode = this->GetNode(currentNode.RightSibling());
        }
    }

    void BTree::IndexScan(
        vector<DataTypes::RowIdentifier> *result,
        DatabaseEngine::IndexState& state,
        const Int rowsToSelect
    )const{

        if (this->IsEmpty())
            return;

        auto currentNode = state.pageId == INVALID_PAGE_ID
                        ? this->SearchLeftMostLeafNode()
                        : this->GetNode(state.pageId);

        const Int startingPosition = state.GetNextKeyIndex();

        while (true)
        {
            MultiThreading::ReaderGuard lock(&currentNode.Latch());

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

            if(!currentNode.HasRightSibling())
                return;

            currentNode = this->GetNode(currentNode.RightSibling());
        }
    }

    void BTree::IndexScan(vector<DataTypes::RowIdentifier> *result, const Expressions::Expression *expression)const{
        if (this->IsEmpty())
            return;

        auto currentNode = this->SearchLeftMostLeafNode();

        while (true){
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

            if(!currentNode.HasRightSibling())
                return;

            currentNode = this->GetNode(currentNode.RightSibling());
        }
    }

    void BTree::IndexScanUpdate(
        const DatabaseEngine::ExecutionProperties& properties,
        const Expressions::Expression *expression,
        const std::vector<Value> &updates
    )const{
        if (this->IsEmpty())
            return;

        auto currentNode = this->SearchLeftMostLeafNode();
        Expressions::EvaluationContext context(Expressions::EvaluationContext::EvaluationContextType::SingleRow, properties.variables);

        while (true){
            MultiThreading::WriterGuard lock(&currentNode.Latch());

            for (Int indexPosition = 0; indexPosition < currentNode.PageSize();indexPosition++){
                auto tuple = currentNode.PeekLeafTuple(indexPosition);

                context.row = &tuple.row;
                const auto value = expression->Evaluate(context);
                if(!value.AsBool())
                    continue;

                const auto result = this->table->UpdateRowNoLock(
                    &currentNode,
                    tuple.row,
                    properties,
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
        const DatabaseEngine::ExecutionProperties& properties,
        const Expressions::Expression *expression,
        const std::vector<Expressions::Expression*>& updates
    )const{
        if (this->IsEmpty())
            return {};

        auto currentNode = this->SearchLeftMostLeafNode();
        Expressions::EvaluationContext context(Expressions::EvaluationContext::EvaluationContextType::SingleRow, properties.variables);

        while (true){
            MultiThreading::WriterGuard lock(&currentNode.Latch());

            for (Int i = 0;i < currentNode.PageSize();i++){
                auto tuple = currentNode.PeekLeafTuple(i);

                context.row = &tuple.row;

                const auto value = expression->Evaluate(context);
                if(!value.AsBool())
                    continue;

                auto result = this->table->UpdateRowNoLock(
                    &currentNode,
                    tuple.row,
                    properties,
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
       const DatabaseEngine::ExecutionProperties& properties,
       const std::vector<Expressions::Expression*>& updates
    )const{
        if (this->IsEmpty())
            return {};

        auto currentNode = this->SearchLeftMostLeafNode();

        while (true){
            MultiThreading::WriterGuard lock(&currentNode.Latch());

            for (Int indexPosition = 0;indexPosition < currentNode.PageSize();indexPosition++){
                auto tuple = currentNode.PeekLeafTuple(indexPosition);

                auto result = this->table->UpdateRowNoLock(
                    &currentNode,
                    tuple.row,
                    properties,
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
        const DatabaseEngine::ExecutionProperties &properties,
        const DataTypes::Indexing::Key &key,
        const std::vector<Value> &updates
    ) const {
        if (this->IsEmpty())
            return {};

        auto currentNode = this->SearchKey(key);

        while (true) {
            MultiThreading::WriterGuard lock(&currentNode.Latch());

            for (Int indexPosition = 0;indexPosition < currentNode.SubKeys(); indexPosition++){
                auto tuple = currentNode.PeekLeafTuple(indexPosition);
                if (key != tuple.key || key < tuple.key)
                    continue;

                auto result = this->table->UpdateRowNoLock(
                    &currentNode,
                    tuple.row,
                    properties,
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
        const DatabaseEngine::ExecutionProperties& properties,
        const Expressions::Expression* expression,
        const DataTypes::Indexing::Key* minKey,
        const DataTypes::Indexing::Key* maxKey,
        const std::vector<Value>& updates
    )const{
        if (this->IsEmpty())
            return {};

        auto currentNode = this->SearchKey(*minKey);

        Expressions::EvaluationContext context(Expressions::EvaluationContext::EvaluationContextType::SingleRow, properties.variables);
        while (true)
        {
            MultiThreading::WriterGuard lock(&currentNode.Latch());

              for (Int indexPosition = 0; indexPosition < currentNode.SubKeys(); indexPosition++){
                auto tuple = currentNode.PeekLeafTuple(indexPosition);

                if (*minKey > tuple.key)
                    continue;

                if (*maxKey < tuple.key)
                    break;

                  context.row = &tuple.row;
                  const auto value = expression->Evaluate(context);
                  if(!value.AsBool())
                    continue;

                auto result = this->table->UpdateRowNoLock(
                    &currentNode,
                    tuple.row,
                    properties,
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
        const DatabaseEngine::ExecutionProperties& properties,
        const DataTypes::Indexing::Key *minKey,
        const DataTypes::Indexing::Key *maxKey,
        const std::vector<Value> &updates
    )const{
        if (this->IsEmpty())
            return {};

        auto currentNode = this->SearchKey(*minKey);

        while (true){
            MultiThreading::WriterGuard lock(&currentNode.Latch());

            for (Int indexPosition = 0; indexPosition < currentNode.SubKeys(); indexPosition++){
                auto tuple = currentNode.PeekLeafTuple(indexPosition);

                if (*minKey > tuple.key)
                    continue;

                if (*maxKey < tuple.key)
                    break;

                auto result = this->table->UpdateRowNoLock(
                    &currentNode,
                    tuple.row,
                    properties,
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

    void BTree::SetTreeType(const TreeType treeType) { this->type = treeType; }

    page_id_t BTree::GetFirstIndexPageId() const { return this->rootPageId; }

    //escalate to table lock
    void BTree::InsertRowsToOtherTree(const Int indexPos, const Int pagesToAllocate)const{
        if (this->IsEmpty())
            return;

        auto currentNode = this->SearchLeftMostLeafNode();

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

        auto currentNode = this->SearchLeftMostLeafNode();

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

        auto root = this->GetNode(this->rootPageId);

        auto currentNode = this->SearchLeftMostLeafNode();

        while (true)
        {
            // for(auto& row: currentNode->DataRowsNoLock(this->table))
            //     DatabaseEngine::StorageTypes::Table::HandleRemoveColumn(currentNode, &row, index);

            if(!currentNode.HasRightSibling())
                return;

            currentNode = this->GetNode(currentNode.RightSibling());
        }

    }

    bool BTree::IsEmpty() const{ return this->rootPageId == INVALID_PAGE_ID; }

    void BTree::CalculateIndexStatistics(
        Headers::IndexStatistics& indexStatistics,
        Headers::TableStatistics& tableStatistics,
        std::vector<Headers::ColumnStatistics>& columnStatistics,
        Dictionary<Int, SortedDictionary<Value, BigInt, ValueComparator>>& sortedValues
    ) const {
        if (this->IsEmpty())
            return;

        auto currentNode = this->SearchLeftMostLeafNode(indexStatistics.depth);

        if (this->type == TreeType::Clustered){
            this->CalculateClusteredStatistics(
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
