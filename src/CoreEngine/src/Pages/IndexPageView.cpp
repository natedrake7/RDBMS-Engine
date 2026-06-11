#include "../../include/Pages/IndexPageView.h"

#include <cassert>
#include <cstring>
#include "DataStorage/InsertPayload.h"
#include "Pages/Additional/Frame.h"

namespace Pages{
    void IndexPageView::InsertFirstTuple(const IndexInsertTuple& tuple) const{
        auto pos = this->NewInsertOffset();
        const auto copyPos = pos;

        tuple.key.Serialize(this->_frame->_data, pos);

        const auto dataSize = tuple.payload->Size();
        std::memcpy(this->_frame->_data + pos, tuple.payload->Data(), dataSize);
        const auto newSlot = SlotDirectory(
            copyPos, pos - copyPos,
            dataSize, tuple.key.size,
            SlotDirectory::SLOT_USED
        );
        this->InsertNewSlot(newSlot);

        auto* headerPtr = this->_frame->Header();
        headerPtr->size++;
        headerPtr->bytesLeft -= (static_cast<Int>(newSlot.Size()) + SlotDirectory::SIZE);
        this->_frame->isDirty = true;
    }

    DataTypes::Indexing::Key IndexPageView::GetKeyByOffset(
        const ::Memory::IAllocator* allocator,
        page_offset_t& offSet
    ) const {
        const auto* additionalHeader = this->GetAdditionalHeader();
        return DataTypes::Indexing::Key::Deserialize(
            allocator,
            this->_frame->_data,
            offSet,
            additionalHeader->SubKeys(),
            additionalHeader->keyTypes
        );
    }

    key_size_t IndexPageView::GetKeySize(const page_offset_t offSet) const{
        key_size_t size = 0;
        for (Int i = 0; i < this->GetAdditionalHeader()->SubKeys(); i++){
            const auto keyDataSize = *reinterpret_cast<key_size_t*>(this->_frame->_data + offSet + size);
            size += keyDataSize + sizeof(key_size_t);
        }
        return size;
    }
    IndexPageView::IndexPageView() {}

    IndexPageView::IndexPageView(Frame* framePtr) : PageView(framePtr) {
        this->initialOffset = Constants::PAGE_HEADER_SIZE + Constants::INDEX_PAGE_ADDITIONAL_HEADER_SIZE;
    }

    IndexPageView::IndexPageView(IndexPageView&& other) noexcept{
        this->_frame = other._frame;
        this->initialOffset = other.initialOffset;

        other._frame = nullptr;
    }

    IndexPageView& IndexPageView::operator=(IndexPageView&& other) noexcept{
        if (this == &other)
            return *this;

        if (this->IsValid())
            this->_frame->pinCount.fetch_sub(1, std::memory_order_relaxed);

        this->_frame = other._frame;
        this->initialOffset = other.initialOffset;

        other._frame = nullptr;
        return *this;
    }

    IndexPageAdditionalHeader* IndexPageView::GetAdditionalHeader() const{
        return reinterpret_cast<IndexPageAdditionalHeader*>(this->_frame->_data + Constants::PAGE_HEADER_SIZE);
    }

    void IndexPageView::SetTreeType(const Constants::TreeType treeType) const{
        this->GetAdditionalHeader()->SetTreeType(treeType);
    }

    void IndexPageView::SetTreeId(const page_id_t treeId) const{
        this->GetAdditionalHeader()->treeId = treeId;
    }

    void IndexPageView::SetKeyTypes(const DataStructures::StaticArray<DataType, 10>& keyTypes) const{
        auto* additionalHeader = this->GetAdditionalHeader();
        for (int i = 0;i < keyTypes.Size(); i++)
            additionalHeader->keyTypes[i] = keyTypes[i];
    }

    bool IndexPageView::IsEmpty() const{
        return this->_frame->Header()->size == 0;
        // return this->framePtr->additionalHeader.indexHeaderPtr->IsEmpty();
    }

    bool IndexPageView::IsLeaf() const{
        return this->GetAdditionalHeader()->IsLeaf();
    }

    bool IndexPageView::IsRoot() const{
        return this->GetAdditionalHeader()->IsRoot();
    }

    UnsignedTinyInt IndexPageView::SubKeys() const{
        return this->GetAdditionalHeader()->SubKeys();
    }

    UnsignedSmallInt IndexPageView::Keys() const{
        return this->IsLeaf()
                   ? this->_frame->Header()->size
                   : this->_frame->Header()->size - 1;
    }

    void IndexPageView::SetIsLeaf(const bool isLeaf) const{
        auto* additionalHeader = this->GetAdditionalHeader();
        additionalHeader->SetIsLeaf(isLeaf);
        additionalHeader->SetIsEmpty(false);
        this->_frame->isDirty = true;
    }

    void IndexPageView::SetIsRoot(const bool isRoot) const{
        auto* additionalHeader = this->GetAdditionalHeader();
        additionalHeader->SetIsRoot(isRoot);
        additionalHeader->SetIsEmpty(false);
        this->_frame->isDirty = true;
    }

    void IndexPageView::SetSubKeys(const UnsignedTinyInt numberOfKeys) const{
        this->GetAdditionalHeader()->SetNumberOfSubKeys(numberOfKeys);
    }

    void IndexPageView::InsertChild(const page_id_t child, const DataTypes::Indexing::Key* key) const{
        auto* headerPtr = this->_frame->Header();
        if (headerPtr->size == 0){
            this->InsertFirstChild(child);
            return;
        }

        auto nextOffset = this->NewInsertOffset();
        const auto offSetCopy = nextOffset;

        key->Serialize(this->_frame->_data, nextOffset);
        std::memcpy(this->_frame->_data + nextOffset, &child, sizeof(page_id_t));

        const auto newSlot = SlotDirectory(
            offSetCopy, nextOffset - offSetCopy,
            sizeof(page_id_t), key->size,
            SlotDirectory::SLOT_USED
        );
        this->InsertNewSlot(newSlot);

        headerPtr->size++;
        this->_frame->isDirty = true;
        headerPtr->bytesLeft -= (static_cast<Int>(newSlot.Size()) + SlotDirectory::SIZE);
    }

    void IndexPageView::InsertChild(
        const page_id_t child,
        const DataTypes::Indexing::Key* key,
        const Int indexPosition
    ) const{
        if (this->IndexOutOfBounds(indexPosition)){
            this->InsertChild(child, key);
            return;
        }

        auto nextOffset = this->NewInsertOffset();
        const auto offSetCopy = nextOffset;

        key->Serialize(this->_frame->_data, nextOffset);
        std::memcpy(this->_frame->_data + nextOffset, &child, sizeof(page_id_t));

        this->AdjustSlotDirectories(indexPosition, offSetCopy, sizeof(page_id_t), key->size);


        this->_frame->Header()->bytesLeft -= (sizeof(page_id_t) + key->size + SlotDirectory::SIZE);
        this->_frame->Header()->size++;
        this->_frame->isDirty = true;
    }

    void IndexPageView::InsertFirstChild(const page_id_t child) const{
        const auto nextOffset = this->NewInsertOffset();

        std::memcpy(this->_frame->_data + nextOffset, &child, sizeof(page_id_t));

        const auto newSlot = SlotDirectory(nextOffset, 0, sizeof(page_id_t), 0, SlotDirectory::SLOT_USED);
        this->InsertNewSlot(newSlot);

        this->_frame->Header()->size++;
        this->_frame->isDirty = true;
        this->_frame->Header()->bytesLeft -= (newSlot.Size() + SlotDirectory::SIZE);
    }

    void IndexPageView::SetLeftSibling(const page_id_t previousPage) const{
        this->GetAdditionalHeader()->previousNode = previousPage;
    }

    void IndexPageView::SetRightSibling(const page_id_t nextPage) const{
        this->GetAdditionalHeader()->nextNode = nextPage;
    }

    page_id_t IndexPageView::LeftSibling() const{
        return this->GetAdditionalHeader()->previousNode;
    }

    page_id_t IndexPageView::RightSibling() const{
        return this->GetAdditionalHeader()->nextNode;
    }

    bool IndexPageView::HasLeftSibling() const{
        return this->GetAdditionalHeader()->previousNode != INVALID_PAGE_ID;
    }

    bool IndexPageView::HasRightSibling() const{
        return this->GetAdditionalHeader()->nextNode != INVALID_PAGE_ID;
    }

    void IndexPageView::InsertTuple(const IndexInsertTuple& tuple) const{
        auto nextOffset = this->NewInsertOffset();
        const auto offSetCopy = nextOffset;

        tuple.key.Serialize(this->_frame->_data, nextOffset);

        const auto size = tuple.payload->Size();
        std::memcpy(this->_frame->_data + nextOffset, tuple.payload->Data(), size);

        const auto newSlot = SlotDirectory(
            offSetCopy, nextOffset - offSetCopy,
            size, tuple.key.size,
            SlotDirectory::SLOT_USED
        );
        this->InsertNewSlot(newSlot);

        auto* headerPtr = this->_frame->Header();
        headerPtr->size++;
        headerPtr->bytesLeft -= (newSlot.Size() + SlotDirectory::SIZE);
        this->_frame->isDirty = true;
    }

    void IndexPageView::InsertTuple(const IndexInsertTuple& tuple, const Int indexPosition) const{
        if (this->IndexOutOfBounds(indexPosition)){
            this->InsertTuple(tuple);
            return;
        }

        auto nextOffset = this->NewInsertOffset();
        const auto offSetCopy = nextOffset;

        tuple.key.Serialize(this->_frame->_data, nextOffset);

        const auto size = tuple.payload->Size();
        std::memcpy(this->_frame->_data + nextOffset, tuple.payload->Data(), size);

        this->AdjustSlotDirectories(indexPosition, offSetCopy, size, tuple.key.size);

        auto* headerPtr = this->_frame->Header();
        headerPtr->bytesLeft -= (size + tuple.key.size + SlotDirectory::SIZE);
        headerPtr->size++;
        this->_frame->isDirty = true;
    }

    DataTypes::Indexing::Key IndexPageView::GetKeyByIndex(const ::Memory::IAllocator* allocator, const Int indexPosition) const{
        const auto slot = this->GetSlotDirectory(indexPosition);
        auto offSet = slot.Offset();
        return this->GetKeyByOffset(allocator, offSet);
    }

    Comparators::Comparator IndexPageView::ComparePageKeyAgainst(const DataTypes::Indexing::Key& key, const Int indexPosition) const{
        const auto slot = this->GetSlotDirectory(indexPosition);
        const auto offSet = slot.Offset();
        const auto* additionalHeader = this->GetAdditionalHeader();

        const auto numOfKeys = std::min(
            static_cast<UnsignedTinyInt>(key.subKeys.Size()),
            additionalHeader->SubKeys()
        );

        auto* dataPtr = this->_frame->_data + offSet;

        for (int i = 0; i < numOfKeys; i++){
            const auto type = additionalHeader->keyTypes[i];

            const auto size = *reinterpret_cast<key_size_t*>(dataPtr);
            auto* data = dataPtr + sizeof(key_size_t);

            auto value = Value::FromMove(
                data,
                size,
                type,
                nullptr
            );

            const auto result = Comparators::Compare(
                value,
                key.subKeys[i].GetValue()
            );

            if (result != Comparators::Comparator::Equal)
                return result;

            dataPtr += size + sizeof(key_size_t);
        }

        return Comparators::Comparator::Equal;
    }

    CoreEngine::StorageTypes::RowHeader IndexPageView::PeekHeader(const Int indexPosition) const{
        const auto slot = this->GetSlotDirectory(indexPosition);
        CoreEngine::StorageTypes::RowHeader header;
        std::memcpy(&header, this->_frame->_data + slot.AbsoluteDataOffset(), Constants::ROW_VERSION_HEADER_SIZE);
        return header;
    }

    bool IndexPageView::IsRowVisible(const Int indexPosition, const CoreEngine::Snapshot& snapshot) const{
        const auto slot = this->GetSlotDirectory(indexPosition);
        const auto* versionHeader = reinterpret_cast<const CoreEngine::StorageTypes::RowHeader*>(
            this->_frame->_data + slot.AbsoluteDataOffset()
        );

        return versionHeader->IsVisibleForTransaction(snapshot);
    }

    page_id_t IndexPageView::GetChild(const Int indexPosition) const{
        const auto slot = this->GetSlotDirectory(indexPosition);
        page_id_t value;
        std::memcpy(&value, this->_frame->_data + slot.AbsoluteDataOffset(), sizeof(page_id_t));
        return value;
    }

    InternalNodeTuple IndexPageView::GetInternalNodeTuple(
        const ::Memory::IAllocator* allocator,
        const Int indexPosition
    ) const{
        InternalNodeTuple tuple;
        const auto slot = this->GetSlotDirectory(indexPosition);

        auto offset = slot.Offset();
        if (indexPosition != 0){
            auto key = this->GetKeyByOffset(allocator, offset);
            tuple.SetKey(key);
        }

        page_id_t pageId = 0;
        std::memcpy(&pageId, this->_frame->_data + offset, sizeof(page_id_t));
        tuple.SetPageId(pageId);

        return tuple;
    }

    void IndexPageView::RemoveKeyFromChild(const Int indexPosition) const{
        auto slot = this->GetSlotDirectory(indexPosition);
        if (indexPosition == 0)
            return;

        slot.SetOffset(slot.AbsoluteDataOffset());
        slot.SetDataSize(slot.DataSize() - slot.KeySize());
        this->UpdateSlotDirectory(slot, indexPosition);
    }

    void IndexPageView::Log(
        const ::Memory::IAllocator* allocator,
        std::ostream& os
    ) const{
        if (this->IsLeaf()){
            os << "Rows: ";
            for (int i = 0;i < this->_frame->Header()->size; i++){
                os << this->MaterializeRow(allocator, i) << " ";
            }
            os << std::endl;
            os << "Keys: ";
            for (int i = 0;i < this->_frame->Header()->size; i++){
                os << this->GetKeyByIndex(allocator, i).ToString(allocator) << " ";
            }
            os << std::endl;
            return;
        }

        os << "Children: ";
        for (int i = 0;i < this->_frame->Header()->size; i++){
            const auto tuple = this->GetInternalNodeTuple(allocator, i);
            os << tuple.pageId << " ";
        }
        os << std::endl;
        os << "Keys: ";
        for (int i = 0;i < this->_frame->Header()->size; i++){
            const auto tuple = this->GetInternalNodeTuple(allocator, i);
            os  << tuple.key.ToString(allocator) << " ";
        }
        os << std::endl;
    }
}
