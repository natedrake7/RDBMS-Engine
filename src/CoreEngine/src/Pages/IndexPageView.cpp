#include <CoreEngine/Pages/IndexPageView.h>
#include <CoreEngine/DataStorage/Row/SerializedRow.h>
#include <CoreEngine/Pages/Additional/Frame.h>

namespace CoreEngine::Pages{
    void IndexPageView::InsertFirstTuple(const IndexInsertTuple& tuple) const{
        auto pos = this->NewInsertOffset();
        const auto copyPos = pos;

        std::memcpy(this->_frame->_data + pos,  tuple.key._data, tuple.key.Size());
        pos += tuple.key.Size();

        const auto dataSize = tuple.payload->Size();
        std::memcpy(this->_frame->_data + pos, tuple.payload->Data(), dataSize);
        const auto newSlot = SlotDirectory(
            copyPos, pos - copyPos,
            dataSize, tuple.key.Size(),
            SlotDirectory::SLOT_USED
        );
        this->InsertNewSlot(newSlot);

        auto* headerPtr = this->_frame->Header();
        headerPtr->IncrementSizeNoLock();
        headerPtr->ConsumeBytesNoLock(newSlot.Size() + SlotDirectory::SIZE);
        this->_frame->MarkDirty();
    }

    DataTypes::Indexing::Key IndexPageView::GetKeyByOffset( const SlotDirectory slot) const{
        return DataTypes::Indexing::Key(
            this->_frame->_data + slot.AbsoluteKeyOffset()
        );
    }

    IndexPageView::IndexPageView(Frame* framePtr) : PageView(framePtr) {
        this->initialOffset = CoreEngine::StorageLayout::PAGE_HEADER_SIZE + CoreEngine::StorageLayout::INDEX_PAGE_ADDITIONAL_HEADER_SIZE;
    }

    IndexPageAdditionalHeader* IndexPageView::GetAdditionalHeader() const{
        return reinterpret_cast<IndexPageAdditionalHeader*>(this->_frame->_data + CoreEngine::StorageLayout::PAGE_HEADER_SIZE);
    }

    void IndexPageView::SetTreeType(const Constants::TreeType treeType) const{
        this->GetAdditionalHeader()->SetTreeType(treeType);
        this->_frame->MarkDirty();
    }

    bool IndexPageView::IsEmpty() const{
        return this->_frame->Header()->_size == 0;
    }

    bool IndexPageView::IsLeaf() const{
        return this->GetAdditionalHeader()->IsLeaf();
    }

    bool IndexPageView::IsRoot() const{
        return this->GetAdditionalHeader()->IsRoot();
    }

    UnsignedSmallInt IndexPageView::Keys() const{
        return this->IsLeaf()
                   ? this->_frame->Header()->_size
                   : this->_frame->Header()->_size - 1;
    }

    void IndexPageView::SetIsLeaf(const bool isLeaf) const{
        auto* additionalHeader = this->GetAdditionalHeader();
        additionalHeader->SetIsLeaf(isLeaf);
        additionalHeader->SetIsEmpty(false);
        this->_frame->MarkDirty();
    }

    void IndexPageView::SetIsRoot(const bool isRoot) const{
        auto* additionalHeader = this->GetAdditionalHeader();
        additionalHeader->SetIsRoot(isRoot);
        additionalHeader->SetIsEmpty(false);
        this->_frame->MarkDirty();
    }

    void IndexPageView::InsertChild(const page_id_t child, const DataTypes::Indexing::Key* key) const{
        auto* headerPtr = this->_frame->Header();
        if (headerPtr->_size == 0){
            this->InsertFirstChild(child);
            return;
        }

        auto nextOffset = this->NewInsertOffset();
        const auto offSetCopy = nextOffset;

        std::memcpy(this->_frame->_data + nextOffset, key->_data, key->Size());
        nextOffset += key->Size();
        std::memcpy(this->_frame->_data + nextOffset, &child, sizeof(page_id_t));

        const auto newSlot = SlotDirectory(
            offSetCopy, nextOffset - offSetCopy,
            sizeof(page_id_t), key->Size(),
            SlotDirectory::SLOT_USED
        );
        this->InsertNewSlot(newSlot);

        headerPtr->IncrementSizeNoLock();
        this->_frame->MarkDirty();
        headerPtr->ConsumeBytesNoLock(static_cast<Int>(newSlot.Size()) + SlotDirectory::SIZE);
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

        std::memcpy(this->_frame->_data + nextOffset, key->_data, key->Size());
        nextOffset += key->Size();
        std::memcpy(this->_frame->_data + nextOffset, &child, sizeof(page_id_t));

        this->AdjustSlotDirectories(indexPosition, offSetCopy, sizeof(page_id_t), key->Size());


        this->_frame->Header()->ConsumeBytesNoLock(sizeof(page_id_t) + key->Size() + SlotDirectory::SIZE);
        this->_frame->Header()->_size++;
        this->_frame->MarkDirty();
    }

    void IndexPageView::InsertFirstChild(const page_id_t child) const{
        const auto nextOffset = this->NewInsertOffset();

        std::memcpy(this->_frame->_data + nextOffset, &child, sizeof(page_id_t));

        const auto newSlot = SlotDirectory(nextOffset, 0, sizeof(page_id_t), 0, SlotDirectory::SLOT_USED);
        this->InsertNewSlot(newSlot);

        this->_frame->Header()->_size++;
        this->_frame->MarkDirty();
        this->_frame->Header()->ConsumeBytesNoLock(newSlot.Size() + SlotDirectory::SIZE);
    }

    void IndexPageView::SetLeftSibling(const page_id_t previousPage) const{
        this->GetAdditionalHeader()->previousNode = previousPage;
        this->_frame->MarkDirty();
    }

    void IndexPageView::SetRightSibling(const page_id_t nextPage) const{
        this->GetAdditionalHeader()->nextNode = nextPage;
        this->_frame->MarkDirty();
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

        std::memcpy(this->_frame->_data + nextOffset, tuple.key._data, tuple.key.Size());
        nextOffset += tuple.key.Size();

        const auto size = tuple.payload->Size();
        std::memcpy(this->_frame->_data + nextOffset, tuple.payload->Data(), size);

        const auto newSlot = SlotDirectory(
            offSetCopy, nextOffset - offSetCopy,
            size, tuple.key.Size(),
            SlotDirectory::SLOT_USED
        );
        this->InsertNewSlot(newSlot);

        auto* headerPtr = this->_frame->Header();
        headerPtr->IncrementSizeNoLock();
        headerPtr->ConsumeBytesNoLock(newSlot.Size() + SlotDirectory::SIZE);
        this->_frame->MarkDirty();
    }

    void IndexPageView::InsertTuple(const IndexInsertTuple& tuple, const Int indexPosition) const{
        if (this->IndexOutOfBounds(indexPosition)){
            this->InsertTuple(tuple);
            return;
        }

        auto nextOffset = this->NewInsertOffset();
        const auto offSetCopy = nextOffset;

        std::memcpy(this->_frame->_data + nextOffset, tuple.key._data, tuple.key.Size());
        nextOffset += tuple.key.Size();

        const auto size = tuple.payload->Size();
        std::memcpy(this->_frame->_data + nextOffset, tuple.payload->Data(), size);

        this->AdjustSlotDirectories(indexPosition, offSetCopy, size, tuple.key.Size());

        auto* headerPtr = this->_frame->Header();
        headerPtr->ConsumeBytesNoLock(size + tuple.key.Size() + SlotDirectory::SIZE);
        headerPtr->IncrementSizeNoLock();
        this->_frame->MarkDirty();
    }

    DataTypes::Indexing::Key IndexPageView::GetKeyByIndex(const Int indexPosition) const{
        const auto slot = this->GetSlotDirectory(indexPosition);
        return this->GetKeyByOffset(slot);
    }

    Comparators::Comparator IndexPageView::ComparePageKeyAgainst(
        const DataTypes::Indexing::Key& key,
        const Int indexPosition
    ) const{
        const auto slot = this->GetSlotDirectory(indexPosition);
        const auto pageKey = DataTypes::Indexing::Key(this->_frame->_data + slot.AbsoluteKeyOffset());
        return DataTypes::Indexing::Key::Compare(pageKey, key);
    }

    Comparators::Comparator IndexPageView::PartialComparePageKeyAgainst(const DataTypes::Indexing::Key& key, const Int indexPosition) const{
        const auto slot = this->GetSlotDirectory(indexPosition);
        const auto pageKey = DataTypes::Indexing::Key(this->_frame->_data + slot.AbsoluteKeyOffset());
        return DataTypes::Indexing::Key::PartialCompare(pageKey, key);
    }

    CoreEngine::StorageTypes::RowHeader IndexPageView::PeekHeader(const Int indexPosition) const{
        const auto slot = this->GetSlotDirectory(indexPosition);
        CoreEngine::StorageTypes::RowHeader header;
        std::memcpy(&header, this->_frame->_data + slot.AbsoluteDataOffset(), sizeof(CoreEngine::StorageTypes::RowHeader));
        return header;
    }

    page_id_t IndexPageView::GetChild(const Int indexPosition) const{
        const auto slot = this->GetSlotDirectory(indexPosition);
        page_id_t value;
        std::memcpy(&value, this->_frame->_data + slot.AbsoluteDataOffset(), sizeof(page_id_t));
        return value;
    }

    InternalNodeTuple IndexPageView::GetInternalNodeTuple(const Int indexPosition) const{
        InternalNodeTuple tuple;
        const auto slot = this->GetSlotDirectory(indexPosition);

        if (indexPosition != 0){
            auto key = this->GetKeyByOffset(slot);
            tuple.SetKey(key);
        }

        page_id_t pageId = 0;
        std::memcpy(&pageId, this->_frame->_data + slot.AbsoluteDataOffset(), sizeof(page_id_t));
        tuple.SetPageId(pageId);

        return tuple;
    }
}
