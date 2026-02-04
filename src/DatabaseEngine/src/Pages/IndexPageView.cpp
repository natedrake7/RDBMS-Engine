#include "../../include/Pages/IndexPageView.h"

#include "DataStorage/InsertPayload.h"
#include "Pages/Additional/Frame.h"

namespace Pages{
    void IndexPageView::InsertFirstChild(const page_id_t child) const{
        const auto nextOffset = this->NewInsertOffset();

        std::memcpy(this->framePtr->data + nextOffset, &child, sizeof(page_id_t));

        const auto newSlot = SlotDirectory(0, sizeof(page_id_t), SlotDirectory::SLOT_USED);
        this->InsertNewSlot(newSlot);

        this->headerPtr->size++;
        this->framePtr->isDirty = true;
        this->headerPtr->bytesLeft -= (newSlot.GetSize() + SlotDirectory::Size);
    }

    void IndexPageView::InsertFirstKey(const DataTypes::Indexing::Key& key) const{
        page_offset_t pos = 0;
        key.Serialize(this->framePtr->data, pos);

        const auto newSlot = SlotDirectory(0, key.size, SlotDirectory::SLOT_USED);
        this->InsertNewSlot(newSlot);

        this->headerPtr->size++;
        this->headerPtr->bytesLeft -= (key.size + SlotDirectory::Size);
        this->framePtr->isDirty = true;
    }

    void IndexPageView::InsertKey(const DataTypes::Indexing::Key& key) const{
        if (this->headerPtr->size == 0){
            this->InsertFirstKey(key);
            return;
        }

        auto nextOffset = this->NewInsertOffset();
        const auto offSetCopy = nextOffset;

        key.Serialize(this->framePtr->data, nextOffset);

        const auto newSlot = SlotDirectory(offSetCopy, key.size, SlotDirectory::SLOT_USED);
        this->InsertNewSlot(newSlot);

        this->headerPtr->size++;
        this->headerPtr->bytesLeft -= (newSlot.GetSize() + SlotDirectory::Size);
        this->framePtr->isDirty = true;
    }

    void IndexPageView::InsertFirstTuple(const IndexInsertTuple& tuple) const{
        page_offset_t pos = 0;
        tuple.key.Serialize(this->framePtr->data, pos);

        const auto size = tuple.payload->Size();
        std::memcpy(this->framePtr->data + pos, tuple.payload->Data(), size);
        pos += size;

        const auto newSlot = SlotDirectory(0, size + tuple.key.size, SlotDirectory::SLOT_USED);
        this->InsertNewSlot(newSlot);

        this->headerPtr->size++;
        this->headerPtr->bytesLeft -= (newSlot.GetSize() + SlotDirectory::Size);
        this->framePtr->isDirty = true;
    }

    DataTypes::Indexing::Key IndexPageView::GetKey(page_offset_t& offSet) const {
        return DataTypes::Indexing::Key::Deserialize(
            this->framePtr->data,
            offSet,
            this->additionalHeaderPtr->SubKeys(),
            this->additionalHeaderPtr->keyTypes
        );
    }

    IndexPageView::IndexPageView() : PageView() {
        this->additionalHeaderPtr = nullptr;
        this->type = PageType::INDEX;
    }

    IndexPageView::IndexPageView(Frame* framePtr) : PageView(framePtr) {
        this->additionalHeaderPtr = reinterpret_cast<IndexPageAdditionalHeader*>(
            this->framePtr->data + PAGE_HEADER_SIZE
        );

        this->type = PageType::INDEX;
    }

    IndexPageView::IndexPageView(IndexPageView&& other) noexcept{
        this->framePtr = other.framePtr;
        this->headerPtr = other.headerPtr;
        this->additionalHeaderPtr = other.additionalHeaderPtr;
        this->type = other.type;

        other.framePtr = nullptr;
        other.headerPtr = nullptr;
        other.additionalHeaderPtr = nullptr;
    }

    IndexPageView& IndexPageView::operator=(IndexPageView&& other) noexcept{
        if (this == &other)
            return *this;

        this->framePtr = other.framePtr;
        this->headerPtr = other.headerPtr;
        this->additionalHeaderPtr = other.additionalHeaderPtr;
        this->type = other.type;

        other.framePtr = nullptr;
        other.headerPtr = nullptr;
        other.additionalHeaderPtr = nullptr;

        return *this;
    }

    void IndexPageView::SetTreeType(const TreeType treeType) const{
        this->additionalHeaderPtr->SetTreeType(treeType);
    }

    void IndexPageView::SetTreeId(const page_id_t treeId) const{
        this->additionalHeaderPtr->treeId = treeId;
    }

    void IndexPageView::SetKeyTypes(const std::vector<DataType>& keyTypes) const{
        for (int i = 0;i < keyTypes.size(); i++)
            this->additionalHeaderPtr->keyTypes[i] = keyTypes[i];
    }

    bool IndexPageView::IsEmpty() const{
        return this->additionalHeaderPtr->IsEmpty();
    }

    bool IndexPageView::IsLeaf() const{
        return this->additionalHeaderPtr->IsLeaf();
    }

    bool IndexPageView::IsRoot() const{
        return this->additionalHeaderPtr->IsRoot();
    }

    UnsignedTinyInt IndexPageView::SubKeys() const{
        return this->additionalHeaderPtr->SubKeys();
    }

    void IndexPageView::SetIsLeaf(const bool isLeaf) const{
        this->additionalHeaderPtr->SetIsLeaf(isLeaf);
        this->additionalHeaderPtr->SetIsEmpty(false);
        this->framePtr->isDirty = true;
    }

    void IndexPageView::SetIsRoot(const bool isRoot) const{
        this->additionalHeaderPtr->SetIsRoot(isRoot);
        this->additionalHeaderPtr->SetIsEmpty(false);
        this->framePtr->isDirty = true;
    }

    void IndexPageView::SetSubKeys(const UnsignedTinyInt numberOfKeys) const{
        this->additionalHeaderPtr->SetNumberOfSubKeys(numberOfKeys);
    }

    void IndexPageView::InsertChild(const page_id_t child, const DataTypes::Indexing::Key* key) const{
        if (this->headerPtr->size == 0){
            this->InsertFirstChild(child);
            return;
        }

        auto nextOffset = this->NewInsertOffset();
        const auto offSetCopy = nextOffset;

        key->Serialize(this->framePtr->data, nextOffset);
        std::memcpy(this->framePtr->data + nextOffset, &child, sizeof(page_id_t));

        const auto newSlot = SlotDirectory(offSetCopy, key->size + sizeof(page_id_t), SlotDirectory::SLOT_USED);
        this->InsertNewSlot(newSlot);

        this->headerPtr->size++;
        this->framePtr->isDirty = true;
        this->headerPtr->bytesLeft -= (newSlot.GetSize() + SlotDirectory::Size);
    }

    void IndexPageView::InsertChild(const page_id_t child, const DataTypes::Indexing::Key* key, const Int indexPosition) const{
        if (this->IndexOutOfBounds(indexPosition)){
            this->InsertChild(child, key);
            return;
        }

        auto nextOffset = this->NewInsertOffset();
        const auto offSetCopy = nextOffset;

        key->Serialize(this->framePtr->data, nextOffset);
        std::memcpy(this->framePtr->data + nextOffset, &child, sizeof(page_id_t));

        const auto slotSize = sizeof(page_id_t) + key->size;
        this->AdjustSlotDirectories(indexPosition, offSetCopy, slotSize);

        this->headerPtr->bytesLeft -= (slotSize + SlotDirectory::Size);
        this->headerPtr->size++;
        this->framePtr->isDirty = true;
    }

    void IndexPageView::InsertChild(const page_id_t child) const{
        this->InsertFirstChild(child);
    }

    void IndexPageView::SetLeftSibling(const page_id_t previousPage) const{
        this->additionalHeaderPtr->previousNode = previousPage;
    }

    void IndexPageView::SetRightSibling(const page_id_t nextPage) const{
        this->additionalHeaderPtr->nextNode = nextPage;
    }

    page_id_t IndexPageView::LeftSibling() const{
        return this->additionalHeaderPtr->previousNode;
    }

    page_id_t IndexPageView::RightSibling() const{
        return this->additionalHeaderPtr->nextNode;
    }

    bool IndexPageView::HasLeftSibling() const{
        return this->additionalHeaderPtr->previousNode != INVALID_PAGE_ID;
    }

    bool IndexPageView::HasRightSibling() const{
        return this->additionalHeaderPtr->nextNode != INVALID_PAGE_ID;
    }

    void IndexPageView::InsertKey(const DataTypes::Indexing::Key& key, const Int indexPosition) const{
        if (this->IndexOutOfBounds(indexPosition)){
            this->InsertKey(key);
            return;
        }

        auto nextOffset = this->NewInsertOffset();
        const auto offSetCopy = nextOffset;

        key.Serialize(this->framePtr->data, nextOffset);

        this->AdjustSlotDirectories(indexPosition, offSetCopy, key.size);

        this->headerPtr->bytesLeft -= (key.size + SlotDirectory::Size);
        this->headerPtr->size++;
        this->framePtr->isDirty = true;
    }

    void IndexPageView::InsertTuple(const IndexInsertTuple& tuple) const{
        if (this->headerPtr->size == 0){
            this->InsertFirstTuple(tuple);
            return;
        }

        auto nextOffset = this->NewInsertOffset();
        const auto offSetCopy = nextOffset;

        tuple.key.Serialize(this->framePtr->data, nextOffset);

        const auto size = tuple.payload->Size();
        std::memcpy(this->framePtr->data + nextOffset, tuple.payload->Data(), size);
        nextOffset += size;

        const auto newSlot = SlotDirectory(offSetCopy, size + tuple.key.size, SlotDirectory::SLOT_USED);
        this->InsertNewSlot(newSlot);

        this->headerPtr->size++;
        this->headerPtr->bytesLeft -= (newSlot.GetSize() + SlotDirectory::Size);
        this->framePtr->isDirty = true;
    }

    void IndexPageView::InsertTuple(const IndexInsertTuple& tuple, const Int indexPosition) const{
        if (this->IndexOutOfBounds(indexPosition)){
            this->InsertTuple(tuple);
            return;
        }

        auto nextOffset = this->NewInsertOffset();
        const auto offSetCopy = nextOffset;

        tuple.key.Serialize(this->framePtr->data, nextOffset);

        const auto size = tuple.payload->Size();
        std::memcpy(this->framePtr->data + nextOffset, tuple.payload->Data(), size);

        const auto slotSize = size + tuple.key.size;
        this->AdjustSlotDirectories(indexPosition, offSetCopy, slotSize);

        this->headerPtr->bytesLeft -= (slotSize + SlotDirectory::Size);
        this->headerPtr->size++;
        this->framePtr->isDirty = true;
    }

    DataTypes::Indexing::Key IndexPageView::GetKey(const Int indexPosition) const{
        const auto slot = this->GetSlotDirectory(indexPosition);
        auto offSet = slot.GetOffset();
        return this->GetKey(offSet);
    }

    LeafNodeTuple IndexPageView::PeekLeafTuple(const Int indexPosition) const{
        const auto slot = this->GetSlotDirectory(indexPosition);

        auto offset = slot.GetOffset();
        auto key = this->GetKey(offset);

        // auto ref = RowReference(this, indexPosition, key.size);
        auto ref = RowReference();
        return LeafNodeTuple(ref, key);
    }

    InternalNodeTuple IndexPageView::PeekInternalNodeTuple(const Int indexPosition) const{
        const auto slot = this->GetSlotDirectory(indexPosition);

        DataTypes::Indexing::Key key;
        auto offset = slot.GetOffset();

        if (indexPosition != 0)
            key = this->GetKey(offset);

        page_id_t pageId = 0;
        std::memcpy(&pageId, this->framePtr->data + offset, sizeof(page_id_t));

        return InternalNodeTuple(key, pageId);
    }

    DatabaseEngine::StorageTypes::RowVersioningHeader IndexPageView::PeekVersionHeader(
        const Int indexPosition,
        Int& outKeySize
    ) const{
        const auto slot = this->GetSlotDirectory(indexPosition);

        auto offset = slot.GetOffset();
        const auto key = this->GetKey(offset);

        outKeySize = key.size;
        DatabaseEngine::StorageTypes::RowVersioningHeader header;
        std::memcpy(&header, this->framePtr->data + offset, Constants::ROW_VERSION_HEADER_SIZE);

        return header;
    }

    page_id_t IndexPageView::GetChild(const Int indexPosition) const{
        const auto slot = this->GetSlotDirectory(indexPosition);

        auto offset = slot.GetOffset();
        if (indexPosition != 0)
            auto key = this->GetKey(offset);

        page_id_t pageId = 0;
        std::memcpy(&pageId, this->framePtr->data + offset, sizeof(page_id_t));
        return pageId;
    }

    void IndexPageView::AppendRowToBuffer(
        std::vector<RowReference>* buffer,
        const DatabaseEngine::StorageTypes::Table* table,
        const DatabaseEngine::Snapshot& snapshot,
        const Int indexPosition
    ){
        Int outKeySize = 0;
        const auto versionHeader = this->PeekVersionHeader(indexPosition, outKeySize);

        if (!versionHeader.IsVisibleForTransaction(snapshot)) {
            if (!versionHeader.HasOlderVersion())
                return;

            // buffer->emplace_back(
            //     DatabaseEngine::VersionDatabase::Get()
            //         .RetrieveRow(snapshot, versionHeader.olderVersionPointer, table)
            // );
            return;
        }

        // buffer->emplace_back(this, indexPosition, outKeySize);
    }
}
