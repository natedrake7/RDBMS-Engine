#include "../../include/Pages/IndexPage.h"
#include <cstring>
#include <iostream>
#include <fstream>
#include <nlohmann/detail/input/parser.hpp>

#include "../../include/BTree.h"
#include "../../include/DataStorage/Table.h"
#include "SystemDatabases/VersionDatabase.h"

namespace Pages {
void IndexPage::WriteAdditionalHeaderToDisk(fstream * filePtr) const
{
    filePtr->write(reinterpret_cast<const char*>(&this->additionalHeader.treeId), sizeof(page_id_t));
    filePtr->write(reinterpret_cast<const char*>(&this->additionalHeader.flags._data), PackedByte::Size);
    filePtr->write(reinterpret_cast<const char*>(this->additionalHeader.keyTypes.data()), MAX_NUMBER_OF_SUB_KEYS * sizeof(DataType));
    filePtr->write(reinterpret_cast<const char*>(&this->additionalHeader.previousNode), sizeof(page_id_t));
    filePtr->write(reinterpret_cast<const char*>(&this->additionalHeader.nextNode), sizeof(page_id_t));
}

void IndexPage::ReadAdditionalHeaderFromDisk(const vector<char>& data, page_offset_t & offSet)
{
    std::memcpy(&this->additionalHeader.treeId, data.data() + offSet, sizeof(page_id_t));
    offSet += sizeof(page_id_t);
    std::memcpy(&this->additionalHeader.flags, data.data() + offSet, PackedByte::Size);
    offSet += PackedByte::Size;
    std::memcpy(&this->additionalHeader.keyTypes, data.data() + offSet, MAX_NUMBER_OF_SUB_KEYS * sizeof(DataType));
    offSet += MAX_NUMBER_OF_SUB_KEYS * sizeof(DataType);
    std::memcpy(&this->additionalHeader.previousNode, data.data() + offSet, sizeof(page_id_t));
    offSet += sizeof(page_id_t);
    std::memcpy(&this->additionalHeader.nextNode, data.data() + offSet, sizeof(page_id_t));
    offSet += sizeof(page_id_t);
}

void IndexPage::InsertFirstTuple(const IndexInsertTuple& tuple){
    page_offset_t pos = 0;
    tuple.key.Serialize(this->data, pos);

    const auto size = tuple.payload->Size();
    std::memcpy(this->data + pos, tuple.payload->Data(), size);
    pos += size;

    const auto newSlot = SlotDirectory(0, size + tuple.key.size, SlotDirectory::SLOT_USED);
    this->InsertNewSlot(newSlot);

    this->header.size++;
    this->isDirty = true;
    this->header.bytesLeft -= (newSlot.GetSize() + SlotDirectory::Size);
}

void IndexPage::InsertTuple(const IndexInsertTuple& tuple){
    if (this->header.size == 0){
        this->InsertFirstTuple(tuple);
        return;
    }

    auto nextOffset = this->NewInsertOffset();
    const auto offSetCopy = nextOffset;

    tuple.key.Serialize(this->data, nextOffset);

    const auto size = tuple.payload->Size();
    std::memcpy(this->data + nextOffset, tuple.payload->Data(), size);
    nextOffset += size;

    const auto newSlot = SlotDirectory(offSetCopy, size + tuple.key.size, SlotDirectory::SLOT_USED);
    this->InsertNewSlot(newSlot);

    this->header.size++;
    this->isDirty = true;
    this->header.bytesLeft -= (newSlot.GetSize() + SlotDirectory::Size);
}

void IndexPage::InsertFirstKey(const DataTypes::Indexing::Key& key){
    page_offset_t pos = 0;
    key.Serialize(this->data, pos);

    const auto newSlot = SlotDirectory(0, key.size, SlotDirectory::SLOT_USED);
    this->InsertNewSlot(newSlot);

    this->header.size++;
    this->isDirty = true;
    this->header.bytesLeft -= (key.size + SlotDirectory::Size);
}

void IndexPage::InsertKey(const DataTypes::Indexing::Key& key){
    if (this->header.size == 0){
        this->InsertFirstKey(key);
        return;
    }

    auto nextOffset = this->NewInsertOffset();
    const auto offSetCopy = nextOffset;

    key.Serialize(this->data, nextOffset);

    const auto newSlot = SlotDirectory(offSetCopy, key.size, SlotDirectory::SLOT_USED);
    this->InsertNewSlot(newSlot);

    this->header.size++;
    this->isDirty = true;
    this->header.bytesLeft -= (newSlot.GetSize() + SlotDirectory::Size);
}

void IndexPage::InsertFirstChild(const page_id_t child){
    const auto nextOffset = this->NewInsertOffset();

    std::memcpy(this->data + nextOffset, &child, sizeof(page_id_t));

    const auto newSlot = SlotDirectory(0, sizeof(page_id_t), SlotDirectory::SLOT_USED);
    this->InsertNewSlot(newSlot);

    auto slot = this->GetSlotDirectory(this->header.size);

    this->header.size++;
    this->isDirty = true;
    this->header.bytesLeft -= (newSlot.GetSize() + SlotDirectory::Size);
}

DataTypes::Indexing::Key IndexPage::GetKey(page_offset_t& offSet) const{
    return DataTypes::Indexing::Key::Deserialize(
        this->data,
        offSet,
        this->additionalHeader.SubKeys(),
        this->additionalHeader.keyTypes
    );
}

void IndexPage::AdjustRows(){
}

void IndexPage::InsertChild(const page_id_t child, const DataTypes::Indexing::Key* key){
    if (this->header.size == 0){
        this->InsertFirstChild(child);
        return;
    }

    auto nextOffset = this->NewInsertOffset();
    const auto offSetCopy = nextOffset;

    key->Serialize(this->data, nextOffset);
    std::memcpy(this->data + nextOffset, &child, sizeof(page_id_t));

    const auto newSlot = SlotDirectory(offSetCopy, key->size + sizeof(page_id_t), SlotDirectory::SLOT_USED);
    this->InsertNewSlot(newSlot);

    auto slotSize = newSlot.GetSize();

    this->header.size++;
    this->isDirty = true;
    this->header.bytesLeft -= (newSlot.GetSize() + SlotDirectory::Size);
}

IndexPage::IndexPage(
    const page_id_t pageId,
    const DatabaseEngine::StorageTypes::Table* table,
    const bool isPageCreation,
    const std::array<DataType, MAX_NUMBER_OF_SUB_KEYS>& keyTypes
) : Page(pageId, INDEX_PAGE_DEFAULT_SIZE, table, isPageCreation)
{
    this->header.type = PageType::INDEX;
    this->header.bytesLeft  = Constants::INDEX_PAGE_DEFAULT_SIZE;
    this->additionalHeader.keyTypes = keyTypes;
    this->additionalHeader.previousNode = INVALID_PAGE_ID;
    this->additionalHeader.nextNode = INVALID_PAGE_ID;
}

IndexPage::IndexPage(const PageHeader &pageHeader) : Page(pageHeader, INDEX_PAGE_DEFAULT_SIZE) {}

void IndexPage::ReadFromDisk(
    const std::vector<char> &data,
    const DatabaseEngine::StorageTypes::Table *table,
    page_offset_t &offSet,
    fstream *filePtr
){
    this->ReadAdditionalHeaderFromDisk(data, offSet);
    std::memcpy(this->data, data.data() + offSet, INDEX_PAGE_DEFAULT_SIZE);
    offSet += INDEX_PAGE_DEFAULT_SIZE;
}

void IndexPage::WriteToDisk(fstream *filePtr){
    this->WritePageHeaderToDisk(filePtr);
    this->WriteAdditionalHeaderToDisk(filePtr);


    filePtr->write(reinterpret_cast<const char*>(this->data), INDEX_PAGE_DEFAULT_SIZE);
}

void IndexPage::SetTreeType(const TreeType & treeType) { this->additionalHeader.SetTreeType(treeType); }

void IndexPage::SetTreeId(const page_id_t  treeId) { this->additionalHeader.treeId = treeId; }

void IndexPage::SetKeyTypes(const std::vector<DataType>& keyTypes){
    for (int i = 0;i < keyTypes.size(); i++)
        this->additionalHeader.keyTypes[i] = keyTypes[i];
}

page_id_t IndexPage::GetTreeId() const { return this->additionalHeader.treeId; }

void IndexPage::UpdateBytesLeft(){
    this->header.bytesLeft = Constants::INDEX_PAGE_DEFAULT_SIZE;

    const auto lastSlot = this->GetSlotDirectory(this->header.size - 1);
    this->header.bytesLeft = INDEX_PAGE_DEFAULT_SIZE - lastSlot.GetOffset() + lastSlot.GetSize() + this->header.size * SlotDirectory::Size;

    this->header.bytesLeft -= 2 * sizeof(page_id_t);

    this->isDirty = true;
}

bool IndexPage::isEmpty() const{ return this->additionalHeader.IsEmpty();}

bool IndexPage::IsLeaf() const{ return this->additionalHeader.IsLeaf(); }

bool IndexPage::IsRoot() const{ return this->additionalHeader.IsRoot(); }

UnsignedTinyInt IndexPage::SubKeys() const{
    return this->additionalHeader.SubKeys();
}

void IndexPage::SetIsLeaf(const bool isLeaf) {
    this->additionalHeader.SetIsLeaf(isLeaf);
    this->additionalHeader.SetIsEmpty(false);
    this->isDirty = true;
}

void IndexPage::SetIsRoot(const bool isRoot) {
    this->additionalHeader.SetIsRoot(isRoot);
    this->additionalHeader.SetIsEmpty(false);
    this->isDirty = true;
}

void IndexPage::SetSubKeys(const UnsignedTinyInt numberOfKeys){
    this->additionalHeader.SetNumberOfSubKeys(numberOfKeys);
}

void IndexPage::InsertChild(
    const page_id_t child,
    const DataTypes::Indexing::Key* key,
    const Int indexPosition
){
    if (this->IndexOutOfBounds(indexPosition)){
        this->InsertChild(child, key);
        return;
    }

    auto nextOffset = this->NewInsertOffset();
    const auto offSetCopy = nextOffset;

    key->Serialize(this->data, nextOffset);
    std::memcpy(this->data + nextOffset, &child, sizeof(page_id_t));

    const auto slotSize = sizeof(page_id_t) + key->size;
    this->AdjustSlotDirectories(indexPosition, offSetCopy, slotSize);

    this->header.bytesLeft -= (slotSize + SlotDirectory::Size);
    this->header.size++;
    this->isDirty = true;
}

void IndexPage::InsertChild(const page_id_t child){
    this->InsertFirstChild(child);
}

void IndexPage::SetPreviousPage(const page_id_t previousPage){ this->additionalHeader.previousNode = previousPage; }

void IndexPage::SetNextPage(const page_id_t nextPage){ this->additionalHeader.nextNode = nextPage; }

page_id_t IndexPage::GetPreviousPage()const{ return this->additionalHeader.previousNode; }

page_id_t IndexPage::GetNextPage()const{ return this->additionalHeader.nextNode; }

bool IndexPage::HasRightSibling() const{ return this->additionalHeader.nextNode != INVALID_PAGE_ID; }

bool IndexPage::HasLeftSibling() const{ return this->additionalHeader.previousNode != INVALID_PAGE_ID; }

void IndexPage::InsertKey(const DataTypes::Indexing::Key& key, const Int indexPosition){
    if (this->IndexOutOfBounds(indexPosition)){
        this->InsertKey(key);
        return;
    }

    auto nextOffset = this->NewInsertOffset();
    const auto offSetCopy = nextOffset;

    key.Serialize(this->data, nextOffset);

    this->AdjustSlotDirectories(indexPosition, offSetCopy, key.size);

    this->header.bytesLeft -= (key.size + SlotDirectory::Size);
    this->header.size++;
    this->isDirty = true;
}

void IndexPage::InsertTuple(const IndexInsertTuple& tuple, const Int indexPosition){
    if (this->IndexOutOfBounds(indexPosition)){
        this->InsertTuple(tuple);
        return;
    }

    auto nextOffset = this->NewInsertOffset();
    const auto offSetCopy = nextOffset;

    tuple.key.Serialize(this->data, nextOffset);

    const auto size = tuple.payload->Size();
    std::memcpy(this->data + nextOffset, tuple.payload->Data(), size);

    const auto slotSize = size + tuple.key.size;
    this->AdjustSlotDirectories(indexPosition, offSetCopy, slotSize);

    this->header.bytesLeft -= (slotSize + SlotDirectory::Size);
    this->header.size++;
    this->isDirty = true;
}

DataTypes::Indexing::Key IndexPage::GetKey(const Int indexPosition) const{
    const auto slot = this->GetSlotDirectory(indexPosition);
    auto offSet = slot.GetOffset();
    return this->GetKey(offSet);
}

LeafNodeTuple IndexPage::PeekLeafTuple(const Int indexPosition){
    const auto slot = this->GetSlotDirectory(indexPosition);

    auto offset = slot.GetOffset();
    auto key = this->GetKey(offset);

    auto ref = RowReference(this, indexPosition, key.size);
    return LeafNodeTuple(ref, key);
}

InternalNodeTuple IndexPage::PeekInternalNodeTuple(const Int indexPosition) const{
    const auto slot = this->GetSlotDirectory(indexPosition);

    DataTypes::Indexing::Key key;
    auto offset = slot.GetOffset();

    if (indexPosition != 0)
        key = this->GetKey(offset);

    page_id_t pageId = 0;
    std::memcpy(&pageId, this->data + offset, sizeof(page_id_t));

    return InternalNodeTuple(key, pageId);
}

DatabaseEngine::StorageTypes::RowVersioningHeader IndexPage::PeekVersionHeader(const Int indexPosition, Int& outKeySize) const{
    const auto slot = this->GetSlotDirectory(indexPosition);

    auto offset = slot.GetOffset();
    const auto key = this->GetKey(offset);

    outKeySize = key.size;
    DatabaseEngine::StorageTypes::RowVersioningHeader header;
    std::memcpy(&header, this->data + offset, Constants::ROW_VERSION_HEADER_SIZE);

    return header;
}

page_id_t IndexPage::GetChild(const Int indexPosition) const{
    const auto slot = this->GetSlotDirectory(indexPosition);

    auto offset = slot.GetOffset();
    if (indexPosition != 0)
        auto key = this->GetKey(offset);

    page_id_t pageId = 0;
    std::memcpy(&pageId, this->data + offset, sizeof(page_id_t));
    return pageId;
}

void IndexPage::UpdatePageSize(){
    this->additionalHeader.SetIsEmpty(false);
    this->isDirty = true;
}

Int IndexPage::NumberOfKeys() const{
    return this->additionalHeader.IsLeaf() ? this->header.size : this->header.size - 1;
}

void IndexPage::AppendRowToBuffer(
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

    buffer->emplace_back(this, indexPosition, outKeySize);
}

void IndexPage::MarkEmpty(){
  this->header.bytesLeft = Constants::INDEX_PAGE_DEFAULT_SIZE;

  this->header.size = 0;

  this->additionalHeader.SetIsEmpty(true);
  this->isDirty = true;
}

TreeType IndexPageAdditionalHeader::GetTreeType() const{
    return PackedByte::ExtractBits<TreeType>(flags._data, TREE_TYPE_BIT_POS, TREE_TYPE_BIT_MASK);
}

bool IndexPageAdditionalHeader::IsLeaf() const{
	return PackedByte::GetBit(flags._data, IS_LEAF_BIT_POS);
}

bool IndexPageAdditionalHeader::IsRoot() const{
	return PackedByte::GetBit(flags._data, IS_ROOT_BIT_POS);
}

bool IndexPageAdditionalHeader::IsEmpty() const{
	return PackedByte::GetBit(flags._data, IS_EMPTY_BIT_POS);
}

UnsignedTinyInt IndexPageAdditionalHeader::SubKeys() const{
    return PackedByte::ExtractBits<UnsignedTinyInt>(flags._data, NUMBER_OF_SUB_KEYS_BIT_POS, NUMBER_OF_SUB_KEYS_BIT_MASK);
}

void IndexPageAdditionalHeader::SetTreeType(const Constants::TreeType& type){
	PackedByte::SetBits(flags._data, type, 0, TREE_TYPE_BIT_MASK);
}

void IndexPageAdditionalHeader::SetIsLeaf(const bool value){
	PackedByte::SetBit(flags._data, IS_LEAF_BIT_POS, value);
}

void IndexPageAdditionalHeader::SetIsRoot(const bool value){
    PackedByte::SetBit(flags._data, IS_ROOT_BIT_POS, value);
}

void IndexPageAdditionalHeader::SetIsEmpty(const bool value){
	PackedByte::SetBit(flags._data, IS_EMPTY_BIT_POS, value);
}

void IndexPageAdditionalHeader::SetNumberOfSubKeys(const UnsignedTinyInt count){
	PackedByte::SetBits(flags._data, count, NUMBER_OF_SUB_KEYS_BIT_POS, NUMBER_OF_SUB_KEYS_BIT_MASK);
}

IndexPageAdditionalHeader::IndexPageAdditionalHeader(){
    this->treeId = 0;

    this->SetTreeType(TreeType::NonClustered);
    this->SetIsLeaf(false);
    this->SetIsRoot(false);
    this->SetIsEmpty(true);
    this->SetNumberOfSubKeys(0);
}

IndexInsertTuple::IndexInsertTuple(){
    this->payload = nullptr;
}

IndexInsertTuple::IndexInsertTuple(
    DataTypes::Indexing::Key& key,
    DatabaseEngine::StorageTypes::InsertPayload* payload
){
    this->key = std::move(key);
    this->payload = payload;
}

IndexInsertTuple::~IndexInsertTuple() = default;

LeafNodeTuple& LeafNodeTuple::operator=(LeafNodeTuple&& other) noexcept
{
    if (this == &other)
        return *this;

    this->key = std::move(other.key);
    this->row = std::move(other.row);

    return *this;
}

LeafNodeTuple::LeafNodeTuple(LeafNodeTuple&& other) noexcept
{
    this->key = std::move(other.key);
    this->row = std::move(other.row);
}

LeafNodeTuple& LeafNodeTuple::operator=(const LeafNodeTuple& other){
    if (this == &other)
        return *this;

    this->key = other.key;
    this->row = other.row;

    return *this;
}

LeafNodeTuple::LeafNodeTuple(const LeafNodeTuple& other){
    this->key = other.key;
    this->row = other.row;
}

LeafNodeTuple::LeafNodeTuple(RowReference& row, DataTypes::Indexing::Key& key){
    this->key = std::move(key);
    this->row = std::move(row);
}

InternalNodeTuple& InternalNodeTuple::operator=(InternalNodeTuple&& other) noexcept{
    if (this == &other)
        return *this;

    this->key = std::move(other.key);
    this->pageId = other.pageId;

    return *this;
}

InternalNodeTuple::InternalNodeTuple(InternalNodeTuple&& other) noexcept{
    this->key = std::move(other.key);
    this->pageId = other.pageId;
}

InternalNodeTuple& InternalNodeTuple::operator=(const InternalNodeTuple& other){
    if (this == &other)
        return *this;

    this->key = other.key;
    this->pageId = other.pageId;

    return *this;
}

InternalNodeTuple::InternalNodeTuple(const InternalNodeTuple& other){
    this->key = other.key;
    this->pageId = other.pageId;
}

InternalNodeTuple::InternalNodeTuple(DataTypes::Indexing::Key& key, const page_id_t pageId){
    this->key = std::move(key);
    this->pageId = pageId;
}
}
