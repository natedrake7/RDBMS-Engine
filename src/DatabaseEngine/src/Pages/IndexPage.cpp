#include "../../include/Pages/IndexPage.h"
#include "../../include/BTree.h"
#include "../../include/DataStorage/Table.h"
#include <cstring>
#include <iostream>
#include <ostream>

namespace Pages {
void IndexPage::WriteAdditionalHeaderToDisk(fstream * filePtr) const
{
    filePtr->write(reinterpret_cast<const char*>(&this->additionalHeader.treeId), sizeof(page_id_t));
    filePtr->write(reinterpret_cast<const char*>(&this->additionalHeader.flags._data), PackedByte::Size);
}

void IndexPage::ReadAdditionalHeaderFromDisk(const vector<char>& data, page_offset_t & offSet)
{
    memcpy(&this->additionalHeader.treeId, data.data() + offSet, sizeof(page_id_t));
    offSet += sizeof(page_id_t);
    memcpy(&this->additionalHeader.flags, data.data() + offSet, PackedByte::Size);
    offSet += PackedByte::Size;
}

void IndexPage::InsertFirstTuple(const LeafNodeTuple& tuple){
    const auto rowSize = tuple.row.TotalSize();

    page_offset_t pos = 0;
    tuple.key.Serialize(this->data, pos);
    tuple.row.Serialize(this->data, pos);

    const auto newSlot = SlotDirectory(0, rowSize + tuple.key.size);
    this->InsertNewSlot(newSlot);

    this->header.pageSize++;
    this->isDirty = true;
    this->header.bytesLeft -= (newSlot.size + SlotDirectory::Size);
}

void IndexPage::InsertTuple(const LeafNodeTuple& tuple){
    if (this->header.pageSize == 0){
        this->InsertFirstTuple(tuple);
        return;
    }

    const auto rowSize = tuple.row.TotalSize();
    auto nextOffset = this->NewRowOffset();
    const auto offSetCopy = nextOffset;

    tuple.key.Serialize(this->data, nextOffset);
    tuple.row.Serialize(this->data, nextOffset);

    const auto newSlot = SlotDirectory(offSetCopy, rowSize + tuple.key.size);
    this->InsertNewSlot(newSlot);

    this->header.pageSize++;
    this->isDirty = true;
    this->header.bytesLeft -= (newSlot.size + SlotDirectory::Size);
}

void IndexPage::InsertFirstKey(const DataTypes::Indexing::Key& key){
    page_offset_t pos = 0;
    key.Serialize(this->data, pos);

    const auto newSlot = SlotDirectory(0, key.size);
    this->InsertNewSlot(newSlot);

    this->header.pageSize++;
    this->isDirty = true;
    this->header.bytesLeft -= (key.size + SlotDirectory::Size);
}

void IndexPage::InsertKey(const DataTypes::Indexing::Key& key){
    if (this->header.pageSize == 0){
        this->InsertFirstKey(key);
        return;
    }

    auto nextOffset = this->NewRowOffset();
    const auto offSetCopy = nextOffset;

    key.Serialize(this->data, nextOffset);

    const auto newSlot = SlotDirectory(offSetCopy, key.size);
    this->InsertNewSlot(newSlot);

    this->header.pageSize++;
    this->isDirty = true;
    this->header.bytesLeft -= (newSlot.size + SlotDirectory::Size);
}

void IndexPage::InsertFirstChild(const page_id_t& child){
    const auto nextOffset = this->NewRowOffset();

    std::memcpy(this->data + nextOffset, &child, sizeof(page_id_t));

    const auto newSlot = SlotDirectory(0, sizeof(page_id_t));
    this->InsertNewSlot(newSlot);

    this->header.pageSize++;
    this->isDirty = true;
    this->header.bytesLeft -= (sizeof(page_id_t) + SlotDirectory::Size);
}

void IndexPage::InsertChild(const page_id_t& child, const DataTypes::Indexing::Key* key){
    if (this->header.pageSize == 0){
        this->InsertFirstChild(child);
        return;
    }

    auto nextOffset = this->NewRowOffset();
    const auto offSetCopy = nextOffset;

    key->Serialize(this->data, nextOffset);
    std::memcpy(this->data + nextOffset, &child, sizeof(page_id_t));

    const auto newSlot = SlotDirectory(offSetCopy, key->size + sizeof(page_id_t));
    this->InsertNewSlot(newSlot);

    this->header.pageSize++;
    this->isDirty = true;
    this->header.bytesLeft -= (newSlot.size + SlotDirectory::Size);
}

IndexPage::IndexPage(
    const page_id_t &pageId,
    const bool &isPageCreation,
    const std::array<DataType, MAX_NUMBER_OF_SUB_KEYS>& keyTypes
) : Page(pageId, INDEX_PAGE_DEFAULT_SIZE, isPageCreation)
{
    this->header.pageType = PageType::INDEX;
    this->nextNode = INVALID_PAGE_ID;
    this->previousNode = INVALID_PAGE_ID;
    this->header.bytesLeft  = Constants::INDEX_PAGE_DEFAULT_SIZE;
    this->additionalHeader.keyTypes = keyTypes;
}

IndexPage::IndexPage(const PageHeader &pageHeader) : Page(pageHeader) {
    this->nextNode = INVALID_PAGE_ID;
    this->previousNode = INVALID_PAGE_ID;
}

void IndexPage::ReadFromDisk(
    const std::vector<char> &data,
    const DatabaseEngine::StorageTypes::Table *table,
    page_offset_t &offSet,
    fstream *filePtr
){
    this->ReadAdditionalHeaderFromDisk(data, offSet);
    std::memcpy(&this->data, data.data() + offSet, INDEX_PAGE_DEFAULT_SIZE);
    offSet += INDEX_PAGE_DEFAULT_SIZE;
}

void IndexPage::WriteToDisk(fstream *filePtr){
    this->WritePageHeaderToDisk(filePtr);
    this->WriteAdditionalHeaderToDisk(filePtr);

    filePtr->write(reinterpret_cast<const char*>(&this->data), INDEX_PAGE_DEFAULT_SIZE);
}

void IndexPage::SetTreeType(const TreeType & treeType) { this->additionalHeader.SetTreeType(treeType); }

void IndexPage::SetTreeId(const page_id_t & treeId) { this->additionalHeader.treeId = treeId; }

void IndexPage::SetKeyTypes(const std::vector<DataType>& keyTypes){
    for (int i = 0;i < keyTypes.size(); i++)
        this->additionalHeader.keyTypes[i] = keyTypes[i];
}

const page_id_t & IndexPage::GetTreeId() const { return this->additionalHeader.treeId; }

void IndexPage::UpdateBytesLeft(){
    this->header.bytesLeft = Constants::INDEX_PAGE_DEFAULT_SIZE;

    const auto lastSlot = this->GetSlotDirectory(this->header.pageSize - 1);
    this->header.bytesLeft = INDEX_PAGE_DEFAULT_SIZE - lastSlot.offset + lastSlot.size + this->header.pageSize * SlotDirectory::Size;
    //
    // if (this->additionalHeader.treeType == TreeType::Clustered) {
    //     for (const auto& row: this->rows)
    //         this->header.bytesLeft -= row->TotalSize();
    // }
    // else
    //     this->header.bytesLeft -= this->nonClusteredData.size() * (sizeof(page_id_t) + sizeof(page_offset_t));

    this->header.bytesLeft -= 2 * sizeof(page_id_t);

    this->isDirty = true;
}

bool IndexPage::isEmpty() const{ return this->additionalHeader.IsEmpty();}

bool IndexPage::IsLeaf() const{ return this->additionalHeader.IsLeaf(); }

bool IndexPage::IsRoot() const{ return this->additionalHeader.IsRoot(); }

UnsignedTinyInt IndexPage::SubKeys() const{
    return this->additionalHeader.SubKeys();
}

void IndexPage::SetIsLeaf(const bool& isLeaf) {
    this->additionalHeader.SetIsLeaf(isLeaf);
    this->additionalHeader.SetIsEmpty(false);
    this->isDirty = true;
}

void IndexPage::SetIsRoot(const bool &isRoot) {
    this->additionalHeader.SetIsRoot(isRoot);
    this->additionalHeader.SetIsEmpty(false);
    this->isDirty = true;
}

void IndexPage::SetSubKeys(const UnsignedTinyInt& numberOfKeys){
    this->additionalHeader.SetNumberOfSubKeys(numberOfKeys);
}

void IndexPage::InsertChild(
    const page_id_t& child,
    const DataTypes::Indexing::Key* key,
    const int& indexPosition
){
    if (this->IndexOutOfBounds(indexPosition)){
        this->InsertChild(child, key);
        return;
    }

    auto nextOffset = this->NewRowOffset();
    const auto offSetCopy = nextOffset;

    key->Serialize(this->data, nextOffset);
    std::memcpy(this->data + nextOffset, &child, sizeof(page_id_t));

    const auto slotSize = sizeof(page_id_t) + key->size;
    this->AdjustSlotDirectories(indexPosition, offSetCopy, slotSize);

    this->header.bytesLeft -= (slotSize + SlotDirectory::Size);
    this->header.pageSize++;
    this->isDirty = true;
}

void IndexPage::InsertChild(const page_id_t &child){
    this->InsertFirstChild(child);
}

void IndexPage::SetPreviousPage(const page_id_t &previousPage){ this->previousNode = previousPage; }

void IndexPage::SetNextPage(const page_id_t &nextPage){ this->nextNode = nextPage; }

const page_id_t & IndexPage::GetPreviousPage()const{ return this->previousNode; }

const page_id_t & IndexPage::GetNextPage()const{ return this->nextNode; }

bool IndexPage::HasRightSibling() const{ return this->nextNode != INVALID_PAGE_ID; }

bool IndexPage::HasLeftSibling() const{ return this->previousNode != INVALID_PAGE_ID; }

void IndexPage::InsertKey(const DataTypes::Indexing::Key& key, const int& indexPosition){
    if (this->IndexOutOfBounds(indexPosition)){
        this->InsertKey(key);
        return;
    }

    auto nextOffset = this->NewRowOffset();
    const auto offSetCopy = nextOffset;

    key.Serialize(this->data, nextOffset);

    this->AdjustSlotDirectories(indexPosition, offSetCopy, key.size);

    this->header.bytesLeft -= (key.size + SlotDirectory::Size);
    this->header.pageSize++;
    this->isDirty = true;
}

void IndexPage::InsertTuple(const LeafNodeTuple& tuple, const int& indexPosition){
    if (this->IndexOutOfBounds(indexPosition)){
        this->InsertTuple(tuple);
        return;
    }

    auto nextOffset = this->NewRowOffset();
    const auto offSetCopy = nextOffset;

    tuple.key.Serialize(this->data, nextOffset);
    tuple.row.Serialize(this->data, nextOffset);

    const auto slotSize = tuple.row.TotalSize() + tuple.key.size;
    this->AdjustSlotDirectories(indexPosition, offSetCopy, slotSize);

    this->header.bytesLeft -= (slotSize + SlotDirectory::Size);
    this->header.pageSize++;
    this->isDirty = true;
}

DataTypes::Indexing::Key IndexPage::GetKey(const int& indexPosition) const{
    const auto slot = this->GetSlotDirectory(indexPosition);
    page_offset_t offset = slot.offset;
    return DataTypes::Indexing::Key::Deserialize(
        this->data,
        offset,
        this->additionalHeader.SubKeys(),
        this->additionalHeader.keyTypes
    );
}

LeafNodeTuple IndexPage::GetLeafTuple(const DatabaseEngine::StorageTypes::Table* table, const int& indexPosition) const{
    const auto slot = this->GetSlotDirectory(indexPosition);
    page_offset_t offSet = slot.offset;

    auto key = DataTypes::Indexing::Key::Deserialize(
        this->data,
        offSet,
        this->additionalHeader.SubKeys(),
        this->additionalHeader.keyTypes
    );

    const auto& columns = table->GetColumns();

    auto row = DatabaseEngine::StorageTypes::Row(*table);
    row.SetId(this->header.pageId, indexPosition);
    row.ReadHeaderFromDisk(this->data, offSet);
    row.ReadVersionHeaderFromDisk(this->data, offSet);
    row.ReadDataFromDisk(this->data, offSet, columns);

    return LeafNodeTuple(row, key);
}

InternalNodeTuple IndexPage::GetInternalNodeTuple(const int& indexPosition) const{
    const auto slot = this->GetSlotDirectory(indexPosition);
    page_offset_t offset = slot.offset;

    DataTypes::Indexing::Key key;
    if (indexPosition != 0){
        key =
            DataTypes::Indexing::Key::Deserialize(
                this->data,
                offset,
                this->additionalHeader.SubKeys(),
                this->additionalHeader.keyTypes
            );
    }

    page_id_t pageId = 0;
    std::memcpy(&pageId, this->data + offset, sizeof(page_id_t));

    return InternalNodeTuple(key, pageId);
}

page_id_t IndexPage::GetChild(const int& indexPosition) const{
    const auto slot = this->GetSlotDirectory(indexPosition);

    page_offset_t offset = slot.offset;
    if (indexPosition != 0){
        auto key =
            DataTypes::Indexing::Key::Deserialize(
                this->data,
                offset,
                this->additionalHeader.SubKeys(),
                this->additionalHeader.keyTypes
            );
    }

    page_id_t pageId = 0;
    std::memcpy(&pageId, this->data + offset, sizeof(page_id_t));
    return pageId;
}

void IndexPage::UpdatePageSize(){
    this->additionalHeader.SetIsEmpty(false);
    this->isDirty = true;
}

Int IndexPage::NumberOfKeys() const{
    return this->additionalHeader.IsLeaf() ? this->header.pageSize : this->header.pageSize - 1;
}

void IndexPage::Resize(const Int& size){
    for (int i = size; i < this->header.pageSize; i++){
        const auto slot = this->GetSlotDirectory(i);
        this->header.bytesLeft += slot.size + SlotDirectory::Size;
    }

    this->header.pageSize = size;
    this->isDirty = true;
}

void IndexPage::MarkEmpty(){
  this->header.bytesLeft = Constants::INDEX_PAGE_DEFAULT_SIZE;

  this->header.pageSize = 0;

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

void IndexPageAdditionalHeader::SetIsLeaf(const bool& value){
	PackedByte::SetBit(flags._data, IS_LEAF_BIT_POS, value);
}

void IndexPageAdditionalHeader::SetIsRoot(const bool& value){
    PackedByte::SetBit(flags._data, IS_ROOT_BIT_POS, value);
}

void IndexPageAdditionalHeader::SetIsEmpty(const bool& value){
	PackedByte::SetBit(flags._data, IS_EMPTY_BIT_POS, value);
}

void IndexPageAdditionalHeader::SetNumberOfSubKeys(const UnsignedTinyInt& count){
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

LeafNodeTuple::LeafNodeTuple(DatabaseEngine::StorageTypes::Row& row, DataTypes::Indexing::Key& key){
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

InternalNodeTuple::InternalNodeTuple(DataTypes::Indexing::Key& key, const page_id_t& pageId){
    this->key = std::move(key);
    this->pageId = pageId;
}
}
