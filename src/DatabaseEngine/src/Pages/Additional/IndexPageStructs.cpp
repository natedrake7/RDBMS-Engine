#include "../../../include/Pages/Additional/IndexPageStructs.h"

namespace Pages{
    Constants::TreeType IndexPageAdditionalHeader::GetTreeType() const{
        return PackedByte::ExtractBits<Constants::TreeType>(flags._data, TREE_TYPE_BIT_POS, TREE_TYPE_BIT_MASK);
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

    void IndexPageAdditionalHeader::SetTreeType(const Constants::TreeType type){
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

        this->SetTreeType(Constants::TreeType::NonClustered);
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