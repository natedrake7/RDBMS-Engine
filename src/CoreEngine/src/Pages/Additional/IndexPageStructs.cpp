#include "../../../include/Pages/Additional/IndexPageStructs.h"
#include "DataStorage/Row.h"

namespace Pages{
    IndexInsertTuple::IndexInsertTuple(){
        this->payload = nullptr;
    }

    IndexInsertTuple::IndexInsertTuple(
        DataTypes::Indexing::Key& key,
        CoreEngine::StorageTypes::InsertPayload* payload
    ){
        this->key = std::move(key);
        this->payload = payload;
    }

    LeafNodeTuple& LeafNodeTuple::operator=(LeafNodeTuple&& other) noexcept
    {
        if (this == &other)
            return *this;

        this->key = std::move(other.key);
        this->row = other.row;

        return *this;
    }

    LeafNodeTuple::LeafNodeTuple(LeafNodeTuple&& other) noexcept
    {
        this->key = std::move(other.key);
        this->row = other.row;
    }


    LeafNodeTuple::LeafNodeTuple(const CoreEngine::StorageTypes::RID row, DataTypes::Indexing::Key& key){
        this->key = std::move(key);
        this->row = row;
    }

    InternalNodeTuple::InternalNodeTuple()
        : pageId(INVALID_PAGE_ID){}

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

    InternalNodeTuple::InternalNodeTuple(DataTypes::Indexing::Key& key, const page_id_t pageId){
        this->key = std::move(key);
        this->pageId = pageId;
    }

    void InternalNodeTuple::SetKey(DataTypes::Indexing::Key& otherKey){
        this->key = std::move(otherKey);
    }

    void InternalNodeTuple::SetPageId(const page_id_t otherPageId){
        this->pageId = otherPageId;
    }
}
