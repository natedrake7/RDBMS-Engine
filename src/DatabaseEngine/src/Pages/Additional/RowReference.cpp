#include "../../../include/Pages/Additional/RowReference.h"

namespace Pages{
   RowReference::RowReference(){
        this->pagePtr = nullptr;
        this->indexPosition = 0;
        this->keySize = 0;
        this->isHeaderInitialized = false;
        this->dataOffset = 0;
    }

    RowReference::RowReference(PageView* pagePtr, const Int indexPosition, const Int offset){
        this->pagePtr = pagePtr;
        this->indexPosition = indexPosition;
        this->keySize = offset;
        this->pagePtr->IncreasePinCount();
        this->isHeaderInitialized = false;
        this->dataOffset = 0;
    }

    RowReference::RowReference(const RowReference& other){
        this->pagePtr = other.pagePtr;
        this->indexPosition = other.indexPosition;
        this->keySize = other.keySize;
        this->pagePtr->IncreasePinCount();
        this->header = other.header;
        this->isHeaderInitialized = other.isHeaderInitialized;
        this->sizes = other.sizes;
        this->dataOffset = other.dataOffset;
    }

    RowReference& RowReference::operator=(const RowReference& other){
        if (this == &other)
            return *this;

        this->pagePtr = other.pagePtr;
        this->indexPosition = other.indexPosition;
        this->keySize = other.keySize;
        this->pagePtr->IncreasePinCount();
        this->header = other.header;
        this->isHeaderInitialized = other.isHeaderInitialized;
        this->sizes = other.sizes;
        this->dataOffset = other.dataOffset;

        return *this;
    }

    RowReference::RowReference(RowReference&& other) noexcept{
        this->pagePtr = other.pagePtr;
        this->indexPosition = other.indexPosition;
        this->keySize = other.keySize;
        this->header = other.header;
        this->isHeaderInitialized = other.isHeaderInitialized;
        this->sizes = other.sizes;
        this->dataOffset = other.dataOffset;

        other.isHeaderInitialized = false;
        other.sizes.clear();
        other.pagePtr = nullptr;
        other.indexPosition = 0;
        other.keySize = 0;
        other.dataOffset = 0;
    }

    RowReference& RowReference::operator=(RowReference&& other) noexcept{
        if (this == &other)
            return *this;

        this->pagePtr = other.pagePtr;
        this->indexPosition = other.indexPosition;
        this->keySize = other.keySize;
        this->header = other.header;
        this->isHeaderInitialized = other.isHeaderInitialized;
        this->sizes = other.sizes;
        this->dataOffset = other.dataOffset;

        other.isHeaderInitialized = false;
        other.sizes.clear();
        other.pagePtr = nullptr;
        other.indexPosition = 0;
        other.keySize = 0;
        other.dataOffset = 0;

        return *this;
    }

    RowReference::~RowReference(){
        if (this->pagePtr)
            this->pagePtr->DecreasePinCount();

        this->pagePtr = nullptr;
    }

    QueryResult RowReference::Materialize()const{
        return this->pagePtr->MaterializeRow(this->indexPosition, this->keySize);
    }

    Value RowReference::PartialMaterialize(const column_index_t columnIndex) const{
        Value value;
        if (this->cache.TryGetValue(columnIndex, value))
            return value;

        value = this->pagePtr->PartialMaterializeRow(this, columnIndex);
        this->cache.Add(columnIndex, std::move(value));
        return  this->cache.Get(columnIndex);
    }
}