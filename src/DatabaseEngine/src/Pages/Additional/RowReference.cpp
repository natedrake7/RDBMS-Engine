#include "../../../include/Pages/Additional/RowReference.h"

#include "BufferPool/StorageManager.h"
#include "Pages/PageView.h"
#include "Pages/Additional/Frame.h"

namespace Pages{
   RowReference::RowReference(){
        this->pageView = PageView();
        this->indexPosition = 0;
        this->keySize = 0;
        this->isHeaderInitialized = false;
        this->dataOffset = 0;
    }

    RowReference::RowReference(Frame* framePtr, const Int indexPosition, const Int offset){
        this->pageView = PageView(framePtr);
        this->indexPosition = indexPosition;
        this->keySize = offset;
        this->isHeaderInitialized = false;
        this->dataOffset = 0;
    }

    RowReference::RowReference(const RowReference& other){
        this->pageView = PageView(other.pageView.GetFrame());
        this->indexPosition = other.indexPosition;
        this->keySize = other.keySize;
        this->header = other.header;
        this->isHeaderInitialized = other.isHeaderInitialized;
        this->sizes = other.sizes;
        this->dataOffset = other.dataOffset;
    }

    RowReference& RowReference::operator=(const RowReference& other){
        if (this == &other)
            return *this;

        this->pageView = PageView(other.pageView.GetFrame());
        this->indexPosition = other.indexPosition;
        this->keySize = other.keySize;
        this->header = other.header;
        this->isHeaderInitialized = other.isHeaderInitialized;
        this->sizes = other.sizes;
        this->dataOffset = other.dataOffset;

        return *this;
    }

    RowReference::RowReference(RowReference&& other) noexcept{
        this->pageView = std::move(other.pageView);
        this->indexPosition = other.indexPosition;
        this->keySize = other.keySize;
        this->header = other.header;
        this->isHeaderInitialized = other.isHeaderInitialized;
        this->sizes = other.sizes;
        this->dataOffset = other.dataOffset;

        other.isHeaderInitialized = false;
        other.sizes.clear();
        other.indexPosition = 0;
        other.keySize = 0;
        other.dataOffset = 0;
    }

    RowReference& RowReference::operator=(RowReference&& other) noexcept{
        if (this == &other)
            return *this;

        this->pageView = std::move(other.pageView);
        this->indexPosition = other.indexPosition;
        this->keySize = other.keySize;
        this->header = other.header;
        this->isHeaderInitialized = other.isHeaderInitialized;
        this->sizes = other.sizes;
        this->dataOffset = other.dataOffset;

        other.isHeaderInitialized = false;
        other.sizes.clear();
        other.indexPosition = 0;
        other.keySize = 0;
        other.dataOffset = 0;

        return *this;
    }

    RowReference::~RowReference() = default;

    QueryResult RowReference::Materialize()const{
        return this->pageView.MaterializeRow(this->indexPosition, this->keySize);
    }

    Value RowReference::PartialMaterialize(const column_index_t columnIndex) const{
        Value value;
        if (this->cache.TryGetValue(columnIndex, value))
            return value;

        value = this->pageView.PartialMaterializeRow(this, columnIndex);
        this->cache.Add(columnIndex, std::move(value));
        return  this->cache.Get(columnIndex);
    }
}
