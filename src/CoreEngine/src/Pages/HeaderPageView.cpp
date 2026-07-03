#include "Database.h"
#include "../../include/Pages/HeaderPageView.h"
#include "DataStorage/Table.h"
#include "Pages/Additional/Frame.h"

namespace Pages{
    object_t* HeaderPageView::GetTableHeaderDataOffset(const Int ordinalPosition) const{
        return this->_frame->_data
            + sizeof(CoreEngine::DatabaseHeader)
            + Constants::PAGE_HEADER_SIZE
            + (ordinalPosition * sizeof(CoreEngine::StorageTypes::TableHeader));
    }

    HeaderPageView::HeaderPageView(Frame* framePtr) : PageView(framePtr){
        this->databaseHeaderPtr = reinterpret_cast<CoreEngine::DatabaseHeader*>(
            framePtr->_data + Constants::PAGE_HEADER_SIZE
        );
    }

    HeaderPageView::HeaderPageView(HeaderPageView&& other) noexcept{
        this->databaseHeaderPtr = other.databaseHeaderPtr;
        this->_frame = other._frame;

        other.databaseHeaderPtr = nullptr;
        other._frame = nullptr;
    }

    HeaderPageView& HeaderPageView::operator=(HeaderPageView&& other) noexcept{
        if (this == &other)
            return *this;

        this->databaseHeaderPtr = other.databaseHeaderPtr;
        this->_frame = other._frame;

        other.databaseHeaderPtr = nullptr;
        other._frame = nullptr;

        return *this;
    }

    CoreEngine::DatabaseHeader* HeaderPageView::GetDatabaseHeaderPtr() const{
        return this->databaseHeaderPtr;
    }

    CoreEngine::StorageTypes::TableHeader* HeaderPageView::GetTableHeaderPtr(const Int ordinalPosition) const{
        return reinterpret_cast<CoreEngine::StorageTypes::TableHeader*>(
            this->GetTableHeaderDataOffset(ordinalPosition)
        );
    }

    void HeaderPageView::SetDatabaseHeader(const CoreEngine::DatabaseHeader& header) const{
        std::memcpy(this->databaseHeaderPtr, &header, sizeof(CoreEngine::DatabaseHeader));
        this->_frame->isDirty = true;
    }

    void HeaderPageView::SetTableHeader(const CoreEngine::StorageTypes::TableHeader& header) const{
        auto* dataOffset = this->GetTableHeaderDataOffset(header.ordinalPosition);
        std::memcpy(dataOffset, &header, sizeof(CoreEngine::StorageTypes::TableHeader));
        this->_frame->isDirty = true;
    }
}
