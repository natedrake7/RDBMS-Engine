#include <CoreEngine/Database.h>
#include <CoreEngine/Pages/HeaderPageView.h>
#include <CoreEngine/DataStorage/Table.h>
#include <CoreEngine/Pages/Additional/Frame.h>

namespace Pages{
    object_t* HeaderPageView::GetTableHeaderDataOffset(const Int ordinalPosition) const{
        return this->_frame->_data
            + sizeof(CoreEngine::DatabaseHeader)
            + CoreEngine::StorageLayout::PAGE_HEADER_SIZE
            + (ordinalPosition * sizeof(CoreEngine::StorageTypes::TableHeader));
    }

    HeaderPageView::HeaderPageView(Frame* framePtr) : PageView(framePtr){
        this->databaseHeaderPtr = reinterpret_cast<CoreEngine::DatabaseHeader*>(
            framePtr->_data + CoreEngine::StorageLayout::PAGE_HEADER_SIZE
        );
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
        this->_frame->_isDirty = true;
    }

    void HeaderPageView::SetTableHeader(const CoreEngine::StorageTypes::TableHeader& header, const SmallInt ordinalPosition) const{
        auto* dataOffset = this->GetTableHeaderDataOffset(ordinalPosition);
        std::memcpy(dataOffset, &header, sizeof(CoreEngine::StorageTypes::TableHeader));
        this->_frame->_isDirty = true;
    }
}
