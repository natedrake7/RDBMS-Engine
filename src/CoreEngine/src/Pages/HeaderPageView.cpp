#include <CoreEngine/Database.h>
#include <CoreEngine/Pages/HeaderPageView.h>
#include <CoreEngine/DataStorage/Table.h>
#include <CoreEngine/Pages/Additional/Frame.h>

namespace CoreEngine::Pages{
    object_t* HeaderPageView::GetTableHeaderDataOffset(const Int ordinalPosition) const{
        return this->_frame->_data
            + sizeof(DatabaseHeader)
            + StorageLayout::PAGE_HEADER_SIZE
            + (ordinalPosition * sizeof(StorageTypes::TableHeader));
    }

    HeaderPageView::HeaderPageView(Frame* framePtr)
        : PageView(framePtr){}

    DatabaseHeader* HeaderPageView::GetDatabaseHeaderPtr() const{
        return reinterpret_cast<DatabaseHeader*>(this->_frame->_data + StorageLayout::PAGE_HEADER_SIZE);
    }

    StorageTypes::TableHeader* HeaderPageView::GetTableHeaderPtr(const Int ordinalPosition) const{
        return reinterpret_cast<StorageTypes::TableHeader*>(
            this->GetTableHeaderDataOffset(ordinalPosition)
        );
    }

    void HeaderPageView::SetDatabaseHeader(const DatabaseHeader& header) const{
        std::memcpy(this->_frame->_data + StorageLayout::PAGE_HEADER_SIZE, &header, sizeof(DatabaseHeader));
        this->_frame->MarkDirty();
    }

    void HeaderPageView::SetTableHeader(const StorageTypes::TableHeader& header, const SmallInt ordinalPosition) const{
        auto* dataOffset = this->GetTableHeaderDataOffset(ordinalPosition);
        std::memcpy(dataOffset, &header, sizeof(StorageTypes::TableHeader));
        this->_frame->MarkDirty();
    }
}
