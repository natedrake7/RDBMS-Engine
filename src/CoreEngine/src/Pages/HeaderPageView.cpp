#include <CoreEngine/Database.h>
#include <CoreEngine/Pages/HeaderPageView.h>
#include <CoreEngine/DataStorage/Table.h>
#include <CoreEngine/Pages/Additional/Frame.h>

namespace CoreEngine::Pages{
    object_t* HeaderPageView::DatabaseHeaderData() const{
        return this->_frame->_data
            + StorageLayout::PAGE_HEADER_SIZE;
    }

    object_t* HeaderPageView::GetTableHeaderData(const UnsignedInt ordinal) const{
        static constexpr UnsignedInt MAX_TABLES =
            (StorageLayout::PAGE_SIZE - StorageLayout::PAGE_HEADER_SIZE - sizeof(DatabaseHeader)) / sizeof(StorageTypes::TableHeader);

        if (ordinal >= MAX_TABLES)
            throw std::length_error("HeaderPageView: table ordinal exceeds the header page");
        
        return this->_frame->_data
                + StorageLayout::PAGE_HEADER_SIZE
                + sizeof(DatabaseHeader)
                + (ordinal * sizeof(StorageTypes::TableHeader));
    }

    HeaderPageView::HeaderPageView(Frame* framePtr)
        : PageView(framePtr){}

    DatabaseHeader HeaderPageView::GetDatabaseHeader() const{
        DatabaseHeader header;
        std::memcpy(&header, this->DatabaseHeaderData(), sizeof(DatabaseHeader));
        return header;
    }

    const StorageTypes::TableHeader* HeaderPageView::GetTableHeaderPtr(const Int ordinalPosition) const{
        return reinterpret_cast<const StorageTypes::TableHeader*>(
            this->GetTableHeaderData(ordinalPosition)
        );
    }

    void HeaderPageView::SetDatabaseHeader(const DatabaseHeader& header) const{
        std::memcpy(this->DatabaseHeaderData(), &header, sizeof(DatabaseHeader));
        this->_frame->MarkDirty();
    }

    void HeaderPageView::SetTableHeader(const StorageTypes::TableHeader& header, const SmallInt ordinalPosition) const{
        auto* dataOffset = this->GetTableHeaderData(ordinalPosition);
        std::memcpy(dataOffset, &header, sizeof(StorageTypes::TableHeader));
        this->_frame->MarkDirty();
    }
}
