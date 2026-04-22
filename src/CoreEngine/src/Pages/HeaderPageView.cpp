#include "Database.h"
#include "../../include/Pages/HeaderPageView.h"
#include "DataStorage/Table.h"
#include "Pages/Additional/Frame.h"

namespace Pages{
    void HeaderPageView::ReadTableHeadersFromDisk(){
        page_offset_t offSet = sizeof(CoreEngine::DatabaseHeader) + Constants::PAGE_HEADER_SIZE;

        this->tablesHeaders.reserve(this->databaseHeaderPtr->numberOfTables);

        for (int i = 0; i < this->databaseHeaderPtr->numberOfTables; i++){
            CoreEngine::StorageTypes::TableHeader tableHeader;

            std::memcpy(&tableHeader.tableId, this->framePtr->data + offSet, sizeof(table_id_t));
            offSet += sizeof(table_id_t);

            std::memcpy(&tableHeader.allocationPageId, this->framePtr->data + offSet, sizeof(page_id_t));
            offSet += sizeof(page_id_t);

            std::memcpy(&tableHeader.numberOfColumns, this->framePtr->data + offSet, sizeof(column_number_t));
            offSet += sizeof(column_number_t);

            std::memcpy(&tableHeader.clusteredIndexPageId, this->framePtr->data + offSet, sizeof(page_id_t));
            offSet += sizeof(page_id_t);

            UnsignedTinyInt numberOfNonClusteredIndexes;
            std::memcpy(&numberOfNonClusteredIndexes, this->framePtr->data + offSet, sizeof(UnsignedTinyInt));
            offSet += sizeof(UnsignedTinyInt);

            for (int j = 0; j < numberOfNonClusteredIndexes; j++){
                page_id_t pageId = INVALID_PAGE_ID;
                std::memcpy(&pageId, this->framePtr->data + offSet, sizeof(page_id_t));

                tableHeader.nonClusteredIndexPageIds.Push(pageId);
                offSet += sizeof(page_id_t);
            }

            this->tablesHeaders.push_back(tableHeader);
        }
    }

    HeaderPageView::HeaderPageView(Frame* framePtr) : PageView(framePtr){
        this->databaseHeaderPtr = reinterpret_cast<CoreEngine::DatabaseHeader*>(
            framePtr->data + Constants::PAGE_HEADER_SIZE
        );
    }

    HeaderPageView::HeaderPageView(HeaderPageView&& other) noexcept{
        this->databaseHeaderPtr = other.databaseHeaderPtr;
        this->tablesHeaders = std::move(other.tablesHeaders);
        this->framePtr = other.framePtr;

        other.databaseHeaderPtr = nullptr;
        other.framePtr = nullptr;
    }

    HeaderPageView& HeaderPageView::operator=(HeaderPageView&& other) noexcept{
        if (this == &other)
            return *this;

        this->databaseHeaderPtr = other.databaseHeaderPtr;
        this->tablesHeaders = std::move(other.tablesHeaders);
        this->framePtr = other.framePtr;

        other.databaseHeaderPtr = nullptr;
        other.framePtr = nullptr;

        return *this;
    }

    CoreEngine::DatabaseHeader* HeaderPageView::GetDatabaseHeaderPtr() const{
        return this->databaseHeaderPtr;
    }

    const std::vector<CoreEngine::StorageTypes::TableHeader>& HeaderPageView::GetTableHeaders()const{
        return this->tablesHeaders;
    }

    const CoreEngine::StorageTypes::TableHeader& HeaderPageView::GetTableHeader(const Int indexPosition) const{
        return this->tablesHeaders[indexPosition];
    }

    void HeaderPageView::SetDatabaseHeader(const CoreEngine::DatabaseHeader& header) const{
        std::memcpy(this->databaseHeaderPtr, &header, sizeof(CoreEngine::DatabaseHeader));
    }

    void HeaderPageView::SetTableHeader(const CoreEngine::StorageTypes::TableHeader& header){
        if (this->tablesHeaders.empty())
            this->tablesHeaders.resize(this->databaseHeaderPtr->numberOfTables);

        this->tablesHeaders[header.ordinalPosition] = header;
    }

    void HeaderPageView::WriteTableHeadersToDisk() const{
        page_offset_t offSet = sizeof(CoreEngine::DatabaseHeader) + Constants::PAGE_HEADER_SIZE;

        for (const auto& tableHeader : this->tablesHeaders){
            std::memcpy(this->framePtr->data + offSet, &tableHeader.tableId, sizeof(table_id_t));
            offSet += sizeof(table_id_t);

            std::memcpy(this->framePtr->data + offSet, &tableHeader.allocationPageId, sizeof(page_id_t));
            offSet += sizeof(page_id_t);

            std::memcpy(this->framePtr->data + offSet, &tableHeader.numberOfColumns, sizeof(column_number_t));
            offSet += sizeof(column_number_t);

            std::memcpy(this->framePtr->data + offSet, &tableHeader.clusteredIndexPageId, sizeof(page_id_t));
            offSet += sizeof(page_id_t);

            auto numberOfNonClusteredIndexes = static_cast<UnsignedTinyInt>(tableHeader.nonClusteredIndexPageIds.Size());
            std::memcpy(this->framePtr->data + offSet, &numberOfNonClusteredIndexes, sizeof(UnsignedTinyInt));
            offSet += sizeof(UnsignedTinyInt);

            for (const auto& pageId : tableHeader.nonClusteredIndexPageIds){
                std::memcpy(this->framePtr->data + offSet, &pageId, sizeof(page_id_t));
                offSet += sizeof(page_id_t);
            }
        }
    }
}
