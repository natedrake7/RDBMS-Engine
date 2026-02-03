#include "../include/Database.h"
#include <vector>
#include "../include/Pages/PageFreeSpacePage.h"
#include "../include/Pages/IndexAllocationMapPage.h"
#include "../include/Pages/IndexPage.h"
#include "../include/DatabaseConstants.h"
#include "../include/DataStorage/Table.h"
#include "../include/BufferPool/StorageManager.h"
#include "Guards/ReaderGuard.h"

namespace DatabaseEngine {
    DataTypes::Indexing::Key Database::CreateKey(
        const std::vector<column_index_t>& indexedColumns,
        const StorageTypes::InsertPayload& payload
    )
    {
        DataTypes::Indexing::Key key;
        // for (const auto &columnId : indexedColumns){
        //     auto data = row->GetColumnByIndex(columnId);
        //     key.InsertKey(DataTypes::Indexing::Key(data));
        // }

        return key;
    }

    DataTypes::Indexing::Key Database::CreateKey(
        const std::vector<column_index_t>& indexedColumns,
        const Pages::RowReference& rowPtr,
        const Int offSet
    ){
        DataTypes::Indexing::Key key;
        for (const auto ordinalPosition : indexedColumns){
            auto data = rowPtr.PartialMaterialize(ordinalPosition - offSet);
            key.InsertKey(DataTypes::Indexing::Key(data));
        }

        return key;
    }

    DataTypes::Indexing::Key Database::CreateKey(
        const std::vector<column_index_t> &indexedColumns,
        const Pages::RowReference& rowPtr,
        const DataTypes::RowIdentifier &rowId
    ){
        DataTypes::Indexing::Key key;
        for (const auto ordinalPosition : indexedColumns){
            auto data = rowPtr.PartialMaterialize(ordinalPosition);
            key.InsertKey(DataTypes::Indexing::Key(data));
        }

        key.InsertKey(DataTypes::Indexing::Key(&rowId, sizeof(rowId), DataType::RowIdentifier));

        return key;
    }

   Pages::PageGuard<Pages::IndexPage> Database::FindOrAllocateNextIndexPage(
        StorageTypes::Table*& table,
	    const page_id_t indexPageId,
	    const Int pagesToAllocate,
	    const Int nonClusteredIndexId
	){
        const auto& tableHeader = table->GetHeader();

        const bool isNonClusteredIndex = nonClusteredIndexId != -1;

        const uint8_t indexId = isNonClusteredIndex
                                ? nonClusteredIndexId
                                : 0;

        const auto treeType = isNonClusteredIndex
                                    ? TreeType::NonClustered
                                    : TreeType::Clustered;

        if(indexPageId == INVALID_PAGE_ID)
            return this->CreateIndexPage(tableHeader.ordinalPosition, pagesToAllocate, treeType, indexId);

        const auto indexAllocationMapPage = Storage::StorageManager::Get().GetAllocationPage(
            this->filename,
            tableHeader.indexAllocationMapPageId,
            table
        );

        std::vector<extent_id_t> allocatedExtents;
        indexAllocationMapPage->GetAllocatedExtents(&allocatedExtents, Database::CalculateExtentId(indexPageId));

        for(const auto& extentId: allocatedExtents){
            const auto firstExtentPageId = Database::CalculateExtentFirstPageId(extentId);

            for(page_id_t nextIndexPageId = firstExtentPageId; nextIndexPageId < firstExtentPageId + EXTENT_SIZE; nextIndexPageId++){
                {
                    const auto pageFreeSpacePage = Database::GetAssociatedPfsPage(this->systemFilename, nextIndexPageId);

                    MultiThreading::ReaderGuard pfsLock(&pageFreeSpacePage->Latch());

                    if (pageFreeSpacePage->GetPageType(nextIndexPageId) != PageType::INDEX)
                        continue;

                    //page is free
                    if(pageFreeSpacePage->GetPageSizeCategory(nextIndexPageId) == 0)
                        continue;
                }

                auto indexPage = Storage::StorageManager::Get().GetIndexPage(this->filename, nextIndexPageId, table);

                if (!indexPage.IsValid())
                    continue;

                bool successfulLock = false;
                auto readerGuard = MultiThreading::ReaderGuard::TryLock(&indexPage->Latch(), successfulLock);

                if(!successfulLock || !indexPage->isEmpty())
                    continue;

                return indexPage;
            }
        }

        return this->CreateIndexPage(tableHeader.ordinalPosition, pagesToAllocate, treeType, indexId);
    }
}