#include <iostream>

#include "../include/Database.h"
#include <vector>
#include "../include/DatabaseConstants.h"
#include "../include/DataStorage/Table.h"
#include "../include/BufferPool/StorageManager.h"
#include "Contexts/ExecutionContext.h"
#include "Guards/ReaderGuard.h"

namespace CoreEngine {
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

    // DataTypes::Indexing::Key Database::CreateKey(
    //     const ExecutionContext& context,
    //     const std::vector<column_index_t>& indexedColumns,
    //     const Pages::RowView& rowPtr,
    //     const Int offSet
    // ){
    //
    //     DataTypes::Indexing::Key key;
    //     for (const auto ordinalPosition : indexedColumns){
    //         auto data = rowPtr.PartialMaterialize(context.GetAllocator(), ordinalPosition - offSet);
    //         key.InsertKey(DataTypes::Indexing::Key(data));
    //     }
    //
    //     return key;
    // }

    // DataTypes::Indexing::Key Database::CreateKey(
    //     const ExecutionContext& context,
    //     const std::vector<column_index_t> &indexedColumns,
    //     const Pages::RowView& rowPtr,
    //     const DataTypes::RowIdentifier &rowId
    // ){
    //     DataTypes::Indexing::Key key;
    //     for (const auto ordinalPosition : indexedColumns){
    //         auto data = rowPtr.PartialMaterialize(context.GetAllocator(), ordinalPosition);
    //         key.InsertKey(DataTypes::Indexing::Key(data));
    //     }
    //
    //     key.InsertKey(DataTypes::Indexing::Key(&rowId, sizeof(rowId), DataType::RowIdentifier, context.GetAllocator()));
    //
    //     return key;
    // }

   Pages::IndexPageView Database::FindOrAllocateNextIndexPage(
        const ::Memory::IAllocator* allocator,
        StorageTypes::Table*& table,
	    const page_id_t parentPageId,
	    const page_id_t splitChildPageId,
	    const Int pagesToAllocate,
	    const Int nonClusteredIndexId
	){
        const auto& tableHeader = table->GetHeader();

        const bool isNonClusteredIndex = nonClusteredIndexId != -1;

        const auto indexId = isNonClusteredIndex
                                ? nonClusteredIndexId
                                : 0;

        const auto treeType = isNonClusteredIndex
                                    ? Constants::TreeType::NonClustered
                                    : Constants::TreeType::Clustered;

        if(parentPageId == INVALID_PAGE_ID)
            return this->CreateIndexPage(
                allocator, tableHeader.ordinalPosition,
                pagesToAllocate, treeType, indexId
            );

        const auto indexAllocationMapPage = Storage::StorageManager::Get().GetAllocationPage(
            this->dataFileKey,
            this->filenameView,
            tableHeader.allocationPageId,
            table
        );

        DataStructures::PolymorphicArray<extent_id_t> allocatedExtents(allocator);
        indexAllocationMapPage.GetAllocatedExtents(&allocatedExtents, Database::CalculateExtentId(parentPageId));

        for(const auto& extentId: allocatedExtents){
            const auto firstExtentPageId = Database::CalculateExtentFirstPageId(extentId);

            for(page_id_t nextIndexPageId = firstExtentPageId; nextIndexPageId < firstExtentPageId + Constants::EXTENT_SIZE; nextIndexPageId++){
                if (
                    nextIndexPageId == parentPageId
                    || nextIndexPageId == splitChildPageId
                ) continue;

                {
                    const auto pageFreeSpacePage = Database::GetAssociatedPfsPage(
                        this->systemFileKey,
                        this->systemFilenameView,
                        nextIndexPageId
                    );

                    MultiThreading::ReaderGuard pfsLock(&pageFreeSpacePage.Latch());

                    if (!pageFreeSpacePage.IsPageAllocated(nextIndexPageId)) continue;

                    const auto pageType = pageFreeSpacePage.GetPageType(nextIndexPageId);
                    if (pageType == Constants::PageType::IAM) continue;
                    if (pageType != Constants::PageType::INDEX) break;

                    //page is free
                    if(pageFreeSpacePage.GetPageSizeCategory(nextIndexPageId) < 6) continue;
                }

                auto indexPage = Storage::StorageManager::Get().GetIndexPage(
                    this->dataFileKey,
                    this->filenameView,
                    nextIndexPageId,
                    table
                );

                bool success = false;
                auto readerGuard = MultiThreading::ReaderGuard::TryLock(&indexPage.Latch(), success);
                if(!success || !indexPage.IsEmpty())
                    continue;

                return indexPage;
            }
        }

        return this->CreateIndexPage(
            allocator, tableHeader.ordinalPosition,
            pagesToAllocate, treeType, indexId
        );
    }
}