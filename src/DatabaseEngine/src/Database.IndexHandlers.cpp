#include "../include/Database.h"
#include <vector>
#include "../include/Pages/PageFreeSpacePage.h"
#include "../include/Pages/IndexAllocationMapPage.h"
#include "../include/Pages/IndexPage.h"
#include "../include/Constants.h"
#include "../include/DataStorage/Table.h"
#include "../include/BufferPool/StorageManager.h"
#include "../include/DataStorage/Block.h"
#include "Guards/ReaderGuard.h"
#include "Guards/WriterGuard.h"

using namespace Pages;
using namespace DatabaseEngine::StorageTypes;
using namespace Storage;
using namespace Indexing;
using namespace std;
using namespace ByteMaps;

namespace DatabaseEngine {
    DataTypes::Indexing::Key Database::CreateKey(const vector<column_index_t>& indexedColumns, const Row* row)
    {
        DataTypes::Indexing::Key key;
        for (const auto &columnId : indexedColumns)
        {
            const auto &keyBlock = row->GetData()[columnId];
            key.InsertKey(DataTypes::Indexing::Key(keyBlock->GetRawData(), keyBlock->GetSize(), keyBlock->GetColumnType()));
        }

        return key;
    }

   DataTypes::Indexing::Key Database::CreateKey(const vector<column_index_t> &indexedColumns, const StorageTypes::Row *row, const Headers::RowIdentifier &rowId){
        DataTypes::Indexing::Key key;
        for (const auto &columnId : indexedColumns)
        {
            const auto &keyBlock = row->GetData()[columnId];
            key.InsertKey(DataTypes::Indexing::Key(keyBlock->GetRawData(), keyBlock->GetSize(), keyBlock->GetColumnType()));
        }

        key.InsertKey(DataTypes::Indexing::Key(&rowId, sizeof(rowId), DataType::RowIdentifier));

        return key;
    }

	PageGuard<IndexPage> Database::FindOrAllocateNextIndexPage(
	    Table*& table,
	    const page_id_t &indexPageId,
	    const int& pagesToAllocate,
	    const int& nonClusteredIndexId
	)
    {
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

        const auto indexAllocationMapPage = StorageManager::Get().GetIndexAllocationMapPage(
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

                auto indexPage = StorageManager::Get().GetIndexPage(this->filename, nextIndexPageId, table);

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