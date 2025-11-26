#include "Database.h"
#include <vector>
#include "./Pages/PageFreeSpace/PageFreeSpacePage.h"
#include "./Pages/IndexMapAllocation/IndexAllocationMapPage.h"
#include "./Pages/IndexPage/IndexPage.h"
#include "Constants.h"
#include "B+Tree/BPlusTree.h"
#include "Pages/Page.h"
#include "Table/Table.h"
#include "Row/Row.h"
#include "Storage/StorageManager/StorageManager.h"
#include "Block/Block.h"

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
            key.InsertKey(DataTypes::Indexing::Key(keyBlock->GetBlockData(), keyBlock->GetBlockSize(), keyBlock->GetColumnType()));
        }

        return key;
    }

   DataTypes::Indexing::Key Database::CreateKey(const vector<column_index_t> &indexedColumns, const StorageTypes::Row *row, const Headers::RowIdentifier &rowId){
        DataTypes::Indexing::Key key;
        for (const auto &columnId : indexedColumns)
        {
            const auto &keyBlock = row->GetData()[columnId];
            key.InsertKey(DataTypes::Indexing::Key(keyBlock->GetBlockData(), keyBlock->GetBlockSize(), keyBlock->GetColumnType()));
        }

        key.InsertKey(DataTypes::Indexing::Key(&rowId, sizeof(rowId), DataType::RowIdentifier));

        return key;
    }

	Pages::PageGuard<Pages::IndexPage> Database::FindOrAllocateNextIndexPage(const table_id_t& tableId, const page_id_t &indexPageId, const int& nonClusteredIndexId)
    {
        const auto& tableHeader = this->GetTable(tableId)->GetTableHeader();

        const Table* table = this->tables.at(tableId);

        const bool isNonClusteredIndex = nonClusteredIndexId != -1;

        const uint8_t indexId = isNonClusteredIndex
                                ? nonClusteredIndexId
                                : 0;

        if(indexPageId == INVALID_PAGE_ID)
        {
            auto newIndexPage = this->CreateIndexPage(tableId, indexId);
            
            newIndexPage->SetTreeType(isNonClusteredIndex 
                                    ? TreeType::NonClustered 
                                    : TreeType::Clustered);

            return newIndexPage;
        }

        const auto indexAllocationMapPage = StorageManager::Get().GetIndexAllocationMapPage(
            this->filename,
            tableHeader.indexAllocationMapPageId,
            table
        );
        
        vector<extent_id_t> allocatedExtents;
        indexAllocationMapPage->GetAllocatedExtents(&allocatedExtents);

        for(const auto& extentId: allocatedExtents)
        {
            const page_id_t firstExtentPageId = Database::CalculateFirstPageIdByExtentId(extentId);

            for(page_id_t nextIndexPageId = firstExtentPageId; nextIndexPageId < firstExtentPageId + EXTENT_SIZE; nextIndexPageId++)
            {
                const auto pageFreeSpacePage = Database::GetAssociatedPfsPage(this->systemFilename, nextIndexPageId);

                if (pageFreeSpacePage->GetPageType(nextIndexPageId) != PageType::INDEX)
                    continue;

                //page is free
                if(pageFreeSpacePage->GetPageSizeCategory(nextIndexPageId) == 0)
                    continue;

                auto indexPage = StorageManager::Get().GetIndexPage(this->filename, nextIndexPageId, table);

                if(!indexPage->isEmpty())
                    continue;

                indexPage->SetTreeType(isNonClusteredIndex 
                                        ? TreeType::NonClustered 
                                        : TreeType::Clustered);
                return indexPage;
            }
        }

        auto newIndexPage = this->CreateIndexPage(tableId, indexId);
            
        newIndexPage->SetTreeType(isNonClusteredIndex 
                                ? TreeType::NonClustered 
                                : TreeType::Clustered);

        return newIndexPage;
    }
}