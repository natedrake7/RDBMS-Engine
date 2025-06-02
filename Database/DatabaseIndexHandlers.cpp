#include "Database.h"
#include <vector>
#include "./Pages/PageFreeSpace/PageFreeSpacePage.h"
#include "./Pages/IndexMapAllocation/IndexAllocationMapPage.h"
#include "./Pages/IndexPage/IndexPage.h"
#include "Constants.h"
#include "../AdditionalLibraries/AdditionalDataTypes/ErrorHandling.h"
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
    Key Database::CreateKey(const vector<column_index_t>& indexedColumns, const Row* row)
    {
        Key key;
        for (const auto &columnId : indexedColumns)
        {
            const auto &keyBlock = row->GetData()[columnId];
            key.InsertKey(Key(keyBlock->GetBlockData(), keyBlock->GetBlockSize(), keyBlock->GetColumnType()));
        }

        return key;
    }

    void Database::UpdateNonClusteredData(const Table& table, Page* nextLeafPage, const page_id_t& nextLeafPageId) const
    {
       if(!table.HasNonClusteredIndexes())
            return;

        Table* tablePtr = this->tables.at(table.GetTableId());

        const auto& nonClusteredIndexes = tablePtr->GetNonClusteredIndexes();
            

        for (int i = 0; i < nonClusteredIndexes.size(); i++)
        {
            const BPlusTree* nonClusteredTree = tablePtr->GetNonClusteredIndexTree(i);

            const auto& rows = nextLeafPage->GetDataRowsUnsafe();

            for (page_offset_t index = 0; index < rows->size(); index++)
            {
                const auto key = Database::CreateKey(nonClusteredIndexes[i], (*rows)[index]);

                nonClusteredTree->UpdateRowData(key, BPlusTreeNonClusteredData(nextLeafPageId, index));
            }
        }
    }

	IndexPage* Database::FindOrAllocateNextIndexPage(const table_id_t& tableId, const page_id_t &indexPageId, const int& nonClusteredIndexId, const bool& findPageDifferentFromCurrent)
    {
        const auto& tableHeader = this->GetTable(tableId)->GetTableHeader();

        const Table* table = this->tables.at(tableId);

        const bool isNonClusteredIndex = nonClusteredIndexId != -1;

        const uint8_t indexId = isNonClusteredIndex
                                ? table->GetNonClusteredIndexId(nonClusteredIndexId)
                                : 0;

        if(indexPageId == 0)
        {
            IndexPage* newIndexPage = this->CreateIndexPage(tableId, indexId);
            
            newIndexPage->SetTreeType(isNonClusteredIndex 
                                    ? TreeType::NonClustered 
                                    : TreeType::Clustered);

            return newIndexPage;
        }

        const IndexAllocationMapPage* indexAllocationMapPage = StorageManager::Get().GetIndexAllocationMapPage(this->filename, tableHeader.indexAllocationMapPageId);
        
        vector<extent_id_t> allocatedExtents;
        indexAllocationMapPage->GetAllocatedExtents(&allocatedExtents);

        for(const auto& extentId: allocatedExtents)
        {
            const page_id_t firstExtentPageId = Database::CalculateSystemPageOffsetByExtentId(extentId);

            for(page_id_t nextIndexPageId = firstExtentPageId; nextIndexPageId < firstExtentPageId + EXTENT_SIZE; nextIndexPageId++)
            {
                const PageFreeSpacePage * pageFreeSpacePage = Database::GetAssociatedPfsPage(this->systemFilename, nextIndexPageId);

                if (pageFreeSpacePage->GetPageType(nextIndexPageId) != PageType::INDEX)
                    continue;

                //page is free
                if(pageFreeSpacePage->GetPageSizeCategory(nextIndexPageId) == 0)
                    continue;

                IndexPage* indexPage = StorageManager::Get().GetIndexPage(this->filename, nextIndexPageId, extentId, table);

                if(!indexPage->isEmpty())
                    continue;

                indexPage->SetTreeType(isNonClusteredIndex 
                                        ? TreeType::NonClustered 
                                        : TreeType::Clustered);
                return indexPage;
            }
        }

        IndexPage* newIndexPage = this->CreateIndexPage(tableId, indexId);
            
        newIndexPage->SetTreeType(isNonClusteredIndex 
                                ? TreeType::NonClustered 
                                : TreeType::Clustered);

        return newIndexPage;
    }
}