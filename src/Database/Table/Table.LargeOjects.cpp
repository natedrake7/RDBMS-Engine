#include "Table.h"
#include "../../AdditionalLibraries/BitMap/BitMap.h"
#include "../Block/Block.h"
#include "../Constants.h"
#include "../Database.h"
#include "../Pages/LargeObject/LargeObjectPage.h"
#include "../Storage/StorageManager/StorageManager.h"
#include "../Pages/Page.h"
#include "../Row/Row.h"

using namespace Pages;
using namespace ByteMaps;
using namespace Storage;

namespace DatabaseEngine::StorageTypes {
    void Table::InsertLargeObjectToPage(Row *row) 
    {
        const vector<column_index_t> largeBlockIndexes = row->GetLargeBlocks();

        if (largeBlockIndexes.empty())
            return;

        RowHeader *rowHeader = row->GetHeader();

        const auto &rowData = row->GetData();

        for (const auto &largeBlockIndex : largeBlockIndexes) 
        {
            rowHeader->largeObjectBitMap->Set(largeBlockIndex, true);

            page_offset_t offset = 0;

            block_size_t remainingBlockSize = rowData[largeBlockIndex]->GetBlockSize();

            RecursiveInsertToLargePage(row, offset, largeBlockIndex, remainingBlockSize,
                                    true, nullptr);
        }
    }

    void Table::RecursiveInsertToLargePage(Row *&row, page_offset_t &offset, const column_index_t &columnIndex, block_size_t &remainingBlockSize, const bool &isFirstRecursion, LargeDataObject **previousDataObject) 
    {
        LargeObjectPage *largeDataPage = this->GetOrCreateLargeDataPage();

        const auto &pageSize = largeDataPage->GetBytesLeft();

        const auto &data = row->GetData()[columnIndex]->GetBlockData();

        if (remainingBlockSize + OBJECT_METADATA_SIZE_T < pageSize)
        {
            largeDataPage->InsertObject(data + offset, remainingBlockSize);

            this->database->SetPageMetaDataToPfs(largeDataPage);

            Table::InsertLargeDataObjectPointerToRow(row, isFirstRecursion,largeDataPage->GetPageId(),columnIndex);

            if (previousDataObject != nullptr) 
            {
              (*previousDataObject)->nextPageId = largeDataPage->GetPageId();
            }

            return;
        }

        // blockSize < pageSize
        const auto bytesToBeInserted = pageSize - OBJECT_METADATA_SIZE_T;

        remainingBlockSize -= bytesToBeInserted;

        LargeDataObject *dataObject = largeDataPage->InsertObject(
            data + offset, bytesToBeInserted);

        this->database->SetPageMetaDataToPfs(largeDataPage);

        if (previousDataObject != nullptr) 
            (*previousDataObject)->nextPageId = largeDataPage->GetPageId();

        offset += bytesToBeInserted;

        this->RecursiveInsertToLargePage(row, offset, columnIndex, remainingBlockSize,
                                        false, &dataObject);

        Table::InsertLargeDataObjectPointerToRow(row, isFirstRecursion,largeDataPage->GetPageId(),columnIndex);
    }

    LargeObjectPage *Table::GetOrCreateLargeDataPage() const 
    {
        LargeObjectPage *largeDataPage = this->database->GetTableLastLargeDataPage(this->header.tableId);

        return (largeDataPage == nullptr)
                    ? this->database->CreateLargeDataPage(this->header.tableId)
                    : largeDataPage;
    }

    void Table::LinkLargePageDataObjectChunks(LargeDataObject *dataObject, const page_id_t &lastLargePageId, const large_page_index_t &objectIndex) 
    {
        if (dataObject != nullptr) 
            dataObject->nextPageId = lastLargePageId;
    }

    void Table::InsertLargeDataObjectPointerToRow(Row *row, const bool &isFirstRecursion, const page_id_t &lastLargePageId, const column_index_t &largeBlockIndex) const
    {
        if (!isFirstRecursion)
            return;

        const DataObjectPointer objectPointer(lastLargePageId);

        Block *block = new Block(&objectPointer, sizeof(DataObjectPointer),
                                this->columns[largeBlockIndex]);

        row->UpdateColumnData(block);
    }

    LargeObjectPage *Table::GetLargeDataPage(const page_id_t &pageId) const {
      const auto extentId = Database::CalculateExtentIdByPageId(pageId);

      return StorageManager::Get().GetLargeDataPage(this->database->GetFileName(), pageId, extentId, this);
//      return this->database->GetLargeDataPage(pageId, this->header.tableId);
    }

    Pages::OverflowPage* Table::GetOverflowPage(const page_id_t & pageId) const{
      const auto extentId = Database::CalculateExtentIdByPageId(pageId);

      return StorageManager::Get().GetOverflowPage(this->database->GetFileName(), pageId, extentId, this);
    }

    const Headers::Index& Table::GetNonClusteredIndexes(const int& indexPos) const { return this->header.nonClusteredIndexes.at(indexPos); }

    const vector<column_index_t> & Table::GetClusteredIndex() const { return this->header.clusteredIndex.columns; }
}
