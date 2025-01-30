#include "Table.h"
#include "../RowCondition/RowCondition.h"
#include "../../AdditionalLibraries/BitMap/BitMap.h"
#include "../Block/Block.h"
#include "../Column/Column.h"
#include "../Constants.h"
#include "../Database.h"
#include "../Pages/LargeObject/LargeDataPage.h"
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

    void Table::RecursiveInsertToLargePage(Row *&row, page_offset_t &offset, const column_index_t &columnIndex, block_size_t &remainingBlockSize, const bool &isFirstRecursion, DataObject **previousDataObject) 
    {
        LargeDataPage *largeDataPage = this->GetOrCreateLargeDataPage();

        const auto &pageSize = largeDataPage->GetBytesLeft();

        const auto &data = row->GetData()[columnIndex]->GetBlockData();

        large_page_index_t objectIndex;

        if (remainingBlockSize + OBJECT_METADATA_SIZE_T < pageSize) 
        {

            largeDataPage->InsertObject(data + offset, remainingBlockSize,
                                        &objectIndex);

            this->database->SetPageMetaDataToPfs(largeDataPage);

            Table::InsertLargeDataObjectPointerToRow(row, isFirstRecursion, objectIndex,
                                                    largeDataPage->GetPageId(),
                                                    columnIndex);

            if (previousDataObject != nullptr) 
            {
            (*previousDataObject)->nextPageId = largeDataPage->GetPageId();
            (*previousDataObject)->nextObjectIndex = objectIndex;
            }

            return;
        }

        // blockSize < pageSize
        const auto bytesToBeInserted = pageSize - OBJECT_METADATA_SIZE_T;

        remainingBlockSize -= bytesToBeInserted;

        DataObject *dataObject = largeDataPage->InsertObject(
            data + offset, bytesToBeInserted, &objectIndex);

        this->database->SetPageMetaDataToPfs(largeDataPage);

        if (previousDataObject != nullptr) 
        {
            (*previousDataObject)->nextPageId = largeDataPage->GetPageId();
            (*previousDataObject)->nextObjectIndex = objectIndex;
        }

        offset += bytesToBeInserted;

        this->RecursiveInsertToLargePage(row, offset, columnIndex, remainingBlockSize,
                                        false, &dataObject);

        Table::InsertLargeDataObjectPointerToRow(row, isFirstRecursion, objectIndex,
                                                largeDataPage->GetPageId(),
                                                columnIndex);
    }

    LargeDataPage *Table::GetOrCreateLargeDataPage() const 
    {
        LargeDataPage *largeDataPage = this->database->GetTableLastLargeDataPage(this->header.tableId, OBJECT_METADATA_SIZE_T + 1);

        return (largeDataPage == nullptr)
                    ? this->database->CreateLargeDataPage(this->header.tableId)
                    : largeDataPage;
    }

    void Table::LinkLargePageDataObjectChunks(DataObject *dataObject, const page_id_t &lastLargePageId, const large_page_index_t &objectIndex) 
    {
        if (dataObject != nullptr) 
        {
            dataObject->nextPageId = lastLargePageId;
            dataObject->nextObjectIndex = objectIndex;
        }
        }

    void Table::InsertLargeDataObjectPointerToRow(Row *row, const bool &isFirstRecursion, const large_page_index_t &objectIndex, const page_id_t &lastLargePageId, const column_index_t &largeBlockIndex) const 
    {
        if (!isFirstRecursion)
            return;

        const DataObjectPointer objectPointer(objectIndex, lastLargePageId);

        Block *block = new Block(&objectPointer, sizeof(DataObjectPointer),
                                this->columns[largeBlockIndex]);

        row->UpdateColumnData(block);
    }

    LargeDataPage *Table::GetLargeDataPage(const page_id_t &pageId) const { return this->database->GetLargeDataPage(pageId, this->header.tableId); }

    const vector<vector<column_index_t>> & Table::GetNonClusteredIndexes() const { return this->header.nonClusteredColumnIndexes; }

    const vector<column_index_t> & Table::GetClusteredIndex() const { return this->header.clusteredColumnIndexes; }
}
