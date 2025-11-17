#include "Table.h"
#include "../../Systemic/DataStructures/BitMap/BitMap.h"
#include "../Block/Block.h"
#include "../Constants.h"
#include "../Database.h"
#include "../Pages/LargeObject/LargeObjectPage.h"
#include "../Storage/StorageManager/StorageManager.h"
#include "../Pages/Page.h"
#include "../Row/Row.h"

namespace DatabaseEngine::StorageTypes {
    void Table::InsertLargeObjectToPage(Row *row) 
    {
        const vector<column_index_t> largeBlockIndexes = row->GetLargeBlocks();

        if (largeBlockIndexes.empty())
            return;

        const RowHeader *rowHeader = row->GetHeader();

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

    void Table::RecursiveInsertToLargePage(
        Row *&row,
        page_offset_t &offset,
        const column_index_t &columnIndex,
        block_size_t &remainingBlockSize,
        const bool &isFirstRecursion,
        Pages::LargeDataObject **previousDataObject
    ){
        auto largeDataPage = this->GetOrCreateLargeDataPage();

        const auto &pageSize = largeDataPage->GetBytesLeft();

        const auto &data = row->GetData()[columnIndex]->GetBlockData();

        if (remainingBlockSize + OBJECT_METADATA_SIZE_T < pageSize)
        {
            largeDataPage->InsertObject(data + offset, remainingBlockSize);

            this->database->SetPageMetaDataToPfs(largeDataPage.Get());

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

        Pages::LargeDataObject *dataObject = largeDataPage->InsertObject(
            data + offset, bytesToBeInserted);

        this->database->SetPageMetaDataToPfs(largeDataPage.Get());

        if (previousDataObject != nullptr) 
            (*previousDataObject)->nextPageId = largeDataPage->GetPageId();

        offset += bytesToBeInserted;

        this->RecursiveInsertToLargePage(row, offset, columnIndex, remainingBlockSize,
                                        false, &dataObject);

        Table::InsertLargeDataObjectPointerToRow(row, isFirstRecursion,largeDataPage->GetPageId(),columnIndex);
    }

    Pages::PageGuard<Pages::LargeObjectPage> Table::GetOrCreateLargeDataPage() const
    {
        auto largeDataPage = this->database->GetTableLastLargeDataPage(this->header.tableId);

        return (largeDataPage.Get() == nullptr)
                    ? this->database->CreateLargeDataPage(this->header.tableId)
                    : largeDataPage;
    }

    void Table::LinkLargePageDataObjectChunks(Pages::LargeDataObject *dataObject, const page_id_t &lastLargePageId)
    {
        if (dataObject != nullptr) 
            dataObject->nextPageId = lastLargePageId;
    }

    void Table::InsertLargeDataObjectPointerToRow(Row *row, const bool &isFirstRecursion, const page_id_t &lastLargePageId, const column_index_t &largeBlockIndex) const
    {
        if (!isFirstRecursion)
            return;

        const Pages::DataObjectPointer objectPointer(lastLargePageId);

        auto *block = new Block(&objectPointer, sizeof(Pages::DataObjectPointer),
                                this->columns[largeBlockIndex]);

        row->UpdateColumnData(block);
    }

    Pages::PageGuard<Pages::LargeObjectPage> Table::GetLargeDataPage(const page_id_t &pageId) const {
      return Storage::StorageManager::Get().GetLargeDataPage(this->database->GetFileName(), pageId, this);
//      return this->database->GetLargeDataPage(pageId, this->header.tableId);
    }

    Pages::PageGuard<Pages::OverflowPage> Table::GetOverflowPage(const page_id_t & pageId) const{
      return Storage::StorageManager::Get().GetOverflowPage(this->database->GetFileName(), pageId, this);
    }

    const Headers::Index& Table::GetNonClusteredIndexes(const int& indexPos) const { return this->header.nonClusteredIndexes.at(indexPos); }

    const vector<column_index_t> & Table::GetClusteredIndex() const { return this->header.clusteredIndex.columns; }
}
