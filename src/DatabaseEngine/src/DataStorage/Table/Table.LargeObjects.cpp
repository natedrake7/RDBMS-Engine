#include "../../../include/DataStorage/Table.h"
#include "../../../../Systemic/include/DataStructures/BitMap.h"
#include "../../../include/DataStorage/Block.h"
#include "../../../include/DataStorage/Row.h"
#include "../../../include/DatabaseConstants.h"
#include "../../../include/Database.h"
#include "../../../include/Pages/LargeObjectPage.h"
#include "../../../include/BufferPool/StorageManager.h"
#include "../../../include/Pages/Page.h"
#include "../../../include/Pages/PageFreeSpacePage.h"

namespace DatabaseEngine::StorageTypes {
    void Table::InsertLargeObjectToPage(Row* row) {
        const vector<column_index_t> largeBlockIndexes = row->GetLargeBlocks();

        if (largeBlockIndexes.empty())
            return;

        const RowHeader *rowHeader = row->GetHeader();

        const auto &rowData = row->GetData();

        for (const auto &largeBlockIndex : largeBlockIndexes) 
        {
            rowHeader->largeObjectBitMap->Set(largeBlockIndex, true);

            page_offset_t offset = 0;
            block_size_t remainingBlockSize = rowData[largeBlockIndex]->Size();

            this->RecursiveInsertToLargePage(
                row,
                offset,
                largeBlockIndex,
                remainingBlockSize,
                true,
                nullptr
            );
        }
    }

    void Table::RecursiveInsertToLargePage(
        Row* row,
        page_offset_t &offset,
        const column_index_t &columnIndex,
        block_size_t &remainingBlockSize,
        const bool &isFirstRecursion,
        Pages::LargeDataObject **previousDataObject
    ){
        auto largeDataPage = this->GetOrCreateLargeDataPage();

        const auto &pageSize = largeDataPage->GetBytesLeft();

        const auto &data = row->GetData()[columnIndex]->Data();

        if (remainingBlockSize + OBJECT_METADATA_SIZE_T < pageSize)
        {
            largeDataPage->InsertObject(data + offset, remainingBlockSize);

            const auto pfsPageId = Database::GetPfsAssociatedPage(largeDataPage->GetPageId());

            auto pfsPage = Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), pfsPageId);

            pfsPage->SetPageMetaData(largeDataPage.Get());

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

        const auto pfsPageId = Database::GetPfsAssociatedPage(largeDataPage->GetPageId());

        auto pfsPage = Database::GetAssociatedPfsPage(this->database->GetSystemFilename(), pfsPageId);

        pfsPage->SetPageMetaData(largeDataPage.Get());

        if (previousDataObject != nullptr) 
            (*previousDataObject)->nextPageId = largeDataPage->GetPageId();

        offset += bytesToBeInserted;

        this->RecursiveInsertToLargePage(row, offset, columnIndex, remainingBlockSize,
                                        false, &dataObject);

        Table::InsertLargeDataObjectPointerToRow(row, isFirstRecursion,largeDataPage->GetPageId(),columnIndex);
    }

    Pages::PageGuard<Pages::LargeObjectPage> Table::GetOrCreateLargeDataPage() const{
        auto largeDataPage = this->database->GetTableLastLargeDataPage(this->header.tableId);

        return !largeDataPage.IsValid()
                    ? this->database->CreateLargeDataPage(this->header.tableId, 1)
                    : largeDataPage;
    }

    void Table::LinkLargePageDataObjectChunks(Pages::LargeDataObject *dataObject, const page_id_t &lastLargePageId){
        if (dataObject != nullptr) 
            dataObject->nextPageId = lastLargePageId;
    }

    void Table::InsertLargeDataObjectPointerToRow(
        Row* row,
        const bool &isFirstRecursion,
        const page_id_t &lastLargePageId,
        const column_index_t &largeBlockIndex
    ) const{
        if (!isFirstRecursion)
            return;

        const Pages::DataObjectPointer objectPointer(lastLargePageId);

        auto *block = new Block(
            &objectPointer,
            sizeof(Pages::DataObjectPointer),
            this->columns[largeBlockIndex]
        );

        row->UpdateColumnData(block);
    }

    Pages::PageGuard<Pages::LargeObjectPage> Table::GetLargeDataPage(const page_id_t &pageId) const {
      return Storage::StorageManager::Get().GetLargeDataPage(this->database->GetFileName(), pageId, this);
    }

    Pages::PageGuard<Pages::OverflowPage> Table::GetOverflowPage(const page_id_t & pageId) const{
      return Storage::StorageManager::Get().GetOverflowPage(this->database->GetFileName(), pageId, this);
    }
}
