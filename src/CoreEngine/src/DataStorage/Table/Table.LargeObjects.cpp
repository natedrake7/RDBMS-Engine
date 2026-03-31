#include "../../../include/DataStorage/Table.h"
#include "../../../include/Database.h"
#include "../../../include/BufferPool/StorageManager.h"
#include "DataStorage/InsertPayload.h"

namespace CoreEngine::StorageTypes {
    void Table::InsertLargeObjectToPage(Pages::RowReference& rowPtr) {
        // const vector<column_index_t> largeBlockIndexes = row->GetLargeBlocks();
        //
        // if (largeBlockIndexes.empty())
        //     return;
        //
        // auto* rowHeader = row->GetHeader();
        //
        // const auto &rowData = row->GetData();
        //
        // for (const auto &largeBlockIndex : largeBlockIndexes)
        // {
        //     rowHeader->largeObjectBitMap.Set(largeBlockIndex, true);
        //
        //     page_offset_t offset = 0;
        //     block_size_t remainingBlockSize = rowData[largeBlockIndex]->Size();
        //
        //     this->RecursiveInsertToLargePage(
        //         row,
        //         offset,
        //         largeBlockIndex,
        //         remainingBlockSize,
        //         true,
        //         nullptr
        //     );
        // }
    }

    page_id_t Table::StoreLargeObject(
        const Value& value,
        page_offset_t &offset,
        block_size_t& remainingBlockSize,
        Pages::LargeObjectView* previousDataObject
    )const{
        // auto page = this->GetOrCreateLargeDataPage();
        //
        // const auto pageSize = page->BytesLeft();
        //
        // const auto& data = value.Data();
        //
        // //can fit in page
        // if (remainingBlockSize + OBJECT_METADATA_SIZE_T < pageSize){
        //     page->InsertObject(data + offset, remainingBlockSize);
        //
        //     auto pfsPage = Database::GetAssociatedPfsPage(
        //         this->database->GetSystemFilename(),
        //         page->PageId()
        //     );
        //     pfsPage.SetPageMetaData(&page);
        //
        //     if (previousDataObject != nullptr){
        //         previousDataObject->nextPageId = page->PageId();
        //         return 0;
        //     }
        //
        //     //if previous object is null, it is first pass so we return the pageId
        //     return page->PageId();
        // }
        // //
        // // // blockSize < pageSize
        // const auto bytesToBeInserted = pageSize - OBJECT_METADATA_SIZE_T;
        //
        // remainingBlockSize -= bytesToBeInserted;
        //
        // auto* dataObject = page->InsertObject(
        //     data + offset, bytesToBeInserted
        // );
        //
        // auto pfsPage = Database::GetAssociatedPfsPage(
        //     this->database->GetSystemFilename(),
        //     page->PageId()
        // );
        // pfsPage->SetPageMetaData(page.Get());
        //
        // if (previousDataObject != nullptr)
        //     previousDataObject->nextPageId = page->PageId();
        //
        // offset += bytesToBeInserted;
        //
        // this->StoreLargeObject(
        //     value,
        //     offset,
        //     remainingBlockSize,
        //     dataObject
        // );
        // return page->PageId();
    }

    Pages::LargeObjectView Table::GetOrCreateLargeDataPage() const{
        auto largeDataPage = this->database->GetTableLastLargeDataPage(this->header.tableId);

        if (largeDataPage.IsValid())
            return largeDataPage;

        return this->database->CreateLargeDataPage(this->header.tableId, 1);
    }

    void Table::LinkLargePageDataObjectChunks(const Pages::LargeObjectView* dataObject, const page_id_t lastLargePageId){
        if (dataObject == nullptr)
            return;

        dataObject->SetNextPageId(lastLargePageId);
    }

    void Table::InsertLargeDataObjectPointerToRow(
        const bool isFirstRecursion,
        const page_id_t lastLargePageId,
        const column_index_t largeBlockIndex
    ) const{
        if (!isFirstRecursion)
            return;

        // row->UpdateColumnData(block);
    }

    Pages::LargeObjectView Table::GetLargeDataPage(const page_id_t pageId) const {
        return Storage::StorageManager::Get().GetLargeDataPage(
            this->database->GetDataFileKey(),
            this->database->GetFileName(),
            pageId,
            this
        );
    }

    Pages::OverflowPageView Table::GetOverflowPage(const page_id_t pageId) const{
        return Storage::StorageManager::Get().GetOverflowPage(
            this->database->GetDataFileKey(),
            this->database->GetFileName(),
            pageId,
            this
        );
    }
}
