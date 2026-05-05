#include "../../../include/DataStorage/Table.h"
#include "../../../include/Database.h"
#include "../../../include/BufferPool/StorageManager.h"
#include "DataStorage/InsertPayload.h"

namespace CoreEngine::StorageTypes {
    void Table::InsertLargeObjectToPage(InsertPayload& payload) {
        // Constants::LARGE_OBJECT_THRESHOLD_SIZE

        // for (const auto& column : this->_columns) {
        //     // payload.
        //     if (column->Size() < Constants::LARGE_OBJECT_THRESHOLD_SIZE)
        //         continue;
        //
        //     // Handle large object insertion for this column
        // }

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
        const ::Memory::IAllocator* allocator,
        const Value& value,
        page_offset_t &offset,
        block_size_t& remainingBlockSize,
        const Pages::LargeObjectView* previousDataObject
    )const{
        const auto page = this->GetOrCreateLargeDataPage(allocator);

        const auto pageSize = page.BytesLeft();

        const auto& data = value.Data();

        // //can fit in page
        if (remainingBlockSize + Constants::OBJECT_METADATA_SIZE_T < pageSize){
            page.SetData(data + offset, remainingBlockSize);

            const auto pfsPage = Database::GetAssociatedPfsPage(
                this->GetSystemFileKey(),
                this->GetSystemFileNameView(),
                page.PageId()
            );
            pfsPage.SetPageMetaData(&page);

            if (previousDataObject != nullptr){
                previousDataObject->SetNextPageId(page.PageId());
                return 0;
            }

            //if previous object is null, it is first pass so we return the pageId
            return page.PageId();
        }

        // blockSize < pageSize
        const auto bytesToBeInserted = pageSize - Constants::OBJECT_METADATA_SIZE_T;
        remainingBlockSize -= bytesToBeInserted;
        page.SetData(
            data + offset, bytesToBeInserted
        );

        const auto pfsPage = Database::GetAssociatedPfsPage(
            this->GetSystemFileKey(),
            this->GetSystemFileNameView(),
            page.PageId()
        );
        pfsPage.SetPageMetaData(&page);

        if (previousDataObject != nullptr)
            previousDataObject->SetNextPageId(page.PageId());

        offset += bytesToBeInserted;

        this->StoreLargeObject(
            allocator,
            value,
            offset,
            remainingBlockSize,
            &page
        );

        return page.PageId();
    }

    Pages::LargeObjectView Table::GetOrCreateLargeDataPage(const ::Memory::IAllocator* allocator) const{
        auto largeDataPage = this->database->GetTableLastLargeDataPage(
            allocator,
            this->header.tableId
        );

        if (largeDataPage.IsValid())
            return largeDataPage;

        return this->database->CreateLargeDataPage(allocator, this->header.tableId, this->header.ordinalPosition);
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
