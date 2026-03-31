#pragma once
#include "../../Systemic/include/DataTypes/DataTypes.h"
#include "../../Systemic/include/DataStructures/HashSet.h"
#include "../../Systemic/include/RowIdentifier.h"

namespace CoreEngine {
    struct ScanState {
        extent_id_t extentId;
        DataTypes::RowIdentifier lastFetchedRowId;

        bool canFetchMore;

        ScanState(){
            this->extentId = 0;
            this->canFetchMore = false;
        }

        [[nodiscard]] Int GetNextKeyIndex()const {
            return (this->lastFetchedRowId.indexId == INVALID_PAGE_INDEX_ID)
                ? 0
                : this->lastFetchedRowId.indexId + 1;
        }

        [[nodiscard]] page_id_t GetPageId(const extent_id_t extentFirstPageId)const {
            return this->lastFetchedRowId.pageId == INVALID_PAGE_ID
                ? extentFirstPageId
                : this->lastFetchedRowId.pageId;
        }

        void Reset(){
            this->extentId = 0;
            this->lastFetchedRowId.indexId = INVALID_PAGE_INDEX_ID;
            this->lastFetchedRowId.pageId = INVALID_PAGE_ID;
            this->canFetchMore = false;
        }
    };

    struct IndexState {
        page_id_t pageId;
        Int lastFetchedKeyIndex;

        bool canFetchMore;

        IndexState() {
            this->pageId = INVALID_PAGE_ID;
            this->lastFetchedKeyIndex = INVALID_PAGE_INDEX_ID;
            this->canFetchMore = false;
        }

        [[nodiscard]] Int GetNextKeyIndex()const {
            return this->lastFetchedKeyIndex == INVALID_PAGE_INDEX_ID
            ? 0
            : this->lastFetchedKeyIndex + 1;
        }

        void Reset(){
            this->pageId = INVALID_PAGE_ID;
            this->lastFetchedKeyIndex = INVALID_PAGE_INDEX_ID;
            this->canFetchMore = false;
        }
    };
}
