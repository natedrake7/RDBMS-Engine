#pragma once
#include "../../Systemic/include/DataTypes/DataTypes.h"
#include "../../Systemic/include/DataStructures/HashSet.h"
#include "../../Systemic/include/RowIdentifier.h"

namespace CoreEngine {
    struct ScanState {
        extent_id_t _extentId;
        StorageTypes::RID _lastRID;

        bool canFetchMore;

        ScanState(){
            this->_extentId = 0;
            this->canFetchMore = false;
        }

        [[nodiscard]] Int GetNextKeyIndex()const{
            return (this->_lastRID._index == INVALID_PAGE_INDEX_ID)
                ? 0
                : this->_lastRID._index + 1;
        }

        [[nodiscard]] page_id_t GetPageId(const page_id_t extentFirstPageId)const {
            return this->_lastRID._pageId == INVALID_PAGE_ID
                ? extentFirstPageId
                : this->_lastRID._pageId;
        }

        void Update(const extent_id_t extentId, const StorageTypes::RID* rowId){
            this->_extentId = extentId;
            this->_lastRID = *rowId;
        }

        void Reset(){
            this->_extentId = 0;
            this->_lastRID._index = INVALID_PAGE_INDEX_ID;
            this->_lastRID._pageId = INVALID_PAGE_ID;
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
