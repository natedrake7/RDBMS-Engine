#pragma once
#include <Systemic/DataTypes/DataTypes.h>
#include <Systemic/DataStructures/HashSet.h>
#include <Systemic/RowIdentifier.h>

namespace CoreEngine {
    struct ScanState {
        extent_id_t _extentId;
        page_offset_t _nextOffset;

        bool _canFetchMore;

        ScanState()
            : _extentId(INVALID_EXTENT_ID), _nextOffset(0), _canFetchMore(false){}

        [[nodiscard]] Int GetNextKeyIndex()const{
            return this->_nextOffset;
        }

        [[nodiscard]] page_id_t GetPageId(const page_id_t extentFirstPageId)const {
            return StorageLayout::ExtentFirstPageId(this->_extentId) + this->_nextOffset;
        }

        void Update(const extent_id_t extentId, const page_offset_t nextOffset){
            this->_extentId = extentId;
            this->_nextOffset = nextOffset;
            this->_canFetchMore = true;
        }

        void Reset(){
            this->_extentId = INVALID_EXTENT_ID;
            this->_nextOffset = 0;
            this->_canFetchMore = false;
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
