#pragma once
#include "../../Systemic/include/DataTypes/DataTypes.h"
#include "../../Systemic/include/DataStructures/Dictionary.h"
#include "../../Systemic/include/DataStructures/HashSet.h"
#include "../../Systemic/include/Headers.h"
#include "Constants.h"

namespace DatabaseEngine {
  struct ScanState {
      extent_id_t extentId;
      Headers::RowIdentifier lastFetchedRowId;

      bool canFetchMore;

      ScanState(){
        this->extentId = 0;
        this->canFetchMore = false;
      }

      [[nodiscard]] int GetNextKeyIndex()const {
        return (this->lastFetchedRowId.indexId == INVALID_PAGE_INDEX_ID)
                ? 0
                : this->lastFetchedRowId.indexId + 1;
      }

      [[nodiscard]] page_id_t GetPageId(const extent_id_t& extentFirstPageId)const {
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
    int32_t lastFetchedKeyIndex;

    bool canFetchMore;

    IndexState() {
      this->pageId = INVALID_PAGE_ID;
      this->lastFetchedKeyIndex = INVALID_PAGE_INDEX_ID;
      this->canFetchMore = false;
    }

    [[nodiscard]] int GetNextKeyIndex()const {
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

  struct Snapshot {
    transaction_id_t transactionId;

    transaction_id_t minimumTransactionId;
    transaction_id_t maximumTransactionId;
    HashSet<transaction_id_t> activeTransactionIds;

    Snapshot() {
      this->transactionId = FIRST_TRANSACTION_ID;
      this->minimumTransactionId = FIRST_TRANSACTION_ID;
      this->maximumTransactionId = FIRST_TRANSACTION_ID;
    }

    [[nodiscard]] bool IsSystemTransaction()const{ return this->transactionId == FIRST_TRANSACTION_ID; }
  };

  struct ExecutionProperties {
    Snapshot snapshot;
    int batchSize;

    const Dictionary<std::string, Variable>* variables;

    ExecutionProperties(const Snapshot &snapshot, const int &batchSize, const Dictionary<std::string, Variable>& variables) {
      this->snapshot = snapshot;
      this->batchSize = batchSize;
      this->variables = &variables;
    }

    ExecutionProperties() {
      this->batchSize = 0;
      this->variables = nullptr;
    }
  };
}