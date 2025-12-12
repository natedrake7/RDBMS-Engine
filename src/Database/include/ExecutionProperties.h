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

      ScanState(){
        this->extentId = 0;
      }

      [[nodiscard]] int GetNextKeyIndex()const {
        return (this->lastFetchedRowId.indexId == Constants::INVALID_PAGE_INDEX_ID)
                ? 0
                : this->lastFetchedRowId.indexId + 1;
      }

      [[nodiscard]] page_id_t GetPageId(const extent_id_t& extentFirstPageId)const {
        return this->lastFetchedRowId.pageId == Constants::INVALID_PAGE_ID
            ? extentFirstPageId
            : this->lastFetchedRowId.pageId;
      }
  };

  struct IndexState {
    page_id_t pageId;
    int32_t lastFetchedKeyIndex;

    IndexState() {
      this->pageId = Constants::INVALID_PAGE_ID;
      this->lastFetchedKeyIndex = Constants::INVALID_PAGE_INDEX_ID;
    }

    [[nodiscard]] int GetNextKeyIndex()const {
      return this->lastFetchedKeyIndex == Constants::INVALID_PAGE_INDEX_ID
        ? 0
        : this->lastFetchedKeyIndex + 1;
    }
  };

  struct Snapshot {
    transaction_id_t transactionId;

    transaction_id_t minimumTransactionId;
    transaction_id_t maximumTransactionId;
    HashSet<transaction_id_t> activeTransactionIds;

    Snapshot() {
      this->transactionId = Constants::FIRST_TRANSACTION_ID;
      this->minimumTransactionId = Constants::FIRST_TRANSACTION_ID;
      this->maximumTransactionId = Constants::FIRST_TRANSACTION_ID;
    }

    [[nodiscard]] bool IsSystemTransaction()const{ return this->transactionId == Constants::FIRST_TRANSACTION_ID; }
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