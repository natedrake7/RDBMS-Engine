#include "../../include/DataStorage/Row.h"
#include "Contexts/ExecutionContext.h"

namespace CoreEngine::StorageTypes {
    RowHeader::RowHeader()
        :   _createdTransactionId(INVALID_TRANSACTION_ID), _deletedTransactionId(INVALID_TRANSACTION_ID),
            _oldVersionPageId(INVALID_PAGE_ID), _oldVersionOffset(0){}

    bool RowHeader::IsVisibleForTransaction(const Snapshot& snapshot) const{
        return this->_deletedTransactionId != FIRST_TRANSACTION_ID
                && (
                    this->_deletedTransactionId < snapshot.maximumTransactionId
                    || this->_deletedTransactionId == INVALID_TRANSACTION_ID
                )
                && !snapshot.activeTransactionIds.Contains(this->_deletedTransactionId)
                && this->_deletedTransactionId != snapshot.transactionId;
    }

    bool RowHeader::IsDeletedForTransaction(const Snapshot& snapshot) const{
        if (snapshot.IsSystemTransaction())
            return true;

        if (this->_createdTransactionId < snapshot.minimumTransactionId)
            return !this->IsDeletedForTransaction(snapshot);

        if (this->_createdTransactionId == snapshot.transactionId
            || this->_createdTransactionId >= snapshot.maximumTransactionId
            || snapshot.activeTransactionIds.Contains(this->_createdTransactionId))
            return false;

        return !this->IsDeletedForTransaction(snapshot);
    }

    RID::RID(const page_id_t pageId, const Int index)
        : _pageId(pageId), _index(index){}
}
