#include "../../include/DataStorage/Row.h"
#include "Contexts/ExecutionContext.h"

namespace CoreEngine::StorageTypes {
    RID::RID()
        :   _pageId(INVALID_PAGE_ID), _index(INVALID_INDEX_ID),
            _flags(0){}

    RID::RID(const page_id_t pageId, const Int index)
        :   _pageId(pageId), _index(index),
            _flags(0){}

    RID::RID(const page_id_t pageId, const Int index, const Source source)
        : _pageId(pageId), _index(index), _flags(0){
        this->SetSource(source);
    }

    void RID::SetSource(const Source source){
        PackedWord<UnsignedSmallInt>::SetBits<SOURCE_SHIFT, SOURCE_MASK>(&this->_flags, source);
    }

    RID::Source RID::GetSource() const{
        return PackedWord<UnsignedSmallInt>::ExtractBits<Source, SOURCE_SHIFT, SOURCE_MASK>(this->_flags);
    }

    RowHeader::RowHeader()
        :   _createdTransactionId(INVALID_TRANSACTION_ID), _deletedTransactionId(INVALID_TRANSACTION_ID),
            _versionRID(){}

    bool RowHeader::IsVisibleForTransaction(const Snapshot& snapshot) const{
        if (!Snapshot::IsWriteVisible(this->_createdTransactionId, snapshot))
            return false;

        return (
            this->_deletedTransactionId == INVALID_TRANSACTION_ID
            || !Snapshot::IsWriteVisible(this->_deletedTransactionId, snapshot)
        );
    }
}
