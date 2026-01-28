#include "../../include/DataStorage/Row.h"
#include "../../../Systemic/include/DataStructures/BitMap.h"
#include "ExecutionProperties.h"

namespace DatabaseEngine::StorageTypes {
    bool RowVersioningHeader::IsVisibleForTransaction(const Snapshot& snapshot) const{
        if (snapshot.IsSystemTransaction())
            return true;

        if (this->createdTransactionId < snapshot.minimumTransactionId)
            return !this->IsDeletedForTransaction(snapshot);

        if (this->createdTransactionId == snapshot.transactionId
            || this->createdTransactionId >= snapshot.maximumTransactionId
            || snapshot.activeTransactionIds.Contains(this->createdTransactionId))
            return false;

        return !this->IsDeletedForTransaction(snapshot);
    }

    bool RowVersioningHeader::IsDeletedForTransaction(const Snapshot& snapshot) const{
        return this->deletedTransactionId != FIRST_TRANSACTION_ID
            && this->deletedTransactionId < snapshot.maximumTransactionId
            && !snapshot.activeTransactionIds.Contains(this->deletedTransactionId)
            && this->deletedTransactionId != snapshot.transactionId;
    }

    RowHeader::RowHeader(const Int bitMapsSize){
        this->nullBitMap = ByteMaps::BitMap(bitMapsSize, false);
        this->largeObjectBitMap = ByteMaps::BitMap(bitMapsSize, false);
        this->overflowBitMap = ByteMaps::BitMap(bitMapsSize, false);
    }

    RowHeader & RowHeader::operator=(const RowHeader &otherHeader){
        if (this == &otherHeader)
            return *this;

        this->nullBitMap = ByteMaps::BitMap(otherHeader.nullBitMap);
        this->largeObjectBitMap = ByteMaps::BitMap(otherHeader.largeObjectBitMap);
        this->overflowBitMap = ByteMaps::BitMap(otherHeader.overflowBitMap);

        this->version = otherHeader.version;

        return *this;
    }

    RowHeader::RowHeader(const RowHeader& otherHeader){
        this->version = otherHeader.version;

        this->nullBitMap = otherHeader.nullBitMap;
        this->largeObjectBitMap = otherHeader.largeObjectBitMap;
        this->overflowBitMap = otherHeader.overflowBitMap;
    }

    RowHeader::RowHeader(RowHeader&& otherHeader) noexcept{
        this->version = otherHeader.version;
        this->nullBitMap = std::move(otherHeader.nullBitMap);
        this->largeObjectBitMap = std::move(otherHeader.largeObjectBitMap);
        this->overflowBitMap = std::move(otherHeader.overflowBitMap);
    }

    RowHeader& RowHeader::operator=(RowHeader&& otherHeader) noexcept{
        if (this == &otherHeader)
            return *this;

        this->version = otherHeader.version;
        this->nullBitMap = std::move(otherHeader.nullBitMap);
        this->largeObjectBitMap = std::move(otherHeader.largeObjectBitMap);
        this->overflowBitMap = std::move(otherHeader.overflowBitMap);

        return *this;
    }

    bool RowHeader::Size() const{
        return this->largeObjectBitMap.GetSizeInBytes()
            + this->overflowBitMap.GetSizeInBytes()
            + this->nullBitMap.GetSizeInBytes()
            + Constants::ROW_VERSION_HEADER_SIZE;
    }
}
