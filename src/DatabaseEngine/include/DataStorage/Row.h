#pragma once
#include "../../Systemic/include/Constants.h"
#include "../DatabaseConstants.h"
#include "../../../Systemic/include/DataStructures/BitMap.h"

namespace DatabaseEngine {
    struct Snapshot;
}

namespace Pages {
    struct OverflowPointer;
    struct OverflowRow;
}

namespace ByteMaps{
    class BitMap;
}

namespace DatabaseEngine::StorageTypes
{
    class Column;
    class Table;
    class Block;

    struct RowVersionPointer {
        page_id_t pageId;
        page_offset_t offset;

        RowVersionPointer() {
            this->pageId = INVALID_PAGE_ID;
            this->offset = 0;
        }
    };

    struct RowVersioningHeader {
        transaction_id_t createdTransactionId;
        transaction_id_t deletedTransactionId;

        RowVersionPointer olderVersionPointer;

        RowVersioningHeader() {
            this->createdTransactionId = INVALID_TRANSACTION_ID;
            this->deletedTransactionId = 0;
        }

        [[nodiscard]] bool IsVisibleForTransaction(const Snapshot& snapshot)const;
        [[nodiscard]] bool IsDeletedForTransaction(const Snapshot& snapshot)const;

        [[nodiscard]] bool HasOlderVersion()const { return this->olderVersionPointer.pageId != INVALID_PAGE_ID; }
    };

    struct RowHeader{
        RowVersioningHeader version;

        ByteMaps::BitMap nullBitMap;
        ByteMaps::BitMap largeObjectBitMap;
        ByteMaps::BitMap overflowBitMap;

        explicit RowHeader();
        explicit RowHeader(Int bitMapsSize);
        RowHeader& operator=(const RowHeader& otherHeader);
        RowHeader(const RowHeader& otherHeader);
        RowHeader(RowHeader&& otherHeader) noexcept;
        RowHeader& operator=(RowHeader&& otherHeader) noexcept;

        [[nodiscard]] bool Size()const;
    };
}