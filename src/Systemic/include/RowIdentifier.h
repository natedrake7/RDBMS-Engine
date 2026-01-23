#pragma once
#include "Constants.h"
#include "DataTypes/DataTypes.h"

namespace DataTypes{
    struct RowIdentifier {
        page_id_t pageId;
        Int indexId;

        RowIdentifier() {
            this->pageId = INVALID_PAGE_ID;
            this->indexId = INVALID_PAGE_INDEX_ID;
        }

        RowIdentifier(const Int pageId, const Int indexId) {
            this->pageId = pageId;
            this->indexId = indexId;
        }

        RowIdentifier(const RowIdentifier& rowId) {
            this->pageId = rowId.pageId;
            this->indexId = rowId.indexId;
        }

        ~RowIdentifier() = default;

        [[nodiscard]] bool IsInvalid() const{
            return this->pageId == INVALID_PAGE_ID && this->indexId == INVALID_PAGE_INDEX_ID;
        }
    };

    inline std::ostream& operator<<(std::ostream& os, const RowIdentifier& rowId) {
        os << "(" << rowId.pageId << "," << rowId.indexId << ")";
        return os;
    }

    inline bool operator==(const RowIdentifier& lhs, const RowIdentifier& rhs) {
        return lhs.pageId == rhs.pageId && lhs.indexId == rhs.indexId;
    }

    inline bool operator!=(const RowIdentifier& lhs, const RowIdentifier& rhs) {
        return !(lhs == rhs);
    }

    inline bool operator>(const RowIdentifier& lhs, const RowIdentifier& rhs) {
        if (lhs.pageId > rhs.pageId)
            return true;

        return lhs.pageId == rhs.pageId && lhs.indexId > rhs.indexId;
    }

    inline bool operator<(const RowIdentifier& lhs, const RowIdentifier& rhs) {
        if (lhs.pageId < rhs.pageId)
            return true;

        return lhs.pageId == rhs.pageId && lhs.indexId < rhs.indexId;
    }

    inline bool operator>=(const RowIdentifier& lhs, const RowIdentifier& rhs) {
        return !(lhs < rhs);
    }

    inline bool operator<=(const RowIdentifier& lhs, const RowIdentifier& rhs) {
        return !(rhs > lhs);
    }
}
