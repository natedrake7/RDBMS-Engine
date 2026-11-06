#pragma once
#include <Systemic/Constants.h>
#include <Systemic/DataTypes/DataTypes.h>

namespace DataTypes{
    struct LobReference{
        page_id_t _rootPageId;
        UInt32_t _totalLength;

        [[nodiscard]] bool IsValid() const{
            return this->_rootPageId != INVALID_PAGE_ID;
        }

        LobReference() = default;
        LobReference(const page_id_t rootPageId, const UInt32_t totalLength)
            : _rootPageId(rootPageId), _totalLength(totalLength) {}
    };

    static_assert(sizeof(LobReference) == 8);
}
