#pragma once
#include "../Constants.h"
#include "DataTypes.h"

namespace DataTypes{
    struct LobReference{
        page_id_t _rootPageId;
        UnsignedInt _totalLength;

        [[nodiscard]] bool IsValid() const{
            return this->_rootPageId != INVALID_PAGE_ID;
        }

        LobReference() = default;
        LobReference(const page_id_t rootPageId, const UnsignedInt totalLength)
            : _rootPageId(rootPageId), _totalLength(totalLength) {}
    };

    static_assert(sizeof(LobReference) == 8);
}
