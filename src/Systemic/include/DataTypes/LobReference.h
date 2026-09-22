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
    };

    static_assert(sizeof(LobReference) == 8);
}
