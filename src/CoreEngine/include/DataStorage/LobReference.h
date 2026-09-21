#pragma once
#include "../DatabaseConstants.h"
#include "../../../Systemic/include/Constants.h"
#include "../../../Systemic/include/DataTypes/DataTypes.h"

namespace CoreEngine::StorageTypes{
    struct LobReference{
        page_id_t _rootPageId;
        UnsignedInt _totalLength;

        [[nodiscard]] bool IsValid() const{
            return this->_rootPageId != INVALID_PAGE_ID;
        }
    };

    static_assert(sizeof(LobReference) == Constants::LOB_REFERENCE_SIZE);
}
