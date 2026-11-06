#pragma once
#include <Systemic/Constants.h>
#include <Systemic/DataTypes/DataTypes.h>

namespace Expressions{
    struct BoundReference {
        UInt16_t _childIndex;
        column_index_t _position;

        BoundReference()
            : _childIndex(0), _position(INVALID_COLUMN_INDEX) {}
    };
}
