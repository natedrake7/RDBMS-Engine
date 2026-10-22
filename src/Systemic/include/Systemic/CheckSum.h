#pragma once

#include <nmmintrin.h>

#include "DataTypes/DataTypes.h"

namespace CheckSum{
    template<UnsignedInt size>
    requires (size % 8 == 0)
    [[nodiscard]] inline UnsignedInt Crc32(const object_t* data){
        UnsignedBigInt crc = 0xFFFFFFFFu;
    }

}
