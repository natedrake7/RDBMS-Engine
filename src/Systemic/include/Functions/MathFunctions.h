#pragma once
#include "../DataTypes/DataTypes.h"

namespace Functions::Math{
    static constexpr BigInt Abs(const BigInt value){
        return value > 0 ? value : -value;
    }
}
