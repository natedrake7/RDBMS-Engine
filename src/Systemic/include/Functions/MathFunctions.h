#pragma once
#include "../DataTypes/DataTypes.h"

namespace Functions::Math{
    template <typename Type>
    static constexpr Type Abs(const BigInt value){
        return value > 0
            ? static_cast<Type>(value)
            : static_cast<Type>(-value);
    }
}
