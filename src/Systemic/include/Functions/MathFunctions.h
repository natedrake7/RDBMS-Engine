#pragma once
#include "../DataTypes/DataTypes.h"

namespace Math{
    template <typename Type>
    static inline constexpr Type Abs(const Type value){
        return value > 0
            ? static_cast<Type>(value)
            : static_cast<Type>(-value);
    }

    template<DataTypes::IsInteger T>
    static inline constexpr T Max(const T lhs, const T rhs){
        return lhs > rhs ? lhs : rhs;
    }

    template<DataTypes::IsInteger T>
    static inline constexpr T Min(const T lhs, const T rhs){
        return lhs < rhs ? lhs : rhs;
    }
}
