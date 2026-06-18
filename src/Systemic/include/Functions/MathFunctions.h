#pragma once

namespace Math{
    template <typename Type>
    static constexpr Type Abs(const Type value){
        return value > 0
            ? static_cast<Type>(value)
            : static_cast<Type>(-value);
    }

    template<DataTypes::IsInteger T>
    static constexpr T Max(const T lhs, const T rhs){
        return lhs > rhs ? lhs : rhs;
    }

    template<DataTypes::IsInteger T>
    static constexpr T Min(const T lhs, const T rhs){
        return lhs < rhs ? lhs : rhs;
    }
}
