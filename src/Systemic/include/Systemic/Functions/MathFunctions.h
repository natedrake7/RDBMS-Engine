#pragma once
#include <Systemic/DataTypes/DataTypes.h>

namespace Math{
    template <typename Type>
    inline constexpr Type Abs(const Type value){
        return value > 0
            ? static_cast<Type>(value)
            : static_cast<Type>(-value);
    }

    template<DataTypes::IsInteger T>
    inline constexpr T Max(const T lhs, const T rhs){
        return lhs > rhs ? lhs : rhs;
    }

    template<DataTypes::IsInteger T>
    inline constexpr T Min(const T lhs, const T rhs){
        return lhs < rhs ? lhs : rhs;
    }

    template<DataTypes::IsInteger T>
    inline constexpr T Ceil(const float value){
        const auto truncated = static_cast<T>(value);
        return (value > static_cast<float>(truncated)) ? truncated + 1 : truncated;
    }

    template<DataTypes::IsInteger T>
    inline constexpr T MinLimit(){
        return std::numeric_limits<T>::min();
    }

    template<DataTypes::IsInteger T>
    inline constexpr T MaxLimit(){
        return std::numeric_limits<T>::max();
    }
}
