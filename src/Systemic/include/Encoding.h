#pragma once
#include "DataTypes/DataTypes.h"

namespace Encoding{
    template<DataTypes::IsInteger T>
    size_t EncodeInteger(object_t* buffer, T value){
        using U = std::make_unsigned_t<T>;

        auto u = static_cast<U>(value);
        if constexpr (std::is_signed_v<T>)
            u ^= (U(1) << (sizeof(T) * 8 - 1));
        for (size_t i = 0; i < sizeof(T); ++i)
            buffer[i] = static_cast<object_t>(u >> ((sizeof(T) - 1 - i) * 8));
        return sizeof(T);
    }

    template<DataTypes::IsInteger T>
    [[nodiscard]] T DecodeInteger(const object_t* buffer){
        using U = std::make_unsigned_t<T>;
        U u = 0;
        for (size_t i = 0; i < sizeof(T); ++i)
            u = (u << 8) | buffer[i];
        if constexpr (std::is_signed_v<T>)
            u ^= (U(1) << (sizeof(T) * 8 - 1));
        return static_cast<T>(u);
    }
}
