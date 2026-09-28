#pragma once
#include <type_traits>
#include <meta>

namespace Reflection{
    template<typename E> requires std::is_enum_v<E>
    inline constexpr auto Enumerators = std::define_static_array(std::meta::enumerators_of(^^E));

    template<typename E> requires std::is_enum_v<E>
    inline constexpr Int EnumCount = static_cast<Int>(Enumerators<E>.size());

}
