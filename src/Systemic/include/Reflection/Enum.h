#pragma once
#include <type_traits>
#include <meta>
#include <array>
#include <string_view>

namespace Reflection{
    template<typename...>
    inline constexpr auto AlwaysFalse = false;

    template<typename E> requires std::is_enum_v<E>
    inline constexpr auto Enumerators = std::define_static_array(std::meta::enumerators_of(^^E));

    template<typename E> requires std::is_enum_v<E>
    inline constexpr auto EnumCount = static_cast<int>(Enumerators<E>.size());

    template<typename E> requires std::is_enum_v<E>
    inline constexpr bool IsContiguous(){
        std::underlying_type_t<E> expected = 0;
        template for (constexpr auto e: Enumerators<E>){
            if (static_cast<std::underlying_type_t<E>>([:e:]) != expected++)
                return false;
        }
        return true;
    }

    template<typename E> requires std::is_enum_v<E> && (IsContiguous<E>())
    inline constexpr auto EnumIdentifiers (){
        std::array<std::string_view, EnumCount<E>> names{};
        std::size_t i = 0;
        template for (constexpr auto e : Enumerators<E>)
            names[i++] = std::meta::identifier_of(e);
        return names;
    }

    template<typename E> requires std::is_enum_v<E>
    constexpr std::string_view EnumIdentifier(const E value){
        if constexpr (IsContiguous<E>()){
            static constexpr auto ENUM_NAMES = EnumIdentifiers<E>();
            return ENUM_NAMES[static_cast<std::size_t>(value)];
        }
        else{
            template for (constexpr auto e : Enumerators<E>){
                if (value == [:e:])
                    return std::meta::identifier_of(e);
            }
            return {};
        }
    }

    template<typename E>
    [[nodiscard]] inline constexpr bool HasAnnotation(const std::meta::info item){
        return !std::meta::annotations_of_with_type(item, ^^E).empty();
    }
}
