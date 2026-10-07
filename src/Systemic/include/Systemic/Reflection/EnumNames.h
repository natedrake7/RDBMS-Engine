#pragma once
#include <optional>

#include <Systemic/Reflection/Enum.h>
#include <Systemic/DataTypes/StringView.h>

namespace Reflection{
    template<typename E> requires std::is_enum_v<E>
    constexpr DataTypes::StringView EnumName(const E value){
        const auto identifier = Reflection::EnumIdentifier(value);
        return DataTypes::StringView::ViewOf(identifier);
    }

    // Runtime input (SQL text, config, wire) -> not found is a normal outcome, not a bug.
    template<typename E> requires std::is_enum_v<E>
    constexpr std::optional<E> EnumFromName(const DataTypes::StringView& name){
        template for (constexpr auto e : Enumerators<E>){
            constexpr auto identifier = std::meta::identifier_of(e);
            if (DataTypes::StringView::EqualsIgnoreCase(
                    name,
                    DataTypes::StringView::ViewOf(identifier)
            )) return [:e:];
        }
        return std::nullopt;
    }

    // Compile-time lookup of a literal: an unknown name is a compile error, not a runtime one.
    template<typename E> requires std::is_enum_v<E>
    consteval E EnumFromLiteral(const DataTypes::StringView& name){
        if (const auto value = Reflection::EnumFromName<E>(name))
            return *value;
        throw "Reflection::EnumFromLiteral: no enumerator with this name";
    }
}