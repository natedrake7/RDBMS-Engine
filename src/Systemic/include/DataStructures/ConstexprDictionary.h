#pragma once
#include <array>

#include "CaseInsensitiveHashers.h"
#include "Hashers.h"
#include "../DataTypes/String.h"

template<typename Key, typename Value>
struct Pair{
    Key key {};
    Value value {};

    constexpr Pair() = default;
    constexpr Pair(const Key& key, const Value& value)
        : key(key), value(value) {}
};

template<typename T>
struct IsStringType : std::false_type {};

template<> struct IsStringType<std::string> : std::true_type {};
template<> struct IsStringType<std::string_view> : std::true_type {};
template<> struct IsStringType<DataTypes::String> : std::true_type {};
template<> struct IsStringType<DataTypes::StringView> : std::true_type {};

template<typename T>
inline constexpr bool IsStringType_v = IsStringType<T>::value;

template<typename Key, typename Value, size_t N, typename Hasher = CaseInsensitiveHash<Key>>
class ConstexprDictionary{
    static constexpr size_t Capacity = N * 2;

    Pair<Key, Value> buckets[Capacity] {};
    bool occupied[Capacity] {};
    // std::array<Pair<Key, Value>, Capacity> buckets{};
    // std::array<bool, Capacity> occupied{};

    constexpr void Insert(const Pair<Key, Value>& pair) {
        const auto idx = Hasher::Hash(pair.key) % Capacity;
        for (size_t i = 0; i < Capacity; ++i) {
            const auto slot = (idx + i) % Capacity;
            if (!this->occupied[slot]) {
                this->buckets[slot]  = pair;
                this->occupied[slot] = true;
                return;
            }
        }
    }

    [[nodiscard]] static constexpr bool KeyEquals(const Key& lhs, const Key& rhs) noexcept {
        if constexpr (std::is_same_v<Hasher, CaseInsensitiveHash<Key>> && IsStringType_v<Key>)
            return DataTypes::String::Compare<StringComparisonType::EqualsIgnoreCase>(lhs, rhs);
        return lhs == rhs;
    }

    public:
        constexpr ConstexprDictionary(std::initializer_list<Pair<Key, Value>> items) {
            for (const auto& item : items)
                this->Insert(item);
        }

        template<typename... Pairs>
        requires (std::is_same_v<std::remove_cvref_t<Pairs>, Pair<Key, Value>> && ...)
        explicit constexpr ConstexprDictionary(Pairs&&... pairs) {
                (this->Insert(std::forward<Pairs>(pairs)), ...);
        }

        [[nodiscard]] constexpr bool Contains(const Key& key) const noexcept {
            const size_t idx = Hasher::Hash(key) % Capacity;
            for (size_t i = 0; i < Capacity; ++i) {
                const size_t slot = (idx + i) % Capacity;
                if (!this->occupied[slot]) return false;
                if (ConstexprDictionary::KeyEquals(this->buckets[slot].key, key)) return true;
            }
            return false;
        }

        [[nodiscard]] constexpr const Value& operator[](const Key* key) const noexcept {
            const size_t idx = Hasher::Hash(*key) % Capacity;
            for (size_t i = 0; i < Capacity; ++i) {
                const size_t slot = (idx + i) % Capacity;
                const auto& pair = this->buckets[slot];

                if (!this->occupied[slot])
                    return pair.value;

                if (ConstexprDictionary::KeyEquals(pair.key, *key))
                    return pair.value;
            }

            return this->buckets[idx].value;
        }

        [[nodiscard]] constexpr const Value& operator[](const Key key) const noexcept {
            const size_t idx = Hasher::Hash(key) % Capacity;
            for (size_t i = 0; i < Capacity; ++i) {
                const size_t slot = (idx + i) % Capacity;
                const auto& pair = this->buckets[slot];

                if (!this->occupied[slot])
                    return pair.value;

                if (ConstexprDictionary::KeyEquals(pair.key, key))
                    return pair.value;
            }

            return this->buckets[idx].value;
        }

        [[nodiscard]] constexpr const Value& Get(const Key* key) const noexcept {
            return this->operator[](key);
        }

        [[nodiscard]] constexpr Value Get(const Key key) const noexcept {
            return this->operator[](key);
        }

        [[nodiscard]] constexpr bool TryGetValue(const Key& key, Value& value) const noexcept{
            const size_t idx = Hasher::Hash(key) % Capacity;
            for (size_t i = 0; i < Capacity; ++i) {
                const size_t slot = (idx + i) % Capacity;
                if (!this->occupied[slot]) return false;

                const auto& pair = this->buckets[slot];
                if (ConstexprDictionary::KeyEquals(pair.key, key)){
                    value = pair.value;
                    return true;
                }
            }
            return false;
        }
};

template<typename Key, typename Value, typename... Rest>
ConstexprDictionary(Pair<Key, Value>, Rest...)
    -> ConstexprDictionary<Key, Value, 1 + sizeof...(Rest)>;

template<typename Hasher, typename Key, typename Value, typename... Rest>
constexpr auto MakeDictionary(Pair<Key, Value> first, Rest... rest) {
    return ConstexprDictionary<Key, Value, 1 + sizeof...(Rest), Hasher>{ first, rest... };
}
