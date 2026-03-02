#pragma once
#include <array>
#include "Hashers.h"

template<typename Key, typename Value>
struct Pair{
    Key key {};
    Value value {};

    constexpr Pair() = default;
    constexpr Pair(const Key& key, const Value& value)
        : key(key), value(value) {}
};

template<typename Key, typename Value, size_t N, typename Hasher = ConstexprHash<Key>>
class ConstexprDictionary{
    static constexpr size_t Capacity = N * 2;

    std::array<Pair<Key, Value>, Capacity> buckets{};
    std::array<bool, Capacity> occupied{};

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

    public:
        constexpr ConstexprDictionary(std::initializer_list<Pair<Key, Value>> items) {
            for (const auto& item : items)
                this->Insert(item);
        }

        [[nodiscard]] constexpr bool Contains(const Key& key) const noexcept {
            const size_t idx = Hasher::Hash(key) % Capacity;
            for (size_t i = 0; i < Capacity; ++i) {
                const size_t slot = (idx + i) % Capacity;
                if (!this->occupied[slot]) return false;
                if (this->buckets[slot].key == key) return true;
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

                if (pair.key == *key)
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

                if (pair.key == key)
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
                if (pair.key == key){
                    value = pair.value;
                    return true;
                }
            }
            return false;
        }
};
