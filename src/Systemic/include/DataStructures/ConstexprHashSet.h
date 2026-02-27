// ConstexprHashSet.h
#pragma once
#include <array>
#include <string_view>
#include "Hashers/BaseHasher.h"


template<typename T, size_t N, typename Hasher = ConstexprHash<T>>
class ConstexprHashSet {
    static constexpr size_t Capacity = N * 2;

    std::array<T, Capacity>    buckets{};
    std::array<bool, Capacity> occupied{};

    constexpr void Insert(const T& key) {
        const auto idx = Hasher::Hash(key) % Capacity;
        for (size_t i = 0; i < Capacity; ++i) {
            const auto slot = (idx + i) % Capacity;
            if (!this->occupied[slot]) {
                this->buckets[slot]  = key;
                this->occupied[slot] = true;
                return;
            }
        }
    }

public:
    constexpr ConstexprHashSet(std::initializer_list<T> items) {
        for (const auto& item : items)
            this->Insert(item);
    }

    [[nodiscard]] constexpr bool Contains(const T& key) const noexcept {
        const size_t idx = Hasher::Hash(key) % Capacity;
        for (size_t i = 0; i < Capacity; ++i) {
            const size_t slot = (idx + i) % Capacity;
            if (!this->occupied[slot]) return false;
            if (this->buckets[slot] == key) return true;
        }
        return false;
    }

    using iterator = T*;
    using const_iterator = const T*;

    constexpr iterator begin() { return this->buckets.begin(); }
    constexpr iterator end() { return this->buckets.end(); }

    constexpr const_iterator begin() const { return this->buckets.begin(); }
    constexpr const_iterator end() const { return this->buckets.end(); }
};
