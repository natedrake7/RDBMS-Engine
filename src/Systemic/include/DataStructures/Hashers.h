#pragma once
#include <cstddef>
#include "../DataTypes/DataTypes.h"
#include "../DataTypes/StringView.h"

template<typename T>
struct ConstexprHash {
    [[nodiscard]] static constexpr size_t Hash(const T& key) noexcept {
        size_t h = 14695981039346656037ULL;

        if constexpr (requires { key.size(); }) {
            for (size_t i = 0; i < key.size(); ++i) {
                h ^= key[i];
                h *= 1099511628211ULL;
            }

            return h;
        }

        if constexpr (requires { key.Size(); }) {
            for (size_t i = 0; i < key.Size(); ++i) {
                h ^= key[i];
                h *= 1099511628211ULL;
            }

            return h;
        }

        const auto bytes = std::bit_cast<std::array<UnsignedTinyInt, sizeof(T)>>(key);
        for (size_t i = 0; i < sizeof(T); ++i) {
            h ^= bytes[i];
            h *= 1099511628211ULL;
        }
        return h;
    }
};

template<>
struct ConstexprHash<const char*> {
    [[nodiscard]] static constexpr size_t Hash(const char* key) noexcept {
        size_t h = 14695981039346656037ULL;
        while (*key) {
            h ^= static_cast<unsigned char>(*key++);
            h *= 1099511628211ULL;
        }
        return h;
    }
};

template<>
struct ConstexprHash<DataTypes::StringView>{
    [[nodiscard]] static constexpr size_t Hash(const DataTypes::StringView& key) noexcept {
        size_t h = 14695981039346656037ULL;
        for (size_t i = 0; i < key.Size(); ++i){
            h ^= static_cast<unsigned char>(key[i]);
            h *= 1099511628211ULL;
        }
        return h;
    }
};

template<>
struct ConstexprHash<DataType> {
    [[nodiscard]] static constexpr size_t Hash(const DataType& key) noexcept {
        return static_cast<size_t>(key);
    }
};

template<>
struct ConstexprHash<UnsignedTinyInt> {
    [[nodiscard]] static constexpr size_t Hash(const DataType& key) noexcept {
        return static_cast<size_t>(key);
    }
};
