#pragma once
#include <cctype>
#include <array>

#include "../DataTypes/DataTypes.h"
#include "../DataTypes/String.h"

template<typename T>
struct CaseInsensitiveHash {
    [[nodiscard]] static constexpr size_t Hash(const T& key) noexcept {
        size_t h = 14695981039346656037ULL;

        if constexpr (requires { key.size(); }) {
            for (size_t i = 0; i < key.size(); ++i) {
                h ^= static_cast<unsigned char>(DataTypes::String::ToLower(static_cast<unsigned char>(key[i])));
                h *= 1099511628211ULL;
            }
            return h;
        }

        if constexpr (requires { key.Size(); }) {
            for (size_t i = 0; i < key.Size(); ++i) {
                h ^= static_cast<unsigned char>(DataTypes::String::ToLower(static_cast<unsigned char>(key[i])));
                h *= 1099511628211ULL;
            }
            return h;
        }

        // Fallback for non-string types — same as ConstexprHash
        const auto bytes = std::bit_cast<std::array<UnsignedTinyInt, sizeof(T)>>(key);
        for (size_t i = 0; i < sizeof(T); ++i) {
            h ^= bytes[i];
            h *= 1099511628211ULL;
        }
        return h;
    }
};

template<>
struct CaseInsensitiveHash<const char*> {
    [[nodiscard]] static constexpr size_t Hash(const char* key) noexcept {
        size_t h = 14695981039346656037ULL;
        while (*key) {
            h ^= static_cast<unsigned char>(DataTypes::String::ToLower(static_cast<unsigned char>(*key++)));
            h *= 1099511628211ULL;
        }
        return h;
    }
};

template<>
struct CaseInsensitiveHash<DataTypes::StringView> {
    [[nodiscard]] static constexpr size_t Hash(const DataTypes::StringView& key) noexcept {
        size_t h = 14695981039346656037ULL;
        for (size_t i = 0; i < key.Size(); ++i) {
            h ^= static_cast<unsigned char>(DataTypes::String::ToLower(static_cast<unsigned char>(key[i])));
            h *= 1099511628211ULL;
        }
        return h;
    }
};
