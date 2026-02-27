#pragma once
#include "BaseHasher.h"
#include "../../DataTypes/StringView.h"

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