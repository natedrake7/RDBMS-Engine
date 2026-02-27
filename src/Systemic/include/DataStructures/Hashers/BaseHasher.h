#pragma once

template<typename T>
struct ConstexprHash {
    [[nodiscard]] static constexpr size_t Hash(const T& key) noexcept {
        size_t h = 14695981039346656037ULL;
        for (const unsigned char c : key) {
            h ^= c;
            h *= 1099511628211ULL;
        }
        return h;
    }
};