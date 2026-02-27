#pragma once
#include "BaseHasher.h"
enum class DataType : uint8_t;

template<>
struct ConstexprHash<DataType> {
    [[nodiscard]] static constexpr size_t Hash(const DataType& key) noexcept {
        return static_cast<size_t>(key);
    }
};
