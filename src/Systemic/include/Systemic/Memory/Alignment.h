#pragma once
#include <cassert>
#include <bit>
#include <cstddef>
#include <cstdint>

namespace Memory{
    inline constexpr std::size_t DEFAULT_ALIGNMENT = alignof(std::max_align_t);

    // Rounds value up to the next multiple of alignment (a power of two)
    [[nodiscard]] constexpr std::size_t AlignUp(const std::size_t value, const std::size_t alignment) noexcept {
        assert(std::has_single_bit(alignment) && "alignment must be a power of two");
        return (value + alignment - 1) & ~(alignment - 1);
    }

    // For asserts only
    [[nodiscard]] inline bool IsAligned(const void* pointer, const std::size_t alignment) noexcept {
        return (reinterpret_cast<std::uintptr_t>(pointer) & (alignment - 1)) == 0;
    }
}
