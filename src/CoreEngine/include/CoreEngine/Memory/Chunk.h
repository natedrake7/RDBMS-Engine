#pragma once
#include <cstdint>
#include <Systemic/DataTypes/DataTypes.h>
#include <Systemic/Memory/Alignment.h>
#include <CoreEngine/DatabaseConstants.h>

namespace CoreEngine::Memory{
    struct Chunk{
        Chunk* _next;
        UnsignedInt _offset;
        UnsignedInt _size;
        alignas(::Memory::DEFAULT_ALIGNMENT) object_t _data[];

        static constexpr UnsignedInt DEFAULT_SIZE = 10 * Constants::KB; //10KB
        static constexpr UnsignedInt MAX_SIZE = Constants::MB; //1MB

        // Offset at which an allocation with this alignment would start. Aligns the address,
        // not just the offset, so alignments above DEFAULT_ALIGNMENT work too.
        [[nodiscard]] UnsignedInt AlignedOffset(const std::size_t alignment) const{
            const auto base = reinterpret_cast<std::uintptr_t>(this->_data);
            return static_cast<UnsignedInt>(::Memory::AlignUp(base + this->_offset, alignment) - base);
        }

        // Worst-case padding before the first allocation of a fresh chunk
        [[nodiscard]] static constexpr UnsignedInt MaxPadding(const std::size_t alignment){
            return alignment > ::Memory::DEFAULT_ALIGNMENT
                ? static_cast<UnsignedInt>(alignment - ::Memory::DEFAULT_ALIGNMENT)
                : 0;
        }
    };
}
