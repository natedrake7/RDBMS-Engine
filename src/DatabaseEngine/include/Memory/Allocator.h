#pragma once
#include "../../../Systemic/include/DataTypes/DataTypes.h"
#include "../../../Systemic/include/Memory/IAllocator.h"

namespace DatabaseEngine::Memory{
    struct Chunk;

    class Allocator final : public virtual ::Memory::IAllocator{
        mutable Chunk* _head;
        mutable Chunk* _tail;

        [[nodiscard]] UnsignedInt NewChunkCapacity(UnsignedInt size)const;
        void AllocateNewChunk(UnsignedInt size)const;

        public:
            explicit Allocator();
            explicit Allocator(UnsignedInt capacity);

            Allocator(Allocator&& other) noexcept;
            Allocator& operator=(Allocator&& other) noexcept;

            ~Allocator() override;

            void* AllocateRaw(UnsignedInt size)const override;
            void Reset() const override;
    };
}
