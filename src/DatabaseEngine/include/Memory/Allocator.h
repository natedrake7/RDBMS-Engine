#pragma once
#include "../../../Systemic/include/DataTypes/DataTypes.h"
#include "../../../Systemic/include/Memory/IAllocator.h"

namespace DatabaseEngine::Memory{
    struct Chunk{
        Chunk* _next;
        UnsignedInt _offset;
        UnsignedInt _size;
        object_t _data[];

        static constexpr UnsignedInt DEFAULT_SIZE = 1024 * 10; //10KB
        static constexpr UnsignedInt MAX_SIZE = 1024 * 1024; //1MB
    };

    class Allocator : public virtual ::Memory::IAllocator{
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
