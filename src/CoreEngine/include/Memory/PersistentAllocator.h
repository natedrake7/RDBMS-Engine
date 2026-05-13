#pragma once
#include "../../../Systemic/include/Memory/IAllocator.h"

namespace CoreEngine{
    class GlobalMemoryManager;
}

namespace CoreEngine::Memory{
    struct Chunk;

    class PersistentAllocator final : public virtual ::Memory::IAllocator{
        mutable Chunk* _head;
        mutable Chunk* _tail;

        [[nodiscard]] UnsignedInt NewChunkCapacity(UnsignedInt size)const;
        void AllocateNewChunk(UnsignedInt size)const;

    public:
        explicit PersistentAllocator();
        explicit PersistentAllocator(UnsignedInt size);
        PersistentAllocator(PersistentAllocator&& other) noexcept;
        PersistentAllocator& operator=(PersistentAllocator&& other) noexcept;

        ~PersistentAllocator() override;

        [[nodiscard]] void* AllocateRaw(UnsignedInt size) const override;
        void Release() const override;
        void Reset() const override;
    };
}
