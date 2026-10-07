#pragma once
#include <Systemic/Memory/IAllocator.h>

namespace CoreEngine{
    class GlobalMemoryManager;
}

namespace CoreEngine::Memory{
    struct Chunk;

    class PersistentAllocator final : public virtual ::Memory::IAllocator{
        mutable Chunk* _head;
        mutable Chunk* _tail;

        [[nodiscard]] UnsignedInt NewChunkCapacity(UnsignedInt size)const;
        void AllocateNewChunk(UnsignedInt size, std::size_t alignment)const;
        void TryFindNewChunk(UnsignedInt size, std::size_t alignment)const;

    public:
        explicit PersistentAllocator();
        explicit PersistentAllocator(UnsignedInt size);
        PersistentAllocator(PersistentAllocator&& other) noexcept;
        PersistentAllocator& operator=(PersistentAllocator&& other) noexcept;

        ~PersistentAllocator() override;

        void Release() const override;
        void Reset() const override;

        ::Memory::AllocationStep RecordAllocationStart() const override;
        void ReleaseFromAllocationStep(::Memory::AllocationStep& step) const override;

    protected:
        [[nodiscard]] void* AllocateAligned(UnsignedInt size, std::size_t alignment) const override;
    };
}
