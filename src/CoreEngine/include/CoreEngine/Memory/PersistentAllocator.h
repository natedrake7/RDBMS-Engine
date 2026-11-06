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

        [[nodiscard]] UInt32_t NewChunkCapacity(UInt32_t size)const;
        void AllocateNewChunk(UInt32_t size, std::size_t alignment)const;
        void TryFindNewChunk(UInt32_t size, std::size_t alignment)const;

    public:
        explicit PersistentAllocator();
        explicit PersistentAllocator(UInt32_t size);
        PersistentAllocator(PersistentAllocator&& other) noexcept;
        PersistentAllocator& operator=(PersistentAllocator&& other) noexcept;

        ~PersistentAllocator() override;

        void Release() const override;
        void Reset() const override;

        ::Memory::AllocationStep RecordAllocationStart() const override;
        void ReleaseFromAllocationStep(::Memory::AllocationStep& step) const override;

    protected:
        [[nodiscard]] void* AllocateAligned(UInt32_t size, std::size_t alignment) const override;
    };
}
