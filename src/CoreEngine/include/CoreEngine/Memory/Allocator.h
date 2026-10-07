#pragma once
#include <Systemic/DataTypes/DataTypes.h>
#include <Systemic/Memory/IAllocator.h>

namespace CoreEngine::Memory{
    struct Chunk;

    class Allocator final : public virtual ::Memory::IAllocator{
        mutable Chunk* __restrict__ _head;
        mutable Chunk* __restrict__ _tail;

        [[nodiscard]] UnsignedInt NewChunkCapacity(UnsignedInt size)const;
        void AllocateNewChunk(UnsignedInt size, std::size_t alignment)const;
        void TryFindNewChunk(UnsignedInt size, std::size_t alignment)const;

        public:
            explicit Allocator();

            Allocator(Allocator&& other) noexcept;
            Allocator& operator=(Allocator&& other) noexcept;

            ~Allocator() override;

            void Release() const override;
            void Reset() const override;

            bool IsEmpty() const;

            ::Memory::AllocationStep RecordAllocationStart() const override;
            void ReleaseFromAllocationStep(::Memory::AllocationStep& step) const override;

        protected:
            [[nodiscard]] void* AllocateAligned(UnsignedInt size, std::size_t alignment) const override;
    };
}
