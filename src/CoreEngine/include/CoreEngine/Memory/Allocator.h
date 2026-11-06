#pragma once
#include <Systemic/DataTypes/DataTypes.h>
#include <Systemic/Memory/IAllocator.h>

namespace CoreEngine::Memory{
    struct Chunk;

    class Allocator final : public virtual ::Memory::IAllocator{
        mutable Chunk* __restrict__ _head;
        mutable Chunk* __restrict__ _tail;

        [[nodiscard]] UInt32_t NewChunkCapacity(UInt32_t size)const;
        void AllocateNewChunk(UInt32_t size, std::size_t alignment)const;
        void TryFindNewChunk(UInt32_t size, std::size_t alignment)const;

        public:
            explicit Allocator();
            explicit Allocator(UInt32_t capacity);

            Allocator(Allocator&& other) noexcept;
            Allocator& operator=(Allocator&& other) noexcept;

            ~Allocator() override;

            void Release() const override;
            void Reset() const override;

            bool IsEmpty() const;

            ::Memory::AllocationStep RecordAllocationStart() const override;
            void ReleaseFromAllocationStep(::Memory::AllocationStep& step) const override;

        protected:
            [[nodiscard]] void* AllocateAligned(UInt32_t size, std::size_t alignment) const override;
    };
}
