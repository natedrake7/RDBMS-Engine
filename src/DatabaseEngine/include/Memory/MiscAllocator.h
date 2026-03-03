#pragma once
#include "../../../Systemic/include/Memory/IAllocator.h"

namespace DatabaseEngine::Memory{
    class MiscAllocator final : public virtual ::Memory::IAllocator{
    public:
        MiscAllocator() = delete;
        MiscAllocator(MiscAllocator&& other) noexcept = delete;
        MiscAllocator& operator=(MiscAllocator&& other) noexcept = delete;
        ~MiscAllocator() override = default;

        [[nodiscard]] void* AllocateRaw(UnsignedInt size) const override;
        void Reset() const override;
        void Free(void* ptr) const override;
    };
}
