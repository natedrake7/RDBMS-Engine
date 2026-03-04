#pragma once
#include "../../../Systemic/include/Memory/IAllocator.h"

namespace DatabaseEngine{
    class GlobalMemoryManager;
}

namespace DatabaseEngine::Memory{
    class MiscAllocator final : public virtual ::Memory::IAllocator{
        GlobalMemoryManager* _globalManager;
        MiscAllocator();
    public:
        MiscAllocator(MiscAllocator&& other) noexcept = delete;
        MiscAllocator& operator=(MiscAllocator&& other) noexcept = delete;
        ~MiscAllocator() override = default;

        inline static IAllocator& Get(){
            static MiscAllocator allocator;
            return allocator;
        }

        [[nodiscard]] void* AllocateRaw(UnsignedInt size) const override;
        void Reset() const override;
        void Free(void* ptr, Int size) const override;
    };
}
