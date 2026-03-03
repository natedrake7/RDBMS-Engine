#include "../../include/Memory/MiscAllocator.h"

#include "Managers/GlobalMemoryManager.h"

namespace DatabaseEngine::Memory{
    void* MiscAllocator::AllocateRaw(const UnsignedInt size) const{
        while (GlobalMemoryManager::Get().TryReserveForMisc(size) == false){}

        auto* ptr = std::malloc(size);

        if (ptr == nullptr){
            GlobalMemoryManager::Get().ReleaseMiscReservation(size);
            throw std::bad_alloc();
        }

        return ptr;
    }

    void MiscAllocator::Reset() const{}

    void MiscAllocator::Free(void* ptr) const{ std::free(ptr); }
}
