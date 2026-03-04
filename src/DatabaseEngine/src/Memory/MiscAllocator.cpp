#include "../../include/Memory/MiscAllocator.h"

#include "Managers/GlobalMemoryManager.h"

namespace DatabaseEngine::Memory{
    MiscAllocator::MiscAllocator()
        : _globalManager(&GlobalMemoryManager::Get()){
        this->_type = MISC_ALLOCATOR_TYPE;
    }

    void* MiscAllocator::AllocateRaw(const UnsignedInt size) const{
        while (this->_globalManager->TryReserveForMisc(size) == false){
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        auto* ptr = std::malloc(size);

        if (ptr == nullptr){
            this->_globalManager->ReleaseMiscReservation(size);
            throw std::bad_alloc();
        }

        return ptr;
    }

    void MiscAllocator::Reset() const{
        throw std::runtime_error("MiscAllocator::Reset: Not implemented");
    }

    void MiscAllocator::Free(void* ptr, const Int size) const{
        this->_globalManager->ReleaseMiscReservation(size);
        std::free(ptr);
    }
}
