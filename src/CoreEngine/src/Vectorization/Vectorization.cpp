#include "../../include/Vectorization/Vectorization.h"

namespace CoreEngine{
    void SelectionVector::AllocateRids(const ::Memory::IAllocator* allocator, const Int index, const Int size){
        this->selectedRids[index] = static_cast<UnsignedInt*>(allocator->AllocateRaw(size * sizeof(UnsignedInt)));
        this->isIdentity = false;
    }

    void SelectionVector::AllocateNullMask(const ::Memory::IAllocator* allocator, const Int index, const Int size){
        this->nullMask[index] = static_cast<UnsignedTinyInt*>(allocator->AllocateRaw(size));
        this->isIdentity = false;
    }
}