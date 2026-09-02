#pragma once
#include "../DataTypes/DataTypes.h"
#include <utility>

namespace Memory {

    struct AllocationStep{
        void* _chunkAddress;
        UnsignedInt _chunkOffset;

        AllocationStep(void* chunkAddress, const UnsignedInt chunkOffset) :
            _chunkAddress(chunkAddress), _chunkOffset(chunkOffset) {}

        [[nodiscard]] static AllocationStep DefaultStep(){
            return AllocationStep(nullptr, 0);
        }
    };

    class IAllocator {
        public:
            virtual ~IAllocator() = default;

            [[nodiscard]] virtual void* AllocateRaw(UnsignedInt size)const = 0;

            template <typename Entity, typename... Args>
            Entity* Allocate(Args&&... args)const;

            virtual void Release() const = 0;
            virtual void Reset() const = 0;

            virtual AllocationStep RecordAllocationStart() const = 0;
            virtual void ReleaseFromAllocationStep(AllocationStep& step) const = 0;
    };

    template <typename Entity, typename... Args>
    Entity* IAllocator::Allocate(Args&&... args) const{
        return new (this->AllocateRaw(sizeof(Entity))) Entity(std::forward<Args>(args)...);
    }
}
