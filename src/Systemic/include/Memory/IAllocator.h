#pragma once
#include "../DataTypes/DataTypes.h"

namespace Memory {
    class IAllocator {
        public:
            virtual ~IAllocator() = default;

            [[nodiscard]] virtual void* AllocateRaw(UnsignedInt size)const = 0;

            template <typename Entity, typename... Args>
            Entity* Allocate(Args&&... args)const;

            virtual void Reset() const = 0;
    };

    template <typename Entity, typename... Args>
    Entity* IAllocator::Allocate(Args&&... args) const{
        return new (this->AllocateRaw(sizeof(Entity))) Entity(std::forward<Args>(args)...);
    }
}
