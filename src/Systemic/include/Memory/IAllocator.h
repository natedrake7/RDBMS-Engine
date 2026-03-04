#pragma once
#include "../DataTypes/DataTypes.h"
#include <utility>

namespace Memory {

    class IAllocator {
        protected:
            Int _type;
        public:
            static constexpr Int BASE_ALLOCATOR_TYPE = 0;
            static constexpr Int MISC_ALLOCATOR_TYPE = 1;
            static constexpr Int EXECUTION_ALLOCATOR_TYPE = 2;

            IAllocator()
                : _type(BASE_ALLOCATOR_TYPE){}
            virtual ~IAllocator() = default;

            [[nodiscard]] bool IsOfType(const Int type) const{ return this->_type == type; }

            [[nodiscard]] virtual void* AllocateRaw(UnsignedInt size)const = 0;

            template <typename Entity, typename... Args>
            Entity* Allocate(Args&&... args)const;

            template<typename Entity>
            void Free(Entity* ptr);

            virtual void Reset() const = 0;
            virtual void Free(void* ptr, Int size) const = 0;
    };

    template <typename Entity, typename... Args>
    Entity* IAllocator::Allocate(Args&&... args) const{
        return new (this->AllocateRaw(sizeof(Entity))) Entity(std::forward<Args>(args)...);
    }

    template <typename Entity>
    void IAllocator::Free(Entity* ptr){
        this->Free(ptr, sizeof(Entity));
    }
}
