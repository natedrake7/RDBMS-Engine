#pragma once
#include "../DataTypes/DataTypes.h"

namespace Memory {
    class Allocator {
        mutable object_t* _buffer;
        mutable UnsignedInt _capacity;
        mutable UnsignedInt _offset;

        public:
            explicit Allocator(UnsignedInt capacity = 1024 * 1024);
            ~Allocator();

            Allocator(Allocator&& other) noexcept;
            Allocator& operator=(Allocator&& other) noexcept;

            [[nodiscard]] bool WillReallocate(UnsignedInt size)const;
            [[nodiscard]] UnsignedInt SetNewCapacity(UnsignedInt size)const;
            void Reallocate() const;

            void* Allocate(UnsignedInt size)const;

            template <typename Entity, typename... Args>
            Entity* Allocate(Args&&... args)const;

            void Reset() const;

            [[nodiscard]] UnsignedInt GetCapacity()const;
    };

    template <typename Entity, typename... Args>
    Entity* Allocator::Allocate(Args&&... args) const{
        return new (Allocate(sizeof(Entity))) Entity(std::forward<Args>(args)...);
    }
}
