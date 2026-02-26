#pragma once
#include "../DataTypes/DataTypes.h"

namespace Memory {
    struct Chunk{
        Chunk* _next;
        UnsignedInt _offset;
        UnsignedInt _size;
        object_t _data[];

        static constexpr UnsignedInt DEFAULT_SIZE = 1024 * 1024 * 10;
        static constexpr UnsignedInt MAX_SIZE = 1024 * 1024 * 10;
    };

    class Allocator {
        mutable Chunk* _head;
        mutable Chunk* _tail;

        [[nodiscard]] UnsignedInt NewChunkCapacity(UnsignedInt size)const;

        public:
            explicit Allocator();
            explicit Allocator(UnsignedInt capacity);
            ~Allocator();

            Allocator(Allocator&& other) noexcept;
            Allocator& operator=(Allocator&& other) noexcept;

            void* Allocate(UnsignedInt size)const;

            template <typename Entity, typename... Args>
            Entity* Allocate(Args&&... args)const;

            void Reset() const;
    };

    template <typename Entity, typename... Args>
    Entity* Allocator::Allocate(Args&&... args) const{
        return new (this->Allocate(sizeof(Entity))) Entity(std::forward<Args>(args)...);
    }
}
