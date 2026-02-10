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

            void* Allocate(UnsignedInt size)const;
            void Reset() const;
    };
}
