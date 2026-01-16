#pragma once
#include "../DataTypes/DataTypes.h"

namespace Memory {

    enum class AllocationType : uint8_t{
        Temporary = 0,
        Persistent = 1
    };

    class Allocator {
        object_t* _buffer;
        BigInt _capacity;
        BigInt _offset;

        AllocationType _allocationType;

        public:
            explicit Allocator(const BigInt& capacity = 1024 * 1024, const AllocationType& allocationType = AllocationType::Temporary);
            ~Allocator();

            void* Allocate(const BigInt& size);


            void Reset();
    };
}
