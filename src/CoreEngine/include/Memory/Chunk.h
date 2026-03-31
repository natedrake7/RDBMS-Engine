#pragma once
#include "../../../Systemic/include/DataTypes/DataTypes.h"

namespace CoreEngine::Memory{
    struct Chunk{
        Chunk* _next;
        UnsignedInt _offset;
        UnsignedInt _size;
        object_t _data[];

        static constexpr UnsignedInt DEFAULT_SIZE = 1024 * 10; //10KB
        static constexpr UnsignedInt MAX_SIZE = 1024 * 1024; //1MB
    };
}
